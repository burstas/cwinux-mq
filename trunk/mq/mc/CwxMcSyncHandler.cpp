#include "CwxMcSyncHandler.h"
#include "CwxMcApp.h"
#include "CwxZlib.h"

/**
@brief 连接可读事件，返回-1，close()会被调用
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMcSyncHandler::onInput() {
    ///接受消息
    int ret = CwxAppHandler4Channel::recvPackage(getHandle(),
        m_uiRecvHeadLen,
        m_uiRecvDataLen,
        m_szHeadBuf,
        m_header,
        m_recvMsgData);
    if (1 != ret) return ret; ///如果失败或者消息没有接收完，返回。
    ///通知收到一个消息
    ret = recvMessage();
    ///如果m_recvMsgData没有释放，则是否m_recvMsgData等待接收下一个消息
    if (m_recvMsgData) CwxMsgBlockAlloc::free(m_recvMsgData);
    this->m_recvMsgData = NULL;
    this->m_uiRecvHeadLen = 0;
    this->m_uiRecvDataLen = 0;
    return ret;
}
/**
@brief 通知连接关闭。
@return 1：不从engine中移除注册；0：从engine中移除注册但不删除handler；-1：从engine中将handle移除并删除。
*/
int CwxMcSyncHandler::onConnClosed() {
    CwxMcSyncSession* pSession = (CwxMcSyncSession*)m_pTss->m_userData;
    ///将session的重连标志设置为true，以便进行重新连接
    pSession->m_bClosed = true;
    ///将连接从连接map中删除
    pSession->m_conns.erase(m_uiConnId);
    return -1;
}

///接收消息，0：成功；-1：失败
int CwxMcSyncHandler::recvMessage(){
    CwxMcSyncSession* pSession = (CwxMcSyncSession*)m_pTss->m_userData;
    ///如果处于关闭状态，直接返回-1关闭连接
    if (pSession->m_bClosed) return -1;
    m_recvMsgData->event().setConnId(m_uiConnId);
    if (!msg || !msg->length()) {
        CWX_ERROR(("Receive empty msg from master"));
        return -1;
    }
    //SID报告的回复，此时，一定是报告失败
    if (CwxMqPoco::MSG_TYPE_SYNC_DATA == msg->event().getMsgHeader().getMsgType() ||
        CwxMqPoco::MSG_TYPE_SYNC_DATA_CHUNK == msg->event().getMsgHeader().getMsgType())
    {
        list<CwxMsgBlock*> msgs;
        if (0 != recvMsg(msg, msgs)) return -1;
        msg = NULL;
        CwxMsgBlock* block = NULL;
        list<CwxMsgBlock*>::iterator msg_iter = msgs.begin();
        while (msg_iter != msgs.end()) {
            block = *msg_iter;
            if (CwxMqPoco::MSG_TYPE_SYNC_DATA  == block->event().getMsgHeader().getMsgType()) {
                ret = dealSyncData(block, pTss);
            } else {
                ret = dealSyncChunkData(block, pTss);
            }
            msgs.pop_front();
            msg_iter = msgs.begin();
            if (block) CwxMsgBlockAlloc::free(block);
            if (0 != ret) break;
            msg_iter++;
        }
        if (msg_iter != msgs.end()) {
            while (msg_iter != msgs.end()) {
                block = *msg_iter;
                CwxMsgBlockAlloc::free(block);
                msg_iter++;
            }
            return -1;
        }
    } else if (CwxMqPoco::MSG_TYPE_SYNC_REPORT_REPLY  == msg->event().getMsgHeader().getMsgType()) {
        if (0 != dealSyncReportReply(msg, pTss))  return -1;
    } else if (CwxMqPoco::MSG_TYPE_SYNC_ERR  == msg->event().getMsgHeader().getMsgType()) {
        dealErrMsg(msg, pTss);
        return -1;
    } else {
        CWX_ERROR(("Receive invalid msg type from master, msg_type=%u", msg->event().getMsgHeader().getMsgType()));
        return -1;
    }
    return 1;
}

int CwxMcSyncHandler::recvMsg(CwxMsgBlock*& msg, list<CwxMsgBlock*>& msgs) {
  CWX_UINT64 ullSeq = 0;
  if (msg->length() < sizeof(ullSeq)) {
    CWX_ERROR(("sync data's size[%u] is invalid, min-size=%u", msg->length(), sizeof(ullSeq)));
    return -1;
  }
  ullSeq = CwxMqPoco::getSeq(msg->rd_ptr());
  CwxMcSyncSession* pSession = (CwxMcSyncSession*)m_pTss->m_userData;
  if (!pSession->recv(ullSeq, msg, msgs)) {
    CWX_ERROR(("Conn-id[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  return 0;
}

int CwxMcSyncHandler::dealErrMsg(CwxMsgBlock*& msg, CwxMqTss* pTss) {
  int ret = 0;
  char const* szErrMsg;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseSyncErr(pTss->m_pReader,
      msg,
      ret,
      szErrMsg,
      pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to parse err msg from mq, err=%s", pTss->m_szBuf2K));
    return -1;
  }
  CWX_ERROR(("Failure to sync from mq, ret=%d, err=%s", ret, szErrMsg));
  return 0;
}

///处理Sync report的reply消息。返回值：0：成功；-1：失败
int CwxMcSyncHandler::dealSyncReportReply(CwxMsgBlock*& msg, ///<收到的消息
    CwxMqTss* pTss ///<tss对象
    )
{
  char szTmp[64];
  ///此时，对端会关闭连接
  CWX_UINT64 ullSessionId = 0;
  if (m_syncSession->m_ullSessionId) {
    CWX_ERROR(
        ("Session id is replied, session id:%s", CwxCommon::toString(m_syncSession->m_ullSessionId, szTmp, 10)));
    return -1;
  }
  if (m_syncSession->m_conns.find(msg->event().getConnId())
      == m_syncSession->m_conns.end()) {
    CWX_ERROR(("Conn-id[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  if (CWX_MQ_ERR_SUCCESS
      != CwxMqPoco::parseReportDataReply(pTss->m_pReader, msg, ullSessionId,
          pTss->m_szBuf2K)) {
    CWX_ERROR(
        ("Failure to parse report-reply msg from master, err=%s", pTss->m_szBuf2K));
    return -1;
  }
  m_syncSession->m_ullSessionId = ullSessionId;
  m_syncSession->m_conns[msg->event().getConnId()] = true;
  ///其他连接报告
  map<CWX_UINT32, bool/*是否已经report*/>::iterator iter =
      m_syncSession->m_conns.begin();
  while (iter != m_syncSession->m_conns.end()) {
    if (iter->first != msg->event().getConnId()) {
      CwxMsgBlock* pBlock = NULL;
      int ret = CwxMqPoco::packReportNewConn(pTss->m_pWriter, pBlock, 0,
          ullSessionId, pTss->m_szBuf2K);
      if (ret != CWX_MQ_ERR_SUCCESS) { ///数据包创建失败
        CWX_ERROR(
            ("Failure to create report package, err:%s", pTss->m_szBuf2K));
        return -1;
      } else {
        ///发送消息
        pBlock->send_ctrl().setConnId(iter->first);
        pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MASTER);
        pBlock->send_ctrl().setHostId(m_syncSession->m_uiHostId);
        pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
        if (0 != m_pApp->sendMsgByConn(pBlock)) { //connect should be closed
          CWX_ERROR(("Failure to report new session connection to master"));
          CwxMsgBlockAlloc::free(pBlock);
          return -1;
        }
      }
      m_syncSession->m_conns[iter->first] = true;
    }
    iter++;
  }
  return 0;
}

///处理收到的sync data。返回值：0：成功；-1：失败
int CwxMqMasterHandler::dealSyncData(CwxMsgBlock*& msg, ///<收到的消息
    CwxMqTss* pTss ///<tss对象
    ) {
  unsigned long ulUnzipLen = 0;
  CWX_UINT64 ullSeq = CwxMqPoco::getSeq(msg->rd_ptr());
  bool bZip = msg->event().getMsgHeader().isAttr(CwxMsgHead::ATTR_COMPRESS);
  if (!msg) {
    CWX_ERROR(("received sync data is empty."));
    return -1;
  }
  int iRet = 0;
  //判断是否压缩数据
  if (bZip) { //压缩数据，需要解压
    //首先准备解压的buf
    if (!prepareUnzipBuf()) {
      CWX_ERROR(("Failure to prepare unzip buf, size:%u", m_uiBufLen));
      return -1;
    }
    ulUnzipLen = m_uiBufLen;
    //解压
    if (!CwxZlib::unzip(m_unzipBuf, ulUnzipLen,
        (const unsigned char*) (msg->rd_ptr() + sizeof(ullSeq)),
        msg->length() - sizeof(ullSeq))) {
      CWX_ERROR(
          ("Failure to unzip recv msg, msg size:%u, buf size:%u", msg->length() - sizeof(ullSeq), m_uiBufLen));
      return -1;
    }
    iRet = saveBinlog(pTss, (char*) m_unzipBuf, ulUnzipLen);
  } else {
    iRet = saveBinlog(pTss, msg->rd_ptr() + sizeof(ullSeq),
        msg->length() - sizeof(ullSeq));
  }
  if (-1 == iRet) {
    return -1;
  }
  //回复发送者
  CwxMsgBlock* reply_block = NULL;
  if (CWX_MQ_ERR_SUCCESS
      != CwxMqPoco::packSyncDataReply(pTss->m_pWriter, reply_block,
          msg->event().getMsgHeader().getTaskId(),
          CwxMqPoco::MSG_TYPE_SYNC_DATA_REPLY, ullSeq, pTss->m_szBuf2K)) {
    CWX_ERROR(("Failure to pack sync data reply, errno=%s", pTss->m_szBuf2K));
    return -1;
  }
  reply_block->send_ctrl().setConnId(msg->event().getConnId());
  reply_block->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MASTER);
  reply_block->send_ctrl().setHostId(0);
  reply_block->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (0 != m_pApp->sendMsgByConn(reply_block)) {      //connect should be closed
    CWX_ERROR(("Failure to send sync data reply to master"));
    CwxMsgBlockAlloc::free(reply_block);
    return -1;
  }
  return 0;
}

//处理收到的chunk模式下的sync data。返回值：0：成功；-1：失败
int CwxMqMasterHandler::dealSyncChunkData(CwxMsgBlock*& msg, ///<收到的消息
    CwxMqTss* pTss ///<tss对象
    ) {
  unsigned long ulUnzipLen = 0;
  CWX_UINT64 ullSeq = CwxMqPoco::getSeq(msg->rd_ptr());
  bool bZip = msg->event().getMsgHeader().isAttr(CwxMsgHead::ATTR_COMPRESS);
  if (!msg) {
    CWX_ERROR(("received sync data is empty."));
    return -1;
  }

  //判断是否压缩数据
  if (bZip) { //压缩数据，需要解压
    //首先准备解压的buf
    if (!prepareUnzipBuf()) {
      CWX_ERROR(("Failure to prepare unzip buf, size:%u", m_uiBufLen));
      return -1;
    }
    ulUnzipLen = m_uiBufLen;
    //解压
    if (!CwxZlib::unzip(m_unzipBuf, ulUnzipLen,
        (const unsigned char*) (msg->rd_ptr() + sizeof(ullSeq)),
        msg->length() - sizeof(ullSeq))) {
      CWX_ERROR(
          ("Failure to unzip recv msg, msg size:%u, buf size:%u", msg->length() - sizeof(ullSeq), m_uiBufLen));
      return -1;
    }
    if (!m_reader.unpack((char*) m_unzipBuf, ulUnzipLen, false, true)) {
      CWX_ERROR(
          ("Failure to unpack master multi-binlog, err:%s", m_reader.getErrMsg()));
      return -1;
    }
  } else {
    if (!m_reader.unpack(msg->rd_ptr() + sizeof(ullSeq),
        msg->length() - sizeof(ullSeq), false, true)) {
      CWX_ERROR(
          ("Failure to unpack master multi-binlog, err:%s", m_reader.getErrMsg()));
      return -1;
    }
  }
  //检测签名
  int bSign = 0;
  if (m_pApp->getConfig().getMaster().m_strSign.length()) {
    CwxKeyValueItem const* pItem = m_reader.getKey(
        m_pApp->getConfig().getMaster().m_strSign.c_str());
    if (pItem) {        //存在签名key
      if (!checkSign(m_reader.getMsg(),
          pItem->m_szKey - CwxPackage::getKeyOffset() - m_reader.getMsg(),
          pItem->m_szData, m_pApp->getConfig().getMaster().m_strSign.c_str())) {
        CWX_ERROR(
            ("Failure to check %s sign", m_pApp->getConfig().getMaster().m_strSign.c_str()));
        return -1;
      }
      bSign = 1;
    }
  }
  for (CWX_UINT32 i = 0; i < m_reader.getKeyNum() - bSign; i++) {
    if (0 != strcmp(m_reader.getKey(i)->m_szKey, CWX_MQ_M)) {
      CWX_ERROR(
          ("Master multi-binlog's key must be:%s, but:%s", CWX_MQ_M, m_reader.getKey(i)->m_szKey));
      return -1;
    }
    if (-1
        == saveBinlog(pTss, m_reader.getKey(i)->m_szData,
            m_reader.getKey(i)->m_uiDataLen)) {
      return -1;
    }
  }

  //回复发送者
  CwxMsgBlock* reply_block = NULL;
  if (CWX_MQ_ERR_SUCCESS
      != CwxMqPoco::packSyncDataReply(pTss->m_pWriter, reply_block,
          msg->event().getMsgHeader().getTaskId(),
          CwxMqPoco::MSG_TYPE_SYNC_DATA_CHUNK_REPLY, ullSeq, pTss->m_szBuf2K)) {
    CWX_ERROR(("Failure to pack sync data reply, errno=%s", pTss->m_szBuf2K));
    return -1;
  }
  CWX_INFO(
      ("Reply master, seq=%s, conn=%d", CwxCommon::toString(ullSeq, pTss->m_szBuf2K, 10), msg->event().getConnId()));
  reply_block->send_ctrl().setConnId(msg->event().getConnId());
  reply_block->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MASTER);
  reply_block->send_ctrl().setHostId(0);
  reply_block->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (0 != m_pApp->sendMsgByConn(reply_block)) {      //connect should be closed
    CWX_ERROR(("Failure to send sync data reply to master"));
    return -1;
  }
  return 0;
}

//0：成功；-1：失败
int CwxMqMasterHandler::saveBinlog(CwxMqTss* pTss, char const* szBinLog,
    CWX_UINT32 uiLen) {
  CWX_UINT32 ttTimestamp;
  CWX_UINT64 ullSid;
  CwxKeyValueItem const* data;
  ///获取binlog的数据
  if (CWX_MQ_ERR_SUCCESS
      != CwxMqPoco::parseSyncData(pTss->m_pReader, szBinLog, uiLen, ullSid,
          ttTimestamp, data, pTss->m_szBuf2K)) {
    CWX_ERROR(("Failure to parse binlog from master, err=%s", pTss->m_szBuf2K));
    return -1;
  }
  //add to binlog
  pTss->m_pWriter->beginPack();
  pTss->m_pWriter->addKeyValue(CWX_MQ_D, data->m_szData, data->m_uiDataLen,
      data->m_bKeyValue);
  pTss->m_pWriter->pack();
  if (0
      != m_pApp->getBinLogMgr()->append(ullSid, (time_t) ttTimestamp, 0,
          pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize(),
          pTss->m_szBuf2K)) {
    CWX_ERROR(
        ("Failure to append binlog to binlog mgr, err=%s", pTss->m_szBuf2K));
    return -1;
  }
  return 0;
}

bool CwxMqMasterHandler::checkSign(char const* data, CWX_UINT32 uiDateLen,
    char const* szSign, char const* sign) {
  if (!sign)
    return true;
  if (strcmp(sign, CWX_MQ_CRC32) == 0) {        //CRC32签名
    CWX_UINT32 uiCrc32 = CwxCrc32::value(data, uiDateLen);
    if (memcmp(&uiCrc32, szSign, sizeof(uiCrc32)) == 0)
      return true;
    return false;
  } else if (strcmp(sign, CWX_MQ_MD5) == 0) {        //md5签名
    CwxMd5 md5;
    unsigned char szMd5[16];
    md5.update((const unsigned char*) data, uiDateLen);
    md5.final(szMd5);
    if (memcmp(szMd5, szSign, 16) == 0)
      return true;
    return false;
  }
  return true;
}

bool CwxMqMasterHandler::prepareUnzipBuf() {
  if (!m_unzipBuf) {
    m_uiBufLen = m_pApp->getConfig().getCommon().m_uiChunkSize * 20;
    if (m_uiBufLen < 20 * 1024 * 1024)
      m_uiBufLen = 20 * 1024 * 1024;
    m_unzipBuf = new unsigned char[m_uiBufLen];
  }
  return m_unzipBuf != NULL;
}
