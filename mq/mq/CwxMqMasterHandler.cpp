#include "CwxMqMasterHandler.h"
#include "CwxMqApp.h"
#include "CwxZlib.h"
#include "CwxMqConnector.h"

//关闭已有连接
void CwxMqMasterHandler::closeSession() {
  CWX_INFO(("UnistorHandler4Master: Close sync session"));
  if (m_syncSession) {
    map<CWX_UINT32, bool>::iterator iter = m_syncSession->m_conns.begin();
    while (iter != m_syncSession->m_conns.end()) {
      m_pApp->noticeCloseConn(iter->first);
      iter++;
    }
    delete m_syncSession;
    m_syncSession = NULL;
  }
}
///创建与master同步的连接。返回值：0：成功；-1：失败
int CwxMqMasterHandler::createSession(CwxMqTss* pTss) {
  if (m_syncSession) closeSession();
  ///重建所有连接
  CwxINetAddr addr;
  if (0 != addr.set(m_pApp->getConfig().getMaster().m_master.getPort(),
          m_pApp->getConfig().getMaster().m_master.getHostName().c_str()))
  {
    CWX_ERROR(("Failure to init addr, addr:%s, port:%u, err=%d", m_pApp->getConfig().getMaster().m_master.getHostName().c_str(), m_pApp->getConfig().getMaster().m_master.getPort(), errno));
    return -1;
  }
  CWX_UINT32 i = 0;
  CWX_UINT32 unConnNum = m_pApp->getConfig().getCommon().m_uiSyncConnNum;
  if (unConnNum < 1) unConnNum = 1;
  int* fds = new int[unConnNum];
  for (i = 0; i < unConnNum; i++) {
    fds[i] = -1;
  }
  CwxTimeValue timeout(CWX_MQ_CONN_TIMEOUT_SECOND);
  CwxTimeouter timeouter(&timeout);
  if (0 != CwxMqConnector::connect(addr, unConnNum, fds, &timeouter, true)) {
    CWX_ERROR(("Failure to connect to addr:%s, port:%u, err=%d", m_pApp->getConfig().getMaster().m_master.getHostName().c_str(), m_pApp->getConfig().getMaster().m_master.getPort(), errno));
    return -1;
  }
  m_uiCurHostId++;
  if (0 == m_uiCurHostId) m_uiCurHostId = 1;
  m_syncSession = new CwxMqSyncSession(m_uiCurHostId);
  ///注册连接id
  int ret = 0;
  for (i = 0; i < unConnNum; i++) {
    if ((ret = m_pApp->noticeHandle4Msg(CwxMqApp::SVR_TYPE_MASTER,
        m_uiCurHostId, fds[i])) < 0)
    {
      CWX_ERROR(("Failure to register sync handler to framework."));
      break;
    }
    m_syncSession->m_conns[(CWX_UINT32) ret] = false;
  }
  if (i != unConnNum) { ///失败
    for (; i < unConnNum; i++) {
      ::close(fds[i]);
    }
    closeSession();
    return -1;
  }
  ///发送report的消息
  ///创建往master报告sid的通信数据包
  CwxMsgBlock* pBlock = NULL;
  ret = CwxMqPoco::packReportData(pTss->m_pWriter,
      pBlock,
      0,
      m_pApp->getBinLogMgr()->getMaxSid(),
      false,
      m_pApp->getConfig().getCommon().m_uiChunkSize,
      NULL,
      m_pApp->getConfig().getMaster().m_master.getUser().c_str(),
      m_pApp->getConfig().getMaster().m_master.getPasswd().c_str(),
      m_pApp->getConfig().getMaster().m_strSign.c_str(),
      m_pApp->getConfig().getMaster().m_bzip, pTss->m_szBuf2K);
  if (ret != CWX_MQ_ERR_SUCCESS) {    ///数据包创建失败
    CWX_ERROR(("Failure to create report package, err:%s", pTss->m_szBuf2K));
    closeSession();
    return -1;
  } else {
    ///发送消息
    pBlock->send_ctrl().setConnId(m_syncSession->m_conns.begin()->first);
    pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MASTER);
    pBlock->send_ctrl().setHostId(0);
    pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    if (0 != m_pApp->sendMsgByConn(pBlock)) {    //connect should be closed
      CWX_ERROR(("Failure to report sid to master"));
      CwxMsgBlockAlloc::free(pBlock);
      closeSession();
      return -1;
    }
  }
  return 0;
}

///超时检查
int CwxMqMasterHandler::onTimeoutCheck(CwxMsgBlock*&, CwxTss* pThrEnv) {
  CWX_INFO(("CwxMqMasterHandler::onTimeoutCheck"));
  if (!m_syncSession) {
    createSession((CwxMqTss*) pThrEnv);
  }
  return 0;
}

///master的连接关闭后，需要清理环境
int CwxMqMasterHandler::onConnClosed(CwxMsgBlock*&, CwxTss*) {
  CWX_ERROR(("Master is closed."));
  //刷新管理器保存数据
  char szErr2K[2048];
  if (0 != m_pApp->getBinLogMgr()->commit(true, szErr2K)) {
    CWX_ERROR(("Failure to commit binlog data, err:%s", szErr2K));
  }
  closeSession();
  return 1;
}

///接收来自master的消息
int CwxMqMasterHandler::onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv) {
  CwxMqTss* pTss = (CwxMqTss*) pThrEnv;
  if (!m_syncSession) {    ///关闭连接
    m_pApp->noticeCloseConn(msg->event().getConnId());
    return 1;
  }
  if (msg->event().getHostId() != m_syncSession->m_uiHostId) {    ///关闭连接
    m_pApp->noticeCloseConn(msg->event().getConnId());
    return 1;
  }
  int ret = -1;
  do {
    if (!msg || !msg->length()) {
      CWX_ERROR(("Receive empty msg from master"));
      break;
    }
    //SID报告的回复，此时，一定是报告失败
    if (CwxMqPoco::MSG_TYPE_SYNC_DATA == msg->event().getMsgHeader().getMsgType() ||
        CwxMqPoco::MSG_TYPE_SYNC_DATA_CHUNK == msg->event().getMsgHeader().getMsgType())
    {
      list<CwxMsgBlock*> msgs;
      if (0 != recvMsg(msg, msgs)) break;
      msg = NULL;
      CwxMsgBlock* block = NULL;
      list<CwxMsgBlock*>::iterator msg_iter = msgs.begin();
      while (msg_iter != msgs.end()) {
        block = *msg_iter;
        if (CwxMqPoco::MSG_TYPE_SYNC_DATA == block->event().getMsgHeader().getMsgType()) {
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
        break;
      }
    } else if (CwxMqPoco::MSG_TYPE_SYNC_REPORT_REPLY == msg->event().getMsgHeader().getMsgType()) {
      if (0 != dealSyncReportReply(msg, pTss)) break;
    } else if (CwxMqPoco::MSG_TYPE_SYNC_ERR == msg->event().getMsgHeader().getMsgType()) {
      dealErrMsg(msg, pTss);
      break;
    } else {
      CWX_ERROR(("Receive invalid msg type from master, msg_type=%u", msg->event().getMsgHeader().getMsgType()));
      break;
    }
    ret = 0;
  } while (0);
  if (0 != ret) {
    closeSession();
  }
  return 1;
}

int CwxMqMasterHandler::recvMsg(CwxMsgBlock*& msg, list<CwxMsgBlock*>& msgs) {
  CWX_UINT64 ullSeq = 0;
  if (!m_syncSession->m_ullSessionId) {
    CWX_ERROR(("No Session"));
    return -1;
  }
  if (msg->length() < sizeof(ullSeq)) {
    CWX_ERROR(("sync data's size[%u] is invalid, min-size=%u", msg->length(), sizeof(ullSeq)));
    return -1;
  }
  map<CWX_UINT32, bool/*是否已经report*/>::iterator conn_iter =
      m_syncSession->m_conns.find(msg->event().getConnId());
  if (conn_iter == m_syncSession->m_conns.end()) {
    CWX_ERROR(("Conn-id[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  ullSeq = CwxMqPoco::getSeq(msg->rd_ptr());
  if (!m_syncSession->recv(ullSeq, msg, msgs)) {
    CWX_ERROR(("Conn-id[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  return 0;
}

int CwxMqMasterHandler::dealErrMsg(CwxMsgBlock*& msg, CwxMqTss* pTss) {
  int ret = 0;
  char const* szErrMsg;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseSyncErr(pTss->m_pReader,
      msg,
      ret,
      szErrMsg,
      pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to parse err msg from master, err=%s", pTss->m_szBuf2K));
    return -1;
  }
  CWX_ERROR(("Failure to sync from master, ret=%d, err=%s", ret, szErrMsg));
  return 0;
}

///处理Sync report的reply消息。返回值：0：成功；-1：失败
int CwxMqMasterHandler::dealSyncReportReply(CwxMsgBlock*& msg, ///<收到的消息
    CwxMqTss* pTss ///<tss对象
    )
{
  char szTmp[64];
  ///此时，对端会关闭连接
  CWX_UINT64 ullSessionId = 0;
  if (m_syncSession->m_ullSessionId) {
    CWX_ERROR(("Session id is replied, session id:%s", CwxCommon::toString(m_syncSession->m_ullSessionId, szTmp, 10)));
    return -1;
  }
  if (m_syncSession->m_conns.find(msg->event().getConnId()) == m_syncSession->m_conns.end()) {
    CWX_ERROR(("Conn-id[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseReportDataReply(pTss->m_pReader,
      msg,
      ullSessionId,
      pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to parse report-reply msg from master, err=%s", pTss->m_szBuf2K));
    return -1;
  }
  m_syncSession->m_ullSessionId = ullSessionId;
  m_syncSession->m_conns[msg->event().getConnId()] = true;
  ///其他连接报告
  map<CWX_UINT32, bool/*是否已经report*/>::iterator iter = m_syncSession->m_conns.begin();
  while (iter != m_syncSession->m_conns.end()) {
    if (iter->first != msg->event().getConnId()) {
      CwxMsgBlock* pBlock = NULL;
      int ret = CwxMqPoco::packReportNewConn(pTss->m_pWriter,
          pBlock,
          0,
          ullSessionId,
          pTss->m_szBuf2K);
      if (ret != CWX_MQ_ERR_SUCCESS) { ///数据包创建失败
        CWX_ERROR(("Failure to create report package, err:%s", pTss->m_szBuf2K));
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
    if (!CwxZlib::unzip(m_unzipBuf,
        ulUnzipLen,
        (const unsigned char*) (msg->rd_ptr() + sizeof(ullSeq)),
        msg->length() - sizeof(ullSeq)))
    {
      CWX_ERROR(("Failure to unzip recv msg, msg size:%u, buf size:%u", msg->length() - sizeof(ullSeq), m_uiBufLen));
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
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncDataReply(pTss->m_pWriter,
      reply_block,
      msg->event().getMsgHeader().getTaskId(),
      CwxMqPoco::MSG_TYPE_SYNC_DATA_REPLY,
      ullSeq,
      pTss->m_szBuf2K))
  {
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
    )
{
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
    if (!CwxZlib::unzip(m_unzipBuf,
        ulUnzipLen,
        (const unsigned char*) (msg->rd_ptr() + sizeof(ullSeq)),
        msg->length() - sizeof(ullSeq)))
    {
      CWX_ERROR(("Failure to unzip recv msg, msg size:%u, buf size:%u", msg->length() - sizeof(ullSeq), m_uiBufLen));
      return -1;
    }
    if (!m_reader.unpack((char*) m_unzipBuf, ulUnzipLen, false, true)) {
      CWX_ERROR(("Failure to unpack master multi-binlog, err:%s", m_reader.getErrMsg()));
      return -1;
    }
  } else {
    if (!m_reader.unpack(msg->rd_ptr() + sizeof(ullSeq),
        msg->length() - sizeof(ullSeq),
        false,
        true))
    {
      CWX_ERROR(("Failure to unpack master multi-binlog, err:%s", m_reader.getErrMsg()));
      return -1;
    }
  }
  //检测签名
  int bSign = 0;
  if (m_pApp->getConfig().getMaster().m_strSign.length()) {
    CwxKeyValueItem const* pItem = m_reader.getKey(m_pApp->getConfig().getMaster().m_strSign.c_str());
    if (pItem) {        //存在签名key
      if (!checkSign(m_reader.getMsg(),
          pItem->m_szKey - CwxPackage::getKeyOffset() - m_reader.getMsg(),
          pItem->m_szData, m_pApp->getConfig().getMaster().m_strSign.c_str()))
      {
        CWX_ERROR(("Failure to check %s sign", m_pApp->getConfig().getMaster().m_strSign.c_str()));
        return -1;
      }
      bSign = 1;
    }
  }
  for (CWX_UINT32 i = 0; i < m_reader.getKeyNum() - bSign; i++) {
    if (0 != strcmp(m_reader.getKey(i)->m_szKey, CWX_MQ_M)) {
      CWX_ERROR(("Master multi-binlog's key must be:%s, but:%s", CWX_MQ_M, m_reader.getKey(i)->m_szKey));
      return -1;
    }
    if (-1 == saveBinlog(pTss,
        m_reader.getKey(i)->m_szData,
        m_reader.getKey(i)->m_uiDataLen))
    {
      return -1;
    }
  }

  //回复发送者
  CwxMsgBlock* reply_block = NULL;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncDataReply(pTss->m_pWriter,
      reply_block,
      msg->event().getMsgHeader().getTaskId(),
      CwxMqPoco::MSG_TYPE_SYNC_DATA_CHUNK_REPLY,
      ullSeq,
      pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to pack sync data reply, errno=%s", pTss->m_szBuf2K));
    return -1;
  }
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
int CwxMqMasterHandler::saveBinlog(CwxMqTss* pTss,
                                   char const* szBinLog,
                                   CWX_UINT32 uiLen)
{
  CWX_UINT32 ttTimestamp;
  CWX_UINT64 ullSid;
  CwxKeyValueItem const* data;
  ///获取binlog的数据
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseSyncData(pTss->m_pReader,
      szBinLog,
      uiLen,
      ullSid,
      ttTimestamp,
      data,
      pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to parse binlog from master, err=%s", pTss->m_szBuf2K));
    return -1;
  }
  //add to binlog
  pTss->m_pWriter->beginPack();
  pTss->m_pWriter->addKeyValue(CWX_MQ_D, data->m_szData, data->m_uiDataLen, data->m_bKeyValue);
  pTss->m_pWriter->pack();
  if (0 != m_pApp->getBinLogMgr()->append(ullSid,
      (time_t) ttTimestamp,
      0,
      pTss->m_pWriter->getMsg(),
      pTss->m_pWriter->getMsgSize(),
      pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to append binlog to binlog mgr, err=%s", pTss->m_szBuf2K));
    return -1;
  }
  return 0;
}

bool CwxMqMasterHandler::checkSign(char const* data,
                                   CWX_UINT32 uiDateLen,
                                   char const* szSign,
                                   char const* sign)
{
  if (!sign) return true;
  if (strcmp(sign, CWX_MQ_CRC32) == 0) {        //CRC32签名
    CWX_UINT32 uiCrc32 = CwxCrc32::value(data, uiDateLen);
    if (memcmp(&uiCrc32, szSign, sizeof(uiCrc32)) == 0) return true;
    return false;
  } else if (strcmp(sign, CWX_MQ_MD5) == 0) {        //md5签名
    CwxMd5 md5;
    unsigned char szMd5[16];
    md5.update((const unsigned char*) data, uiDateLen);
    md5.final(szMd5);
    if (memcmp(szMd5, szSign, 16) == 0) return true;
    return false;
  }
  return true;
}

bool CwxMqMasterHandler::prepareUnzipBuf() {
  if (!m_unzipBuf) {
    m_uiBufLen = m_pApp->getConfig().getCommon().m_uiChunkSize * 20;
    if (m_uiBufLen < 20 * 1024 * 1024)  m_uiBufLen = 20 * 1024 * 1024;
    m_unzipBuf = new unsigned char[m_uiBufLen];
  }
  return m_unzipBuf != NULL;
}
