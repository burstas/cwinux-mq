#include "CwxMqQueueHandler.h"
#include "CwxMqApp.h"
#include "CwxMsgHead.h"
/**
@brief 连接可读事件，返回-1，close()会被调用
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqQueueHandler::onInput() {
  ///接受消息
  int ret = CwxAppHandler4Channel::recvPackage(getHandle(),
    m_uiRecvHeadLen,
    m_uiRecvDataLen,
    m_szHeadBuf,
    m_header,
    m_recvMsgData);
  if (1 != ret) return ret; ///如果失败或者消息没有接收完，返回。
  ///获取fetch 线程的tss对象
  CwxMqTss* tss = (CwxMqTss*) CwxTss::instance();
  ///通知收到一个消息
  ret = recvMessage(tss);
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
int CwxMqQueueHandler::onConnClosed() {
  return -1;
}

/**
@brief Handler的redo事件，在每次dispatch时执行。
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqQueueHandler::onRedo() {
  ///获取tss实例
  CwxMqTss* tss = (CwxMqTss*) CwxTss::instance();
  int iRet = sentBinlog(tss);
  if (0 == iRet) {    ///继续等待消息
    m_conn.m_bWaiting = true;
    channel()->regRedoHander(this);
  } else if (-1 == iRet) {    ///错误，关闭连接
    return -1;
  }
  m_conn.m_bWaiting = false;
  return 0;
}

/**
@brief 通知连接完成一个消息的发送。<br>
只有在Msg指定FINISH_NOTICE的时候才调用.
@param [in,out] msg 传入发送完毕的消息，若返回NULL，则msg有上层释放，否则底层释放。
@return 
CwxMsgSendCtrl::UNDO_CONN：不修改连接的接收状态
CwxMsgSendCtrl::RESUME_CONN：让连接从suspend状态变为数据接收状态。
CwxMsgSendCtrl::SUSPEND_CONN：让连接从数据接收状态变为suspend状态
*/
CWX_UINT32 CwxMqQueueHandler::onEndSendMsg(CwxMsgBlock*& msg) {
  ///获取tss实例
  CwxMqTss* tss = (CwxMqTss*) CwxTss::instance();
  int iRet = m_pApp->getQueueMgr()->endSendMsg(m_conn.m_strQueueName,
    msg->event().m_ullArg, true, tss->m_szBuf2K);
  //0：成功，-1：失败，-2：队列不存在
  if (-1 == iRet) {    //内部错误，此时必须关闭连接
    CWX_ERROR(("Queue[%s]: Failure to endSendMsg， err: %s", m_conn.m_strQueueName.c_str(), tss->m_szBuf2K));
  } else if (-2 == iRet) {    //队列不存在
    CWX_ERROR(("Queue[%s]: No queue"));
  }
  //此msg由queue mgr负责管理，外层不能释放
  msg = NULL;
  return CwxMsgSendCtrl::UNDO_CONN;
}

/**
@brief 通知连接上，一个消息发送失败。<br>
只有在Msg指定FAIL_NOTICE的时候才调用.
@param [in,out] msg 发送失败的消息，若返回NULL，则msg有上层释放，否则底层释放。
@return void。
*/
void CwxMqQueueHandler::onFailSendMsg(CwxMsgBlock*& msg) {
  CwxMqTss* tss = (CwxMqTss*) CwxTss::instance();
  backMq(msg->event().m_ullArg, tss);
  //此msg由queue mgr负责管理，外层不能释放
  msg = NULL;
}

///收到一个消息，0：成功；-1：失败
int CwxMqQueueHandler::recvMessage(CwxMqTss* pTss) {
  if (CwxMqPoco::MSG_TYPE_FETCH_DATA == m_header.getMsgType()) {    ///mq的获取消息
    return fetchMq(pTss);
  } else if (CwxMqPoco::MSG_TYPE_CREATE_QUEUE == m_header.getMsgType()) { ///创建队列的消息
    return createQueue(pTss);
  } else if (CwxMqPoco::MSG_TYPE_DEL_QUEUE == m_header.getMsgType()) { ///删除队列的消息
    return delQueue(pTss);
  }
  ///若其他消息，则返回错误
  CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u",
    m_header.getMsgType());
  CWX_ERROR((pTss->m_szBuf2K));
  CwxMsgBlock* block = packEmptyFetchMsg(pTss, CWX_MQ_ERR_ERROR,
    pTss->m_szBuf2K);
  if (!block) {
    CWX_ERROR(("No memory to malloc package"));
    return -1;
  }
  if (-1 == replyFetchMq(pTss, block, false, true)) return -1;
  return 0;
}

///fetch mq
int CwxMqQueueHandler::fetchMq(CwxMqTss* pTss) {
  int iRet = CWX_MQ_ERR_SUCCESS;
  bool bBlock = false;
  bool bClose = false;
  char const* queue_name = NULL;
  char const* user = NULL;
  char const* passwd = NULL;
  CwxMsgBlock* block = NULL;
  do {
    if (!m_recvMsgData) {
      strcpy(pTss->m_szBuf2K, "No data.");
      CWX_DEBUG((pTss->m_szBuf2K));
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
    iRet = CwxMqPoco::parseFetchMq(pTss->m_pReader,
      m_recvMsgData,
      bBlock,
      queue_name,
      user, 
      passwd,
      pTss->m_szBuf2K);
    ///如果解析失败，则进入错误消息处理
    if (CWX_MQ_ERR_SUCCESS != iRet)  break;
    ///如果当前mq的获取处于waiting状态或还没有多的确认，则直接忽略
    if (m_conn.m_bWaiting)  return 0;
    ///如果是第一次获取或者改变了消息对了，则校验队列权限
    if (!m_conn.m_strQueueName.length() ||  ///第一次获取
      m_conn.m_strQueueName != queue_name ///新队列
      )
    {
      m_conn.m_strQueueName.erase(); ///清空当前队列
      string strQueue = queue_name ? queue_name : "";
      string strUser = user ? user : "";
      string strPasswd = passwd ? passwd : "";
      ///鉴权队列的用户名、口令
      iRet = m_pApp->getQueueMgr()->authQueue(strQueue, strUser, strPasswd);
      ///如果队列不存在，则返回错误消息
      if (0 == iRet) {
        iRet = CWX_MQ_ERR_ERROR;
        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "No queue:%s",
          strQueue.c_str());
        CWX_DEBUG((pTss->m_szBuf2K));
        break;
      }
      ///如果权限错误，则返回错误消息
      if (-1 == iRet) {
        iRet = CWX_MQ_ERR_FAIL_AUTH;
        CwxCommon::snprintf(pTss->m_szBuf2K, 2048,
          "Failure to auth user[%s] passwd[%s]", user, passwd);
        CWX_DEBUG((pTss->m_szBuf2K));
        break;
      }
      ///初始化连接
      m_conn.reset();
      ///设置队列名
      m_conn.m_strQueueName = strQueue;
    }
    ///设置是否block
    m_conn.m_bBlock = bBlock;
    ///发送消息
    int ret = sentBinlog(pTss);
    if (0 == ret) { ///等待消息
      m_conn.m_bWaiting = true;
      channel()->regRedoHander(this);
      return 0;
    } else if (-1 == ret) { ///发送失败，关闭连接。此属于内存不足、连接关闭等等严重的错误
      return -1;
    }
    //发送了一个消息
    m_conn.m_bWaiting = false;
    return 0;
  } while (0);

  ///回复错误消息
  m_conn.m_bWaiting = false;
  block = packEmptyFetchMsg(pTss, iRet, pTss->m_szBuf2K);
  if (!block) { ///分配空间错误，直接关闭连接
    CWX_ERROR(("No memory to malloc package"));
    return -1;
  }
  ///回复失败，直接关闭连接
  if (-1 == replyFetchMq(pTss, block, false, bClose))
    return -1;
  return 0;

}
///create queue
int CwxMqQueueHandler::createQueue(CwxMqTss* pTss) {
  int iRet = CWX_MQ_ERR_SUCCESS;
  char const* queue_name = NULL;
  char const* user = NULL;
  char const* passwd = NULL;
  char const* auth_user = NULL;
  char const* auth_passwd = NULL;
  CWX_UINT64 ullSid = 0;
  do {
    if (!m_recvMsgData) {
      strcpy(pTss->m_szBuf2K, "No data.");
      CWX_DEBUG((pTss->m_szBuf2K));
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
    iRet = CwxMqPoco::parseCreateQueue(pTss->m_pReader, m_recvMsgData,
      queue_name, user, passwd, auth_user, auth_passwd, ullSid,
      pTss->m_szBuf2K);
    ///如果解析失败，则进入错误消息处理
    if (CWX_MQ_ERR_SUCCESS != iRet)
      break;
    //校验权限
    if (m_pApp->getConfig().getMq().m_mq.getUser().length()) {
      if ((m_pApp->getConfig().getMq().m_mq.getUser() != auth_user)
        || (m_pApp->getConfig().getMq().m_mq.getPasswd() != auth_passwd)) {
          iRet = CWX_MQ_ERR_FAIL_AUTH;
          strcpy(pTss->m_szBuf2K, "No auth");
          break;
      }
    }
    //检测参数
    if (!strlen(queue_name)) {
      iRet = CWX_MQ_ERR_ERROR;
      strcpy(pTss->m_szBuf2K, "queue name is empty");
      break;
    }
    if (strlen(queue_name) > CWX_MQ_MAX_QUEUE_NAME_LEN) {
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
        "Queue name[%s] is too long, max:%u", queue_name,
        CWX_MQ_MAX_QUEUE_NAME_LEN);
      break;
    }
    if (!CwxMqQueueMgr::isInvalidQueueName(queue_name)) {
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
        "Queue name[%s] is invalid, charactor must be in [a-z, A-Z, 0-9, -, _]",
        queue_name);
      break;
    }
    if (strlen(user) > CWX_MQ_MAX_QUEUE_USER_LEN) {
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
        "Queue's user name[%s] is too long, max:%u", user,
        CWX_MQ_MAX_QUEUE_USER_LEN);
      break;
    }
    if (strlen(passwd) > CWX_MQ_MAX_QUEUE_PASSWD_LEN) {
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
        "Queue's passwd [%s] is too long, max:%u", passwd,
        CWX_MQ_MAX_QUEUE_PASSWD_LEN);
      break;
    }
    if (0 == ullSid)
      ullSid = m_pApp->getBinLogMgr()->getMaxSid();
    iRet = m_pApp->getQueueMgr()->addQueue(string(queue_name), ullSid,
      string(user), string(passwd), pTss->m_szBuf2K);
    if (1 == iRet) { //成功
      iRet = CWX_MQ_ERR_SUCCESS;
      break;
    } else if (0 == iRet) { //exist
      iRet = CWX_MQ_ERR_ERROR;
      strcpy(pTss->m_szBuf2K, "Queue exists");
      break;
    } else { //内部错误
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
  } while (0);

  CwxMsgBlock* block = NULL;
  iRet = CwxMqPoco::packCreateQueueReply(pTss->m_pWriter, block, iRet,
    pTss->m_szBuf2K, pTss->m_szBuf2K);
  if (CWX_MQ_ERR_SUCCESS != iRet) {
    CWX_ERROR(("Failure to pack create-queue-reply, err:%s", pTss->m_szBuf2K));
    return -1;
  }
  block->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
  block->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_QUEUE);
  block->send_ctrl().setHostId(0);
  block->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  ///将消息放到连接的发送队列等待发送
  if (!putMsg(block)) { ///放发送队列失败
    CWX_ERROR(("Failure to reply create queue"));
    CwxMsgBlockAlloc::free(block);
    return -1;
  }
  return 0;
}
///del queue
int CwxMqQueueHandler::delQueue(CwxMqTss* pTss) {
  int iRet = CWX_MQ_ERR_SUCCESS;
  char const* queue_name = NULL;
  char const* user = NULL;
  char const* passwd = NULL;
  char const* auth_user = NULL;
  char const* auth_passwd = NULL;
  do {
    if (!m_recvMsgData) {
      strcpy(pTss->m_szBuf2K, "No data.");
      CWX_DEBUG((pTss->m_szBuf2K));
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
    iRet = CwxMqPoco::parseDelQueue(pTss->m_pReader, m_recvMsgData, queue_name,
      user, passwd, auth_user, auth_passwd, pTss->m_szBuf2K);
    ///如果解析失败，则进入错误消息处理
    if (CWX_MQ_ERR_SUCCESS != iRet)
      break;
    //校验权限
    if (m_pApp->getConfig().getMq().m_mq.getUser().length()) {
      if ((m_pApp->getConfig().getMq().m_mq.getUser() != auth_user)
        || (m_pApp->getConfig().getMq().m_mq.getPasswd() != auth_passwd)) {
          iRet = CWX_MQ_ERR_FAIL_AUTH;
          strcpy(pTss->m_szBuf2K, "No auth");
          break;
      }
    }
    iRet = m_pApp->getQueueMgr()->authQueue(string(queue_name), string(user),
      string(passwd));
    if (0 == iRet) { //队列不存在
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Queue[%s] doesn't exists",
        queue_name);
      break;
    }
    ///如果权限错误，则返回错误消息
    if (-1 == iRet) {
      iRet = CWX_MQ_ERR_FAIL_AUTH;
      strcpy(pTss->m_szBuf2K, "No auth");
      break;
    }
    iRet = m_pApp->getQueueMgr()->delQueue(string(queue_name), pTss->m_szBuf2K);
    if (1 == iRet) { //成功
      iRet = CWX_MQ_ERR_SUCCESS;
      break;
    } else if (0 == iRet) { //exist
      iRet = CWX_MQ_ERR_ERROR;
      strcpy(pTss->m_szBuf2K, "Queue exists");
      break;
    } else { //内部错误
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
  } while (0);

  CwxMsgBlock* block = NULL;
  iRet = CwxMqPoco::packDelQueueReply(pTss->m_pWriter, block, iRet,
    pTss->m_szBuf2K, pTss->m_szBuf2K);
  if (CWX_MQ_ERR_SUCCESS != iRet) {
    CWX_ERROR(("Failure to pack del-queue-reply, err:%s", pTss->m_szBuf2K));
    return -1;
  }
  block->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
  block->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_QUEUE);
  block->send_ctrl().setHostId(0);
  block->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  ///将消息放到连接的发送队列等待发送
  if (!putMsg(block)) { ///放发送队列失败
    CWX_ERROR(("Failure to reply delete queue"));
    CwxMsgBlockAlloc::free(block);
    return -1;
  }
  return 0;
}

CwxMsgBlock* CwxMqQueueHandler::packEmptyFetchMsg(CwxMqTss* pTss, int iRet,
                                                  char const* szErrMsg)
{
  CwxMsgBlock* pBlock = NULL;
  CwxKeyValueItem kv;
  iRet = CwxMqPoco::packFetchMqReply(pTss->m_pWriter, pBlock, iRet, szErrMsg,
    kv, pTss->m_szBuf2K);
  return pBlock;
}

int CwxMqQueueHandler::replyFetchMq(CwxMqTss* pTss,
                                    CwxMsgBlock* msg,
                                    bool bBinlog,
                                    bool bClose)
{
  msg->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
  msg->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_QUEUE);
  msg->send_ctrl().setHostId(0);
  if (bBinlog) { ///如果是binlog，此时需要发送完毕的时候通知
    if (bClose) ///是否关闭连接
      msg->send_ctrl().setMsgAttr(
      CwxMsgSendCtrl::FAIL_NOTICE | CwxMsgSendCtrl::FINISH_NOTICE
      | CwxMsgSendCtrl::CLOSE_NOTICE);
    else
      msg->send_ctrl().setMsgAttr(
      CwxMsgSendCtrl::FAIL_NOTICE | CwxMsgSendCtrl::FINISH_NOTICE);
  } else {
    if (bClose)
      msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
    else
      msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  }
  ///将消息放到连接的发送队列等待发送
  if (!putMsg(msg)) { ///放发送队列失败
    CWX_ERROR(("Failure to reply fetch mq"));
    ///如果是binlog，需要将消息返回到mq manager
    if (bBinlog)
      backMq(msg->event().m_ullArg, pTss);
    else
      ///否则释放消息
      CwxMsgBlockAlloc::free(msg);
    return -1;
  }
  return 0;
}

void CwxMqQueueHandler::backMq(CWX_UINT64 ullSid, CwxMqTss* pTss) {
  int iRet = m_pApp->getQueueMgr()->endSendMsg(m_conn.m_strQueueName, ullSid,
    false, pTss->m_szBuf2K);
  if (0 != iRet) {
    if (-1 == iRet) { ///队列错误
      CWX_ERROR(("Failure to back queue[%s]'s message, err:%s", m_conn.m_strQueueName.c_str(), pTss->m_szBuf2K));
    } else { ///队列不存在
      CWX_ERROR(("Failure to back queue[%s]'s message for no existing", m_conn.m_strQueueName.c_str()));
    }
  }
}

///发送消息，0：没有消息发送；1：发送一个；-1：发送失败
int CwxMqQueueHandler::sentBinlog(CwxMqTss* pTss) {
  CwxMsgBlock* pBlock = NULL;
  int err_no = CWX_MQ_ERR_SUCCESS;
  int iState = 0;
  iState = m_pApp->getQueueMgr()->getNextBinlog(pTss, m_conn.m_strQueueName,
    pBlock, err_no, pTss->m_szBuf2K);
  if (-1 == iState) { ///获取消息失败
    CWX_ERROR(("Failure to read binlog ,err:%s", pTss->m_szBuf2K));
    pBlock = packEmptyFetchMsg(pTss, CWX_MQ_ERR_ERROR, pTss->m_szBuf2K);
    if (!pBlock) {
      CWX_ERROR(("No memory to malloc package"));
      return -1;
    }
    ///此log不是binlog。
    if (0 != replyFetchMq(pTss, pBlock, false, false))
      return -1;
    return 1;
  } else if (0 == iState) { ///已经完成
    if (!m_conn.m_bBlock) { ///如果不是block类型，回复没有消息
      pBlock = packEmptyFetchMsg(pTss, CWX_MQ_ERR_ERROR, "No message");
      if (!pBlock) {
        CWX_ERROR(("No memory to malloc package"));
        return -1;
      }
      if (0 != replyFetchMq(pTss, pBlock, false, false))
        return -1;
      return 1;
    }
    //未完成，等待block
    return 0;
  } else if (1 == iState) { ///获取了一个消息
    if (0 != replyFetchMq(pTss, pBlock, true, false)) {
      return -1;
    }
    return 1;
  } else if (2 == iState) { //未完成
    return 0;
  }
  //此时，iState为-2
  //no queue
  CWX_ERROR(("Not find queue:%s", m_conn.m_strQueueName.c_str()));
  string strErr = string("No queue:") + m_conn.m_strQueueName;
  pBlock = packEmptyFetchMsg(pTss, CWX_MQ_ERR_ERROR, strErr.c_str());
  if (!pBlock) {
    CWX_ERROR(("No memory to malloc package"));
    return -1;
  }
  if (0 != replyFetchMq(pTss, pBlock, false, false)) return -1;
  return 1;
}
