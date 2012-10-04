#include "CwxMcQueueHandler.h"
#include "CwxMcApp.h"
#include "CwxMsgHead.h"
/**
 @brief 连接可读事件，返回-1，close()会被调用
 @return -1：处理失败，会调用close()； 0：处理成功
 */
int CwxMcQueueHandler::onInput() {
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
int CwxMcQueueHandler::onConnClosed() {
  return -1;
}

/**
 @brief Handler的redo事件，在每次dispatch时执行。
 @return -1：处理失败，会调用close()； 0：处理成功
 */
int CwxMcQueueHandler::onRedo() {
  ///获取tss实例
  CwxMqTss* tss = (CwxMqTss*) CwxTss::instance();
  int iRet = sentMq(tss);
  if (0 == iRet) {    ///继续等待消息
    m_conn.m_bWaiting = true;
    channel()->regRedoHander(this);
  } else if (-1 == iRet) {    ///错误，关闭连接
    return -1;
  }
  m_conn.m_bWaiting = false;
  return 0;
}

///收到一个消息，0：成功；-1：失败
int CwxMcQueueHandler::recvMessage(CwxMqTss* pTss) {
  if (CwxMqPoco::MSG_TYPE_FETCH_DATA == m_header.getMsgType()) { ///mq的获取消息
    return fetchMq(pTss);
  }
  ///若其他消息，则返回错误
  CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u",
      m_header.getMsgType());
  CWX_ERROR((pTss->m_szBuf2K));
  CwxMsgBlock* block = packEmptyFetchMsg(pTss,
      CWX_MQ_ERR_ERROR,
      pTss->m_szBuf2K);
  if (!block) {
    CWX_ERROR(("No memory to malloc package"));
    return -1;
  }
  if (-1 == replyFetchMq(pTss, block, true)) return -1;
  return 0;
}

///fetch mq
int CwxMcQueueHandler::fetchMq(CwxMqTss* pTss) {
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
    iRet = CwxMqPoco::parseFetchMq(pTss->m_pReader, m_recvMsgData, bBlock,
        queue_name, user, passwd, pTss->m_szBuf2K);
    ///如果解析失败，则进入错误消息处理
    if (CWX_MQ_ERR_SUCCESS != iRet)
      break;
    ///如果当前mq的获取处于waiting状态或还没有多的确认，则直接忽略
    if (m_conn.m_bWaiting) return 0;
    ///检验队列名字
    if (m_pApp->getConfig().getMq().m_strName != queue_name){
        iRet = CWX_MQ_ERR_ERROR;
        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "No queue:%s",
            queue_name?queue_name:"");
        CWX_DEBUG((pTss->m_szBuf2K));
        break;
    }
    ///如果是第一次获取或者改变了消息对了，则校验队列权限
    if (!m_conn.m_strQueueName.length()){
        ///鉴权队列的用户名、口令
        if (m_pApp->getConfig().getMq().m_mq.getUser().length()){
            if ((m_pApp->getConfig().getMq().m_mq.getUser() != user) ||
                (m_pApp->getConfig().getMq().m_mq.getPasswd() != passwd))
            {
                iRet = CWX_MQ_ERR_FAIL_AUTH;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048,
                    "Failure to auth user[%s] passwd[%s]",
                    user?user:"",
                    passwd?passwd:"");
                CWX_DEBUG((pTss->m_szBuf2K));
                break;
            }
        }
      ///设置队列名
      m_conn.m_strQueueName = queue_name;
    }
    ///设置是否block
    m_conn.m_bBlock = bBlock;
    ///发送消息
    int ret = sentMq(pTss);
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
  if (-1 == replyFetchMq(pTss, block, bClose)) return -1;
  return 0;

}

CwxMsgBlock* CwxMcQueueHandler::packEmptyFetchMsg(CwxMqTss* pTss,
                                                  int iRet,
                                                  char const* szErrMsg)
{
  CwxMsgBlock* pBlock = NULL;
  CwxKeyValueItem kv;
  iRet = CwxMqPoco::packFetchMqReply(pTss->m_pWriter,
      pBlock,
      iRet,
      szErrMsg,
      kv,
      pTss->m_szBuf2K);
  return pBlock;
}

int CwxMcQueueHandler::replyFetchMq(CwxMqTss* pTss,
                                    CwxMsgBlock* msg,
                                    bool bClose)
{
  msg->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
  msg->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_QUEUE);
  msg->send_ctrl().setHostId(0);
  if (bClose)
      msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
  else
      msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  ///将消息放到连接的发送队列等待发送
  if (!putMsg(msg)) { ///放发送队列失败
    CWX_ERROR(("Failure to reply fetch mq"));
    ///释放消息
    CwxMsgBlockAlloc::free(msg);
    return -1;
  }
  return 0;
}

///发送消息，0：没有消息发送；1：发送一个；-1：发送失败
int CwxMcQueueHandler::sentMq(CwxMqTss* pTss) {
  CwxMsgBlock* pBlock = NULL;
  CwxMcQueueItem* log = m_pApp->getQueue()->pop();
  if (!log){
    if (!m_conn.m_bBlock) { ///如果不是block类型，回复没有消息
      pBlock = packEmptyFetchMsg(pTss, CWX_MQ_ERR_ERROR, "No message");
      if (!pBlock) {
        CWX_ERROR(("No memory to malloc package"));
        return -1;
      }
      if (0 != replyFetchMq(pTss, pBlock, false)) return -1;
      return 1;
    }
    //未完成，等待block
    return 0;
  } 
  ///获取了一个消息
  CwxKeyValueItem kv;
  kv.m_szData = log->getData();
  kv.m_uiDataLen = log->getDataSize();
  kv.m_bKeyValue = false;
  CwxMqPoco::packFetchMqReply(pTss->m_pWriter,
      pBlock,
      CWX_MQ_ERR_SUCCESS,
      szErrMsg,
      kv,
      pTss->m_szBuf2K);
  CwxMcQueueItem::destoryItem(log);
  if (!pBlock) {
      CWX_ERROR(("No memory to malloc package"));
      return -1;
  }
  if (0 != replyFetchMq(pTss, pBlock, false))  return -1;
  return 1;
}
