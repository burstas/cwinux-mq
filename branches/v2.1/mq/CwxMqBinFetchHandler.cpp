#include "CwxMqBinFetchHandler.h"
#include "CwxMqApp.h"
#include "CwxMsgHead.h"
/**
@brief 连接可读事件，返回-1，close()会被调用
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqBinFetchHandler::onInput()
{
    int ret = CwxAppHandler4Channel::recvPackage(getHandle(),
        m_uiRecvHeadLen,
        m_uiRecvDataLen,
        m_szHeadBuf,
        m_header,
        m_recvMsgData);
    if (1 != ret) return ret;
    CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
    ret = recvMessage(tss);
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
int CwxMqBinFetchHandler::onConnClosed()
{
    return -1;
}


///0：成功；-1：失败
int CwxMqBinFetchHandler::recvMessage(CwxMqTss* pTss)
{
    int iRet = CWX_MQ_SUCCESS;
    bool bClose = false;
    if (CwxMqPoco::MSG_TYPE_FETCH_DATA == m_header.getMsgType())
    {
        return fetchMq(pTss);
    }
    else if (CwxMqPoco::MSG_TYPE_CREATE_QUEUE == m_header.getMsgType())
    {
        return createQueue(pTss);
    }
    else if (CwxMqPoco::MSG_TYPE_DEL_QUEUE == m_header.getMsgType())
    {
        return delQueue(pTss);
    }
    ///若其他消息，则返回错误
    CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u", m_header.getMsgType());
    CWX_ERROR((pTss->m_szBuf2K));
    CwxMsgBlock* block = packErrMsg(pTss, CWX_MQ_INVALID_MSG_TYPE, pTss->m_szBuf2K);
    if (!block)
    {
        CWX_ERROR(("No memory to malloc package"));
        return -1;
    }
    if (-1 == reply(pTss, block, m_conn.m_pQueue, iRet, true)) return -1;
    return 0;
}

///fetch mq
int CwxMqBinFetchHandler::fetchMq(CwxMqTss* pTss)
{
    int iRet = CWX_MQ_SUCCESS;
    bool bBlock = false;
    bool bClose = false;
    char const* queue_name = NULL;
    char const* user=NULL;
    char const* passwd=NULL;
    CwxMsgBlock* block = NULL;
    CWX_UINT32  timeout = 0;
    do
    {
        iRet = CwxMqPoco::parseFetchMq(pTss->m_pReader,
            m_recvMsgData,
            bBlock,
            queue_name,
            user,
            passwd,
            timeout,
            pTss->m_szBuf2K);
        if (CWX_MQ_SUCCESS != iRet) break;
        if (m_conn.m_bWaiting || !m_conn.m_bSent) 
        {///重复发送消息，直接忽略
            return 0;
        }
        ///如果是第一次获取，则获取队列的名字
        if (!m_conn.m_strQueueName.length())
        {
            string strQueue = queue_name?queue_name:"";
            string strUser = user?user:"";
            string strPasswd = passwd?passwd:"";
            iRet = m_pApp->getQueueMgr()->authQueue(strQueue, strUser, strPasswd);
            if (0 == iRet)
            {
                iRet = CWX_MQ_NO_QUEUE;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "No queue:%s", strQueue.c_str());
                CWX_DEBUG((pTss->m_szBuf2K));
                break;
            }
            if (-1 == iRet)
            {
                iRet = CWX_MQ_FAIL_AUTH;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                CWX_DEBUG((pTss->m_szBuf2K));
                break;
            }
            m_conn.reset();
            m_conn.m_strQueueName = strQueue;
        }
        else if (m_conn.m_bCommit && m_conn.m_ullSendSid)
        {//如果队列是commit类型的，而且发送的消息没有commit，则首先commit
            iRet = m_pApp->getQueueMgr()->commitBinlog(m_conn.m_strQueueName,
                m_conn.m_ullSendSid,
                pTss->m_szBuf2K);
            if (0 == iRet)
            {//消息不存在
                iRet = CWX_MQ_TIMEOUT;
                char szSid[64];
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "sid[%s] is timeout", CwxCommon::toString(m_conn.m_ullSendSid, szSid, 10));
                CWX_DEBUG((pTss->m_szBuf2K));
                break;
            }
            else if (1 == iRet)
            {//成功commit
            }
            else if (-1 == iRet)
            {
                iRet = CWX_MQ_INNER_ERR;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to commit msg, err:", pTss->m_szBuf2K);
                CWX_ERROR((pTss->m_szBuf2K));
                break;

            }
            else if (-2 == iRet)
            {
                iRet = CWX_MQ_NO_QUEUE;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "No queue:%s", strQueue.c_str());
                CWX_DEBUG((pTss->m_szBuf2K));
                break;
            }
        }
        m_conn.m_bBlock = bBlock;
        m_conn.m_uiTimeout = timeout;
        int ret = sentBinlog(pTss, m_conn);
        if (0 == ret)
        {
            m_conn.m_bWaiting = true;
            channel()->regRedoHander(this);
        }
        else if (-1 == ret)
        {
            return -1;
        }
        m_conn.m_bWaiting = false;
        return 0;
    }while(0);

    m_conn.m_bWaiting = false;
    block = packErrMsg(pTss, iRet, pTss->m_szBuf2K);
    if (!block)
    {
        CWX_ERROR(("No memory to malloc package"));
        return -1;
    }
    if (-1 == reply(pTss, block, m_conn.m_pQueue, iRet, bClose)) return -1;
    return 0;

}
///create queue
int CwxMqBinFetchHandler::createQueue(CwxMqTss* pTss)
{

}
///del queue
int CwxMqBinFetchHandler::delQueue(CwxMqTss* pTss)
{

}


/**
@brief Handler的redo事件，在每次dispatch时执行。
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqBinFetchHandler::onRedo()
{
    CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
    int iRet = sentBinlog(tss, m_conn);
    if (0 == iRet)
    {
        m_conn.m_bWaiting = true;
        channel()->regRedoHander(this);
    }
    else if (-1 == iRet)
    {
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
CWX_UINT32 CwxMqBinFetchHandler::onEndSendMsg(CwxMsgBlock*& msg)
{
    CwxMqQueue* pQueue = m_pApp->getQueueMgr()->getQueue(msg->event().m_uiArg);
    CWX_ASSERT(pQueue);
    int ret = m_pApp->getSysFile()->setSid(pQueue->getName(), msg->event().m_ullArg, true);
    if (1 != ret)
    {
        if (-1 == ret)
            CWX_ERROR(("Failure to set the send sid to sys file, err:%s", m_pApp->getSysFile()->getErrMsg()));
        else
            CWX_ERROR(("Can't find queue[%u] in sys file", pQueue->getName().c_str()));
    }
    m_pApp->incMqUncommitNum();
    if ((m_pApp->getMqUncommitNum() >= m_pApp->getConfig().getBinLog().m_uiMqFetchFlushNum) ||
        (time(NULL) > (time_t)(m_pApp->getMqLastCommitTime() + m_pApp->getConfig().getBinLog().m_uiMqFetchFlushSecond)))
    {
        CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
        if (0 != m_pApp->commit_mq(tss->m_szBuf2K))
        {
            CWX_ERROR(("Failure to commit sys file, err=%s", tss->m_szBuf2K));
        }
    }
    return CwxMsgSendCtrl::UNDO_CONN;
}

/**
@brief 通知连接上，一个消息发送失败。<br>
只有在Msg指定FAIL_NOTICE的时候才调用.
@param [in,out] msg 发送失败的消息，若返回NULL，则msg有上层释放，否则底层释放。
@return void。
*/
void CwxMqBinFetchHandler::onFailSendMsg(CwxMsgBlock*& msg)
{
    CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
    back(tss, msg);
    msg = NULL;
}


CwxMsgBlock* CwxMqBinFetchHandler::packEmptyMsg(CwxMqTss* pTss,
                        int iRet,
                        char const* szErrMsg
                        )
{
    CwxMsgBlock* pBlock = NULL;
    CwxKeyValueItem kv;
    iRet = CwxMqPoco::packFetchMqReply(pTss->m_pWriter,
        pBlock,
        iRet,
        szErrMsg,
        0,
        0,
        kv,
        0,
        0,
        0,
        pTss->m_szBuf2K);
    return pBlock;
}

int CwxMqBinFetchHandler::replyFetchMq(CwxMqTss* pTss,
                                CwxMsgBlock* msg,
                                string const& strQueue,
                                int ret,
                                bool bBinlog,
                                bool bClose)
{
    msg->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
    msg->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_FETCH);
    msg->send_ctrl().setHostId(0);
    if (bBinlog)
    {
        if (bClose)
            msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::FAIL_NOTICE | CwxMsgSendCtrl::FINISH_NOTICE|CwxMsgSendCtrl::CLOSE_NOTICE);
        else
            msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::FAIL_NOTICE | CwxMsgSendCtrl::FINISH_NOTICE);
    }
    else
    {
        if (bClose)
            msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
        else
            msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    }
    if (!putMsg(msg))
    {
        CWX_ERROR(("Failure to reply fetch mq"));
        if (bBinlog)
            backMq(pTss, msg);
        else
            CwxMsgBlockAlloc::free(msg);
        return -1;
    }
    return 0;
}

void CwxMqBinFetchHandler::backMq(CwxMqTss* pTss, CwxMsgBlock* msg)
{
    int iRet = m_pApp->getQueueMgr()->endSendMsg(m_conn.m_strQueueName,
        m_conn.m_ullSendSid,
        false,
        pTss->m_szBuf2K);
    if (1 != iRet)
    {
        char szSid[32];
        if (0 == iRet)
        {
            CWX_DEBUG(("Sid[%s] is timeout, queue[%s]",
                m_conn.m_strQueueName.c_str(),
                CwxCommon::toString(m_conn.m_ullSendSid, szSid, 10)));
        }
        else if(-1 == iRet)
        {
            CWX_ERROR(("Failure to back queue[%s]'s message, err:%s",
                m_conn.m_strQueueName.c_str(),
                pTss->m_szBuf2K));
        }
        else
        {
            CWX_ERROR(("Failure to back queue[%s]'s message for no existing",
                m_conn.m_strQueueName.c_str()));
        }
    }
    //清空连接的发送sid
    m_conn.m_ullSendSid = 0;
    m_conn.m_bSent = true;
}

///发送消息，0：没有消息发送；1：发送一个；-1：发送失败
int CwxMqBinFetchHandler::sentBinlog(CwxMqTss* pTss)
{
    CwxMsgBlock* pBlock=NULL;
    int err_no = CWX_MQ_SUCCESS;
    int iState = 0;
    iState = m_pApp->getQueueMgr()->getNextBinlog(pTss,
        m_conn.m_strQueueName,
        pBlock,
        m_conn.m_uiTimeout,
        err_no,
        m_conn.m_bCommit,
        pTss->m_szBuf2K);
    if (-1 == iState)
    {
        CWX_ERROR(("Failure to read binlog ,err:%s", pTss->m_szBuf2K));
        pBlock = packErrMsg(pTss, iState, pTss->m_szBuf2K);
        if (!pBlock)
        {
            CWX_ERROR(("No memory to malloc package"));
            return -1;
        }
        if (0 != reply(pTss, pBlock, m_conn.m_strQueueName, err_no, true))
            return -1;
        return 1;
    }
    else if (0 == iState) ///已经完成
    {
        if (!m_conn.m_bBlock)
        {
            pBlock = packErrMsg(pTss, CWX_MQ_NO_MSG, "No message");
            if (!pBlock)
            {
                CWX_ERROR(("No memory to malloc package"));
                return -1;
            }
            if (0 != reply(pTss, pBlock, m_conn.m_strQueueName, CWX_MQ_NO_MSG, false))
                return -1;
            return 1;
        }
    }
    else if (1 == iState)
    {
        m_conn.m_ullSendSid = pBlock->event().m_ullArg;
        if (0 != reply(pTss, pBlock, m_conn.m_strQueueName, CWX_MQ_SUCCESS, false))
            return -1;
        return 1;
    }
    else if (2 == iState)
    {//未完成
        return 0;
    }
    //no queue
    CWX_ERROR(("Not find queue:%s", m_conn.m_strQueueName.c_str()));
    return -1;
}
