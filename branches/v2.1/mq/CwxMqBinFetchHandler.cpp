#include "CwxMqBinFetchHandler.h"
#include "CwxMqApp.h"
#include "CwxMsgHead.h"
/**
@brief 连接可读事件，返回-1，close()会被调用
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqBinFetchHandler::onInput()
{
    ///接受消息
    int ret = CwxAppHandler4Channel::recvPackage(getHandle(),
        m_uiRecvHeadLen,
        m_uiRecvDataLen,
        m_szHeadBuf,
        m_header,
        m_recvMsgData);

    if (1 != ret) return ret; ///如果失败或者消息没有接收完，返回。
    ///获取fetch 线程的tss对象
    CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
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
int CwxMqBinFetchHandler::onConnClosed()
{
    ///如果当前还有发送的mq消息没有完成，则回收此消息
    ///对于非commit的队列，消息发送完毕后，m_ullSendSid会置0。
    ///对于commit队列，消息commit后，m_ullSendSid会置0
    if (m_conn.m_ullSendSid)
    {
        CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
        backMq(tss);
        m_conn.m_ullSendSid = 0;
    }
    return -1;
}

/**
@brief Handler的redo事件，在每次dispatch时执行。
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqBinFetchHandler::onRedo()
{
    ///获取tss实例
    CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
    int iRet = sentBinlog(tss);
    if (0 == iRet)
    {///继续等待消息
        m_conn.m_bWaiting = true;
        channel()->regRedoHander(this);
    }
    else if (-1 == iRet)
    {///错误，关闭连接
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
    ///获取tss实例
    CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
    CWX_ASSERT(!m_conn->m_bSent);
    int iRet = m_pApp->getQueueMgr()->endSendMsg(m_conn.m_strQueueName,
        m_conn.m_ullSendSid,
        true,
        tss->m_szBuf2K);
    //0：不存在，1：成功，-1：失败，-2：队列不存在
    if (0 == iRet)
    {//消息已经超时，此一定是commit类型的队列
        char szSid[64];
        CWX_DEBUG(("Queue[%s]: sid[%s] is timeout",
            m_conn.m_strQueueName.c_str(), 
            CwxCommon::toString(m_conn.m_ullSendSid, szSid, 10)));
    }
    else if (-1 == iRet)
    {//内部错误，此时必须关闭连接
        CWX_ERROR(("Queue[%s]: Failure to endSendMsg， err: %s",
            m_conn.m_strQueueName.c_str(), 
            tss->m_szBuf2K));
    }
    else if (-2 == iRet)
    {//队列不存在
        CWX_ERROR(("Queue[%s]: No queue"));
    }
    ///设置消息已经发送完毕
    m_conn->m_bSent = true;
    if (!m_conn->m_bCommit)
    {///如果是commit队列的，则清空m_conn->m_ullSendSid，否则等待commit的消息
        m_conn->m_ullSendSid = 0;
    }
    else if (1 != iRet)
    {
        ///对于commit队列不存在或内部错误或消息发送超时，都需要清空。
        ///此放置错误的commit，因为消息已经进入了可再发送的队列
        m_conn->m_ullSendSid = 0;
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
    if (m_conn.m_ullSendSid)
    {
        CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
        backMq(tss);
        m_conn.m_ullSendSid = 0;
    }
}



///收到一个消息，0：成功；-1：失败
int CwxMqBinFetchHandler::recvMessage(CwxMqTss* pTss)
{
    if (CwxMqPoco::MSG_TYPE_FETCH_DATA == m_header.getMsgType())
    {///mq的获取消息
        return fetchMq(pTss);
    }
    else if (CwxMqPoco::MSG_TYPE_FETCH_COMMIT == m_header.getMsgType())
    {///mq的单独commit消息
        return fetchMqCommit(pTss);
    }
    else if (CwxMqPoco::MSG_TYPE_CREATE_QUEUE == m_header.getMsgType())
    {///创建队列的消息
        return createQueue(pTss);
    }
    else if (CwxMqPoco::MSG_TYPE_DEL_QUEUE == m_header.getMsgType())
    {///删除队列的消息
        return delQueue(pTss);
    }
    ///若其他消息，则返回错误
    CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u", m_header.getMsgType());
    CWX_ERROR((pTss->m_szBuf2K));
    CwxMsgBlock* block = packEmptyFetchMsg(pTss, CWX_MQ_INVALID_MSG_TYPE, pTss->m_szBuf2K);
    if (!block)
    {
        CWX_ERROR(("No memory to malloc package"));
        return -1;
    }
    if (-1 == reply(pTss, block, m_conn.m_pQueue, CWX_MQ_INVALID_MSG_TYPE, true)) return -1;
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
        ///如果解析失败，则进入错误消息处理
        if (CWX_MQ_SUCCESS != iRet) break; 
        ///如果当前mq的获取处于waiting状态或还没有多的确认，则直接忽略
        if (m_conn.m_bWaiting || m_conn.m_ullSendSid) 
        {
            return 0;
        }
        ///如果是第一次获取或者改变了消息对了，则校验队列权限
        if (!m_conn.m_strQueueName.length() ||  ///第一次获取
            m_conn.m_strQueueName != queue_name ///新队列
            )
        {
            m_conn.m_strQueueName.erase(); ///清空当前队列
            string strQueue = queue_name?queue_name:"";
            string strUser = user?user:"";
            string strPasswd = passwd?passwd:"";
            ///鉴权队列的用户名、口令
            iRet = m_pApp->getQueueMgr()->authQueue(strQueue, strUser, strPasswd);
            ///如果队列不存在，则返回错误消息
            if (0 == iRet)
            {
                iRet = CWX_MQ_NO_QUEUE;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "No queue:%s", strQueue.c_str());
                CWX_DEBUG((pTss->m_szBuf2K));
                break;
            }
            ///如果权限错误，则返回错误消息
            if (-1 == iRet)
            {
                iRet = CWX_MQ_FAIL_AUTH;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                CWX_DEBUG((pTss->m_szBuf2K));
                break;
            }
            ///初始化连接
            m_conn.reset();
            ///设置队列名
            m_conn.m_strQueueName = strQueue;
        }
        else if (m_conn.m_bCommit && m_conn.m_ullSendSid)
        {//如果队列是commit类型的，而且发送的消息没有commit，则首先commit
            ///此时提交失败是不单独回复的，若需要回复，则采用commit的消息
            iRet = m_pApp->getQueueMgr()->commitBinlog(m_conn.m_strQueueName,
                m_conn.m_ullSendSid,
                pTss->m_szBuf2K);
            if (0 == iRet)
            {//消息不存在，此时消息已经超时
                char szSid[64];
                CWX_DEBUG(("queue[%s]: sid[%s] is timeout", m_conn.m_strQueueName.c_str(), CwxCommon::toString(m_conn.m_ullSendSid, szSid, 10)));
            }
            else if (-1 == iRet)
            {//内部提交失败
                CWX_ERROR(("queue[%s]: Failure to commit msg, err:%s", m_conn.m_strQueueName.c_str(), pTss->m_szBuf2K));
            }
            else if (-2 == iRet)
            {
                CWX_DEBUG(("queue[%s]: No queue", m_conn.m_strQueueName.c_str()));
            }
            //清空m_ullSendSid
            m_conn.m_ullSendSid = 0;
        }
        ///设置是否block
        m_conn.m_bBlock = bBlock;
        ///设置timeout值
        m_conn.m_uiTimeout = timeout;
        ///发送消息
        int ret = sentBinlog(pTss);
        if (0 == ret)
        {///等待消息
            m_conn.m_bWaiting = true;
            channel()->regRedoHander(this);
        }
        else if (-1 == ret)
        {///发送失败，关闭连接。此属于内存不足、连接关闭等等严重的错误
            return -1;
        }
        //发送了一个消息
        m_conn.m_bWaiting = false;
        return 0;
    }while(0);
    
    ///回复错误消息
    m_conn.m_bWaiting = false;
    block = packEmptyFetchMsg(pTss, iRet, pTss->m_szBuf2K);
    if (!block)
    {///分配空间错误，直接关闭连接
        CWX_ERROR(("No memory to malloc package"));
        return -1;
    }
    ///回复失败，直接关闭连接
    if (-1 == replyFetchMq(pTss, block, false, bClose)) return -1;
    return 0;

}

///commit mq,返回值,0：成功；-1：失败
int CwxMqBinFetchHandler::fetchMqCommit(CwxMqTss* pTss)
{

}

///create queue
int CwxMqBinFetchHandler::createQueue(CwxMqTss* pTss)
{

}
///del queue
int CwxMqBinFetchHandler::delQueue(CwxMqTss* pTss)
{

}




CwxMsgBlock* CwxMqBinFetchHandler::packEmptyFetchMsg(CwxMqTss* pTss,
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
            backMq(pTss);
        else
            CwxMsgBlockAlloc::free(msg);
        return -1;
    }
    return 0;
}

void CwxMqBinFetchHandler::backMq(CwxMqTss* pTss)
{
    int iRet = 0;
    if (m_conn->m_ullSendSid)
    {
        if (!m_conn->m_bSent)
        {
            iRet = m_pApp->getQueueMgr()->endSendMsg(m_conn.m_strQueueName,
                m_conn.m_ullSendSid,
                false,
                pTss->m_szBuf2K);
        }
        else
        {
            CWX_ASSERT(m_conn->m_bCommit);
            iRet = m_pApp->getQueueMgr()->commitBinlog(m_conn.m_strQueueName,
                 m_conn.m_ullSendSid,
                 false,
                 pTss->m_szBuf2K);
        }
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
