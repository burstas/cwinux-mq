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
    bool bBlock = false;
    bool bClose = false;
    char const* queue_name = NULL;
    char const* user=NULL;
    char const* passwd=NULL;
    CwxMsgBlock* block = NULL;

    if (CwxMqPoco::MSG_TYPE_FETCH_DATA == m_header.getMsgType())
    {
        do
        {
            iRet = CwxMqPoco::parseFetchMq(pTss->m_pReader,
                m_recvMsgData,
                bBlock,
                queue_name,
                user,
                passwd,
                pTss->m_szBuf2K);
            if (CWX_MQ_SUCCESS != iRet) break;
            if (m_conn.m_bWaiting)
            {///重复发送消息，直接忽略
                return 0;
            }
            m_conn.m_bBlock = bBlock;
            if (!m_conn.m_pQueue)
            {
                string strQueue = queue_name?queue_name:"";
                m_conn.m_pQueue = m_pApp->getQueueMgr()->getQueue(strQueue);
                if (!m_conn.m_pQueue)
                {
                    iRet = CWX_MQ_NO_QUEUE;
                    CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "No queue:%s", strQueue.c_str());
                    CWX_DEBUG((pTss->m_szBuf2K));
                    break;
                }
                if (m_conn.m_pQueue->getUserName().length())
                {
                    if ( (m_conn.m_pQueue->getUserName() != user) ||
                        (m_conn.m_pQueue->getPasswd() != passwd))
                    {
                        m_conn.m_pQueue = NULL;
                        iRet = CWX_MQ_FAIL_AUTH;
                        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                        CWX_DEBUG((pTss->m_szBuf2K));
                        break;
                    }
                }
            }

            int ret = sentBinlog(pTss, &m_conn);
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
    }
    else
    {
        bClose = true;
        ///若其他消息，则返回错误
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u", m_header.getMsgType());
        CWX_ERROR((pTss->m_szBuf2K));
        iRet = CWX_MQ_INVALID_MSG_TYPE;
    }
    
    m_conn.m_bWaiting = false;

    if (CWX_MQ_SUCCESS != iRet)
    {
        block = packErrMsg(pTss, iRet, pTss->m_szBuf2K);
        if (!block)
        {
            CWX_ERROR(("No memory to malloc package"));
            return -1;
        }
    }
    if (-1 == reply(pTss, block, m_conn.m_pQueue, iRet, bClose)) return -1;
    return 0;
}

/**
@brief Handler的redo事件，在每次dispatch时执行。
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqBinFetchHandler::onRedo()
{
    CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
    int iRet = sentBinlog(tss, &m_conn);
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


CwxMsgBlock* CwxMqBinFetchHandler::packErrMsg(CwxMqTss* pTss,
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

int CwxMqBinFetchHandler::reply(CwxMqTss* pTss,
                                CwxMsgBlock* msg,
                                CwxMqQueue* pQueue,
                                int ret,
                                bool bClose)
{
    msg->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
    msg->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_FETCH);
    msg->send_ctrl().setHostId(0);
    if (CWX_MQ_SUCCESS == ret)
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
        if (CWX_MQ_SUCCESS == ret)
        {
            if (unpackMsg(pTss, msg))
                pQueue->backMsg(pTss->m_header, pTss->m_kvData);
            CwxMsgBlockAlloc::free(msg);
        }
        else
        {
            CwxMsgBlockAlloc::free(msg);
        }
        return -1;
    }
    return 0;
}

void CwxMqBinFetchHandler::back(CwxMqTss* pTss, CwxMsgBlock* msg)
{
    CwxMqQueue* pQueue = m_pApp->getQueueMgr()->getQueue(msg->event().m_uiArg);
    CWX_ASSERT(pQueue);
    if (unpackMsg(pTss, msg))
        pQueue->backMsg(pTss->m_header, pTss->m_kvData);
    
    CwxMsgBlockAlloc::free(msg);
}

///发送消息，0：没有消息发送；1：发送一个；-1：发送失败
int CwxMqBinFetchHandler::sentBinlog(CwxMqTss* pTss, CwxMqFetchConn * pConn)
{
    CwxMsgBlock* pBlock=NULL;
    int err_no = CWX_MQ_SUCCESS;
    int iState = 0;
    if (pConn->m_pQueue)
    {
        iState = pConn->m_pQueue->getNextBinlog(pTss,
            err_no,
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
            if (0 != reply(pTss, pBlock, pConn->m_pQueue, err_no, true)) return -1;
            return 1;
        }
        else if (0 == iState) ///已经完成
        {
            if (!pConn->m_bBlock)
            {
                pBlock = packErrMsg(pTss, CWX_MQ_NO_MSG, "No message");
                if (!pBlock)
                {
                    CWX_ERROR(("No memory to malloc package"));
                    return -1;
                }
                if (0 != reply(pTss, pBlock, pConn->m_pQueue, CWX_MQ_NO_MSG, false)) return -1;
                return 1;
            }
        }
        else if (1 == iState)
        {
            ///形成binlog发送的数据包
            if (CWX_MQ_SUCCESS != CwxMqPoco::packFetchMqReply(pTss->m_pWriter,
                pBlock,
                CWX_MQ_SUCCESS,
                "",
                pTss->m_header.getSid(),
                pTss->m_header.getDatetime(),
                pTss->m_kvData,
                pTss->m_header.getGroup(),
                pTss->m_header.getType(),
                pTss->m_header.getAttr(),
                pTss->m_szBuf2K))
            {
                CWX_ERROR((pTss->m_szBuf2K));
                pBlock = packErrMsg(pTss, iState, pTss->m_szBuf2K);
                if (!pBlock)
                {
                    CWX_ERROR(("No memory to malloc package"));
                    return -1;
                }
                if (0 != reply(pTss, pBlock, pConn->m_pQueue, err_no, true)) return -1;
                return 1;
            }
            else
            {
                pBlock->event().m_ullArg = pTss->m_header.getSid();
                pBlock->event().m_uiArg = pConn->m_pQueue->getId();
                if (0 != reply(pTss, pBlock, pConn->m_pQueue, CWX_MQ_SUCCESS, false)) return -1;
                return 1;
            }

        }
        else if (2 == iState)
        {//未完成
            return 0;
        }
    }
    //no queue
    return -1;
}

///解压发送失败的msg消息
bool CwxMqBinFetchHandler::unpackMsg(CwxMqTss* pTss, CwxMsgBlock* msg)
{
    int  ret;
    char const* errMsg = NULL;
    CWX_UINT64 ullSid;
    CWX_UINT32 uiTimeStamp;
    CWX_UINT32 group;
    CWX_UINT32 type;
    CWX_UINT32 attr;
    msg->rd_ptr(CwxMsgHead::MSG_HEAD_LEN);
    if (CWX_MQ_SUCCESS != CwxMqPoco::parseFetchMqReply(pTss->m_pReader,
        msg,
        ret,
        errMsg,
        ullSid,
        uiTimeStamp,
        &pTss->m_kvData,
        group,
        type,
        attr,
        pTss->m_szBuf2K))
    {
        CWX_ERROR(("Failure to unpack the failure sent fetch message, err:%s", pTss->m_szBuf2K));
        return false;
    }
    pTss->m_header.setSid(ullSid);
    pTss->m_header.setDatetime(uiTimeStamp);
    pTss->m_header.setGroup(group);
    pTss->m_header.setType(type);
    pTss->m_header.setAttr(attr);
    return true;
}
