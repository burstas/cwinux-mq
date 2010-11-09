#include "CwxMqFetchHandler.h"
#include "CwxMqApp.h"

///连接建立后，需要维护连接上数据的分发
int CwxMqFetchHandler::onConnCreated(CwxMsgBlock*& msg, CwxAppTss* )
{
    ///连接必须必须不存在
    CWX_ASSERT(m_fetchConns.m_clientMap.find(msg->event().getConnId()) == m_fetchConns.m_clientMap.end());
    ///将连接添加到map中
    CwxMqFetchConn* pConn = m_fetchConns.m_connPool->malloc();
    pConn->m_uiConnId = msg->event().getConnId();
    m_fetchConns.m_clientMap[msg->event().getConnId()] = pConn;
    pConn->m_bTail = false;
    CWX_DEBUG(("Add fetch conn for conn-id[%u]", msg->event().getConnId()));
    return 1;
}

///连接关闭后，需要清理环境
int CwxMqFetchHandler::onConnClosed(CwxMsgBlock*& msg, CwxAppTss* )
{
    map<CWX_UINT32, CwxMqFetchConn*>::iterator iter = m_fetchConns.m_clientMap.find(msg->event().getConnId());
    ///连接必须存在
    CWX_ASSERT(iter != m_fetchConns.m_clientMap.end());
    CwxMqFetchConn* pConn = iter->second;
    ///将连接从map中删除
    m_fetchConns.m_clientMap.erase(iter);
    ///删除binlog读取的cursor
    if (pConn->m_bTail)
    {
        m_fetchConns.m_connWaitTail.remove(pConn);
    }
    m_fetchConns.m_connPool->free(pConn);
    CWX_DEBUG(("remove fetch conn for conn-id[%u]", msg->event().getConnId()));
    return 1;
}


///echo请求的处理函数
int CwxMqFetchHandler::onRecvMsg(CwxMsgBlock*& msg, CwxAppTss* pThrEnv)
{
    map<CWX_UINT32, CwxMqFetchConn*>::iterator iter = m_fetchConns.m_clientMap.find(msg->event().getConnId());
    ///连接必须存在
    CWX_ASSERT(iter != m_fetchConns.m_clientMap.end());
    CwxMqFetchConn* pConn = iter->second;
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    int iRet = CWX_MQ_SUCCESS;
    bool bBlock = false;
    bool bClose = false;
    char const* queue_name = NULL;
    char const* user=NULL;
    char const* passwd=NULL;
    CwxMsgBlock* block = NULL;

    if (CwxMqPoco::MSG_TYPE_FETCH_DATA == msg->event().getMsgHeader().getMsgType())
    {
        do
        {
            iRet = CwxMqPoco::parseFetchMq(pTss,
                msg,
                bBlock,
                queue_name,
                user,
                passwd,
                pTss->m_szBuf2K);
            if (CWX_MQ_SUCCESS != iRet) break;
            if (pConn->m_bTail)
            {///重复发送消息，直接忽略
                return 1;
            }
            if (!pConn->m_pQueue)
            {
                string strQueue = queue_name?queue_name:"";
                pConn->m_pQueue = m_pApp->getQueueMgr()->getQueue(strQueue);
                if (!pConn->m_pQueue)
                {
                    iRet = CWX_MQ_NO_QUEUE;
                    CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "No queue:%s", strQueue.c_str());
                    CWX_DEBUG((pTss->m_szBuf2K));
                    break;
                }
                if (pConn->m_pQueue->getUserName().length())
                {
                    if ( (pConn->m_pQueue->getUserName() != user) ||
                        (pConn->m_pQueue->getPasswd() != passwd))
                    {
                        pConn->m_pQueue = NULL;
                        iRet = CWX_MQ_FAIL_AUTH;
                        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                        CWX_DEBUG((pTss->m_szBuf2K));
                        break;
                    }
                }
            }

            int ret = pConn->m_pQueue->getNextBinlog(pTss, false, block, iRet, bClose);
            ///0：没有消息；
            ///1：获取一个消息；
            ///2：达到了搜索点，但没有发现消息；
            ///-1：失败；
            if (-1 == ret)
            {
                CWX_ERROR((pTss->m_szBuf2K));
                break;
            }
            if (0 == ret) //没有消息
            {
                if (bBlock)
                {
                    pConn->m_bBlock = true;
                    pConn->m_bTail = true;
                    m_fetchConns.m_connWaitTail.push_head(pConn);
                    return 1;
                }
                else
                {
                    iRet = CWX_MQ_NO_MSG;
                    strcpy(pTss->m_szBuf2K, "No message");
                    break;
                }
            }
            else if (2 == ret) //没有遍历完
            {
                ///设置当前的sid
                ret = m_pApp->getSysFile()->setSid(pConn->m_pQueue->getName(), pConn->m_pQueue->getCurSid());
                if (1 != ret)
                {
                    iRet = CWX_MQ_INNER_ERR;
                    if (-1 == ret)
                        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to set sys-file, errno:%s", m_pApp->getSysFile()->getErrMsg());
                    else
                        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to set sys-file, queue can't be found:%s", pConn->m_pQueue->getName().c_str());
                    CWX_ERROR((pTss->m_szBuf2K));
                    break;
                }
                pConn->m_bTail = true;
                pConn->m_bBlock = bBlock;
                m_fetchConns.m_connWaitTail.push_head(pConn);
                noticeContinue(pTss, pConn->m_uiConnId);
                return 1; ///返回
            }

        }while(0);
    }
    else
    {
        bClose = true;
        ///若其他消息，则返回错误
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u", msg->event().getMsgHeader().getMsgType());
        CWX_ERROR((pTss->m_szBuf2K));
        iRet = CWX_MQ_INVALID_MSG_TYPE;
    }
    if (pConn->m_bTail)
    {
        pConn->m_bTail = false;
        m_fetchConns.m_connWaitTail.remove(pConn);
    }

    if (CWX_MQ_SUCCESS != iRet)
    {
        block = packErrMsg(pTss, iRet, pTss->m_szBuf2K);
        if (!block)
        {
            CWX_ERROR(("No memory to malloc package"));
            m_pApp->noticeCloseConn(msg->event().getConnId());
            return 1;
        }
    }
    reply(block, msg->event().getConnId(), pConn->m_pQueue, iRet, bClose);
    return 1;
}

///处理binlog发送完毕的消息
int CwxMqFetchHandler::onEndSendMsg(CwxMsgBlock*& msg, CwxAppTss* pTss)
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
        if (0 != m_pApp->commit_mq(pTss->m_szBuf2K))
        {
            CWX_ERROR(("Failure to commit sys file, err=%s", pTss->m_szBuf2K));
        }
    }
    return 1;

}

///处理发送失败的binlog
int CwxMqFetchHandler::onFailSendMsg(CwxMsgBlock*& msg, CwxAppTss* )
{
    back(msg);
    msg = NULL;
    return 1;
}

///处理继续发送的消息
int CwxMqFetchHandler::onUserEvent(CwxMsgBlock*& msg, CwxAppTss* pThrEnv)
{
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    map<CWX_UINT32, CwxMqFetchConn*>::iterator iter = m_fetchConns.m_clientMap.find(msg->event().getConnId());
    if (iter != m_fetchConns.m_clientMap.end())
    {
        CwxMqFetchConn * pConn = iter->second;
        if (pConn->m_pQueue && pConn->m_bTail)
        {
            sentBinlog(pTss, pConn);
        }
    }
    return 1;
}



void CwxMqFetchHandler::dispatch(CwxMqTss* pTss)
{
    CwxMqFetchConn * pConn = (CwxMqFetchConn *)m_fetchConns.m_connWaitTail.head();
    while(pConn)
    {
        CWX_ASSERT(pConn->m_bTail);
        sentBinlog(pTss, pConn);
        pConn = pConn->m_next;
    }
}

CwxMsgBlock* CwxMqFetchHandler::packErrMsg(CwxMqTss* pTss,
                        int iRet,
                        char const* szErrMsg
                        )
{
    CwxMsgBlock* pBlock = NULL;
    CwxKeyValueItem kv;
    iRet = CwxMqPoco::packFetchMqReply(pTss,
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

void CwxMqFetchHandler::reply(CwxMsgBlock* msg,
           CWX_UINT32 uiConnId,
           CwxMqQueue* pQueue,
           int ret,
           bool bClose)
{
    msg->send_ctrl().setConnId(uiConnId);
    msg->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_FETCH);
    msg->send_ctrl().setHostId(0);
    CWX_UINT32 uiMsgAttr = 0;
    if (CWX_MQ_SUCCESS == ret)
    {
        uiMsgAttr |= CwxMsgSendCtrl::FAIL_NOTICE | CwxMsgSendCtrl::FINISH_NOTICE; 
    }
    if (bClose)
        msg->send_ctrl().setMsgAttr(uiMsgAttr|CwxMsgSendCtrl::CLOSE_NOTICE);
    else
        msg->send_ctrl().setMsgAttr(uiMsgAttr);
    if (0 != m_pApp->sendMsgByConn(msg))
    {
        CWX_ERROR(("Failure to reply fetch mq"));
        if (CWX_MQ_SUCCESS == ret)
            pQueue->backMsg(msg);
        else
            CwxMsgBlockAlloc::free(msg);
        m_pApp->noticeCloseConn(uiConnId);
    }
}

void CwxMqFetchHandler::back(CwxMsgBlock* msg)
{
    CwxMqQueue* pQueue = m_pApp->getQueueMgr()->getQueue(msg->event().m_uiArg);
    CWX_ASSERT(pQueue);
    pQueue->backMsg(msg);
}
void CwxMqFetchHandler::noticeContinue(CwxMqTss* , CWX_UINT32 uiConnId)
{
    CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
    msg->event().setSvrId(CwxMqApp::SVR_TYPE_FETCH);
    msg->event().setEvent(CwxMqApp::MQ_CONTINUE_SEND_EVENT);
    msg->event().setConnId(uiConnId);
    m_pApp->getWriteThreadPool()->append(msg);
}

///发送消息
void CwxMqFetchHandler::sentBinlog(CwxMqTss* pTss, CwxMqFetchConn * pConn)
{
    CwxMsgBlock* pBlock=NULL;
    int err_no = CWX_MQ_SUCCESS;
    bool bClose = false;
    int iState = 0;
    if (pConn->m_pQueue && pConn->m_bTail)
    {
        iState = pConn->m_pQueue->getNextBinlog(pTss,
            false,
            pBlock,
            err_no,
            bClose);
        if (-1 == iState)
        {
            CWX_ERROR(("Failure to read binlog ,err:%s", pTss->m_szBuf2K));
            pBlock = packErrMsg(pTss, iState, pTss->m_szBuf2K);
            if (!pBlock)
            {
                CWX_ERROR(("No memory to malloc package"));
                m_pApp->noticeCloseConn(pConn->m_uiConnId);
            }
            pConn->m_bTail = false;
            m_fetchConns.m_connWaitTail.remove(pConn);
            reply(pBlock, pConn->m_uiConnId, pConn->m_pQueue, err_no, bClose);
        }
        else if (0 == iState) ///已经完成
        {
            if (!pConn->m_bBlock)
            {
                pBlock = packErrMsg(pTss, CWX_MQ_NO_MSG, "No message");
                if (!pBlock)
                {
                    CWX_ERROR(("No memory to malloc package"));
                    m_pApp->noticeCloseConn(pConn->m_uiConnId);
                }
                pConn->m_bTail = false;
                m_fetchConns.m_connWaitTail.remove(pConn);
                reply(pBlock, pConn->m_uiConnId, pConn->m_pQueue, CWX_MQ_NO_MSG, false);
            }
        }
        else if (1 == iState)
        {
            pConn->m_bTail = false;
            m_fetchConns.m_connWaitTail.remove(pConn);
            reply(pBlock, pConn->m_uiConnId, pConn->m_pQueue, CWX_MQ_SUCCESS, false);
        }
        else if (2 == iState)
        {//未完成
            noticeContinue(pTss, pConn->m_uiConnId);
        }
    }
}
