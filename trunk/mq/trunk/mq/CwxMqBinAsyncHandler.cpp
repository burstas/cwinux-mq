#include "CwxMqBinAsyncHandler.h"
#include "CwxMqApp.h"
///构造函数
CwxMqBinAsyncHandler::CwxMqBinAsyncHandler(CwxMqApp* pApp, CwxAppChannel* channel):CwxAppHandler4Channel(channel)
{
    m_pApp = pApp;
    m_pCursor = NULL;
    m_uiRecvHeadLen = 0;
    m_uiRecvDataLen = 0;
    m_recvMsgData = 0;
}
///析构函数
CwxMqBinAsyncHandler::~CwxMqBinAsyncHandler()
{
    if (m_recvMsgData) CwxMsgBlockAlloc::free(m_recvMsgData);
    if (m_dispatch->m_pCursor) m_pApp->getBinLogMgr()->destoryCurser(m_dispatch->m_pCursor);
}

/**
@brief 连接可读事件，返回-1，close()会被调用
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqBinAsyncHandler::onInput()
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

//1：不从engine中移除注册；0：从engine中移除注册但不删除handler；-1：从engine中将handle移除并删除。
int CwxMqBinAsyncHandler::onConnClosed()
{
    return -1;
}



///连接关闭后，需要清理环境
int CwxMqBinAsyncHandler::recvMessage(CwxMqTss* pTss)
{
    CWX_UINT64 ullSid = 0;
    CWX_UINT32 uiChunk = 0;
    bool  bNewly = false;
    char const* subscribe=NULL;
    char const* user=NULL;
    char const* passwd=NULL;
    int iRet = CWX_MQ_SUCCESS;
    int iState = 0;
    do 
    {
        if (CwxMqPoco::MSG_TYPE_SYNC_DATA_REPLY == m_header.getMsgType())
        {
            if (!m_dispatch.m_bSync)
            { ///如果连接不是同步状态，则是错误
                strcpy(pTss->m_szBuf2K, "Client no in sync state");
                CWX_DEBUG((pTss->m_szBuf2K));
                iRet = CWX_MQ_INVALID_MSG_TYPE;
                break;
            }
            ///若是同步sid的报告消息,则获取报告的sid
            iRet = CwxMqPoco::parseSyncDataReply(pTss->m_pReader,
                m_recvMsgData,
                ullSid,
                pTss->m_szBuf2K);
            if (CWX_MQ_SUCCESS != iRet)
            {
                CWX_DEBUG(("Failure to parse sync_data reply package, err:%s", pTss->m_szBuf2K));
                break;
            }
            ///检查返回的sid
            if (ullSid != m_dispatch.m_ullSid)
            {
                char szBuf1[64];
                char szBuf2[64];
                CwxCommon::snprintf(pTss->m_szBuf2K, "Reply sid[%s] is not the right sid[%s], close conn.",
                    CwxCommon::toString(ullSid, szBuf1, 10),
                    CwxCommon::toString(m_dispatch.m_ullSid, szBuf2, 10));
                CWX_ERROR((pTss->m_szBuf2K));
               iRet = CWX_MQ_INVALID_SID;
                break;
            }
            ///发送下一条binlog
            iState = sendBinLog(m_pApp, m_dispatch, pTss);
            if (-1 == iState)
            {
                CWX_ERROR((pTss->m_szBuf2K));
                return -1; ///关闭连接
            }
            else if (0 == iState)
            {///产生continue的消息
                m_pApp->getAsyncDispChannel()->regRedoHander(this);
                m_dispatch.m_bContinue = true;
            }
            ///返回
            return 0;
        }
        else if (CwxMqPoco::MSG_TYPE_SYNC_REPORT == m_header.getMsgType())
        {
            if (m_dispatch->m_pCursor)
            {
                iRet = CWX_MQ_INVALID_MSG;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Can't report sync sid duplicatly.");
                CWX_ERROR((pTss->m_szBuf2K));
                break;
            }
            ///若是同步sid的报告消息,则获取报告的sid
            iRet = CwxMqPoco::parseReportData(pTss,
                msg,
                ullSid,
                bNewly,
                uiChunk,
                subscribe,
                user,
                passwd,
                pTss->m_szBuf2K);
            if (CWX_MQ_SUCCESS != iRet)
            {///若不存在，则错误返回
                CWX_ERROR(("Failure to parse report msg, err=%s%s", pTss->m_szBuf2K));
                break;
            }
            if (m_pApp->getConfig().getCommon().m_bMaster)
            {
                if (m_pApp->getConfig().getMaster().m_async_bin.getUser().length())
                {
                    if ( (m_pApp->getConfig().getMaster().m_async_bin.getUser() != user) ||
                        (m_pApp->getConfig().getMaster().m_async_bin.getPasswd() != passwd))
                    {
                        iRet = CWX_MQ_FAIL_AUTH;
                        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                        CWX_DEBUG((pTss->m_szBuf2K));
                        break;
                    }
                }
            }
            else
            {
                if (m_pApp->getConfig().getSlave().m_async_bin.getUser().length())
                {
                    if ( (m_pApp->getConfig().getSlave().m_async_bin.getUser() != user) ||
                        (m_pApp->getConfig().getSlave().m_async_bin.getPasswd() != passwd))
                    {
                        iRet = CWX_MQ_FAIL_AUTH;
                        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                        CWX_DEBUG((pTss->m_szBuf2K));
                        break;
                    }
                }
            }
            string strSubcribe=subscribe?subscribe:"";
            string strErrMsg;
            if (!CwxMqPoco::parseSubsribe(subscribe, m_dispatch->m_subscribe, strErrMsg))
            {
                iRet = CWX_MQ_INVALID_SUBSCRIBE;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Invalid subscribe[%s], err=%s", strSubcribe.c_str(), strErrMsg.c_str());
                CWX_DEBUG((pTss->m_szBuf2K));
                break;
            }
            m_dispatch->m_bSync = true;
            m_dispatch->m_uiChunk = uiChunk;
            if (m_dispatch.m_uiChunk)
            {
                if (m_dispatch.m_uiChunk > CwxMqConfigCmn::MAX_CHUNK_SIZE_KB) m_dispatch.m_uiChunk = CwxMqConfigCmn::MAX_CHUNK_SIZE_KB;
                if (m_dispatch.m_uiChunk < CwxMqConfigCmn::MIN_CHUNK_SIZE_KB) m_dispatch.m_uiChunk = CwxMqConfigCmn::MIN_CHUNK_SIZE_KB;
                m_dispatch.m_uiChunk *= 1024;
            }
            if (bNewly)
            {///不sid为空，则取当前最大sid-1
                ullSid = m_pApp->getBinLogMgr()->getMaxSid();
                if (ullSid) ullSid--;
            }
            ///回复iRet的值
            iRet = CWX_MQ_SUCCESS;
            ///创建binlog读取的cursor
            CwxBinLogCursor* pCursor = m_pApp->getBinLogMgr()->createCurser();
            if (!pCursor)
            {
                iRet = CWX_MQ_INNER_ERR;
                strcpy(pTss->m_szBuf2K, "Failure to create cursor");
                CWX_ERROR((pTss->m_szBuf2K));
                break;
            }
            ///设置cursor
            m_dispatch->m_pCursor = pCursor;
            m_dispatch->m_ullStartSid = ullSid;
            m_dispatch->m_bSync = true;
            if (-1 == iState)
            {
                CWX_ERROR((pTss->m_szBuf2K));
                return -1; ///关闭连接
            }
            else if (0 == iState)
            {///产生continue的消息
                m_pApp->getAsyncDispChannel()->regRedoHander(this);
                m_dispatch.m_bContinue = true;
            }
            ///返回
            return 0;
        }
        else
        {
            ///若其他消息，则返回错误
            CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u", msg->event().getMsgHeader().getMsgType());
            iRet = CWX_MQ_INVALID_MSG_TYPE;
            CWX_ERROR((pTss->m_szBuf2K));
        }

    } while(0);
    

    ///形成失败时候的回复数据包
    CwxMsgBlock* pBlock = NULL;
    if (CWX_MQ_SUCCESS != CwxMqPoco::packReportDataReply(pTss,
        pBlock,
        m_header.getTaskId(),
        iRet,
        ullSid,
        pTss->m_szBuf2K,
        pTss->m_szBuf2K))
    {
        CWX_ERROR(("Failure to create binlog reply package, err:%s", pTss->m_szBuf2K));
        return -1;
    }
    ///发送回复的数据包
    pBlock->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
    pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_ASYNC_BIN);
    pBlock->send_ctrl().setHostId(0);
    pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
    if (0 != this->putMsg(pBlock))
    {
        CWX_ERROR(("Failure to send msg to reciever, conn[%u]", msg->event().getConnId()));
        CwxMsgBlockAlloc::free(pBlock);
        return -1;
        ///关闭连接
    }
    return 0;
}

///处理binlog发送完毕的消息
int CwxMqBinAsyncHandler::onEndSendMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    map<CWX_UINT32, CwxMqDispatchConn*>::iterator iter = m_dispatchConns->m_clientMap.find(msg->event().getConnId());
    ///连接必须存在
    CWX_ASSERT(iter != m_dispatchConns->m_clientMap.end());
    CwxMqDispatchConn* conn = iter->second;
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    CWX_UINT64 ullSid = msg->event().m_ullArg;
    if (!conn->m_window.isSyncing()){ ///如果连接不是同步状态，则是错误
        strcpy(pTss->m_szBuf2K, "Client no in sync state");
        CWX_ERROR((pTss->m_szBuf2K));
        //关闭连接
        m_pApp->noticeCloseConn(msg->event().getConnId());
        return 1;
    }
    if(!conn->m_window.recvOneMsg(ullSid))
    {
        char buf[64];
        CWX_ERROR(("Async msg can't find in window, sid[%s", CwxCommon::toString(ullSid, buf, 10)));
        CWX_ASSERT(0);
    }
    conn->m_recvWindow.insert(ullSid);
    ///发送下一条binlog
    int iState = sendBinLog(m_pApp, conn, pTss);
    if (-1 == iState)
    {
        CWX_ERROR(("Failure to send bin log, err:%s", pTss->m_szBuf2K));
    }
    else if (0 == iState)
    {///产生continue的消息
        noticeContinue(pTss, conn);
    }
    ///返回
    return 1;
}


///处理新消息的时间
int CwxMqBinAsyncHandler::onUserEvent(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    CwxMqDispatchConn* conn = NULL;
    map<CWX_UINT32, CwxMqDispatchConn*>::iterator iter = m_dispatchConns->m_clientMap.find(msg->event().getConnId());
    if (iter != m_dispatchConns->m_clientMap.end())
    {
        conn = iter->second;
        conn->m_bContinue = false;
        int iState = sendBinLog(m_pApp, conn, pTss);
        if (-1 == iState)
        {
            CWX_ERROR(("Failure to send bin log, , err:%s", pTss->m_szBuf2K));
        }
        else if (0 == iState)
        {///产生continue的消息
            noticeContinue(pTss, conn);
        }
    }

    return 1;
}


void CwxMqBinAsyncHandler::dispatch(CwxMqTss* pTss)
{
    int iState = 0;
    CwxMqDispatchConn* conn=(CwxMqDispatchConn *)m_dispatchConns->m_connTail.head();
    //首先分发完成的消息
    while(conn)
    {
        iState = sendBinLog(m_pApp, conn, pTss);
        if (-1 == iState)
        {
            CWX_ERROR(("Failure to dispatch message, conn[%u]", conn->m_window.getConnId()));
            //关闭连接
            m_pApp->noticeCloseConn(conn->m_window.getConnId());
        }
        else if (0 == iState) //未完成
        {///产生continue的消息
            noticeContinue(pTss, conn);
        }
        conn = conn->m_next;
    }
}

///0：未完成状态；
///1：完成状态；
///-1：失败；
int CwxMqBinAsyncHandler::sendBinLog(CwxMqApp* pApp,
                                  CwxMqDispatchConn* conn,
                                  CwxMqTss* pTss)
{
    int iRet = 0;
    CWX_UINT32 uiDataLen;
    char* pBuf = NULL;
    CWX_ASSERT(conn->m_window.isSyncing());
    CWX_ASSERT(conn->m_window.getHandle());
    //if (conn->m_window.getUsedSize()) return 1; ///<还有消息在发送队列
    if (!conn->m_window.isEnableSend()) return 1; ///<若发送窗口已满，则不发送，为完成状态
    if (conn->m_recvWindow.size() > pApp->getConfig().getCommon().m_uiDispatchWindowSize) return 1;
    CwxBinLogCursor* pCursor = (CwxBinLogCursor*)conn->m_window.getHandle();
    CwxMsgBlock* pBlock = NULL;
    CwxKeyValueItem const* pItem = NULL;
    if (pApp->getBinLogMgr()->isUnseek(pCursor))
    {//若binlog的读取cursor悬空，则定位
        if (conn->m_window.getStartSid() < pApp->getBinLogMgr()->getMaxSid())
        {
            iRet = pApp->getBinLogMgr()->seek(pCursor, conn->m_window.getStartSid());
            if (-1 == iRet)
            {
                CWX_ERROR(("Failure to seek,  err:%s", pCursor->getErrMsg()));
                return -1;
            }
            else if (0 == iRet)
            {
                char szBuf1[64];
                char szBuf2[64];
                CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Should seek to sid[%s] with max_sid[[%s], but not.",
                    CwxCommon::toString(conn->m_window.getStartSid(), szBuf1),
                    CwxCommon::toString(pApp->getBinLogMgr()->getMaxSid(), szBuf2));
                CWX_ERROR((pTss->m_szBuf2K));
                return -1;
            }
            ///若成功定位，则读取当前记录
            conn->m_bNext = conn->m_window.getStartSid()==pCursor->getHeader().getSid()?true:false;
        }
        else
        {///若需要同步发送的sid不小于当前最小的sid，则依旧为悬空状态
            return 1;///完成状态
        }
    }
    ///如果是发送下一条binlog，则移动cursor
    if (conn->m_bNext)
    {
        iRet = pApp->getBinLogMgr()->next(pCursor);
        if (0 == iRet)
        {
            return 1; ///完成状态
        }
        if (-1 == iRet)
        {///<失败
            CWX_ERROR(("Failure to seek cursor, err:%s", pCursor->getErrMsg()));
            return -1;
        }
    }
    conn->m_bNext = true;
    CWX_UINT32 uiSkipNum = 0;
    while(1)
    {
        while (!CwxMqPoco::isSubscribe(conn->m_subscribe,
            conn->m_bSync,
            pCursor->getHeader().getGroup(),
            pCursor->getHeader().getType()))
        {
            iRet = pApp->getBinLogMgr()->next(pCursor);
            if (0 == iRet) return 1; ///完成状态
            if (-1 == iRet)
            {///<失败
                CWX_ERROR(("Failure to seek cursor, err:%s", pCursor->getErrMsg()));
                return -1;
            }
            uiSkipNum ++;
            if (!CwxMqPoco::isContinueSeek(uiSkipNum)) return 0; ///未完成状态
            continue;
        }
        ///获取binlog的data长度
        uiDataLen = pCursor->getHeader().getLogLen();
        ///准备data读取的buf
        pBuf = pTss->getBuf(uiDataLen);        
        ///读取data
        iRet = pApp->getBinLogMgr()->fetch(pCursor, pBuf, uiDataLen);
        if (-1 == iRet)
        {//读取失败
            CWX_ERROR(("Failure to fetch data, err:%s", pCursor->getErrMsg()));
            return -1;
        }
        ///unpack data的数据包
        if (pTss->m_pReader->unpack(pBuf, uiDataLen, false, true))
        {
            ///获取CWX_MQ_DATA的key，此为真正data数据
            pItem = pTss->m_pReader->getKey(CWX_MQ_DATA);
            if (pItem)
            {
                ///形成binlog发送的数据包
                if (CWX_MQ_SUCCESS != CwxMqPoco::packSyncData(pTss,
                    pBlock,
                    0,
                    pCursor->getHeader().getSid(),
                    pCursor->getHeader().getDatetime(),
                    *pItem,
                    pCursor->getHeader().getGroup(),
                    pCursor->getHeader().getType(),
                    pCursor->getHeader().getAttr(),
                    pTss->m_szBuf2K))
                {
                    ///形成数据包失败
                    CWX_ERROR(("Failure to pack binlog package, err:%s", pTss->m_szBuf2K));
                }
                else
                {
                    ///根据svr类型，发送数据包
                    pBlock->send_ctrl().setConnId(conn->m_window.getConnId());
                    pBlock->send_ctrl().setSvrId(conn->m_window.getSvrId());
                    pBlock->send_ctrl().setHostId(0);
                    pBlock->event().m_ullArg = pCursor->getHeader().getSid();
                    pBlock->event().setTaskId(pCursor->getHeader().getSid()&0xFFFFFFFF);
                    pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::FINISH_NOTICE);
                    if (0 != pApp->sendMsgByConn(pBlock))
                    {
                        CWX_ERROR(("Failure to send binlog"));
                        CwxMsgBlockAlloc::free(pBlock);
                        return -1;
                    }
                    ///通知窗口发送了一个binlog
                    conn->m_window.sendOneMsg(pCursor->getHeader().getSid());
                    return 1; ///发送了一条消息
                }
            }
            else
            {///读取的数据无效                
                CWX_ERROR(("Can't find key[%s] in binlog, sid=%s", CWX_MQ_DATA,
                    CwxCommon::toString(pCursor->getHeader().getSid(), pTss->m_szBuf2K)));
            }            
        }
        else
        {///binlog的数据格式错误，不是kv
            CWX_ERROR(("Can't unpack binlog, sid=%s", CwxCommon::toString(pCursor->getHeader().getSid(), pTss->m_szBuf2K)));
        }
        uiSkipNum ++;
        if (!CwxMqPoco::isContinueSeek(uiSkipNum)) return 0;
/*        if (conn->m_window.isEnableSend())
        {///若发送窗口没有满,则准备发送下一个
            iRet = pApp->getBinLogMgr()->next(pCursor);
            if (0 == iRet) return 1; ///完成状态
            if (-1 == iRet)
            {///<失败
                CWX_ERROR(("Failure to seek cursor, err:%s", pCursor->getErrMsg()));
                return -1;
            }
        }
        else
        {///<若窗口已满，则退出发送
            return 0; ///未完成状态
        }*/
    }
    ///永远也不会到达
    return 0; ///未完成状态
}

void CwxMqBinAsyncHandler::noticeContinue(CwxMqTss* , CwxMqDispatchConn* conn)
{
    if (!conn->m_bContinue)
    {
        conn->m_bContinue = true;
        CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
        msg->event().setSvrId(CwxMqApp::SVR_TYPE_ASYNC);
        msg->event().setEvent(CwxMqApp::MQ_CONTINUE_SEND_EVENT);
        msg->event().setConnId(conn->m_window.getConnId());
        m_pApp->getWriteThreadPool()->append(msg);
    }
}
