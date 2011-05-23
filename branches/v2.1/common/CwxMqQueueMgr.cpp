#include "CwxMqQueueMgr.h"

CwxMqQueue::CwxMqQueue(string strName,
                       string strUser,
                       string strPasswd,
                       CWX_UINT64 ullStartSid,
                       bool    bCommit,
                       string strSubscribe,
                       CWX_UINT32 uiDefTimeout,
                       CWX_UINT32 uiMaxTimeout,
                       CwxBinLogMgr* pBinlog)
{
    m_strName = strName;
    m_strUser = strUser;
    m_strPasswd = strPasswd;
    m_ullStartSid = ullStartSid;
    m_bCommit = bCommit;
    m_uiDefTimeout = uiDefTimeout;
    m_uiMaxTimeout = uiMaxTimeout;
    m_strSubScribe = strSubscribe;
    m_binLog = pBinlog;
    m_pUncommitMsg =NULL;
    m_cursor = NULL;
    m_memMsgTail = NULL;
}

CwxMqQueue::~CwxMqQueue()
{
    if (m_cursor) m_binLog->destoryCurser(m_cursor);
    if (m_memMsgTail->count())
    {
        CwxMsgBlock* msg = NULL;
        while((msg=m_memMsgTail->pop_head()))
        {
            CwxMsgBlockAlloc::free(msg);
        }
    }
    delete m_memMsgTail;
    if (m_pUncommitMsg)
    {
        CwxMqQueueHeapItem* item=NULL;
        while((item = m_pUncommitMsg->pop()))
        {
            delete item;
        }
        delete m_pUncommitMsg;
    }
    if (!m_bCommit)
    {
        map<CWX_UINT64, void*>::iterator iter = m_uncommitMap.begin();
        while(iter != m_uncommitMap.end())
        {
            CwxMsgBlockAlloc::free((CwxMsgBlock*)iter->second);
            iter++;
        }
    }
    m_uncommitMap.clear();
}

int CwxMqQueue::init(string& strErrMsg)
{
    if (m_memMsgTail)
    {
        if (m_memMsgTail->count())
        {
            CwxMsgBlock* msg = NULL;
            while((msg=m_memMsgTail->pop_head()))
            {
                CwxMsgBlockAlloc::free(msg);
            }
        }
        delete m_memMsgTail;
    }
    m_memMsgTail = new CwxSTail<CwxMsgBlock>;

    if (m_cursor) m_binLog->destoryCurser(m_cursor);
    m_cursor = NULL;

    m_dispatchSid.clear();

    if (m_pUncommitMsg)
    {
        CwxMqQueueHeapItem* item=NULL;
        while((item = m_pUncommitMsg->pop()))
        {
            delete item;
        }
        delete m_pUncommitMsg;
        m_pUncommitMsg = NULL;
    }
    if (!m_bCommit)
    {
        map<CWX_UINT64, void*>::iterator iter = m_uncommitMap.begin();
        while(iter != m_uncommitMap.end())
        {
            CwxMsgBlockAlloc::free((CwxMsgBlock*)iter->second);
            iter++;
        }
    }
    m_uncommitMap.clear();

    if (m_bCommit)
    {
        m_pUncommitMsg = new CwxMinHeap<CwxMqQueueHeapItem>(2048);
    }
    if (!CwxMqPoco::parseSubsribe(m_strSubScribe, m_subscribe, strErrMsg))
    {
        return -1;
    }
    return 0;
}

///0：没有消息；
///1：获取一个消息；
///2：达到了搜索点，但没有发现消息；
///-1：失败；
int CwxMqQueue::getNextBinlog(CwxMqTss* pTss,
                              CwxMsgBlock*&msg,
                              CWX_UINT32 uiTimeout,
                              int& err_num,
                              char* szErr2K)
{
    int iRet = 0;
    msg =  NULL;
    if (m_bCommit)
    {
        if (uiTimeout = 0) uiTimeout = m_uiDefTimeout;
        if (uiTimeout > m_uiMaxTimeout) uiTimeout = m_uiMaxTimeout;
    }

    if (m_memMsgTail && m_memMsgTail->count())
    {
        msg = m_memMsgTail->pop_head();
    }
    else
    {
        iRet = fetchNextBinlog(pTss, msg, err_num, szErr2K);
        if (1 != iRet) return iRet;
    }
    
    if (m_bCommit)
    {
        CWX_UINT32 uiTimestamp = time(NULL);
        uiTimestamp += uiTimeout;
        CwxMqQueueHeapItem item = new CwxMqQueueHeapItem();
        item->msg(msg);
        item->timestamp(uiTimestamp);
        item->sid(msg->event().m_ullArg);
        m_uncommitMap[item->sid()] = item;
        m_pUncommitMsg->push(item);
    }
    else
    {
        m_uncommitMap[msg->event().m_ullArg] = msg;
    }
    return 1;
}

///ullSid的消息已经完成发送。
///对于非commit类型的队列，此表示消息已经写到socket buf。
///对于commit类型的队列，此表示已经收到对方的commit确认。
///返回值：0：不存在，1：成功.
int CwxMqQueue::commitBinlog(CWX_UINT64 ullSid, bool bCommit=true)
{
    map<CWX_UINT64, void*>::iterator iter=m_uncommitMap.find(ullSid);
    if (iter == m_uncommitMap.end()) return 0;
    if (m_bCommit)
    {
        CwxMqQueueHeapItem* item = (CwxMqQueueHeapItem*)iter->second;
        m_pUncommitMsg->erase(item);
        if (bCommit)
        {
            delete item;
        }
        else
        {
            m_memMsgTail->push_head(item->msg());
            item->msg(NULL);
            delete item;
        }
    }
    else
    {
        CwxMsgBlock* msg = (CwxMsgBlock*)iter->second;
        if (bCommit)
        {
            CwxMsgBlockAlloc::free(msg);
        }
        else
        {
            m_memMsgTail->pop_head(msg);
        }
    }
    m_uncommitMap.erase(iter);
    return 1;
}

///检测commit类型队列超时的消息
void CwxMqQueue::checkTimeout()
{
    if (m_bCommit)
    {
        CWX_UINT32 uiTime = time(NULL);
        CwxMqQueueHeapItem * item = NULL;
        while(m_pUncommitMsg->count())
        {
            if (m_pUncommitMsg->top()->timestamp() > uiTime) break;
            item = m_pUncommitMsg->pop();
            m_uncommitMap.erase(m_uncommitMap.find(item->sid()));
            m_memMsgTail->push_head(item->msg());
            item->msg(NULL);
            delete item;
        }
    }
}

///0：没有消息；
///1：获取一个消息；
///2：达到了搜索点，但没有发现消息；
///-1：失败；
int CwxMqQueue::fetchNextBinlog(CwxMqTss* pTss,
                    CwxMsgBlock*&msg,
                    int& err_num,
                    char* szErr2K)
{
    int iRet = 0;

    if (!m_cursor)
    {
        if (m_ullStartSid < m_binLog->getMaxSid())
        {
            m_cursor = m_binLog->createCurser();
            if (!m_cursor)
            {
                err_num = CWX_MQ_INNER_ERR;
                strcpy(szErr2K, "Failure to create cursor");
                return -1;
            }
            iRet = m_binLog->seek(m_cursor, m_ullStartSid);
            if (1 != iRet)
            {
                if (-1 == iRet)
                {
                    strcpy(szErr2K, m_cursor->getErrMsg());
                }
                else
                {
                    strcpy(szErr2K, "Binlog's seek should return 1 but zero");
                }
                m_binLog->destoryCurser(m_cursor);
                m_cursor = NULL;
                err_num = CWX_MQ_INNER_ERR;
                return -1;
            }
            if (m_ullStartSid ==m_cursor->getHeader().getSid())
            {
                iRet = m_binLog->next(m_cursor);
                if (0 == iRet) return 0; ///<到了尾部
                if (-1 == iRet)
                {///<失败
                    strcpy(szErr2K, m_cursor->getErrMsg());
                    err_num = CWX_MQ_INNER_ERR;
                    return -1;
                }
            }
        }
        else
        {
            return 0;
        }
    }
    else
    {
        iRet = m_binLog->next(m_cursor);
        if (0 == iRet) return 0; ///<到了尾部
        if (-1 == iRet)
        {///<失败
            strcpy(szErr2K, m_cursor->getErrMsg());
            err_num = CWX_MQ_INNER_ERR;
            return -1;
        }
    }
    do 
    {
        CWX_UINT32 uiSkipNum = 0;
        while(!CwxMqPoco::isSubscribe(m_subscribe,
            false,
            m_cursor->getHeader().getGroup(),
            m_cursor->getHeader().getType()))
        {
            iRet = m_binLog->next(m_cursor);
            if (0 == iRet) return 0; ///<到了尾部
            if (-1 == iRet)
            {///<失败
                strcpy(szErr2K, m_cursor->getErrMsg());
                err_num = CWX_MQ_INNER_ERR;
                return -1;
            }
            uiSkipNum ++;
            if (!CwxMqPoco::isContinueSeek(uiSkipNum)) return 2;
            continue;
        }
        if (!m_dispatchSid.size() ||
            (m_dispatchSid.find(m_cursor->getHeader()->getSid()) == m_dispatchSid.end()))
        {
            //fetch data
            ///获取binlog的data长度
            CWX_UINT32 uiDataLen = m_cursor->getHeader().getLogLen();
            ///准备data读取的buf
            char* pBuf = pTss->getBuf(uiDataLen);        
            ///读取data
            iRet = m_binLog->fetch(m_cursor, pBuf, uiDataLen);
            if (-1 == iRet)
            {//读取失败
                strcpy(szErr2K, m_cursor->getErrMsg());
                err_num = CWX_MQ_INNER_ERR;
                return -1;
            }
            ///unpack data的数据包
            if (pTss->m_pReader->unpack(pBuf, uiDataLen, false, true))
            {
                ///获取CWX_MQ_DATA的key，此为真正data数据
                CwxKeyValueItem const* pItem = pTss->m_pReader->getKey(CWX_MQ_DATA);
                if (pItem)
                {
                    ///形成binlog发送的数据包
                    if (CWX_MQ_SUCCESS != CwxMqPoco::packFetchMqReply(pTss,
                        msg,
                        CWX_MQ_SUCCESS,
                        "",
                        m_cursor->getHeader().getSid(),
                        m_cursor->getHeader().getDatetime(),
                        *pItem,
                        m_cursor->getHeader().getGroup(),
                        m_cursor->getHeader().getType(),
                        m_cursor->getHeader().getAttr(),
                        pTss->m_szBuf2K))
                    {
                        ///形成数据包失败
                        err_num = CWX_MQ_INNER_ERR;
                        return -1;
                    }
                    else
                    {
                        msg->event().m_ullArg = m_cursor->getHeader().getSid();
                        err_num = CWX_MQ_SUCCESS;
                        return 1;
                    }
                }
                else
                {///读取的数据无效
                    char szBuf[64];
                    CWX_ERROR(("Can't find key[%s] in binlog, sid=%s", CWX_MQ_DATA,
                        CwxCommon::toString(m_cursor->getHeader().getSid(), szBuf)));
                }            
            }
            else
            {///binlog的数据格式错误，不是kv
                char szBuf[64];
                CWX_ERROR(("Can't unpack binlog, sid=%s",
                    CwxCommon::toString(m_cursor->getHeader().getSid(), szBuf)));
            }
        }
        else
        {
            m_dispatchSid.erase(m_cursor->getHeader()->getSid());
        }
        uiSkipNum ++;
        if (!CwxMqPoco::isContinueSeek(uiSkipNum)) return 2;
        iRet = m_binLog->next(m_cursor);
        if (0 == iRet) return 0; ///<到了尾部
        if (-1 == iRet)
        {///<失败
            strcpy(szErr2K, m_cursor->getErrMsg());
            err_num = CWX_MQ_INNER_ERR;
            return -1;
        }
    }while(1);
    return 0;
}

CWX_UINT64 CwxMqQueue::getMqNum()
{
    if (!m_cursor)
    {
        if (m_ullStartSid < m_binLog->getMaxSid())
        {
            m_cursor = m_binLog->createCurser();
            if (!m_cursor)
            {
                return 0;
            }
            int iRet = m_binLog->seek(m_cursor, m_ullStartSid);
            if (1 != iRet)
            {
                m_binLog->destoryCurser(m_cursor);
                m_cursor = NULL;
                return 0;
            }
        }
        return 0;
    }
    return m_binLog->leftLogNum(m_cursor) + m_memMsgTail->count();
}


CwxMqQueueMgr::CwxMqQueueMgr()
{
}

CwxMqQueueMgr::~CwxMqQueueMgr()
{
    map<string, CwxMqQueue*>::iterator iter =  m_nameQueues.begin();
    while(iter != m_nameQueues.end())
    {
        delete iter->second;
        iter++;
    }
    m_nameQueues.clear();
    m_idQueues.clear();
}

int CwxMqQueueMgr::init(CwxBinLogMgr* binLog,
                        map<string, CWX_UINT64> const& queueSid,
                        map<string, CwxMqConfigQueue> const& queueInfo)
{
    CWX_UINT32 uiId = QUEUE_ID_START;
    CwxMqQueue* mq = NULL;
    map<string, CWX_UINT64>::const_iterator iter=queueSid.begin();
    map<string, CwxMqConfigQueue>::const_iterator iter_info ;
    string errMsg;
    while(iter != queueSid.end())
    {
        iter_info = queueInfo.find(iter->first);
        CWX_ASSERT(iter_info != queueInfo.end());
        mq = new CwxMqQueue(uiId,
            iter->first,
            iter_info->second.m_strUser,
            iter_info->second.m_strPasswd,
            iter->second,
            binLog);
        if (!CwxMqPoco::parseSubsribe(iter_info->second.m_strSubScribe, mq->getSubscribe(), errMsg))
        {
            delete mq;
            return -1;
        }
        m_nameQueues[iter->first] = mq;
        m_idQueues[uiId++] = mq;
        iter++;
    }
    return 0;
}
