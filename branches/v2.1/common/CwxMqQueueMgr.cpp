#include "CwxMqQueueMgr.h"

CwxMqQueue::CwxMqQueue(string strName,
                       string strUser,
                       string strPasswd,
                       bool    bCommit,
                       string strSubscribe,
                       CWX_UINT32 uiDefTimeout,
                       CWX_UINT32 uiMaxTimeout,
                       CwxBinLogMgr* pBinlog)
{
    m_strName = strName;
    m_strUser = strUser;
    m_strPasswd = strPasswd;
    m_bCommit = bCommit;
    m_uiDefTimeout = uiDefTimeout;
    m_uiMaxTimeout = uiMaxTimeout;
    m_strSubScribe = strSubscribe;
    m_binLog = pBinlog;
    m_pUncommitMsg =NULL;
    m_cursor = NULL;
}

CwxMqQueue::~CwxMqQueue()
{
    if (m_cursor) m_binLog->destoryCurser(m_cursor);
    if (m_memMsgMap.size())
    {
        map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_memMsgMap.begin();
        while(iter != m_memMsgMap.end())
        {
            CwxMsgBlockAlloc::free(iter->second);
            iter++;
        }
        m_memMsgMap.clear();
    }
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

int CwxMqQueue::init(CWX_UINT64 ullLastCommitSid,
                     set<CWX_UINT64> const& uncommitSid,
                     set<CWX_UINT64> const& commitSid,
                     string& strErrMsg)
{
    if (m_memMsgMap.size())
    {
        map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_memMsgMap.begin();
        while(iter != m_memMsgMap.end())
        {
            CwxMsgBlockAlloc::free(iter->second);
            iter++;
        }
        m_memMsgMap.clear();
    }

    if (m_cursor) m_binLog->destoryCurser(m_cursor);
    m_cursor = NULL;


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

    m_ullLastCommitSid = ullLastCommitSid; ///<日志文件记录的cursor的sid
    m_lastUncommitSid = uncommitSid; ///<m_ullLastCommitSid之前未commit的binlog
    m_lastCommitSid = commitSid;

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

    if (m_memMsgMap.size())
    {
        msg = m_memMsgMap.begin()->second;
        m_memMsgMap.erase(m_memMsgMap.begin())
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
        item->send(false);
        m_uncommitMap[item->sid()] = item;
        m_pUncommitMsg->push(item);
    }
    else
    {
        m_uncommitMap[msg->event().m_ullArg] = msg;
    }
    return 1;
}

///用于commit类型的队列，提交commit消息。
///返回值：0：不存在，1：成功.
int CwxMqQueue::commitBinlog(CWX_UINT64 ullSid, bool bCommit)
{
    if (!m_bCommit) return 0;
    map<CWX_UINT64, void*>::iterator iter=m_uncommitMap.find(ullSid);
    if (iter == m_uncommitMap.end()) return 0;
    ///由于是单线程环境，此时，消息一定没有超时
    CwxMqQueueHeapItem* item = (CwxMqQueueHeapItem*)iter->second;
    CWX_ASSERT(-1 != item->index());
    //从堆中删除元素
    m_pUncommitMsg->erase(item);
    //从uncommit map中删除元素
    m_uncommitMap->erase(iter);
    if (!bCommit)
    {
        m_memMsgMap[item->sid()] = item->msg();
        item->msg(NULL);
    }
    //删除元素自身，同时
    delete item;
    return 1;
}

///消息发送完毕，bSend=true表示已经发送成功；false表示发送失败
///返回值：0：不存在，1：成功.
int CwxMqQueue::endSendMsg(CWX_UINT64 ullSid, bool bSend)
{
    map<CWX_UINT64, void*>::iterator iter=m_uncommitMap.find(ullSid);
    if (iter == m_uncommitMap.end()) return 0;
    if (m_bCommit)
    {
        CwxMqQueueHeapItem* item = (CwxMqQueueHeapItem*)iter->second;
        if (-1 == item->index())
        {///消息已经超时
            ///从未commit map中删除消息
            m_uncommitMap.erase(iter);
            ///将消息放到内存消息map
            m_memMsgMap[ullSid] = item->msg();
            item->msg(NULL);
            ///删除item
            delete item;
        }
        else if (!bSend)
        {///消息发送失败
            ///从heap中删除消息
            m_pUncommitMsg->erase(item);///<从未
            ///从未commit map中删除消息
            m_uncommitMap.erase(iter);
            ///将消息放到内存消息map
            m_memMsgMap[ullSid] = item->msg();
            item->msg(NULL);
            ///删除item
            delete item;
        }
        else
        {///消息发送成功而且没有超时
            item->send(true);
        }
    }
    else
    {
        CwxMsgBlock* msg = (CwxMsgBlock*)iter->second;
        if (bSend)
        {
            CwxMsgBlockAlloc::free(msg);
        }
        else
        {
            m_memMsgMap[ullSid] = msg;
        }
        m_uncommitMap.erase(iter);
    }
    return 1;
}

///检测commit类型队列超时的消息
void CwxMqQueue::checkTimeout(CWX_UINT32 ttTimestamp)
{
    if (m_bCommit)
    {
        CwxMqQueueHeapItem * item = NULL;
        while(m_pUncommitMsg->count())
        {
            if (m_pUncommitMsg->top()->timestamp() > ttTimestamp) break;
            ///消息超时
            item = m_pUncommitMsg->pop();
            if (item->send())
            {///消息已经发送完毕
                ///从未提交map中删除消息
                m_uncommitMap.erase(item->sid());
                ///将消息放到未发送的内存map中
                m_memMsgMap[item->sid()] = item->msg();
                item->msg(NULL);
                delete item;
            }
            else
            {///消息还没有发送完毕，此时将index置为-1，表示已经超时
                item->index(-1)
            }
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
        CWX_UINT64 ullStartSid = getStartSid();
        if (ullStartSid < m_binLog->getMaxSid())
        {
            m_cursor = m_binLog->createCurser();
            if (!m_cursor)
            {
                err_num = CWX_MQ_INNER_ERR;
                strcpy(szErr2K, "Failure to create cursor");
                return -1;
            }
            iRet = m_binLog->seek(m_cursor, ullStartSid);
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
            if (ullStartSid ==m_cursor->getHeader().getSid())
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
    CWX_UINT32 uiSkipNum = 0;
    bool bFetch = false;
    do 
    {
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
        bFetch = false;
        if (m_cursor->getHeader().getSid() <= m_ullLastCommitSid)
        {//只取m_lastUncommitSid中的数据
            if (m_lastUncommitSid.size() && 
                (m_lastUncommitSid.find(m_cursor->getHeader().getSid()) != m_lastUncommitSid.end()))
            {
                bFetch = true;
                m_lastUncommitSid.erase(m_lastUncommitSid.find(m_cursor->getHeader().getSid()));
            }
        }
        else
        {//不取m_lastCommitSid中已经commit的数据
            if (!m_lastCommitSid.size()||
                (m_lastCommitSid.find(m_cursor->getHeader().getSid()) == m_lastCommitSid.end()))
            {
                bFetch = true;
            }
            else
            {
                m_lastCommitSid.erase(m_lastCommitSid.find(m_cursor->getHeader().getSid()));
            }
        }

        if (bFetch)
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
    CWX_UINT64 ullStartSid = getStartSid();
    if (!m_cursor)
    {
        if (ullStartSid < m_binLog->getMaxSid())
        {
            m_cursor = m_binLog->createCurser();
            if (!m_cursor)
            {
                return 0;
            }
            int iRet = m_binLog->seek(m_cursor, ullStartSid);
            if (1 != iRet)
            {
                m_binLog->destoryCurser(m_cursor);
                m_cursor = NULL;
                return 0;
            }
        }
        return 0;
    }
    return m_binLog->leftLogNum(m_cursor) + m_memMsgMap.size();
}

void CwxMqQueue::getQueueDumpInfo(CWX_UINT64& ullLastCommitSid,
                      set<CWX_UINT64>& uncommitSid,
                      set<CWX_UINT64>& commitSid)
{
    if (m_cursor && (CwxBinLogMgr::CURSOR_STATE_READY == m_cursor->getSeekState()))
    {///cursor有效，此时，m_lastUncommitSid中小于cursor sid的记录应该删除
        ///原因是：1、已有记录已经失效；2、才内存或uncommit中记录。
        set<CWX_UINT64>::iterator iter = m_lastUncommitSid.begin();
        while(iter != m_lastUncommitSid.end())
        {
            if (*iter >= m_cursor->getHeader().getSid()) break;
            m_lastUncommitSid.erase(iter);
            iter = m_lastUncommitSid.begin();
        }
        ///m_lastCommitSid中，小于cursor sid的记录应该删除
        ///因为其记录的是cursor sid后的commit记录。
        iter = m_lastCommitSid.begin();
        while(iter != m_lastCommitSid.end())
        {
            if (*iter >= m_cursor->getHeader().getSid()) break;
            m_lastCommitSid.erase(iter);
            iter = m_lastCommitSid.begin();
        }
        ullLastCommitSid = m_cursor->getHeader().getSid();
    }
    else
    {
        ullLastCommitSid = m_ullLastCommitSid;
    }
    {//形成未commit的sid
        uncommitSid.clear();
        //添加m_lastUncommitSid中的记录
        uncommitSid = m_lastUncommitSid;
        //添加m_uncommitMap中的记录
        {
            map<CWX_UINT64, void*>::iterator iter = m_uncommitMap.begin();
            while(iter != m_uncommitMap.end())
            {
                uncommitSid.insert(iter->first);
                iter++;
            }
        }
        //添加m_memMsgMap中的记录
        {
            map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_memMsgMap.begin();
            while(iter != m_memMsgMap.end())
            {
                uncommitSid.insert(iter->first);
                iter++;
            }
        }
    }
    {///形成commit的记录
        commitSid.clear();
        commitSid = m_lastCommitSid;
    }
}

CwxMqQueueMgr::CwxMqQueueMgr(string const& strQueueLogFile,
                             CWX_UINT32 uiMaxFsyncNum)
{
    m_strQueueLogFile = strQueueLogFile;
    m_uiMaxFsyncNum = uiMaxFsyncNum;
    m_mqLogFile = NULL;
    m_binLog = NULL;
    m_strErrMsg = "Not init".
}

CwxMqQueueMgr::~CwxMqQueueMgr()
{
    map<string, CwxMqQueue*>::iterator iter =  m_queues.begin();
    while(iter != m_nameQueues.end())
    {
        delete iter->second;
        iter++;
    }
    if (m_mqLogFile) delete m_mqLogFile;
}

int CwxMqQueueMgr::init(CwxBinLogMgr* binLog)
{
    bool bSuccess = false;
    CwxMqQueue* mq = NULL;
    if (m_mqLogFile) delete m_mqLogFile;
    m_mqLogFile = new CwxMqQueueLogFile(m_uiMaxFsyncNum, m_strQueueLogFile);
    map<string, CwxMqQueueInfo> queues;
    map<string, set<CWX_UINT64>*> uncommitSets;
    map<string, set<CWX_UINT64>*> commitSets;
    
    if (0 != m_mqLogFile->init(queues, uncommitSets, commitSets))
    {
        char szBuf[2048];
        CwxCommon::snprintf(szBuf, 2047, "Failure to init mq queue log-file, err:%s", m_mqLogFile->getErrMsg());
        m_strErrMsg = szBuf;
        delete m_mqLogFile;
        m_mqLogFile = NULL;
        return -1;
    }
    set<CWX_UINT64> empty;
    map<string, CwxMqQueueInfo>::iterator iter_queue = queues.begin();
    set<CWX_UINT64>* pUncommitSet = NULL;
    set<CWX_UINT64>* pCommitSet = NULL;
    do 
    {
        while(iter_queue != queues.end())
        {
            mq = new CwxMqQueue(iter_queue->second.m_strName, 
                iter_queue->second.m_strUser,
                iter_queue->second.m_strPasswd,
                iter_queue->second.m_bCommit,
                iter_queue->second.m_strSubScribe,
                iter_queue->second.m_uiDefTimeout,
                iter_queue->second.m_uiMaxTimeout,
                m_binLog);
            if (uncommitSets.find(iter_queue->second.m_strName) != uncommitSets.end())
            {
                pUncommitSet = uncommitSets[iter_queue->second.m_strName];
            }
            else
            {
                pUncommitSet = &empty;
            }
            if (commitSets.find(iter_queue->second.m_strName) != commitSets.end())
            {
                pCommitSet = commitSets[iter_queue->second.m_strName];
            }
            else
            {
                pCommitSet = &empty;
            }
            if (0 != mq->init(iter_queue->second.m_ullCursorSid, *pUncommitSet, *pCommitSet, m_strErrMsg))
            {
                break;
            }
            iter_queue ++;
        }
    } while(0);
    map<string, set<CWX_UINT64>*>::iterator iter = uncommitSets.begin();
    while(iter != uncommitSets.end())
    {
        delete iter->second;
        iter++;
    }
    iter = commitSets.begin();
    while(iter != commitSets.end())
    {
        delete iter->second;
        iter++;
    }
    if (iter_queue != queues.end())
    {
        delete m_mqLogFile;
        m_mqLogFile = NULL;
    }
    return iter_queue == queues.end()?0:-1;
}


///0：没有消息；
///1：获取一个消息；
///2：达到了搜索点，但没有发现消息；
///-1：失败；
///-2：队列不存在
int CwxMqQueueMgr::getNextBinlog(CwxMqTss* pTss,
                  string const& strQueue,
                  CwxMsgBlock*&msg,
                  CWX_UINT32 uiTimeout,
                  int& err_num,
                  bool& bCommitType, ///<是否为commit类型的队列
                  char* szErr2K)
{
    if (m_mqLogFile)
    {
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, CwxMqQueue*>::iterator iter = m_queues.find(strQueue);
        if (iter == m_queues.end()) return -2;
        bCommitType = iter->second->isCommit();
        return iter->second->getNextBinlog(pTss, msg, uiTimeout, err_num, szErr2K);
    }
    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
    return -1;
}

///用于commit类型的队列，提交commit消息。
///返回值：0：不存在，1：成功，-1：失败；-2：队列不存在
int CwxMqQueueMgr::commitBinlog(string const& strQueue,
                 CWX_UINT64 ullSid,
                 bool bCommit,
                 char* szErr2K)
{
    if (m_mqLogFile)
    {
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, CwxMqQueue*>::iterator iter = m_queues.find(strQueue);
        if (iter == m_queues.end()) return -2;
        int ret = iter->second->commitBinlog(ullSid, bCommit);
        if (0 == ret) return 0;
        if (1 == ret)
        {
            int num = m_mqLogFile->log(iter->second->getName().c_str(), ullSid);
            if (-1 == num)
            {
                m_strErrMsg = m_mqLogFile->getErrMsg();
                delete m_mqLogFile;
                m_mqLogFile = NULL;
                if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
                return -1;
            }
            if (num >= MQ_SWITCH_LOG_NUM)
            {
                if (!_save())
                {
                    delete m_mqLogFile;
                    m_mqLogFile = NULL;
                    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
                    return -1;
                }
            }
            return 1;
        }
    }
    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
    return -1;
}

///消息发送完毕，bSend=true表示已经发送成功；false表示发送失败
///返回值：0：不存在，1：成功，-1：失败，-2：队列不存在
int CwxMqQueueMgr::endSendMsg(string const& strQueue,
               CWX_UINT64 ullSid,
               bool bSend,
               char* szErr2K)
{
    if (m_mqLogFile)
    {
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, CwxMqQueue*>::iterator iter = m_queues.find(strQueue);
        if (iter == m_queues.end()) return -2;
        int ret = iter->second->endSendMsg(ullSid, bSend);
        if (0 == ret) return 0;
        if (1 == ret) 
        {
            if (!iter->second->isCommit())
            {
                int num = m_mqLogFile->log(iter->second->getName().c_str(), ullSid);
                if (-1 == num)
                {
                    m_strErrMsg = m_mqLogFile->getErrMsg();
                    delete m_mqLogFile;
                    m_mqLogFile = NULL;
                    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
                    return -1;
                }
                if (num >= MQ_SWITCH_LOG_NUM)
                {
                    if (!_save())
                    {
                        delete m_mqLogFile;
                        m_mqLogFile = NULL;
                        if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
                        return -1;
                    }
                }
            }
            return 1;
        }
    }
    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
    return -1;
}


///强行flush mq的log文件
void CwxMqQueueMgr::commit()
{
    if (m_mqLogFile) m_mqLogFile->fsync();
}

///检测commit类型队列超时的消息
void CwxMqQueueMgr::checkTimeout(CWX_UINT32 ttTimestamp)
{
    CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
    map<string, CwxMqQueue*>::iterator iter = m_queues.begin();
    while(iter != m_queues.end())
    {
        if (iter->second->isCommit()) iter->second->checkTimeout(ttTimestamp);
        iter++;
    }
}

///1：成功
///0：存在
///-1：其他错误
int CwxMqQueueMgr::addQueue(string const& strQueue,
                            CWX_UINT64 ullSid,
                            bool bCommit,
                            string const& strUser,
                            string const& strPasswd,
                            string const& strScribe,
                            CWX_UINT32 uiDefTimeout,
                            CWX_UINT32 uiMaxTimeout,
                            char* szErr2K)
{
    if (m_mqLogFile)
    {
        CwxWriteLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, CwxMqQueue*>::iterator iter = m_queues.find(strQueue);
        if (iter != m_queues.end()) return 0;
        set<CWX_UINT64> empty;
        CwxMqQueue* mq = new CwxMqQueue(strQueue, 
            strUser,
            strPasswd,
            bCommit,
            strScribe,
            uiDefTimeout,
            uiMaxTimeout,
            m_binLog);
        string strErr;
        if (0 != mq->init(ullSid, empty, empty, strErr))
        {
            delete mq;
            return -1;
        }
        m_queues[strQueue] = mq;
        if (!_save())
        {
            delete m_mqLogFile;
            m_mqLogFile = NULL;
            if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
            return -1;
        }
        return 1;
    }
    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
    return -1;
}
///1：成功
///0：不存在
///-1：其他错误
int CwxMqQueueMgr::delQueue(string const& strQueue,
             char* szErr2K)
{
    if (m_mqLogFile)
    {
        CwxWriteLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, CwxMqQueue*>::iterator iter = m_queues.find(strQueue);
        if (iter == m_queues.end()) return 0;
        delete iter->second;
        m_queues->erase(iter);
        if (!_save())
        {
            delete m_mqLogFile;
            m_mqLogFile = NULL;
            if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
            return -1;
        }
        return 1;
    }
    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
    return -1;
}

void CwxMqQueueMgr::getQueuesInfo(list<CwxMqQueueInfo>& queues) const
{
    CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
    CwxMqQueueInfo info;
    map<string, CwxMqQueue*>::const_iterator iter = m_queues.begin();
    while(iter != m_queues.end())
    {
        info.m_strName = iter->second->getName();
        info.m_strUser = iter->second->getUserName();
        info.m_bCommit = iter->second->isCommit();
        info.m_uiDefTimeout = iter->second->getDefTimeout();
        info.m_uiMaxTimeout = iter->second->getMaxTimeout();
        info.m_strSubScribe = iter->second->getSubscribeRule();
        info.m_ullCursorSid = iter->second->getCursorSid();
        info.m_ullLeftNum = iter->second->getMqNum();
        info.m_uiWaitCommitNum = iter->second->getWaitCommitNum();
        info.m_uiMemLogNum = iter->second->getMemMsgMap().size();
        if (iter->second->getCursor())
        {
            info.m_ucQueueState = iter->second->getCursor()->getSeekState();
            if (CwxBinLogMgr::CURSOR_STATE_ERROR == info.m_ucQueueState)
            {
                info.m_strQueueErrMsg = iter->second->getCursor()->getErrMsg();
            }
            else
            {
                info.m_strQueueErrMsg = "";
            }
        }
        else
        {
            info.m_ucQueueState = CwxBinLogMgr::CURSOR_STATE_UNSEEK;
            info.m_strQueueErrMsg = "";
        }
        queues.push_back(info);
        iter++;
    }
}

bool CwxMqQueueMgr::_save()
{
    if (m_mqLogFile)
    {
        map<string, CwxMqQueueInfo> queues;
        map<string, set<CWX_UINT64>*> uncommitSets;
        map<string, set<CWX_UINT64>*> commitSets;
        CwxMqQueueInfo queueInfo;
        set<CWX_UINT64>* sidUncommitSet=NULL;
        set<CWX_UINT64>* sidCommitSet =NULL;
        map<string, CwxMqQueue*>::iterator iter = m_queues.begin();
        while(iter != m_queues.end())
        {
            queueInfo.m_strName = iter->second->getName();
            queueInfo.m_strUser = iter->second->getUserName();
            queueInfo.m_strPasswd = iter->second->getPasswd();
            queueInfo.m_bCommit = iter->second->isCommit();
            queueInfo.m_uiDefTimeout = iter->second->getDefTimeout();
            queueInfo.m_uiMaxTimeout = iter->second->getMaxTimeout();
            queueInfo.m_strSubScribe = iter->second->getSubscribeRule();
            queueInfo.m_ullCursorSid = iter->second->getCursorSid();
            queueInfo[queueInfo.m_strName] = queueInfo;

            if (!sidUncommitSet) sidUncommitSet = new set<CWX_UINT64>;
            if (!sidCommitSet) sidCommitSet = new set<CWX_UINT64>;

            iter->second->getQueueDumpInfo(queueInfo.m_ullCursorSid, *sidUncommitSet, *sidCommitSet);
            if (sidUncommitSet->size())
            {
                uncommitSets[queueInfo.m_strName] = sidUncommitSet;
                sidUncommitSet = NULL;
            }
            if (sidCommitSet->size())
            {
                commitSets[queueInfo->m_strName] = sidCommitSet;
                sidCommitSet = NULL;
            }
            iter++;
        }
        if (sidUncommitSet) delete  sidUncommitSet;
        if (sidCommitSet) delete sidCommitSet;

        //保存到log中
        if (0 != m_mqLogFile->save(queues, uncommitSets, commitSets))
        {
            m_strErrMsg = m_mqLogFile->getErrMsg();
            return false;
        }
        //清空map
        map<string, set<CWX_UINT64>*>::iterator iter_sid = uncommitSets.begin();
        while(iter_sid != uncommitSets.end())
        {
            delete iter_sid->second;
            iter_sid ++;
        }
        iter_sid = commitSets.begin();
        while(iter_sid != commitSets.end())
        {
            delete iter_sid->second;
            iter_sid ++;
        }
    }
    return false;
}
