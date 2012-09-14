#include "CwxMqQueueMgr.h"

CwxMqQueue::CwxMqQueue(string strName,
                       string strUser,
                       string strPasswd,
                       string strSubscribe,
                       CwxBinLogMgr* pBinlog)
{
    m_strName = strName;
    m_strUser = strUser;
    m_strPasswd = strPasswd;
    m_strSubScribe = strSubscribe;
    m_binLog = pBinlog;
    m_cursor = NULL;
}

CwxMqQueue::~CwxMqQueue(){
    if (m_cursor) m_binLog->destoryCurser(m_cursor);
    if (m_memMsgMap.size()){
        map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_memMsgMap.begin();
        while(iter != m_memMsgMap.end()){
            CwxMsgBlockAlloc::free(iter->second);
            iter++;
        }
        m_memMsgMap.clear();
    }
    map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_uncommitMap.begin();
    while(iter != m_uncommitMap.end())
    {
        CwxMsgBlockAlloc::free((CwxMsgBlock*)iter->second);
        iter++;
    }
    m_uncommitMap.clear();
}

int CwxMqQueue::init(CWX_UINT64 ullLastCommitSid,
                     set<CWX_UINT64> const& uncommitSid,
                     set<CWX_UINT64> const& commitSid,
                     string& strErrMsg)
{
    if (m_memMsgMap.size()){
        map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_memMsgMap.begin();
        while(iter != m_memMsgMap.end()){
            CwxMsgBlockAlloc::free(iter->second);
            iter++;
        }
        m_memMsgMap.clear();
    }
    if (m_cursor) m_binLog->destoryCurser(m_cursor);
    m_cursor = NULL;

    map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_uncommitMap.begin();
    while(iter != m_uncommitMap.end()){
        CwxMsgBlockAlloc::free((CwxMsgBlock*)iter->second);
        iter++;
    }
    m_uncommitMap.clear();

    if (!CwxMqPoco::parseSubsribe(m_strSubScribe, m_subscribe, strErrMsg)){
        return -1;
    }

    m_ullLastCommitSid = ullLastCommitSid; ///<日志文件记录的cursor的sid
    m_lastUncommitSid = uncommitSid; ///<m_ullLastCommitSid之前未commit的binlog
    m_lastCommitSid = commitSid;
	//创建cursor
	{
		m_cursor = m_binLog->createCurser(getStartSid());
		if (!m_cursor){
			strErrMsg = "Failure to create cursor.";
			return -1;
		}
	}
    return 0;
}

///0：没有消息；
///1：获取一个消息；
///2：达到了搜索点，但没有发现消息；
///-1：失败；
int CwxMqQueue::getNextBinlog(CwxMqTss* pTss,
                              CwxMsgBlock*&msg,
                              int& err_num,
                              char* szErr2K)
{
    int iRet = 0;
    msg =  NULL;
    if (m_memMsgMap.size()){
        msg = m_memMsgMap.begin()->second;
        m_memMsgMap.erase(m_memMsgMap.begin());
    }else{
        iRet = fetchNextBinlog(pTss, msg, err_num, szErr2K);
        if (1 != iRet) return iRet;
    }
    m_uncommitMap[msg->event().m_ullArg] = msg;
    return 1;
}

///消息发送完毕，bSend=true表示已经发送成功；false表示发送失败
void CwxMqQueue::endSendMsg(CWX_UINT64 ullSid, bool bSend){
    map<CWX_UINT64, CwxMsgBlock*>::iterator iter=m_uncommitMap.find(ullSid);
    CWX_ASSERT(iter != m_uncommitMap.end());
    CwxMsgBlock* msg = (CwxMsgBlock*)iter->second;
    if (bSend){///发送成功
        CwxMsgBlockAlloc::free(msg);
    }else{///发送失败，需要重新消费消息
        m_memMsgMap[ullSid] = msg;
    }
    ///从未commit的map中删除
    m_uncommitMap.erase(iter);
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

	if (!m_cursor || 
        (CwxBinLogCursor::CURSOR_STATE_READY != m_cursor->getSeekState()))
    {
        CWX_UINT64 ullStartSid = getStartSid();
        if (ullStartSid < m_binLog->getMaxSid()){
			if (!m_cursor){
				m_cursor = m_binLog->createCurser();
				if (!m_cursor){
					err_num = CWX_MQ_ERR_ERROR;
					strcpy(szErr2K, "Failure to create cursor");
					return -1;
				}
			}
            iRet = m_binLog->seek(m_cursor, ullStartSid);
            if (1 != iRet){
                if (-1 == iRet){
                    strcpy(szErr2K, m_cursor->getErrMsg());
                }else{
                    strcpy(szErr2K, "Binlog's seek should return 1 but zero");
                }
                m_binLog->destoryCurser(m_cursor);
                m_cursor = NULL;
                err_num = CWX_MQ_ERR_ERROR;
                return -1;
            }
            if (ullStartSid ==m_cursor->getHeader().getSid()){
                iRet = m_binLog->next(m_cursor);
                if (0 == iRet) return 0; ///<到了尾部
                if (-1 == iRet){///<失败
                    strcpy(szErr2K, m_cursor->getErrMsg());
                    err_num = CWX_MQ_ERR_ERROR;
                    return -1;
                }
            }
        }else{
            return 0;
        }
    }else{
        iRet = m_binLog->next(m_cursor);
        if (0 == iRet) return 0; ///<到了尾部
        if (-1 == iRet){///<失败
            strcpy(szErr2K, m_cursor->getErrMsg());
            err_num = CWX_MQ_ERR_ERROR;
            return -1;
        }
    }
    CWX_UINT32 uiSkipNum = 0;
    bool bFetch = false;
    do {
        while(!CwxMqPoco::isSubscribe(m_subscribe,
            m_cursor->getHeader().getGroup()))
        {
            iRet = m_binLog->next(m_cursor);
            if (0 == iRet) return 0; ///<到了尾部
            if (-1 == iRet){///<失败
                strcpy(szErr2K, m_cursor->getErrMsg());
                err_num = CWX_MQ_ERR_ERROR;
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
        }else{//不取m_lastCommitSid中已经commit的数据
            if (!m_lastCommitSid.size()||
                (m_lastCommitSid.find(m_cursor->getHeader().getSid()) == m_lastCommitSid.end()))
            {
                bFetch = true;
            }else{
                m_lastCommitSid.erase(m_lastCommitSid.find(m_cursor->getHeader().getSid()));
            }
        }
        if (bFetch){
            //fetch data
            ///获取binlog的data长度
            CWX_UINT32 uiDataLen = m_cursor->getHeader().getLogLen();
            ///准备data读取的buf
            char* pBuf = pTss->getBuf(uiDataLen);        
            ///读取data
            iRet = m_binLog->fetch(m_cursor, pBuf, uiDataLen);
            if (-1 == iRet){//读取失败
                strcpy(szErr2K, m_cursor->getErrMsg());
                err_num = CWX_MQ_ERR_ERROR;
                return -1;
            }
            ///unpack data的数据包
            if (pTss->m_pReader->unpack(pBuf, uiDataLen, false, true)){
                ///获取CWX_MQ_D的key，此为真正data数据
                CwxKeyValueItem const* pItem = pTss->m_pReader->getKey(CWX_MQ_D);
                if (pItem){
                    ///形成binlog发送的数据包
                    if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packFetchMqReply(pTss->m_pWriter,
                        msg,
                        CWX_MQ_ERR_SUCCESS,
                        "",
                        m_cursor->getHeader().getSid(),
                        m_cursor->getHeader().getDatetime(),
                        *pItem,
                        m_cursor->getHeader().getGroup(),
                        pTss->m_szBuf2K))
                    {
                        ///形成数据包失败
                        err_num = CWX_MQ_ERR_ERROR;
                        return -1;
                    }else{
                        msg->event().m_ullArg = m_cursor->getHeader().getSid();
                        err_num = CWX_MQ_ERR_SUCCESS;
                        return 1;
                    }
                }else{///读取的数据无效
                    char szBuf[64];
                    CWX_ERROR(("Can't find key[%s] in binlog, sid=%s", CWX_MQ_D,
                        CwxCommon::toString(m_cursor->getHeader().getSid(), szBuf)));
                }            
            }else{///binlog的数据格式错误，不是kv
                char szBuf[64];
                CWX_ERROR(("Can't unpack binlog, sid=%s",
                    CwxCommon::toString(m_cursor->getHeader().getSid(), szBuf)));
            }
        }
        uiSkipNum ++;
        if (!CwxMqPoco::isContinueSeek(uiSkipNum)) return 2;
        iRet = m_binLog->next(m_cursor);
        if (0 == iRet) return 0; ///<到了尾部
        if (-1 == iRet){///<失败
            strcpy(szErr2K, m_cursor->getErrMsg());
            err_num = CWX_MQ_ERR_ERROR;
            return -1;
        }
    }while(1);
    return 0;
}

CWX_UINT64 CwxMqQueue::getMqNum(){
    if (!m_cursor || (CwxBinLogCursor::CURSOR_STATE_READY != m_cursor->getSeekState()))
    {
        CWX_UINT64 ullStartSid = getStartSid();
        if (ullStartSid < m_binLog->getMaxSid()){
			if (!m_cursor){
				m_cursor = m_binLog->createCurser();
				if (!m_cursor){
					return 0;
				}
			}
            int iRet = m_binLog->seek(m_cursor, ullStartSid);
            if (1 != iRet){
                m_binLog->destoryCurser(m_cursor);
                m_cursor = NULL;
                return 0;
            }
        }else{
			return 0;
		}
    }
    return m_binLog->leftLogNum(m_cursor) + m_memMsgMap.size();
}

void CwxMqQueue::getQueueDumpInfo(CWX_UINT64& ullLastCommitSid,
                      set<CWX_UINT64>& uncommitSid,
                      set<CWX_UINT64>& commitSid)
{
    if (m_cursor && (CwxBinLogCursor::CURSOR_STATE_READY == m_cursor->getSeekState()))
    {///cursor有效，此时，m_lastUncommitSid中小于cursor sid的记录应该删除
        ///原因是：1、已有记录已经失效；2、才内存或uncommit中记录。
        set<CWX_UINT64>::iterator iter = m_lastUncommitSid.begin();
        while(iter != m_lastUncommitSid.end()){
            if (*iter >= m_cursor->getHeader().getSid()) break;
            m_lastUncommitSid.erase(iter);
            iter = m_lastUncommitSid.begin();
        }
        ///m_lastCommitSid中，小于cursor sid的记录应该删除
        ///因为其记录的是cursor sid后的commit记录。
        iter = m_lastCommitSid.begin();
        while(iter != m_lastCommitSid.end()){
            if (*iter >= m_cursor->getHeader().getSid()) break;
            m_lastCommitSid.erase(iter);
            iter = m_lastCommitSid.begin();
        }
        ullLastCommitSid = m_cursor->getHeader().getSid();
        ///如果当前cursor没有移到m_ullLastCommitSid的位置，
        ///依然采用m_ullLastCommitSid为cursor的位置。
        if (ullLastCommitSid < m_ullLastCommitSid) ullLastCommitSid = m_ullLastCommitSid;
    }else{
        ullLastCommitSid = m_ullLastCommitSid;
    }
    {//形成未commit的sid
        uncommitSid.clear();
        //添加m_lastUncommitSid中的记录
        uncommitSid = m_lastUncommitSid;
        //添加m_uncommitMap中的记录
        {
            map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_uncommitMap.begin();
            while(iter != m_uncommitMap.end()){
                uncommitSid.insert(iter->first);
                iter++;
            }
        }
        //添加m_memMsgMap中的记录
        {
            map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_memMsgMap.begin();
            while(iter != m_memMsgMap.end()){
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

CwxMqQueueMgr::CwxMqQueueMgr(string const& strQueueLogFilePath,
                             CWX_UINT32 uiMaxFsyncNum)
{
    m_strQueueLogFilePath = strQueueLogFilePath;
	if (m_strQueueLogFilePath[m_strQueueLogFilePath.length()-1] != '/')
		m_strQueueLogFilePath += "/";
    m_uiMaxFsyncNum = uiMaxFsyncNum;
    m_binLog = NULL;
    m_strErrMsg = "Not init";
	m_bValid = false;
}

CwxMqQueueMgr::~CwxMqQueueMgr(){
    map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >::iterator iter =  m_queues.begin();
    while(iter != m_queues.end()){
        delete iter->second.first;
		delete iter->second.second;
        iter++;
    }
}

int CwxMqQueueMgr::init(CwxBinLogMgr* binLog){
    CwxMqQueue* mq = NULL;
	CwxMqQueueLogFile* mqLogFile = NULL;
	string  strMqLogFile;
    m_binLog = binLog;
	m_bValid = false;

	//清空数据
	{
		map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >::iterator iter =  m_queues.begin();
		while(iter != m_queues.end()){
			delete iter->second.first;
			delete iter->second.second;
			iter++;
		}
		m_queues.clear();
	}

	//初始化队列
	{
		pair<CwxMqQueue*, CwxMqQueueLogFile*> mq_pair;
		CwxMqQueueInfo queue;
		string strQueueFile;
		string strQueuePathFile;
		set<CWX_UINT64> uncommitSets;
		set<CWX_UINT64> commitSets;
		set<string/*queue name*/ > queues;
		set<string/*queue name*/ >::iterator iter;
		if (!_fetchLogFile(queues)) return -1;
		iter  = queues.begin();
		while(iter != queues.end()){
			strQueuePathFile = m_strQueueLogFilePath + _getQueueLogFile(*iter, strQueueFile);
			mqLogFile = new CwxMqQueueLogFile(m_uiMaxFsyncNum, strQueuePathFile);
			queue.m_strName.erase();
			uncommitSets.clear();
			commitSets.clear();
			if (0 != mqLogFile->init(queue, uncommitSets, commitSets)){
				char szBuf[2048];
				CwxCommon::snprintf(szBuf, 2047, "Failure to init mq queue log-file:%s, err:%s",
					strQueuePathFile.c_str(),
					mqLogFile->getErrMsg());
				m_strErrMsg = szBuf;
				m_bValid = false;
				delete mqLogFile;
				return -1;
			}
			if (queue.m_strName.empty()){//空队列文件，删除
				delete mqLogFile;
				CwxMqQueueLogFile::removeFile(strQueuePathFile);
				iter++;
				continue;
			}
			if (*iter != queue.m_strName){
				char szBuf[2048];
				CwxCommon::snprintf(szBuf, 2047, "queue log file[%s]'s queue name should be [%s], but it's [%s]",
					strQueuePathFile.c_str(),
					iter->c_str(),
					queue.m_strName.c_str());
				m_strErrMsg = szBuf;
				m_bValid = false;
				delete mqLogFile;
				return -1;
			}
			mq = new CwxMqQueue(queue.m_strName, 
				queue.m_strUser,
				queue.m_strPasswd,
				queue.m_strSubScribe,
				m_binLog);
			if (0 != mq->init(queue.m_ullCursorSid,
                uncommitSets,
                commitSets,
                m_strErrMsg))
			{
				delete mqLogFile;
				delete mq;
				m_bValid = false;
				return -1;
			}
			mq_pair.first = mq;
			mq_pair.second = mqLogFile;
			m_queues[queue.m_strName] = mq_pair;
			iter++;
		}
	}
	m_bValid = true;

	return 0;
}


///0：没有消息；
///1：获取一个消息；
///2：达到了搜索点，但没有发现消息；
///-1：失败；
///-2：队列不存在
int CwxMqQueueMgr::getNextBinlog(CwxMqTss* pTss,
                  string const& strQueue,
                  CwxMsgBlock*&msg,
                  int& err_num,
                  char* szErr2K)
{
    if (m_bValid){
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >::iterator iter = m_queues.find(strQueue);
        if (iter == m_queues.end()) return -2;
		if (!iter->second.second->isValid()){
			if (szErr2K) strcpy(szErr2K, iter->second.second->getErrMsg());
			return -1;
		}
        return iter->second.first->getNextBinlog(pTss, msg, err_num, szErr2K);
    }
    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
    return -1;
}

///消息发送完毕，bSend=true表示已经发送成功；false表示发送失败
///返回值：0：成功，-1：失败，-2：队列不存在
int CwxMqQueueMgr::endSendMsg(string const& strQueue,
               CWX_UINT64 ullSid,
               bool bSend,
               char* szErr2K)
{
    if (m_bValid){
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >::iterator iter = m_queues.find(strQueue);
        if (iter == m_queues.end()) return -2;
		if (!iter->second.second->isValid()){
			if (szErr2K) strcpy(szErr2K, iter->second.second->getErrMsg());
			return -1;
		}
       iter->second.first->endSendMsg(ullSid, bSend);
       int num = iter->second.second->log(ullSid);
       if (-1 == num){
           if (szErr2K) strcpy(szErr2K, iter->second.second->getErrMsg());
           return -1;
       }
       if (num >= MQ_SWITCH_LOG_NUM){
           if (!_save(iter->second.first, iter->second.second)){
               if (szErr2K) strcpy(szErr2K, iter->second.second->getErrMsg());
               return -1;
           }
       }
       return 0;
    }
    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
    return -1;
}


///强行flush mq的log文件
void CwxMqQueueMgr::commit(){
	if (m_bValid){
		map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >::iterator iter = m_queues.begin();
		while(iter != m_queues.end()){
			iter->second.second->fsync();
			iter++;
		}
	}
}

///1：成功
///0：存在
///-1：其他错误
int CwxMqQueueMgr::addQueue(string const& strQueue,
                            CWX_UINT64 ullSid,
                            string const& strUser,
                            string const& strPasswd,
                            string const& strScribe,
                            char* szErr2K)
{
    if (m_bValid){
        CwxWriteLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >::iterator iter = m_queues.find(strQueue);
        if (iter != m_queues.end()) return 0;
        set<CWX_UINT64> empty;
        CwxMqQueue* mq = new CwxMqQueue(strQueue, 
            strUser,
            strPasswd,
            strScribe,
            m_binLog);
        string strErr;
        if (0 != mq->init(ullSid, empty, empty, strErr)){
            delete mq;
			if (szErr2K) strcpy(szErr2K, strErr.c_str());
            return -1;
        }
		//create mq log file
		string strQueueFile;
		string strQueuePathFile;
		CwxMqQueueInfo queue;
		strQueuePathFile = m_strQueueLogFilePath + _getQueueLogFile(strQueue, strQueueFile);
		CwxMqQueueLogFile::removeFile(strQueuePathFile);
		CwxMqQueueLogFile* mqLogFile = new CwxMqQueueLogFile(m_uiMaxFsyncNum, strQueuePathFile);
		queue.m_strName.erase();
		if (0 != mqLogFile->init(queue, empty, empty)){
			char szBuf[2048];
			CwxCommon::snprintf(szBuf, 2047, "Failure to init mq queue log-file:%s, err:%s",
				strQueuePathFile.c_str(),
				mqLogFile->getErrMsg());
			delete mqLogFile;
			delete mq;
			if (szErr2K) strcpy(szErr2K, szBuf);
			return -1;
		}
        if (!_save(mq, mqLogFile)){
			char szBuf[2048];
			CwxCommon::snprintf(szBuf, 2047, "Failure to save mq queue log-file:%s, err:%s",
				strQueuePathFile.c_str(),
				mqLogFile->getErrMsg());
			delete mqLogFile;
			delete mq;
			if (szErr2K) strcpy(szErr2K, szBuf);
            return -1;
        }
		pair<CwxMqQueue*, CwxMqQueueLogFile*> item;
		item.first = mq;
		item.second = mqLogFile;
		m_queues[strQueue] = item;
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
    if (m_bValid){
        CwxWriteLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >::iterator iter = m_queues.find(strQueue);
        if (iter == m_queues.end()) return 0;
        delete iter->second.first;
		delete iter->second.second;
        m_queues.erase(iter);
		string strQueueFile;
		string strQueuePathFile;
		_getQueueLogFile(strQueue, strQueueFile);
		strQueuePathFile = m_strQueueLogFilePath + strQueueFile;
		CwxMqQueueLogFile::removeFile(strQueuePathFile);
        return 1;
    }
    if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
    return -1;
}

void CwxMqQueueMgr::getQueuesInfo(list<CwxMqQueueInfo>& queues){
    CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
    CwxMqQueueInfo info;
    map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >::const_iterator iter = m_queues.begin();
    while(iter != m_queues.end()){
        info.m_strName = iter->second.first->getName();
        info.m_strUser = iter->second.first->getUserName();
        info.m_strSubScribe = iter->second.first->getSubscribeRule();
        info.m_ullCursorSid = iter->second.first->getCursorSid();
        info.m_ullLeftNum = iter->second.first->getMqNum();
        info.m_uiWaitCommitNum = iter->second.first->getWaitCommitNum();
        info.m_uiMemLogNum = iter->second.first->getMemMsgMap().size();
        if (iter->second.first->getCursor()){
            info.m_ucQueueState = iter->second.first->getCursor()->getSeekState();
            if (CwxBinLogCursor::CURSOR_STATE_ERROR == info.m_ucQueueState){
                info.m_strQueueErrMsg = iter->second.first->getCursor()->getErrMsg();
            }else{
                info.m_strQueueErrMsg = "";
            }
        }else{
            info.m_ucQueueState = CwxBinLogCursor::CURSOR_STATE_UNSEEK;
            info.m_strQueueErrMsg = "";
        }
		info.m_bQueueLogFileValid = iter->second.second->isValid();
		if (!info.m_bQueueLogFileValid){
			info.m_strQueueLogFileErrMsg = iter->second.second->getErrMsg();
		}else{
			info.m_strQueueLogFileErrMsg.erase();
		}
        queues.push_back(info);
        iter++;
    }
}

bool CwxMqQueueMgr::_save(CwxMqQueue* queue, CwxMqQueueLogFile* logFile){
	CwxMqQueueInfo queueInfo;
	set<CWX_UINT64> uncommitSets;
	set<CWX_UINT64> commitSets;
	queueInfo.m_strName = queue->getName();
	queueInfo.m_strUser = queue->getUserName();
	queueInfo.m_strPasswd = queue->getPasswd();
	queueInfo.m_strSubScribe = queue->getSubscribeRule();
	queueInfo.m_ullCursorSid = queue->getCursorSid();
	queue->getQueueDumpInfo(queueInfo.m_ullCursorSid, uncommitSets, commitSets);
	//保存到log中
	if (0 != logFile->save(queueInfo, uncommitSets, commitSets)){
		return false;
	}
	return true;
}

bool CwxMqQueueMgr::_fetchLogFile(set<string/*queue name*/> & queues){
	//如果binlog的目录不存在，则创建此目录
	if (!CwxFile::isDir(m_strQueueLogFilePath.c_str())){
		if (!CwxFile::createDir(m_strQueueLogFilePath.c_str())){
			char szBuf[2048];
			CwxCommon::snprintf(szBuf, 2047, "Failure to create mq log path:%s, errno=%d", m_strQueueLogFilePath.c_str(), errno);
			m_strErrMsg = szBuf;
			m_bValid = false;
			return false;
		}
	}
	//获取目录下的所有文件
	list<string> pathfiles;
	if (!CwxFile::getDirFile(m_strQueueLogFilePath, pathfiles)){
		char szBuf[2048];
		CwxCommon::snprintf(szBuf, 2047, "Failure to fetch mq log, path:%s, errno=%d", m_strQueueLogFilePath.c_str(), errno);
		m_strErrMsg = szBuf;
		m_bValid = false;
		return false;
	}
	//提取目录下的所有binlog文件，并放到map中，利用map的排序，逆序打开文件
	string strQueue;
	list<string>::iterator iter=pathfiles.begin();
	queues.clear();
	while(iter != pathfiles.end()){
		if (_isQueueLogFile(*iter, strQueue)){
			queues.insert(strQueue);
		}
		iter++;
	}
	return true;
}

bool CwxMqQueueMgr::_isQueueLogFile(string const& file, string& queue){
	string strFile;
	list<string> items;
	list<string>::iterator iter;
	CwxCommon::split(file, items, '.');
	if ((3 != items.size())&&(4 != items.size())) return false;
	iter = items.begin();
	if (*iter != "queue") return false;
	iter++;
	queue = *iter;
	if (!isInvalidQueueName(queue.c_str())) return false;
	iter++;
	if (*iter != "log") return false;
	return true;
}

string& CwxMqQueueMgr::_getQueueLogFile(string const& queue, string& strFile){
	strFile = "queue.";
	strFile += queue;
	strFile += ".log";
	return strFile;
}
