#include "CwxMqQueueMgr.h"

CwxMqQueue::CwxMqQueue(string strName,
                       string strUser,
                       string strPasswd,
                       CwxBinLogMgr* pBinlog)
{
  m_strName = strName;
  m_strUser = strUser;
  m_strPasswd = strPasswd;
  m_binLog = pBinlog;
  m_cursor = NULL;
}

CwxMqQueue::~CwxMqQueue() {
  if (m_cursor)
    m_binLog->destoryCurser(m_cursor);
  if (m_memMsgMap.size()) {
    map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_memMsgMap.begin();
    while (iter != m_memMsgMap.end()) {
      CwxMsgBlockAlloc::free(iter->second);
      iter++;
    }
    m_memMsgMap.clear();
  }
  map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_uncommitMap.begin();
  while (iter != m_uncommitMap.end()) {
    CwxMsgBlockAlloc::free((CwxMsgBlock*) iter->second);
    iter++;
  }
  m_uncommitMap.clear();
}

int CwxMqQueue::init(CWX_UINT64 ullSid, string& strErrMsg) {
  if (m_memMsgMap.size()) {
    map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_memMsgMap.begin();
    while (iter != m_memMsgMap.end()) {
      CwxMsgBlockAlloc::free(iter->second);
      iter++;
    }
    m_memMsgMap.clear();
  }
  if (m_cursor)
    m_binLog->destoryCurser(m_cursor);
  m_cursor = NULL;

  map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_uncommitMap.begin();
  while (iter != m_uncommitMap.end()) {
    CwxMsgBlockAlloc::free((CwxMsgBlock*) iter->second);
    iter++;
  }
  m_uncommitMap.clear();
  //创建cursor
  {
    m_cursor = m_binLog->createCurser(ullSid);
    if (!m_cursor) {
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
  msg = NULL;
  if (m_memMsgMap.size()) {
    msg = m_memMsgMap.begin()->second;
    m_memMsgMap.erase(m_memMsgMap.begin());
  } else {
    iRet = fetchNextBinlog(pTss, msg, err_num, szErr2K);
    if (1 != iRet)
      return iRet;
  }
  m_uncommitMap[msg->event().m_ullArg] = msg;
  return 1;
}

///消息发送完毕，bSend=true表示已经发送成功；false表示发送失败
void CwxMqQueue::endSendMsg(CWX_UINT64 ullSid, bool bSend) {
  map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_uncommitMap.find(ullSid);
  CWX_ASSERT(iter != m_uncommitMap.end());
  CwxMsgBlock* msg = (CwxMsgBlock*) iter->second;
  if (bSend) { ///发送成功
    CwxMsgBlockAlloc::free(msg);
  } else { ///发送失败，需要重新消费消息
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
  if (m_cursor->isUnseek()) { //若binlog的读取cursor悬空，则定位
      iRet = m_binLog->seek(m_cursor, m_cursor->getSeekSid());
      if (-1 == iRet) {
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Failure to seek,  err:%s",
          m_cursor->getErrMsg());
        return -1;
      } else if (0 == iRet) {
        return 0;
      }
  }
  do {
    int iRet = m_binLog->next(m_cursor);
    if (0 == iRet)
      return 0; ///<到了尾部
    if (-1 == iRet) { ///<失败
      strcpy(szErr2K, m_cursor->getErrMsg());
      err_num = CWX_MQ_ERR_ERROR;
      return -1;
    }
    //fetch data
    ///获取binlog的data长度
    CWX_UINT32 uiDataLen = m_cursor->getHeader().getLogLen();
    ///准备data读取的buf
    char* pBuf = pTss->getBuf(uiDataLen);
    ///读取data
    iRet = m_binLog->fetch(m_cursor, pBuf, uiDataLen);
    if (-1 == iRet) {        //读取失败
      strcpy(szErr2K, m_cursor->getErrMsg());
      err_num = CWX_MQ_ERR_ERROR;
      return -1;
    }
    ///unpack data的数据包
    if (pTss->m_pReader->unpack(pBuf, uiDataLen, false, true)) {
      ///获取CWX_MQ_D的key，此为真正data数据
      CwxKeyValueItem const* pItem = pTss->m_pReader->getKey(CWX_MQ_D);
      if (pItem) {
        ///形成binlog发送的数据包
        if (CWX_MQ_ERR_SUCCESS
          != CwxMqPoco::packFetchMqReply(pTss->m_pWriter, msg,
          CWX_MQ_ERR_SUCCESS, "", *pItem, pTss->m_szBuf2K)) {
            ///形成数据包失败
            err_num = CWX_MQ_ERR_ERROR;
            return -1;
        } else {
          msg->event().m_ullArg = m_cursor->getHeader().getSid();
          err_num = CWX_MQ_ERR_SUCCESS;
          return 1;
        }
      } else {        ///读取的数据无效
        char szBuf[64];
        CWX_ERROR(
          ("Can't find key[%s] in binlog, sid=%s", CWX_MQ_D, CwxCommon::toString(m_cursor->getHeader().getSid(), szBuf)));
      }
    } else {        ///binlog的数据格式错误，不是kv
      char szBuf[64];
      CWX_ERROR(
        ("Can't unpack binlog, sid=%s", CwxCommon::toString(m_cursor->getHeader().getSid(), szBuf)));
    }
  } while (1);
  return 0;
}

CWX_UINT64 CwxMqQueue::getMqNum() {
  return m_binLog->leftLogNum(m_cursor) + m_memMsgMap.size();
}

CwxMqQueueMgr::CwxMqQueueMgr(string const& strQueuePath,
                             CWX_UINT32 uiFlushNum,
                             CWX_UINT32 uiFlushSecond)
{
  m_strQueuePath = strQueuePath;
  if (m_strQueuePath[m_strQueuePath.length() - 1] != '/')
    m_strQueuePath += "/";
  m_uiFlushNum = uiFlushNum;
  m_uiFlushSecond = uiFlushSecond;
  m_binLog = NULL;
  m_strErrMsg = "Not init";
  m_bValid = false;
}

CwxMqQueueMgr::~CwxMqQueueMgr() {
  map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::iterator iter =
    m_queues.begin();
  while (iter != m_queues.end()) {
    delete iter->second.first;
    delete iter->second.second;
    iter++;
  }
}

int CwxMqQueueMgr::init(CwxBinLogMgr* binLog) {
  m_binLog = binLog;
  m_bValid = false;
  //清空数据
  {
    map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::iterator iter =
      m_queues.begin();
    while (iter != m_queues.end()) {
      delete iter->second.first;
      delete iter->second.second;
      iter++;
    }
    m_queues.clear();
  }
  //初始化队列
  {
    pair<CwxMqQueue*, CwxSidLogFile*> mq_pair;
    CwxMqQueue* mq = NULL;
    CwxSidLogFile* mqLogFile = NULL;
    int ret = 0;
    string strQueueFile;
    string strQueuePathFile;
    set < string/*queue name*/> queues;
    set<string/*queue name*/>::iterator iter;
    if (!_fetchLogFile(queues))
      return -1;
    iter = queues.begin();
    while (iter != queues.end()) {
      strQueuePathFile = m_strQueuePath + _getQueueLogFile(*iter, strQueueFile);
      mqLogFile = new CwxSidLogFile(m_uiFlushNum, m_uiFlushSecond,
        strQueuePathFile);
      ret = mqLogFile->load();
      if (-1 == ret) {
        char szBuf[2048];
        CwxCommon::snprintf(szBuf, 2047,
          "Failure to init mq queue log-file:%s, err:%s",
          strQueuePathFile.c_str(), mqLogFile->getErrMsg());
        m_strErrMsg = szBuf;
        m_bValid = false;
        delete mqLogFile;
        return -1;
      } else if (0 == ret) {        //空队列文件，删除
        delete mqLogFile;
        CwxSidLogFile::removeFile(strQueuePathFile);
        iter++;
        continue;
      }
      if (*iter != mqLogFile->getName()) {
        char szBuf[2048];
        CwxCommon::snprintf(szBuf, 2047,
          "queue log file[%s]'s queue name should be [%s], but it's [%s]",
          strQueuePathFile.c_str(), iter->c_str(),
          mqLogFile->getName().c_str());
        m_strErrMsg = szBuf;
        m_bValid = false;
        delete mqLogFile;
        return -1;
      }
      mq = new CwxMqQueue(mqLogFile->getName(), mqLogFile->getUserName(),
        mqLogFile->getPasswd(), m_binLog);
      if (0 != mq->init(mqLogFile->getCurMaxSid(), m_strErrMsg)) {
        delete mqLogFile;
        delete mq;
        m_bValid = false;
        return -1;
      }
      mq_pair.first = mq;
      mq_pair.second = mqLogFile;
      m_queues[mqLogFile->getName()] = mq_pair;
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
  if (m_bValid) {
    CwxReadLockGuard<CwxRwLock> lock(&m_lock);
    map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::iterator iter =
      m_queues.find(strQueue);
    if (iter == m_queues.end())
      return -2;
    if (!iter->second.second->isValid()) {
      if (szErr2K)
        strcpy(szErr2K, iter->second.second->getErrMsg());
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
  if (m_bValid) {
    CwxReadLockGuard<CwxRwLock> lock(&m_lock);
    map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::iterator iter =
      m_queues.find(strQueue);
    if (iter == m_queues.end()) return -2;
    if (!iter->second.second->isValid()) {
      if (szErr2K)
        strcpy(szErr2K, iter->second.second->getErrMsg());
      return -1;
    }
    iter->second.first->endSendMsg(ullSid, bSend);
    if (-1 == iter->second.second->log(ullSid)){
      if (szErr2K) strcpy(szErr2K, iter->second.second->getErrMsg());
      return -1;
    }
    return 0;
  }
  if (szErr2K) strcpy(szErr2K, m_strErrMsg.c_str());
  return -1;
}

// 时间commit检查；
void CwxMqQueueMgr::timeout(CWX_UINT32 uiNow) {
  if (m_bValid) {
    map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::iterator iter =
      m_queues.begin();
    while (iter != m_queues.end()) {
      iter->second.second->timeout(uiNow);
      iter++;
    }
  }
}

///强行flush mq的log文件
void CwxMqQueueMgr::commit() {
  if (m_bValid) {
    map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::iterator iter =
      m_queues.begin();
    while (iter != m_queues.end()) {
      iter->second.second->syncFile();
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
                            char* szErr2K)
{
  if (m_bValid)
  {
    CwxWriteLockGuard<CwxRwLock> lock(&m_lock);
    map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::iterator iter =
      m_queues.find(strQueue);
    if (iter != m_queues.end())
      return 0;
    CwxMqQueue* mq = new CwxMqQueue(strQueue, strUser, strPasswd, m_binLog);
    string strErr;
    if (0 != mq->init(ullSid, strErr)) {
      delete mq;
      if (szErr2K)
        strcpy(szErr2K, strErr.c_str());
      return -1;
    }
    //create mq log file
    string strQueueFile;
    string strQueuePathFile;
    strQueuePathFile = m_strQueuePath
      + _getQueueLogFile(strQueue, strQueueFile);
    CwxSidLogFile::removeFile(strQueuePathFile);
    CwxSidLogFile* mqLogFile = new CwxSidLogFile(m_uiFlushNum, m_uiFlushSecond,
      strQueuePathFile);
    if (0 != mqLogFile->create(strQueue, ullSid, strUser, strPasswd)) {
      char szBuf[2048];
      CwxCommon::snprintf(szBuf, 2047,
        "Failure to create queue log-file:%s, err:%s",
        strQueuePathFile.c_str(), mqLogFile->getErrMsg());
      delete mqLogFile;
      delete mq;
      if (szErr2K)
        strcpy(szErr2K, szBuf);
      return -1;
    }
    pair<CwxMqQueue*, CwxSidLogFile*> item;
    item.first = mq;
    item.second = mqLogFile;
    m_queues[strQueue] = item;
    return 1;
  }
  if (szErr2K)
    strcpy(szErr2K, m_strErrMsg.c_str());
  return -1;
}

///1：成功
///0：不存在
///-1：其他错误
int CwxMqQueueMgr::delQueue(string const& strQueue, char* szErr2K) {
  if (m_bValid) {
    CwxWriteLockGuard<CwxRwLock> lock(&m_lock);
    map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::iterator iter =
      m_queues.find(strQueue);
    if (iter == m_queues.end())
      return 0;
    delete iter->second.first;
    delete iter->second.second;
    m_queues.erase(iter);
    string strQueueFile;
    string strQueuePathFile;
    _getQueueLogFile(strQueue, strQueueFile);
    strQueuePathFile = m_strQueuePath + strQueueFile;
    CwxSidLogFile::removeFile(strQueuePathFile);
    return 1;
  }
  if (szErr2K)
    strcpy(szErr2K, m_strErrMsg.c_str());
  return -1;
}

void CwxMqQueueMgr::getQueuesInfo(list<CwxMqQueueInfo>& queues) {
  CwxReadLockGuard<CwxRwLock> lock(&m_lock);
  CwxMqQueueInfo info;
  map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::const_iterator iter =
    m_queues.begin();
  while (iter != m_queues.end()) {
    info.m_strName = iter->second.first->getName();
    info.m_strUser = iter->second.first->getUserName();
    info.m_ullCursorSid = iter->second.first->getCurSid();
    info.m_ullLeftNum = iter->second.first->getMqNum();
    queues.push_back(info);
    iter++;
  }
}

bool CwxMqQueueMgr::_fetchLogFile(set<string/*queue name*/> & queues) {
  //如果binlog的目录不存在，则创建此目录
  if (!CwxFile::isDir(m_strQueuePath.c_str())) {
    if (!CwxFile::createDir(m_strQueuePath.c_str())) {
      char szBuf[2048];
      CwxCommon::snprintf(szBuf, 2047,
        "Failure to create mq log path:%s, errno=%d", m_strQueuePath.c_str(),
        errno);
      m_strErrMsg = szBuf;
      m_bValid = false;
      return false;
    }
  }
  //获取目录下的所有文件
  list < string > pathfiles;
  if (!CwxFile::getDirFile(m_strQueuePath, pathfiles)) {
    char szBuf[2048];
    CwxCommon::snprintf(szBuf, 2047,
      "Failure to fetch mq log, path:%s, errno=%d", m_strQueuePath.c_str(),
      errno);
    m_strErrMsg = szBuf;
    m_bValid = false;
    return false;
  }
  //提取目录下的所有binlog文件，并放到map中，利用map的排序，逆序打开文件
  string strQueue;
  list<string>::iterator iter = pathfiles.begin();
  queues.clear();
  while (iter != pathfiles.end()) {
    if (_isQueueLogFile(*iter, strQueue)) {
      queues.insert(strQueue);
    }
    iter++;
  }
  return true;
}

bool CwxMqQueueMgr::_isQueueLogFile(string const& file, string& queue) {
  string strFile;
  list < string > items;
  list<string>::iterator iter;
  CwxCommon::split(file, items, '.');
  if ((3 != items.size()) && (4 != items.size()))
    return false;
  iter = items.begin();
  if (*iter != "queue")
    return false;
  iter++;
  queue = *iter;
  if (!isInvalidQueueName(queue.c_str()))
    return false;
  iter++;
  if (*iter != "log")
    return false;
  return true;
}

string& CwxMqQueueMgr::_getQueueLogFile(string const& queue, string& strFile) {
  strFile = "queue.";
  strFile += queue;
  strFile += ".log";
  return strFile;
}
