#include "CwxMcApp.h"
#include "CwxDate.h"
///构造函数
CwxMcApp::CwxMcApp() {
  m_queue = NULL;
  m_queueThreadPool = NULL;
  m_queueChannel = NULL;
  m_ttCurTime = 0;
  m_uiSyncHostFileModifyTime = 0;
  m_uiCurHostId = 0;
  memset(m_szBuf, 0x00, MAX_MONITOR_REPLY_SIZE);
}

///析构函数
CwxMcApp::~CwxMcApp() {
}

///初始化
int CwxMcApp::init(int argc, char** argv) {
  string strErrMsg;
  ///首先调用架构的init api
  if (CwxAppFramework::init(argc, argv) == -1) return -1;
  ///检查是否通过-f指定了配置文件，若没有，则采用默认的配置文件
  if ((NULL == this->getConfFile()) || (strlen(this->getConfFile()) == 0)) {
    this->setConfFile("mc.conf");
  }
  ///加载配置文件，若失败则退出
  if (0 != m_config.loadConfig(getConfFile())) {
    CWX_ERROR((m_config.getErrMsg()));
    return -1;
  }
  ///加载sync的host信息
  if (-1 == loadSyncHostForChange(true)){
    return -1;
  }
  ///设置运行日志的输出level
  setLogLevel(CwxLogger::LEVEL_ERROR | CwxLogger::LEVEL_INFO
    | CwxLogger::LEVEL_WARNING);
  return 0;
}

///配置运行环境信息
int CwxMcApp::initRunEnv() {
  ///设置系统的时钟间隔，最小刻度为1ms，此为0.1s。
  this->setClick(100); //0.1s
  ///设置工作目录
  this->setWorkDir(m_config.getCommon().m_strWorkDir.c_str());
  ///设置循环运行日志的数量
  this->setLogFileNum(LOG_FILE_NUM);
  ///设置每个日志文件的大小
  this->setLogFileSize(LOG_FILE_SIZE * 1024 * 1024);
  ///调用架构的initRunEnv，使以上设置的参数生效
  if (CwxAppFramework::initRunEnv() == -1) return -1;
  ///将加载的配置文件信息输出到日志文件中，以供查看检查
  m_config.outputConfig();
  m_config.outputSyncHost();
  ///block各种signal
  this->blockSignal(SIGTERM);
  this->blockSignal(SIGUSR1);
  this->blockSignal(SIGUSR2);
  this->blockSignal(SIGCHLD);
  this->blockSignal(SIGCLD);
  this->blockSignal(SIGHUP);
  this->blockSignal(SIGPIPE);
  this->blockSignal(SIGALRM);
  this->blockSignal(SIGCONT);
  this->blockSignal(SIGSTOP);
  this->blockSignal(SIGTSTP);
  this->blockSignal(SIGTTOU);

  //set version
  this->setAppVersion(CWX_MC_VERSION);
  //set last modify date
  this->setLastModifyDatetime(CWX_MC_MODIFY_DATE);
  //set compile date
  this->setLastCompileDatetime(CWX_COMPILE_DATE(_BUILD_DATE));
  ///设置启动时间
  CwxDate::getDateY4MDHMS2(time(NULL), m_strStartTime);

  /// 创建队列
  m_queue = new CwxMcQueue(m_config.getMq().m_uiCacheTimeout,
    m_config.getMq().m_uiCacheMSize);


  ///启动网络连接与监听
  if (0 != startNetwork()) return -1;
  //创建queue线程池
  m_queueChannel = new CwxAppChannel();
  m_queueThreadPool = new CwxThreadPool(1,
    &getCommander(),
    CwxMcApp::queueThreadMain,
    this);
  ///启动线程
  CwxTss** pTss = new CwxTss*[1];
  pTss[0] = new CwxMqTss();
  ((CwxMqTss*) pTss[0])->init();
  if (0 != m_queueThreadPool->start(pTss)) {
    CWX_ERROR(("Failure to start queue thread pool"));
    return -1;
  }

  //创建同步的线程池
  {
    map<string, CwxMcConfigHost>::const_iterator iter = m_config.getSyncHosts().m_hosts.begin();
    while(iter != m_config.getSyncHosts().m_hosts.end()){
      if (0 != startSync(iter->second)) return -1;
      ++iter;
    }
  }
  return 0;
}

///时钟函数
void CwxMcApp::onTime(CwxTimeValue const& current) {
  ///调用基类的onTime函数
  CwxAppFramework::onTime(current);
  m_ttCurTime = current.sec();
  ///检查超时
  static CWX_UINT32 ttTimeBase = 0; ///<时钟回跳的base时钟
  static CWX_UINT32 ttLastTime = m_ttCurTime; ///<上一次检查的时间
  bool bClockBack = isClockBack(ttTimeBase, m_ttCurTime);
  if (bClockBack || (m_ttCurTime >= ttLastTime + 1)) {
    CwxMsgBlock* pBlock = NULL;
    ttLastTime = m_ttCurTime;
    // 往queue线程池append TIMEOUT_CHECK
    if (m_queueThreadPool) {
      pBlock = CwxMsgBlockAlloc::malloc(0);
      pBlock->event().setSvrId(SVR_TYPE_QUEUE);
      pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
      //将超时检查事件，放入事件队列
      m_queueThreadPool->append(pBlock);
    }
    // 往所有的sync线程池append TIMEOUT_CHECK
    map<string, CwxMcSyncSession*>::iterator iter = m_syncs.begin();
    while (iter != m_syncs.end()) {
      pBlock = CwxMsgBlockAlloc::malloc(0);
      pBlock->event().setSvrId(SVR_TYPE_SYNC);
      pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
      iter->second->m_threadPool->append(pBlock);
      ++iter;
    }
    // 检查sync host是否改变
    checkSyncHostModify();
  }
}

///信号处理函数
void CwxMcApp::onSignal(int signum) {
  switch (signum) {
    case SIGQUIT:
      ///若监控进程通知退出，则推出
      CWX_INFO(("Recv exit signal, exit right now."));
      this->stop();
      break;
    default:
      ///其他信号，全部忽略
      CWX_INFO(("Recv signal=%d, ignore it.", signum));
      break;
  }
}

int CwxMcApp::onConnCreated(CWX_UINT32 uiSvrId,
                            CWX_UINT32 uiHostId,
                            CWX_HANDLE handle,
                            bool&)
{
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
  msg->event().setSvrId(uiSvrId);
  msg->event().setHostId(uiHostId);
  msg->event().setConnId(CWX_APP_INVALID_CONN_ID);
  msg->event().setIoHandle(handle);
  msg->event().setEvent(CwxEventInfo::CONN_CREATED);
  ///将消息放到线程池队列中，有内部的线程调用其处理handle来处理
  if (SVR_TYPE_QUEUE == uiSvrId) {
    if (m_queueThreadPool->append(msg) <= 1) m_queueChannel->notice();
  } else {
    CWX_ASSERT(SVR_TYPE_MONITOR == uiSvrId);
  }
  return 0;
}

///连接建立
int CwxMcApp::onConnCreated(CwxAppHandler4Msg& conn, bool&, bool&) {
  if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId()) { ///如果是监控的连接建立，则建立一个string的buf，用于缓存不完整的命令
    string* buf = new string();
    conn.getConnInfo().setUserData(buf);
  } else {
    CWX_ASSERT(0);
  }
  return 0;
}

///连接关闭
int CwxMcApp::onConnClosed(CwxAppHandler4Msg& conn) {
  if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId()) { ///若是监控的连接关闭，则必须释放先前所创建的string对象。
    if (conn.getConnInfo().getUserData()) {
      delete (string*) conn.getConnInfo().getUserData();
      conn.getConnInfo().setUserData(NULL);
    }
  } else {
    CWX_ASSERT(0);
  }
  return 0;
}

///收到消息的响应函数
int CwxMcApp::onRecvMsg(CwxAppHandler4Msg& conn, bool&) {
  if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId()) {
    char szBuf[1024];
    ssize_t recv_size = CwxSocket::recv(conn.getHandle(), szBuf, 1024);
    if (recv_size <= 0) { //error or signal
      if ((0 == recv_size) || ((errno != EWOULDBLOCK) && (errno != EINTR))) {
        return -1; //error
      } else { //signal or no data
        return 0;
      }
    }
    ///监控消息
    return monitorStats(szBuf, (CWX_UINT32) recv_size, conn);
  } else {
    CWX_ASSERT(0);
  }
  return -1;
}

void CwxMcApp::destroy() {
  // 释放消息获取的线程池及channel
  if (m_queueThreadPool) {
    m_queueThreadPool->stop();
    delete m_queueThreadPool;
    m_queueThreadPool = NULL;
  }
  if (m_queueChannel) {
    delete m_queueChannel;
    m_queueChannel = NULL;
  }
  // 释放数据同步的线程池及channel
  map<string, CwxMcSyncSession*>::iterator iter = m_syncs.begin();
  while (iter != m_syncs.end()) {
    stopSync(iter->first);
    iter = m_syncs.begin();
  }
  if (m_queue){
    delete m_queue;
    m_queue = NULL;
  }
  CwxAppFramework::destroy();
}

/// 停止sync。返回值，0：成功；-1：失败
int CwxMcApp::stopSync(string const& strHostName){
  map<string, CwxMcSyncSession*>::iterator iter = m_syncs.find(strHostName);
  if (iter == m_syncs.end()) return 0;
  // 将host从map中删除
  m_syncs.erase(iter);
  CwxMcSyncSession* pSession = iter->second;
  // 停止线程
  if (pSession->m_threadPool){
    pSession->m_threadPool->stop();
    delete pSession->m_threadPool;
    pSession->m_threadPool = NULL;
  }
  // 关闭channel
  if (pSession->m_channel){
    delete pSession->m_channel;
    pSession->m_channel = NULL;
  }
  // 关闭store
  if (pSession->m_store){
    pSession->m_store->flush();
    delete pSession->m_store;
    pSession->m_store = NULL;
  }
  // 删除session对象
  delete pSession;
  return 0;
}

/// 启动sync。返回值，0：成功；-1：失败
int CwxMcApp::startSync(CwxMcConfigHost const& hostInfo){
  if (m_syncs.find(hostInfo.m_host) != m_syncs.end()){
    CWX_ERROR(("Host[%s] exist.", hostInfo.m_host.c_str()));
    return -1;
  }
  set<CWX_UINT32> hostIds;
  map<string, CwxMcSyncSession*>::iterator iter = m_syncs.begin();
  while(iter != m_syncs.end()){
    hostIds.insert(iter->second->m_uiHostId);
    ++iter;
  }
  // 选取host id
  while(true){
    m_uiCurHostId++;
    if (!m_uiCurHostId) m_uiCurHostId = 1;
    if (hostIds.find(m_uiCurHostId) == hostIds.end()) break;
  }

  CwxMcSyncSession* pSession = NULL;
  pSession = new CwxMcSyncSession();
  m_syncs[hostInfo.m_host] = pSession;
  pSession->m_syncHost = hostInfo;
  pSession->m_uiHostId = m_uiCurHostId;
  pSession->m_bClosed = true;
  pSession->m_bNeedClosed = false;
  pSession->m_pApp = this;

  pSession->m_store = new CwxMcStore(m_config.getStore().m_strPath,
    hostInfo.m_host,
    m_config.getStore().m_uiLogMSize,
    m_config.getStore().m_uiSwitchSecond,
    m_config.getStore().m_uiFlushNum,
    m_config.getStore().m_uiFlushSecond,
    m_config.getStore().m_uiReserveDay,
    m_config.getStore().m_bAppendReturn,
    m_config.getStore().m_strRecordPrefix);

  if (0 != pSession->m_store->init()){
    CWX_ERROR(("Failure to init host[%s]'s store, err=%s",
      hostInfo.m_host.c_str(), pSession->m_store->getErrMsg()));
    return -1;
  }
  pSession->m_channel = new CwxAppChannel();
  //创建线程池
  pSession->m_threadPool = new CwxThreadPool(1,
    &getCommander(),
    CwxMcApp::syncThreadMain,
    pSession);
  ///启动线程
  CwxTss** pTss = new CwxTss*[1];
  pTss[0] = new CwxMqTss();
  ((CwxMqTss*) pTss[0])->init();
  if (0 != pSession->m_threadPool->start(pTss)) {
    CWX_ERROR(("Failure to start sync thread pool,"));
    return -1;
  }
  return 0;
}
/// 更新sync。返回值，0：成功；-1：失败
int CwxMcApp::updateSync(CwxMcConfigHost const& hostInfo){
  map<string, CwxMcSyncSession*>::iterator iter = m_syncs.find(hostInfo.m_host);
  if (iter == m_syncs.end()){
    CWX_ERROR(("Host[%s] doesn't exist.", hostInfo.m_host.c_str()));
    return -1;
  }
  CwxMcConfigHost* host = new CwxMcConfigHost(hostInfo);
  CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(sizeof(CwxMcConfigHost*));
  memcpy(pBlock->wr_ptr(), &host, sizeof(host));
  pBlock->wr_ptr(sizeof(host));
  pBlock->event().setSvrId(SVR_TYPE_SYNC);
  pBlock->event().setEvent(EVENT_TYPE_SYNC_CHANGE);
  iter->second->m_threadPool->append(pBlock);
  return 0;
}

/// 检查是否sync host发生了改变
void CwxMcApp::checkSyncHostModify(){
  int ret = loadSyncHostForChange(false);
  if (1 == ret){
    map<string, CwxMcSyncSession*>::iterator sync_iter;
    map<string, CwxMcConfigHost>::const_iterator iter = m_config.getSyncHosts().m_hosts.begin();
    while(iter != m_config.getSyncHosts().m_hosts.end()){
      sync_iter = m_syncs.find(iter->first);
      if (sync_iter == m_syncs.end()) { // 新加的host
        if(0 != startSync(iter->second)){
          CWX_ERROR(("Failure to start sync[%s], exit.", iter->first.c_str()));
          this->stop();
          return;
        }
      }else{//检查是否改变
        if ((iter->second.m_port != sync_iter->second->m_syncHost.m_port) ||
          (iter->second.m_user != sync_iter->second->m_syncHost.m_user) ||
          (iter->second.m_limit != sync_iter->second->m_syncHost.m_limit) ||
          (iter->second.m_passwd != sync_iter->second->m_syncHost.m_passwd))
        {
          if (0 != updateSync(iter->second)) {
            CWX_ERROR(("Failure to update sync[%s], exit.", iter->first.c_str()));
            this->stop();
            return;
          }
        }
      }
      ++iter;
    }
    // 检查是否有sync被取消
    sync_iter = m_syncs.begin();
    while(sync_iter != m_syncs.end()){
      if (m_config.getSyncHosts().m_hosts.find(sync_iter->first) == m_config.getSyncHosts().m_hosts.end()){
        if (0 != stopSync(sync_iter->first)){
          CWX_ERROR(("Failure to stop sync[%s], exit.", sync_iter->first.c_str()));
          this->stop();
          return;
        }
        sync_iter = m_syncs.begin();
        continue;
      }
      ++sync_iter;
    }
  }
}

/// 检查sync host文件的变化，若变化则加载。
/// 返回值，-1：失败；1：变化并加载；0：没有变化
int CwxMcApp::loadSyncHostForChange(bool bForceLoad){
  string strFile = m_config.getCommon().m_strWorkDir + "mc_sync_host.conf";
  CWX_DEBUG(("Check sync host file:%s", strFile.c_str()));
  CWX_UINT32 uiModifyTime = CwxFile::getFileMTime(strFile.c_str());
  if (0 == uiModifyTime){
    CWX_ERROR(("Failure to get sync host file:%s, errno=%d",
      strFile.c_str(), errno));
    return -1;
  }
  if ((uiModifyTime != m_uiSyncHostFileModifyTime) || bForceLoad){
    if (0 != m_config.loadSyncHost(strFile)){
      CWX_ERROR(("Failure to load sync host file:%s, errno=%s",
        strFile.c_str(), errno));
      return -1;
    }
    m_uiSyncHostFileModifyTime = uiModifyTime;
    return 1;
  }
  return 0;
}


int CwxMcApp::startNetwork() {
  ///打开监听的服务器端口号
  if (m_config.getCommon().m_monitor.getHostName().length()) {
    if (0 > this->noticeTcpListen(SVR_TYPE_MONITOR,
      m_config.getCommon().m_monitor.getHostName().c_str(),
      m_config.getCommon().m_monitor.getPort(), true)) {
        CWX_ERROR(("Can't register the monitor tcp accept listen: addr=%s, port=%d",
          m_config.getCommon().m_monitor.getHostName().c_str(),
          m_config.getCommon().m_monitor.getPort()));
        return -1;
    }
  }
  //打开mq获取的监听端口
  if (m_config.getMq().m_mq.getHostName().length()) {
    if (0 > this->noticeTcpListen(SVR_TYPE_QUEUE,
      m_config.getMq().m_mq.getHostName().c_str(),
      m_config.getMq().m_mq.getPort(),
      false,
      CWX_APP_EVENT_MODE,
      CwxMcApp::setQueueSockAttr, this)) {
        CWX_ERROR(("Can't register the queue tcp accept listen: addr=%s, port=%d",
          m_config.getMq().m_mq.getHostName().c_str(),
          m_config.getMq().m_mq.getPort()));
        return -1;
    }
  }
  return 0;
}

int CwxMcApp::monitorStats(char const* buf, CWX_UINT32 uiDataLen, CwxAppHandler4Msg& conn) {
  string* strCmd = (string*) conn.getConnInfo().getUserData();
  strCmd->append(buf, uiDataLen);
  CwxMsgBlock* msg = NULL;
  string::size_type end = 0;
  do {
    CwxCommon::trim(*strCmd);
    end = strCmd->find('\n');
    if (string::npos == end) {
      if (strCmd->length() > 10) { //无效的命令
        strCmd->erase(); ///清空接受到的命令
        ///回复信息
        msg = CwxMsgBlockAlloc::malloc(1024);
        strcpy(msg->wr_ptr(), "ERROR\r\n");
        msg->wr_ptr(strlen(msg->wr_ptr()));
      } else {
        return 0;
      }
    } else {
      if (memcmp(strCmd->c_str(), "stats", 5) == 0) {
        strCmd->erase(); ///清空接受到的命令
        CWX_UINT32 uiLen = packMonitorInfo();
        msg = CwxMsgBlockAlloc::malloc(uiLen);
        memcpy(msg->wr_ptr(), m_szBuf, uiLen);
        msg->wr_ptr(uiLen);
      } else if (memcmp(strCmd->c_str(), "quit", 4) == 0) {
        return -1;
      } else { //无效的命令
        strCmd->erase(); ///清空接受到的命令
        ///回复信息
        msg = CwxMsgBlockAlloc::malloc(1024);
        strcpy(msg->wr_ptr(), "ERROR\r\n");
        msg->wr_ptr(strlen(msg->wr_ptr()));
      }
    }
  } while (0);

  msg->send_ctrl().setConnId(conn.getConnInfo().getConnId());
  msg->send_ctrl().setSvrId(CwxMcApp::SVR_TYPE_MONITOR);
  msg->send_ctrl().setHostId(0);
  msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (-1 == sendMsgByConn(msg)) {
    CWX_ERROR(("Failure to send monitor reply"));
    CwxMsgBlockAlloc::free(msg);
    return -1;
  }
  return 0;
}

#define MQ_MONITOR_APPEND(){\
  uiLen = strlen(szLine);\
  if (uiPos + uiLen > MAX_MONITOR_REPLY_SIZE - 20) break;\
  memcpy(m_szBuf + uiPos, szLine, uiLen);\
  uiPos += uiLen; }

CWX_UINT32 CwxMcApp::packMonitorInfo() {
  string strValue;
  char szTmp[64];
  char szLine[4096];
  CWX_UINT32 uiLen = 0;
  CWX_UINT32 uiPos = 0;
  do {
    //输出进程pid
    CwxCommon::snprintf(szLine, 4096, "STAT pid %d\r\n", getpid());
    MQ_MONITOR_APPEND();
    //输出父进程pid
    CwxCommon::snprintf(szLine, 4096, "STAT ppid %d\r\n", getppid());
    MQ_MONITOR_APPEND();
    //版本号
    CwxCommon::snprintf(szLine, 4096, "STAT version %s\r\n",
      this->getAppVersion().c_str());
    MQ_MONITOR_APPEND();
    //修改时间
    CwxCommon::snprintf(szLine, 4096, "STAT modify %s\r\n",
      this->getLastModifyDatetime().c_str());
    MQ_MONITOR_APPEND();
    //编译时间
    CwxCommon::snprintf(szLine, 4096, "STAT compile %s\r\n",
      this->getLastCompileDatetime().c_str());
    MQ_MONITOR_APPEND();
    //启动时间
    CwxCommon::snprintf(szLine, 4096, "STAT start %s\r\n",
      m_strStartTime.c_str());
    MQ_MONITOR_APPEND();
    //queue的名字
    CwxCommon::snprintf(szLine, 4096, "STAT queue_name %s\r\n",
      m_config.getMq().m_strName.c_str());
    MQ_MONITOR_APPEND();
    //size
    CwxCommon::snprintf(szLine, 4096, "STAT queue_msize %u\r\n",
      m_config.getMq().m_uiCacheMSize);
    MQ_MONITOR_APPEND();
    //queue的最大时间戳
    CwxCommon::snprintf(szLine, 4096, "STAT queue_max_time %u\r\n",
      m_queue->getMaxTimestamp());
    MQ_MONITOR_APPEND();
    //queue的最小时间戳
    CwxCommon::snprintf(szLine, 4096, "STAT queue_min_time %u\r\n",
      m_queue->getMinTimestamp());
    MQ_MONITOR_APPEND();
    //记录的数量
    CwxCommon::snprintf(szLine, 4096, "STAT queue_count %u\r\n",
      m_queue->getCount());
    MQ_MONITOR_APPEND();
    //占用的msize
    CwxCommon::snprintf(szLine, 4096, "STAT queue_used_msize %u\r\n",
      (CWX_UINT32)(m_queue->getSize()/(1024*1024)));
    MQ_MONITOR_APPEND();
    //丢弃的数量
    CwxCommon::snprintf(szLine, 4096, "STAT queue_discard %s\r\n",
      CwxCommon::toString(m_queue->getDiscardNum(), szTmp, 10));
    MQ_MONITOR_APPEND();
    map<string, CwxMcSyncSession*>::iterator iter = m_syncs.begin();
    while(iter != m_syncs.end()){
      //输出sync的状态，格式为ip|sid|timestamp|valid
      CwxCommon::snprintf(szLine, 4096, "STAT sync %s|%s|%u|%d\r\n",
        iter->first.c_str(),
        CwxCommon::toString(iter->second->m_ullLogSid, szTmp, 10),
        iter->second->m_uiLogTimeStamp,
        !iter->second->m_bNeedClosed && !iter->second->m_bClosed?1:0);
      MQ_MONITOR_APPEND();
      ++iter;
    }
  } while (0);
  strcpy(m_szBuf + uiPos, "END\r\n");
  return strlen(m_szBuf);

}

///sync channel的线程函数，arg为app对象
void* CwxMcApp::syncThreadMain(CwxTss* tss,
                               CwxMsgQueue* queue,
                               void* arg)
{
  CwxMqTss* pTss = (CwxMqTss*)tss;
  CwxMcSyncSession* pSession = (CwxMcSyncSession*) arg;
  pTss->m_userData = arg;
  if (0 != pSession->m_channel->open()) {
    CWX_ERROR(("Failure to open sync channel"));
    //停止app
    pSession->m_pApp->stop();
    return NULL;
  }
  while (1) {
    //获取队列中的消息并处理
    if (0 != dealSyncThreadMsg(queue, pSession, pTss, pSession->m_channel)) break;
    if (-1 == pSession->m_channel->dispatch(5)) {
      CWX_ERROR(("Failure to invoke CwxAppChannel::dispatch()"));
      sleep(1);
    }
    if (0 != CwxMcSyncHandler::checkSyncLimit(pTss)) {
      pSession->m_bNeedClosed = true;
    }
    if (pSession->isCloseSession()){
      CwxMcSyncHandler::closeSession(pTss);
    }else if (pSession->isNeedCreate()){
      CwxMcSyncHandler::createSession(pTss);
    }else if (pSession->isTimeout(pSession->m_pApp->getCurTime())){
      CwxMcSyncHandler::closeSession(pTss);
    }
  }
  pSession->m_channel->stop();
  pSession->m_channel->close();
  return NULL;
}
///sync channel的队列消息函数。返回值：0：正常；-1：队列停止
int CwxMcApp::dealSyncThreadMsg(CwxMsgQueue* queue,
                                CwxMcSyncSession* pSession,
                                CwxMqTss* tss,
                                CwxAppChannel* )
{
  int iRet = 0;
  CwxMsgBlock* block = NULL;
  while (!queue->isEmpty()) {
    do {
      iRet = queue->dequeue(block);
      if (-1 == iRet) return -1;
      CWX_ASSERT(block->event().getSvrId() == SVR_TYPE_SYNC);
      if (block->event().getEvent() == CwxEventInfo::TIMEOUT_CHECK){
        pSession->m_store->timeout(pSession->m_pApp->getCurTime());
      }else if (block->event().getEvent() == EVENT_TYPE_SYNC_CHANGE){
        CwxMcConfigHost* host = NULL;
        memcpy(&host, block->rd_ptr(), sizeof(host));
        pSession->m_syncHost = *host;
        pSession->m_bNeedClosed = true;
      }else{
        CWX_ERROR(("Unknown event type[%d] for sync thread pool.", block->event().getEvent()));
      }
    } while (0);
    CwxMsgBlockAlloc::free(block);
    block = NULL;
  }
  if (queue->isDeactived()) return -1;
  return 0;
}


///queue channel的线程函数，arg为app对象
void* CwxMcApp::queueThreadMain(CwxTss*, CwxMsgQueue* queue, void* arg) {
  CwxMcApp* app = (CwxMcApp*) arg;
  if (0 != app->m_queueChannel->open()) {
    CWX_ERROR(("Failure to open queue channel"));
    return NULL;
  }
  while (1) {
    //获取队列中的消息并处理
    if (0 != dealQueueThreadMsg(queue, app, app->m_queueChannel)) break;
    if (-1 == app->m_queueChannel->dispatch(5)) {
      CWX_ERROR(("Failure to invoke CwxAppChannel::dispatch()"));
      sleep(1);
    }
  }
  app->m_queueChannel->stop();
  app->m_queueChannel->close();
  if (!app->isStopped()) {
    CWX_INFO(("Stop app for queue channel thread stopped."));
    app->stop();
  }
  return NULL;
}
///queue channel的队列消息函数。返回值：0：正常；-1：队列停止
int CwxMcApp::dealQueueThreadMsg(CwxMsgQueue* queue, CwxMcApp* app, CwxAppChannel* channel) {
  int iRet = 0;
  CwxMsgBlock* block = NULL;
  CwxAppHandler4Channel* handler = NULL;
  while (!queue->isEmpty()) {
    do {
      iRet = queue->dequeue(block);
      if (-1 == iRet) return -1;
      if ((block->event().getEvent() == CwxEventInfo::CONN_CREATED)
        && (block->event().getSvrId() == SVR_TYPE_QUEUE)) {
          if (channel->isRegIoHandle(block->event().getIoHandle())) {
            CWX_ERROR(("Handler[%] is register", block->event().getIoHandle()));
            break;
          }
          if (block->event().getSvrId() == SVR_TYPE_QUEUE) {
            handler = new CwxMcQueueHandler(app, channel);
          } else {
            CWX_ERROR(("Invalid svr_type[%d], close handle[%d]",
              block->event().getSvrId(),
              block->event().getIoHandle()));
            ::close(block->event().getIoHandle());
          }
          handler->setHandle(block->event().getIoHandle());
          if (0 != handler->open()) {
            CWX_ERROR(("Failure to register handler[%d]", handler->getHandle()));
            delete handler;
            break;
          }
      } else {
        CWX_ASSERT(block->event().getEvent() == CwxEventInfo::TIMEOUT_CHECK);
        CWX_ASSERT(block->event().getSvrId() == SVR_TYPE_QUEUE);
        app->getQueue()->checkTimeout(app->getCurTime());
      }
    } while (0);
    CwxMsgBlockAlloc::free(block);
    block = NULL;
  }
  if (queue->isDeactived()) return -1;
  return 0;
}


///设置mq连接的熟悉
int CwxMcApp::setQueueSockAttr(CWX_HANDLE handle, void* arg) {
  CwxMcApp* app = (CwxMcApp*) arg;
  if (app->getConfig().getMq().m_mq.isKeepAlive()) {
    if (0 != CwxSocket::setKeepalive(handle,
      true,
      CWX_APP_DEF_KEEPALIVE_IDLE,
      CWX_APP_DEF_KEEPALIVE_INTERNAL,
      CWX_APP_DEF_KEEPALIVE_COUNT))
    {
      CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
        app->getConfig().getMq().m_mq.getHostName().c_str(),
        app->getConfig().getMq().m_mq.getPort(),
        errno));
      return -1;
    }
  }

  int flags = 1;
  if (setsockopt(handle,
    IPPROTO_TCP, TCP_NODELAY,
    (void *) &flags,
    sizeof(flags)) != 0) 
  {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
      app->getConfig().getMq().m_mq.getHostName().c_str(),
      app->getConfig().getMq().m_mq.getPort(), errno));
    return -1;
  }
  struct linger ling = { 0, 0 };
  if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
      app->getConfig().getMq().m_mq.getHostName().c_str(),
      app->getConfig().getMq().m_mq.getPort(), errno));
    return -1;
  }
  return 0;
}
