#include "CwxMcApp.h"
#include "CwxDate.h"
///构造函数
CwxMcApp::CwxMcApp() {
  m_queue = NULL;
  m_queueThreadPool = NULL;
  m_queueChannel = NULL;
  m_ttCurTime = 0;
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
  this->setAppVersion(CWX_MQ_VERSION);
  //set last modify date
  this->setLastModifyDatetime(CWX_MQ_MODIFY_DATE);
  //set compile date
  this->setLastCompileDatetime(CWX_COMPILE_DATE(_BUILD_DATE));
  ///设置启动时间
  CwxDate::getDateY4MDHMS2(time(NULL), m_strStartTime);

  ///启动网络连接与监听
  if (0 != startNetwork()) return -1;
  //创建queue线程池
  m_queueChannel = new CwxAppChannel();
  m_queueThreadPool = new CwxThreadPool(CwxAppFramework::THREAD_GROUP_USER_START + 2,
      1,
      getThreadPoolMgr(),
      &getCommander(),
      CwxMqApp::queueThreadMain,
      this);
  ///启动线程
  CwxMqTss** pTss = new CwxTss*[1];
  pTss[0] = new CwxMqTss();
  ((CwxMqTss*) pTss[0])->init();
  if (0 != m_queueThreadPool->start(pTss)) {
      CWX_ERROR(("Failure to start queue thread pool"));
      return -1;
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
    ttLastTime = m_ttCurTime;
    //往queue线程池append TIMEOUT_CHECK
    if (m_queueThreadPool) {
      CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
      pBlock->event().setSvrId(SVR_TYPE_QUEUE);
      pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
      //将超时检查事件，放入事件队列
      m_queueThreadPool->append(pBlock);
    }
  }
}

///信号处理函数
void CwxMqApp::onSignal(int signum) {
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
      if (m_mqThreadPool->append(msg) <= 1)
          m_mqChannel->notice();
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
  if (m_recvThreadPool) {
    m_recvThreadPool->stop();
    delete m_recvThreadPool;
    m_recvThreadPool = NULL;
  }
  if (m_dispThreadPool) {
    m_dispThreadPool->stop();
    delete m_dispThreadPool;
    m_dispThreadPool = NULL;
  }
  if (m_dispChannel) {
    delete m_dispChannel;
    m_dispChannel = NULL;
  }
  if (m_mqThreadPool) {
    m_mqThreadPool->stop();
    delete m_mqThreadPool;
    m_mqThreadPool = NULL;
  }
  if (m_mqChannel) {
    delete m_mqChannel;
    m_mqChannel = NULL;
  }
  if (m_queueMgr) {
    delete m_queueMgr;
    m_queueMgr = NULL;
  }
  if (m_masterHandler) {
    delete m_masterHandler;
    m_masterHandler = NULL;
  }
  if (m_recvHandler) {
    delete m_recvHandler;
    m_recvHandler = NULL;
  }
  if (m_pBinLogMgr) {
    m_pBinLogMgr->commit(true);
    delete m_pBinLogMgr;
    m_pBinLogMgr = NULL;
  }
  CwxAppFramework::destroy();
}

int CwxMqApp::startNetwork() {
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
  ///打开连接
  if (m_config.getCommon().m_bMaster) {
    ///打开bin协议消息接收的listen
    if (0 > this->noticeTcpListen(SVR_TYPE_RECV,
            m_config.getRecv().m_recv.getHostName().c_str(),
            m_config.getRecv().m_recv.getPort(),
            false,
            CWX_APP_MSG_MODE,
            CwxMqApp::setMasterRecvSockAttr, this)) {
      CWX_ERROR(("Can't register the recv tcp accept listen: addr=%s, port=%d",
          m_config.getRecv().m_recv.getHostName().c_str(),
          m_config.getRecv().m_recv.getPort()));
      return -1;
    }
  }
  ///打开分发端口
  if (m_config.getDispatch().m_async.getHostName().length()) {
    if (0 > this->noticeTcpListen(SVR_TYPE_DISP,
            m_config.getDispatch().m_async.getHostName().c_str(),
            m_config.getDispatch().m_async.getPort(),
            false,
            CWX_APP_EVENT_MODE,
            CwxMqApp::setDispatchSockAttr, this)) {
      CWX_ERROR(("Can't register the async-dispatch tcp accept listen: addr=%s, port=%d",
          m_config.getDispatch().m_async.getHostName().c_str(),
          m_config.getDispatch().m_async.getPort()));
      return -1;
    }
  }
  //打开bin mq获取的监听端口
  if (m_config.getMq().m_mq.getHostName().length()) {
    if (0 > this->noticeTcpListen(SVR_TYPE_QUEUE,
            m_config.getMq().m_mq.getHostName().c_str(),
            m_config.getMq().m_mq.getPort(),
            false,
            CWX_APP_EVENT_MODE,
            CwxMqApp::setMqSockAttr, this)) {
      CWX_ERROR(("Can't register the mq-fetch tcp accept listen: addr=%s, port=%d",
          m_config.getMq().m_mq.getHostName().c_str(),
          m_config.getMq().m_mq.getPort()));
      return -1;
    }
  }
  return 0;
}

int CwxMqApp::monitorStats(char const* buf,
    CWX_UINT32 uiDataLen,
    CwxAppHandler4Msg& conn) {
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
  msg->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MONITOR);
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

CWX_UINT32 CwxMqApp::packMonitorInfo() {
  string strValue;
  char szTmp[128];
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
    //master
    CwxCommon::snprintf(szLine, 4096, "STAT server_type %s\r\n",
        getConfig().getCommon().m_bMaster ? "master" : "slave");
    MQ_MONITOR_APPEND();
    //binlog
    CwxCommon::snprintf(szLine, 4096, "STAT binlog_state %s\r\n",
        m_pBinLogMgr->isInvalid() ? "invalid" : "valid");
    MQ_MONITOR_APPEND();
    CwxCommon::snprintf(szLine, 4096, "STAT binlog_error %s\r\n",
        m_pBinLogMgr->isInvalid() ? m_pBinLogMgr->getInvalidMsg() : "");
    MQ_MONITOR_APPEND();
    CwxCommon::snprintf(szLine, 4096, "STAT min_sid %s\r\n",
        CwxCommon::toString(m_pBinLogMgr->getMinSid(), szTmp));
    MQ_MONITOR_APPEND();
    CwxDate::getDateY4MDHMS2(m_pBinLogMgr->getMinTimestamp(), strValue);
    CwxCommon::snprintf(szLine, 4096, "STAT min_sid_time %s\r\n",
        strValue.c_str());
    MQ_MONITOR_APPEND();
    CwxCommon::snprintf(szLine, 4096, "STAT min_binlog_file %s\r\n",
        m_pBinLogMgr->getMinFile(strValue).c_str());
    MQ_MONITOR_APPEND();
    CwxCommon::snprintf(szLine, 4096, "STAT max_sid %s\r\n",
        CwxCommon::toString(m_pBinLogMgr->getMaxSid(), szTmp));
    MQ_MONITOR_APPEND();
    CwxDate::getDateY4MDHMS2(m_pBinLogMgr->getMaxTimestamp(), strValue);
    CwxCommon::snprintf(szLine, 4096, "STAT max_sid_time %s\r\n",
        strValue.c_str());
    MQ_MONITOR_APPEND();
    CwxCommon::snprintf(szLine, 4096, "STAT max_binlog_file %s\r\n",
        m_pBinLogMgr->getMaxFile(strValue).c_str());
    MQ_MONITOR_APPEND();
    char szSid1[64];
    char szSid2[64];
    list<CwxMqQueueInfo> queues;
    m_queueMgr->getQueuesInfo(queues);
    list<CwxMqQueueInfo>::iterator iter = queues.begin();
    while (iter != queues.end()) {
      CwxCommon::toString(iter->m_ullCursorSid, szSid1, 10);
      CwxCommon::toString(iter->m_ullLeftNum, szSid2, 10);
      CwxCommon::snprintf(szLine, 4096, "STAT name(%s)|sid(%s)|msg(%s)\r\n",
          iter->m_strName.c_str(), szSid1, szSid2);
      MQ_MONITOR_APPEND();
      iter++;
    }
  } while (0);
  strcpy(m_szBuf + uiPos, "END\r\n");
  return strlen(m_szBuf);

}

///分发channel的线程函数，arg为app对象
void* CwxMqApp::dispatchThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg) {
  CwxMqApp* app = (CwxMqApp*) arg;
  if (0 != app->getDispChannel()->open()) {
    CWX_ERROR(("Failure to open dispatch channel"));
    return NULL;
  }
  while (1) {
    //获取队列中的消息并处理
    if (0!= dealDispatchThreadQueueMsg(tss,
        queue,
        app,
        app->getDispChannel())) {
      break;
    }
    if (-1 == app->getDispChannel()->dispatch(1)) {
      CWX_ERROR(("Failure to CwxAppChannel::dispatch()"));
      sleep(1);
    }
    ///检查关闭的连接
    CwxMqDispHandler::dealClosedSession(app, (CwxMqTss*) tss);
  }
  ///释放分发的资源
  CwxMqDispHandler::destroy(app);

  app->getDispChannel()->stop();
  app->getDispChannel()->close();
  if (!app->isStopped()) {
    CWX_INFO(("Stop app for disptch channel thread stopped."));
    app->stop();
  }
  return NULL;
}
///分发channel的队列消息函数。返回值：0：正常；-1：队列停止
int CwxMqApp::dealDispatchThreadQueueMsg(CwxTss* tss,
    CwxMsgQueue* queue,
    CwxMqApp* app,
    CwxAppChannel*) {
  int iRet = 0;
  CwxMsgBlock* block = NULL;
  while (!queue->isEmpty()) {
    iRet = queue->dequeue(block);
    if (-1 == iRet) return -1;
    CwxMqDispHandler::doEvent(app, (CwxMqTss*) tss, block);
    if (block) CwxMsgBlockAlloc::free(block);
    block = NULL;
  }
  if (queue->isDeactived()) return -1;
  return 0;
}

///分发mq channel的线程函数，arg为app对象
void* CwxMqApp::mqFetchThreadMain(CwxTss*,
    CwxMsgQueue* queue,
    void* arg) {
  CwxMqApp* app = (CwxMqApp*) arg;
  if (0 != app->getMqChannel()->open()) {
    CWX_ERROR(("Failure to open queue channel"));
    return NULL;
  }
  while (1) {
    //获取队列中的消息并处理
    if (0 != dealMqFetchThreadQueueMsg(queue, app, app->getMqChannel()))
      break;
    if (-1 == app->getMqChannel()->dispatch(1)) {
      CWX_ERROR(("Failure to invoke CwxAppChannel::dispatch()"));
      sleep(1);
    }
  }
  app->getMqChannel()->stop();
  app->getMqChannel()->close();
  if (!app->isStopped()) {
    CWX_INFO(("Stop app for queue channel thread stopped."));
    app->stop();
  }
  return NULL;

}
///分发mq channel的队列消息函数。返回值：0：正常；-1：队列停止
int CwxMqApp::dealMqFetchThreadQueueMsg(CwxMsgQueue* queue,
    CwxMqApp* app,
    CwxAppChannel* channel) {
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
          handler = new CwxMqQueueHandler(app, channel);
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
        app->getQueueMgr()->timeout(app->getCurTime());
      }
    } while (0);
    CwxMsgBlockAlloc::free(block);
    block = NULL;
  }
  if (queue->isDeactived()) return -1;
  return 0;
}

///设置master recv连接的属性
int CwxMqApp::setMasterRecvSockAttr(CWX_HANDLE handle,
    void* arg) {
  CwxMqApp* app = (CwxMqApp*) arg;
  if (app->getConfig().getRecv().m_recv.isKeepAlive()) {
    if (0 != CwxSocket::setKeepalive(handle,
        true,
        CWX_APP_DEF_KEEPALIVE_IDLE,
        CWX_APP_DEF_KEEPALIVE_INTERNAL,
        CWX_APP_DEF_KEEPALIVE_COUNT)) {
      CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
          app->getConfig().getRecv().m_recv.getHostName().c_str(),
          app->getConfig().getRecv().m_recv.getPort(), errno));
      return -1;
    }
  }

  int flags = 1;
  if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *) &flags,
      sizeof(flags)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
        app->getConfig().getRecv().m_recv.getHostName().c_str(),
        app->getConfig().getRecv().m_recv.getPort(),
        errno));
    return -1;
  }
  struct linger ling = { 0, 0 };
  if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
        app->getConfig().getRecv().m_recv.getHostName().c_str(),
        app->getConfig().getRecv().m_recv.getPort(), errno));
    return -1;
  }
  return 0;

}
///设置slave dispatch连接的属性
int CwxMqApp::setDispatchSockAttr(CWX_HANDLE handle, void* arg) {
  CwxMqApp* app = (CwxMqApp*) arg;
  if (app->getConfig().getCommon().m_uiSockBufSize) {
    int iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023) / 1024;
    iSockBuf *= 1024;
    while (setsockopt(handle, SOL_SOCKET, SO_SNDBUF, (void*) &iSockBuf,
        sizeof(iSockBuf)) < 0) {
      iSockBuf -= 1024;
      if (iSockBuf <= 1024) break;
    }
    iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023) / 1024;
    iSockBuf *= 1024;
    while (setsockopt(handle, SOL_SOCKET, SO_RCVBUF, (void *) &iSockBuf,
        sizeof(iSockBuf)) < 0) {
      iSockBuf -= 1024;
      if (iSockBuf <= 1024)
        break;
    }
  }

  if (app->getConfig().getDispatch().m_async.isKeepAlive()) {
    if (0!= CwxSocket::setKeepalive(handle,
        true,
        CWX_APP_DEF_KEEPALIVE_IDLE,
        CWX_APP_DEF_KEEPALIVE_INTERNAL,
        CWX_APP_DEF_KEEPALIVE_COUNT)) {
      CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
          app->getConfig().getDispatch().m_async.getHostName().c_str(),
          app->getConfig().getDispatch().m_async.getPort(), errno));
      return -1;
    }
  }

  int flags = 1;
  if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *) &flags,
      sizeof(flags)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
        app->getConfig().getDispatch().m_async.getHostName().c_str(),
        app->getConfig().getDispatch().m_async.getPort(), errno));
    return -1;
  }
  struct linger ling = { 0, 0 };
  if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
        app->getConfig().getDispatch().m_async.getHostName().c_str(),
        app->getConfig().getDispatch().m_async.getPort(), errno));
    return -1;
  }
  return 0;
}

///设置slave report连接的熟悉
int CwxMqApp::setSlaveReportSockAttr(CWX_HANDLE handle, void* arg) {
  CwxMqApp* app = (CwxMqApp*) arg;
  if (app->getConfig().getCommon().m_uiSockBufSize) {
    int iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023) / 1024;
    iSockBuf *= 1024;
    while (setsockopt(handle, SOL_SOCKET, SO_SNDBUF, (void*) &iSockBuf,
        sizeof(iSockBuf)) < 0) {
      iSockBuf -= 1024;
      if (iSockBuf <= 1024) break;
    }
    iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023) / 1024;
    iSockBuf *= 1024;
    while (setsockopt(handle, SOL_SOCKET, SO_RCVBUF, (void *) &iSockBuf,
        sizeof(iSockBuf)) < 0) {
      iSockBuf -= 1024;
      if (iSockBuf <= 1024) break;
    }
  }

  if (app->getConfig().getMaster().m_master.isKeepAlive()) {
    if (0 != CwxSocket::setKeepalive(handle,
        true,
        CWX_APP_DEF_KEEPALIVE_IDLE,
        CWX_APP_DEF_KEEPALIVE_INTERNAL,
        CWX_APP_DEF_KEEPALIVE_COUNT)) {
      CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
          app->getConfig().getMaster().m_master.getHostName().c_str(),
          app->getConfig().getMaster().m_master.getPort(), errno));
      return -1;
    }
  }

  int flags = 1;
  if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *) &flags, sizeof(flags)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
        app->getConfig().getMaster().m_master.getHostName().c_str(),
        app->getConfig().getMaster().m_master.getPort(), errno));
    return -1;
  }
  struct linger ling = { 0, 0 };
  if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
        app->getConfig().getMaster().m_master.getHostName().c_str(),
        app->getConfig().getMaster().m_master.getPort(), errno));
    return -1;
  }
  return 0;

}
///设置mq连接的熟悉
int CwxMqApp::setMqSockAttr(CWX_HANDLE handle, void* arg) {
  CwxMqApp* app = (CwxMqApp*) arg;
  if (app->getConfig().getMq().m_mq.isKeepAlive()) {
    if (0 != CwxSocket::setKeepalive(handle,
        true,
        CWX_APP_DEF_KEEPALIVE_IDLE,
        CWX_APP_DEF_KEEPALIVE_INTERNAL,
        CWX_APP_DEF_KEEPALIVE_COUNT)) {
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
      sizeof(flags)) != 0) {
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
