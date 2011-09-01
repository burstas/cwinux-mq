#include "CwxMqApp.h"
#include "CwxDate.h"
///构造函数
CwxMqApp::CwxMqApp()
{
    m_bFirstBinLog =  true;
    m_ttLastCommitTime = 0;
    m_uiUnCommitLogNum = 0;
    m_ttMqLastCommitTime = 0;
    m_uiCurSid = 0;
    m_pBinLogMgr = NULL;
    m_pMasterHandler = NULL;
    m_pBinRecvHandler = NULL;
    m_queueMgr = NULL;
    m_pRecvThreadPool = NULL;
    m_pAsyncDispThreadPool = NULL;
    m_asyncDispChannel = NULL;
    m_pMqThreadPool = NULL;
    m_mqChannel = NULL;
    memset(m_szBuf, 0x00, MAX_MONITOR_REPLY_SIZE);
}

///析构函数
CwxMqApp::~CwxMqApp()
{
}

///初始化
int CwxMqApp::init(int argc, char** argv)
{
    string strErrMsg;
    ///首先调用架构的init api
    if (CwxAppFramework::init(argc, argv) == -1) return -1;
    ///检查是否通过-f指定了配置文件，若没有，则采用默认的配置文件
    if ((NULL == this->getConfFile()) || (strlen(this->getConfFile()) == 0))
    {
        this->setConfFile("svr_conf.xml");
    }
    ///加载配置文件，若失败则退出
    if (0 != m_config.loadConfig(getConfFile()))
    {
        CWX_ERROR((m_config.getErrMsg()));
        return -1;
    }
    ///设置运行日志的输出level
    setLogLevel(CwxLogger::LEVEL_ERROR|CwxLogger::LEVEL_INFO|CwxLogger::LEVEL_WARNING);
    return 0;
}

///配置运行环境信息
int CwxMqApp::initRunEnv()
{
    ///设置系统的时钟间隔，最小刻度为1ms，此为1s。
    this->setClick(1000);//1s
    ///设置工作目录
    this->setWorkDir(m_config.getCommon().m_strWorkDir.c_str());
    ///设置循环运行日志的数量
    this->setLogFileNum(LOG_FILE_NUM);
    ///设置每个日志文件的大小
    this->setLogFileSize(LOG_FILE_SIZE*1024*1024);
    ///调用架构的initRunEnv，使以上设置的参数生效
    if (CwxAppFramework::initRunEnv() == -1 ) return -1;
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
    ///设置服务状态
    this->setAppRunValid(false);
    ///设置服务信息
    this->setAppRunFailReason("Starting..............");
    ///设置启动时间
    CwxDate::getDateY4MDHMS2(time(NULL), m_strStartTime);

    //启动binlog管理器
    if (0 != startBinLogMgr()) return -1;

    CwxMqPoco::init(NULL);
    
    if (m_config.getCommon().m_bMaster)
    {
        ///注册数据接收handler
        if (m_config.getMaster().m_recv.getHostName().length())
        {
            m_pBinRecvHandler = new CwxMqBinRecvHandler(this);
            getCommander().regHandle(SVR_TYPE_RECV, m_pBinRecvHandler);
        }
    }else{
        ///注册slave的master数据接收handler
        m_pMasterHandler = new CwxMqMasterHandler(this);
        getCommander().regHandle(SVR_TYPE_MASTER, m_pMasterHandler);
    }

    ///启动网络连接与监听
    if (0 != startNetwork()) return -1;

    ///创建recv线程池对象，此线程池中线程的group-id为THREAD_GROUP_USER_START，
    ///线程池的线程数量为1。
    m_pRecvThreadPool = new CwxThreadPool(CwxAppFramework::THREAD_GROUP_USER_START,
        1,
        getThreadPoolMgr(),
        &getCommander());
    ///创建线程的tss对象
    CwxTss** pTss = new CwxTss*[1];
    pTss[0] = new CwxMqTss();
    ((CwxMqTss*)pTss[0])->init();
    ///启动线程
    if ( 0 != m_pRecvThreadPool->start(pTss))
    {
        CWX_ERROR(("Failure to start recv thread pool"));
        return -1;
    }
    //创建分发线程池
    if (m_config.getCommon().m_bMaster)
    {
        if (m_config.getMaster().m_async.getHostName().length() ||
            m_config.getMaster().m_async.getUnixDomain().length())
        {
            m_asyncDispChannel = new CwxAppChannel();
            m_pAsyncDispThreadPool = new CwxThreadPool( CwxAppFramework::THREAD_GROUP_USER_START + 1,
                1,
                getThreadPoolMgr(),
                &getCommander(),
                CwxMqApp::DispatchThreadMain,
                this);
            ///启动线程
            pTss = new CwxTss*[1];
            pTss[0] = new CwxMqTss();
            ((CwxMqTss*)pTss[0])->init();
            if ( 0 != m_pAsyncDispThreadPool->start(pTss)){
                CWX_ERROR(("Failure to start dispatch thread pool"));
                return -1;
            }
        }
    }
    else
    {
        if (m_config.getSlave().m_async.getHostName().length() ||
            m_config.getSlave().m_async.getUnixDomain().length())
        {
            m_asyncDispChannel = new CwxAppChannel();
            m_pAsyncDispThreadPool = new CwxThreadPool( CwxAppFramework::THREAD_GROUP_USER_START + 1,
                1,
                getThreadPoolMgr(),
                &getCommander(),
                CwxMqApp::DispatchThreadMain,
                this);
            ///启动线程
            pTss = new CwxTss*[1];
            pTss[0] = new CwxMqTss();
            ((CwxMqTss*)pTss[0])->init();
            if ( 0 != m_pAsyncDispThreadPool->start(pTss)){
                CWX_ERROR(("Failure to start dispatch thread pool"));
                return -1;
            }
        }
    }

    //创建mq线程池
    if (m_config.getMq().m_mq.getHostName().length() ||
        m_config.getMq().m_mq.getUnixDomain().length())
    {
        m_mqChannel = new CwxAppChannel();
        m_pMqThreadPool = new CwxThreadPool( CwxAppFramework::THREAD_GROUP_USER_START + 2,
            1,
            getThreadPoolMgr(),
            &getCommander(),
            CwxMqApp::MqThreadMain,
            this);
        ///启动线程
        pTss = new CwxTss*[1];
        pTss[0] = new CwxMqTss();
        ((CwxMqTss*)pTss[0])->init();
        if ( 0 != m_pMqThreadPool->start(pTss)){
            CWX_ERROR(("Failure to start mq thread pool"));
            return -1;
        }
    }
    return 0;
}

///时钟函数
void CwxMqApp::onTime(CwxTimeValue const& current)
{
    ///调用基类的onTime函数
    CwxAppFramework::onTime(current);
    ///检查超时
    static CWX_UINT32 ttBinLogLastTime = time(NULL);
    static CWX_UINT32 ttMqLogLastTime = time(NULL);
    CWX_UINT32 uiNow = time(NULL);
    if (m_config.getCommon().m_bMaster && (uiNow >= ttBinLogLastTime + 1))
    {
        ttBinLogLastTime = uiNow;
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(SVR_TYPE_RECV);
        pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
        //将超时检查事件，放入事件队列
        m_pRecvThreadPool->append(pBlock);
    }
    if (m_queueMgr &&
        (uiNow >= ttMqLogLastTime + 1))
    {
        ttMqLogLastTime = uiNow;
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(SVR_TYPE_FETCH);
        pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
        //将超时检查事件，放入事件队列
        m_pMqThreadPool->append(pBlock);
    }
}

///信号处理函数
void CwxMqApp::onSignal(int signum)
{
    switch(signum)
    {
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

int CwxMqApp::onConnCreated(CWX_UINT32 uiSvrId,
                          CWX_UINT32 uiHostId,
                          CWX_HANDLE handle,
                          bool& )
{
    CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
    msg->event().setSvrId(uiSvrId);
    msg->event().setHostId(uiHostId);
    msg->event().setConnId(CWX_APP_INVALID_CONN_ID);
    msg->event().setIoHandle(handle);
    msg->event().setEvent(CwxEventInfo::CONN_CREATED);
    ///将消息放到线程池队列中，有内部的线程调用其处理handle来处理
    if (SVR_TYPE_ASYNC == uiSvrId)
    {
        if (m_pAsyncDispThreadPool->append(msg) <= 1)
        {
            m_asyncDispChannel->notice();
        }
    }
    else if (SVR_TYPE_FETCH == uiSvrId)
    {
        if (m_pMqThreadPool->append(msg) <= 1)
        {
            m_mqChannel->notice();
        }
    }
    else
    {
        CWX_ASSERT(SVR_TYPE_MONITOR == uiSvrId);
    }
    return 0;
}

///连接建立
int CwxMqApp::onConnCreated(CwxAppHandler4Msg& conn, bool& , bool& )
{
    if ((SVR_TYPE_RECV == conn.getConnInfo().getSvrId()) ||
        (SVR_TYPE_MASTER == conn.getConnInfo().getSvrId()))
    {
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(conn.getConnInfo().getSvrId());
        pBlock->event().setHostId(conn.getConnInfo().getHostId());
        pBlock->event().setConnId(conn.getConnInfo().getConnId());
        ///设置事件类型
        pBlock->event().setEvent(CwxEventInfo::CONN_CREATED);
        ///将事件添加到消息队列
        m_pRecvThreadPool->append(pBlock);
    }
    else if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId())
    {///如果是监控的连接建立，则建立一个string的buf，用于缓存不完整的命令
        string* buf = new string();
        conn.getConnInfo().setUserData(buf);
    }
    else
    {
        CWX_ASSERT(0);
    }
    return 0;
}

///连接关闭
int CwxMqApp::onConnClosed(CwxAppHandler4Msg& conn)
{
    if ((SVR_TYPE_RECV == conn.getConnInfo().getSvrId()) ||
        (SVR_TYPE_MASTER == conn.getConnInfo().getSvrId()))
    {
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(conn.getConnInfo().getSvrId());
        pBlock->event().setHostId(conn.getConnInfo().getHostId());
        pBlock->event().setConnId(conn.getConnInfo().getConnId());
        ///设置事件类型
        pBlock->event().setEvent(CwxEventInfo::CONN_CLOSED);
        m_pRecvThreadPool->append(pBlock);
    }
    else if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId())
    {///若是监控的连接关闭，则必须释放先前所创建的string对象。
        if (conn.getConnInfo().getUserData())
        {
            delete (string*)conn.getConnInfo().getUserData();
            conn.getConnInfo().setUserData(NULL);
        }
    }
    else
    {
        CWX_ASSERT(0);
    }
    return 0;
}

///收到消息
int CwxMqApp::onRecvMsg(CwxMsgBlock* msg,
                        CwxAppHandler4Msg& conn,
                        CwxMsgHead const& header,
                        bool& )
{
    if ((SVR_TYPE_RECV == conn.getConnInfo().getSvrId()) ||
        (SVR_TYPE_MASTER == conn.getConnInfo().getSvrId()))
    {
        msg->event().setSvrId(conn.getConnInfo().getSvrId());
        msg->event().setHostId(conn.getConnInfo().getHostId());
        msg->event().setConnId(conn.getConnInfo().getConnId());
        ///保存消息头
        msg->event().setMsgHeader(header);
        ///设置事件类型
        msg->event().setEvent(CwxEventInfo::RECV_MSG);
        ///将消息放到线程池队列中，有内部的线程调用其处理handle来处理
        m_pRecvThreadPool->append(msg);
        return 0;
    }
    else
    {
        CWX_ASSERT(0);
    }
    return 0;
}

///收到消息的响应函数
int CwxMqApp::onRecvMsg(CwxAppHandler4Msg& conn,
                      bool& )
{
    if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId())
    {
        char  szBuf[1024];
        ssize_t recv_size = CwxSocket::recv(conn.getHandle(),
            szBuf,
            1024);
        if (recv_size <=0 )
        { //error or signal
            if ((0==recv_size) || ((errno != EWOULDBLOCK) && (errno != EINTR)))
            {
                return -1; //error
            }
            else
            {//signal or no data
                return 0;
            }
        }
        ///监控消息
        return monitorStats(szBuf, (CWX_UINT32)recv_size, conn);
    }
    else
    {
        CWX_ASSERT(0);
    }
    return -1;
}


void CwxMqApp::destroy()
{
    if (m_pRecvThreadPool)
    {
        m_pRecvThreadPool->stop();
        delete m_pRecvThreadPool;
        m_pRecvThreadPool = NULL;
    }
    if (m_pAsyncDispThreadPool)
    {
        m_pAsyncDispThreadPool->stop();
        delete m_pAsyncDispThreadPool;
        m_pAsyncDispThreadPool = NULL;
    }
    if (m_asyncDispChannel)
    {
        delete m_asyncDispChannel;
        m_asyncDispChannel = NULL;
    }
    if (m_pMqThreadPool)
    {
        m_pMqThreadPool->stop();
        delete m_pMqThreadPool;
        m_pMqThreadPool = NULL;
    }
    if (m_mqChannel)
    {
        delete m_mqChannel;
        m_mqChannel = NULL;
    }
    if (m_queueMgr)
    {
        delete m_queueMgr;
        m_queueMgr = NULL;
    }
    if (m_pMasterHandler)
    {
        delete m_pMasterHandler;
        m_pMasterHandler = NULL;
    }
    if (m_pBinRecvHandler)
    {
        delete m_pBinRecvHandler;
        m_pBinRecvHandler = NULL;
    }
    if (m_pBinLogMgr)
    {
        m_pBinLogMgr->commit(true);
        delete m_pBinLogMgr;
        m_pBinLogMgr = NULL;
    }
    CwxMqPoco::destory();

    CwxAppFramework::destroy();
}


///启动binlog管理器，-1：失败；0：成功
int CwxMqApp::startBinLogMgr()
{
    ///初始化binlog
    {
        CWX_UINT64 ullBinLogSize = m_config.getBinLog().m_uiBinLogMSize;
        ullBinLogSize *= 1024 * 1024;
		if (ullBinLogSize > CwxBinLogMgr::MAX_BINLOG_FILE_SIZE) ullBinLogSize = CwxBinLogMgr::MAX_BINLOG_FILE_SIZE;
        m_pBinLogMgr = new CwxBinLogMgr(m_config.getBinLog().m_strBinlogPath.c_str(),
            m_config.getBinLog().m_strBinlogPrex.c_str(),
            ullBinLogSize,
            m_config.getBinLog().m_bDelOutdayLogFile);
        if (0 != m_pBinLogMgr->init(m_config.getBinLog().m_uiMgrFileNum,
			m_config.getBinLog().m_bCache,
            CWX_TSS_2K_BUF))
        {///<如果失败，则返回-1
            CWX_ERROR(("Failure to init binlog manager, error:%s", CWX_TSS_2K_BUF));
            return -1;
        }
        m_bFirstBinLog = true;
        m_ttLastCommitTime = time(NULL);
        m_uiUnCommitLogNum = 0;
        ///提取sid
		m_uiCurSid = m_pBinLogMgr->getMaxSid() + CWX_MQ_MAX_BINLOG_FLUSH_COUNT + 1;
    }
    //初始化MQ
    if (m_config.getMq().m_mq.getHostName().length() ||
        m_config.getMq().m_mq.getUnixDomain().length())
    {
        m_ttMqLastCommitTime = time(NULL);
        //初始化队列管理器
        if (m_queueMgr) delete m_queueMgr;
        m_queueMgr = new CwxMqQueueMgr(m_config.getMq().m_strLogFilePath,
            m_config.getMq().m_uiFlushNum);
        if (0 != m_queueMgr->init(m_pBinLogMgr))
        {
            CWX_ERROR(("Failure to init mq queue manager, err=%s", m_queueMgr->getErrMsg().c_str()));
            return -1;
        }
    }
    ///成功
    return 0;
}

int CwxMqApp::startNetwork()
{
    ///打开监听的服务器端口号
    if (m_config.getCommon().m_monitor.getHostName().length())
    {
        if (0 > this->noticeTcpListen(SVR_TYPE_MONITOR,
            m_config.getCommon().m_monitor.getHostName().c_str(),
            m_config.getCommon().m_monitor.getPort(),
            true))
        {
            CWX_ERROR(("Can't register the monitor tcp accept listen: addr=%s, port=%d",
                m_config.getCommon().m_monitor.getHostName().c_str(),
                m_config.getCommon().m_monitor.getPort()));
            return -1;
        }
    }
    ///打开连接
    if (m_config.getCommon().m_bMaster)
    {
        ///打开bin协议消息接收的listen
        if (m_config.getMaster().m_recv.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_RECV, 
                m_config.getMaster().m_recv.getHostName().c_str(),
                m_config.getMaster().m_recv.getPort(),
                false,
                CWX_APP_MSG_MODE,
                CwxMqApp::setMasterRecvSockAttr,
                this))
            {
                CWX_ERROR(("Can't register the recv tcp accept listen: addr=%s, port=%d",
                    m_config.getMaster().m_recv.getHostName().c_str(),
                    m_config.getMaster().m_recv.getPort()));
                return -1;
            }
        }
        if (m_config.getMaster().m_recv.getUnixDomain().length())
        {
            if (0 > this->noticeLsockListen(SVR_TYPE_RECV,
                m_config.getMaster().m_recv.getUnixDomain().c_str(),
                false))
            {
                CWX_ERROR(("Can't register the recv unix-domain accept listen: path-file=%s",
                    m_config.getMaster().m_recv.getUnixDomain().c_str()));
                return -1;
            }
        }
        ///打开bin协议异步分发
        if (m_config.getMaster().m_async.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_ASYNC, 
                m_config.getMaster().m_async.getHostName().c_str(),
                m_config.getMaster().m_async.getPort(),
                false,
                CWX_APP_EVENT_MODE,
                CwxMqApp::setMasterDispatchSockAttr,
                this))
            {
                CWX_ERROR(("Can't register the async-dispatch tcp accept listen: addr=%s, port=%d",
                    m_config.getMaster().m_async.getHostName().c_str(),
                    m_config.getMaster().m_async.getPort()));
                return -1;
            }
        }
        if (m_config.getMaster().m_async.getUnixDomain().length())
        {
            if (0 > this->noticeLsockListen(SVR_TYPE_ASYNC,
                m_config.getMaster().m_async.getUnixDomain().c_str(),
                false,
                CWX_APP_EVENT_MODE))
            {
                CWX_ERROR(("Can't register the async-dispatch unix-domain accept listen: path-file=%s",
                    m_config.getMaster().m_async.getUnixDomain().c_str()));
                return -1;
            }
        }

    }
    else
    {
        ///连接bin协议master
        if (m_config.getSlave().m_master.getUnixDomain().length())
        {
            if (0 > this->noticeLsockConnect(SVR_TYPE_MASTER, 0, 
                m_config.getSlave().m_master.getUnixDomain().c_str(),
                false,
                1,
                2))
            {
                CWX_ERROR(("Can't register the master unix-domain connect: path-file=%s",
                    m_config.getSlave().m_master.getUnixDomain().c_str()));
                return -1;
            }
        }
        else if (m_config.getSlave().m_master.getHostName().length())
        {
            if (0 > this->noticeTcpConnect(SVR_TYPE_MASTER, 0, 
                m_config.getSlave().m_master.getHostName().c_str(),
                m_config.getSlave().m_master.getPort(),
                false,
                1,
                2,
                CwxMqApp::setSlaveReportSockAttr,
                this))
            {
                CWX_ERROR(("Can't register the master tcp connect: addr=%s, port=%d",
                    m_config.getSlave().m_master.getHostName().c_str(),
                    m_config.getSlave().m_master.getPort()));
                return -1;
            }
        }
        else
        {
            CWX_ERROR(("Not config master dispatch"));
            return -1;
        }
        ///bin协议异步分发
        if (m_config.getSlave().m_async.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_ASYNC, 
                m_config.getSlave().m_async.getHostName().c_str(),
                m_config.getSlave().m_async.getPort(),
                false,
                CWX_APP_EVENT_MODE,
                CwxMqApp::setSlaveDispatchSockAttr,
                this))
            {
                CWX_ERROR(("Can't register the async-dispatch tcp accept listen: addr=%s, port=%d",
                    m_config.getSlave().m_async.getHostName().c_str(),
                    m_config.getSlave().m_async.getPort()));
                return -1;
            }
        }
        if (m_config.getSlave().m_async.getUnixDomain().length())
        {
            if (0 > this->noticeLsockListen(SVR_TYPE_ASYNC,
                m_config.getSlave().m_async.getUnixDomain().c_str(),
                false,
                CWX_APP_EVENT_MODE))
            {
                CWX_ERROR(("Can't register the async-dispatch unix-domain accept listen: path-file=%s",
                    m_config.getSlave().m_async.getUnixDomain().c_str()));
                return -1;
            }
        }
    }
    //打开bin mq获取的监听端口
    if (m_config.getMq().m_mq.getHostName().length())
    {
        if (0 > this->noticeTcpListen(SVR_TYPE_FETCH, 
            m_config.getMq().m_mq.getHostName().c_str(),
            m_config.getMq().m_mq.getPort(),
            false,
            CWX_APP_EVENT_MODE,
            CwxMqApp::setMqSockAttr,
            this))
        {
            CWX_ERROR(("Can't register the mq-fetch tcp accept listen: addr=%s, port=%d",
                m_config.getMq().m_mq.getHostName().c_str(),
                m_config.getMq().m_mq.getPort()));
            return -1;
        }
    }
    if (m_config.getMq().m_mq.getUnixDomain().length())
    {
        if (0 > this->noticeLsockListen(SVR_TYPE_FETCH,
            m_config.getMq().m_mq.getUnixDomain().c_str(),
            false,
            CWX_APP_EVENT_MODE))
        {
            CWX_ERROR(("Can't register the mq-fetch unix-domain accept listen: path-file=%s",
                m_config.getMq().m_mq.getUnixDomain().c_str()));
            return -1;
        }
    }
    return 0;
}

int CwxMqApp::commit_mq()
{
    if (getMqLastCommitTime() + m_config.getMq().m_uiFlushSecond < (CWX_UINT32)time(NULL))
    {
        CWX_INFO(("Begin flush mq queue log file......."));
        getQueueMgr()->commit();
        setMqLastCommitTime(time(NULL));
        CWX_INFO(("End flush mq queue log file......."));
    }
    return 0;
}


int CwxMqApp::monitorStats(char const* buf, CWX_UINT32 uiDataLen, CwxAppHandler4Msg& conn)
{
    string* strCmd = (string*)conn.getConnInfo().getUserData();
    strCmd->append(buf, uiDataLen);
    CwxMsgBlock* msg = NULL;
    string::size_type end = 0;
    do
    {
        CwxCommon::trim(*strCmd);
        end = strCmd->find('\n');
        if (string::npos == end)
        {
            if (strCmd->length() > 10)
            {//无效的命令
                strCmd->erase(); ///清空接受到的命令
                ///回复信息
                msg = CwxMsgBlockAlloc::malloc(1024);
                strcpy(msg->wr_ptr(), "ERROR\r\n");
                msg->wr_ptr(strlen(msg->wr_ptr()));
            }
            else
            {
                return 0;
            }
        }
        else
        {
            if (memcmp(strCmd->c_str(), "stats", 5) == 0)
            {
                strCmd->erase(); ///清空接受到的命令
                CWX_UINT32 uiLen = packMonitorInfo();
                msg = CwxMsgBlockAlloc::malloc(uiLen);
                memcpy(msg->wr_ptr(), m_szBuf, uiLen);
                msg->wr_ptr(uiLen);
            }
            else if(memcmp(strCmd->c_str(), "quit", 4) == 0)
            {
                return -1;
            }  
            else
            {//无效的命令
                strCmd->erase(); ///清空接受到的命令
                ///回复信息
                msg = CwxMsgBlockAlloc::malloc(1024);
                strcpy(msg->wr_ptr(), "ERROR\r\n");
                msg->wr_ptr(strlen(msg->wr_ptr()));
            }
        }
    }
    while(0);

    msg->send_ctrl().setConnId(conn.getConnInfo().getConnId());
    msg->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MONITOR);
    msg->send_ctrl().setHostId(0);
    msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    if (-1 == sendMsgByConn(msg))
    {
        CWX_ERROR(("Failure to send monitor reply"));
        CwxMsgBlockAlloc::free(msg);
        return -1;
    }
    return 0;
}

#define MQ_MONITOR_APPEND()\
    uiLen = strlen(szLine);\
    if (uiPos + uiLen > MAX_MONITOR_REPLY_SIZE - 20) break;\
    memcpy(m_szBuf + uiPos, szLine, uiLen);\
    uiPos += uiLen; \

CWX_UINT32 CwxMqApp::packMonitorInfo()
{
    string strValue;
    char szTmp[128];
    char szLine[4096];
    CWX_UINT32 uiLen = 0;
    CWX_UINT32 uiPos = 0;
    //更新服务状态
    updateAppRunState();
    do
    {
        //输出进程pid
        CwxCommon::snprintf(szLine, 4096, "STAT pid %d\r\n", getpid());
        MQ_MONITOR_APPEND();
        //输出父进程pid
        CwxCommon::snprintf(szLine, 4096, "STAT ppid %d\r\n", getppid());
        MQ_MONITOR_APPEND();
        //版本号
        CwxCommon::snprintf(szLine, 4096, "STAT version %s\r\n", this->getAppVersion().c_str());
        MQ_MONITOR_APPEND();
        //修改时间
        CwxCommon::snprintf(szLine, 4096, "STAT modify %s\r\n", this->getLastModifyDatetime().c_str());
        MQ_MONITOR_APPEND();
        //编译时间
        CwxCommon::snprintf(szLine, 4096, "STAT compile %s\r\n", this->getLastCompileDatetime().c_str());
        MQ_MONITOR_APPEND();
        //启动时间
        CwxCommon::snprintf(szLine, 4096, "STAT start %s\r\n", m_strStartTime.c_str());
        MQ_MONITOR_APPEND();
        //master
        CwxCommon::snprintf(szLine, 4096, "STAT server_type %s\r\n", getConfig().getCommon().m_bMaster?"master":"slave");
        MQ_MONITOR_APPEND();
        //state
        CwxCommon::snprintf(szLine, 4096, "STAT state %s\r\n", isAppRunValid()?"valid":"invalid");
        MQ_MONITOR_APPEND();
        //error
        CwxCommon::snprintf(szLine, 4096, "STAT err '%s'\r\n", isAppRunValid()?"":getAppRunFailReason().c_str());
        MQ_MONITOR_APPEND();
        //binlog
        CwxCommon::snprintf(szLine, 4096, "STAT binlog_state %s\r\n",
            m_pBinLogMgr->isInvalid()?"invalid":"valid");
        MQ_MONITOR_APPEND();
        CwxCommon::snprintf(szLine, 4096, "STAT binlog_error %s\r\n",
            m_pBinLogMgr->isInvalid()? m_pBinLogMgr->getInvalidMsg():"");
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
        char const* state="";
        while(iter != queues.end())
        {
            if (iter->m_ucQueueState==CwxBinLogMgr::CURSOR_STATE_UNSEEK)
                state = "unseek";
            else if (iter->m_ucQueueState==CwxBinLogMgr::CURSOR_STATE_ERROR)
                state = "error";
            else if (iter->m_ucQueueState==CwxBinLogMgr::CURSOR_STATE_READY)
                state = "ready";
            else
                state = "unknown";

            CwxCommon::toString(iter->m_ullCursorSid, szSid1, 10);
            CwxCommon::toString(iter->m_ullLeftNum, szSid2, 10);
            CwxCommon::snprintf(szLine, 4096, "STAT name(%s)|commit(%s)|def_timeout(%u)|max_timeout(%u)|cursor_state(%s)|log_state(%s)|sid(%s)|msg(%s)|subscribe(%s)|cursor_err(%s)|log_err(%s)\r\n",
                iter->m_strName.c_str(),
                iter->m_bCommit?"yes":"no",
				iter->m_uiDefTimeout,
				iter->m_uiMaxTimeout,
                state,
				iter->m_bQueueLogFileValid?"yes":"no",
                szSid1,
                szSid2,
                iter->m_strSubScribe.c_str(),
				iter->m_ucQueueState==CwxBinLogMgr::CURSOR_STATE_ERROR?iter->m_strQueueErrMsg.c_str():"",
				iter->m_bQueueLogFileValid?"":iter->m_strQueueLogFileErrMsg.c_str());
            MQ_MONITOR_APPEND();
            iter++;
        }
    }
    while(0);
    strcpy(m_szBuf + uiPos, "END\r\n");
    return strlen(m_szBuf);

}

///分发channel的线程函数，arg为app对象
void* CwxMqApp::DispatchThreadMain(CwxTss* , CwxMsgQueue* queue, void* arg)
{
    CwxMqApp* app = (CwxMqApp*) arg;
    if (0 != app->getAsyncDispChannel()->open())
    {
        CWX_ERROR(("Failure to open async dispatch channel"));
        return NULL;
    }
    while(1)
    {
        //获取队列中的消息并处理
        if (0 != DispatchThreadDoQueue(queue, app, app->getAsyncDispChannel())) break;
        if (-1 == app->getAsyncDispChannel()->dispatch(1))
        {
            CWX_ERROR(("Failure to invoke async dispatch channel CwxAppChannel::dispatch()"));
            sleep(1);
        }
    }
    app->getAsyncDispChannel()->stop();
    app->getAsyncDispChannel()->close();
    if (!app->isStopped())
    {
        CWX_INFO(("Stop app for disptch channel thread stopped."));
        app->stop();
    }
    return NULL;
}
///分发channel的队列消息函数。返回值：0：正常；-1：队列停止
int CwxMqApp::DispatchThreadDoQueue(CwxMsgQueue* queue, CwxMqApp* app, CwxAppChannel* channel)
{
    int iRet = 0;
    CwxMsgBlock* block = NULL;
    CwxAppHandler4Channel* handler = NULL;
    while (!queue->isEmpty())
    {
        do 
        {
            iRet = queue->dequeue(block);
            if (-1 == iRet) return -1;
            CWX_ASSERT(block->event().getEvent() == CwxEventInfo::CONN_CREATED);
            CWX_ASSERT(block->event().getSvrId() == SVR_TYPE_ASYNC);

            if (channel->isRegIoHandle(block->event().getIoHandle()))
            {
                CWX_ERROR(("Handler[%] is register", block->event().getIoHandle()));
                break;
            }
            if (block->event().getSvrId() == SVR_TYPE_ASYNC)
            {
                handler = new CwxMqBinAsyncHandler(app, channel);
            }
            else
            {
                CWX_ERROR(("Invalid svr_type[%d], close handle[%d]", block->event().getSvrId(), block->event().getIoHandle()));
                ::close(block->event().getIoHandle());
            }
            handler->setHandle(block->event().getIoHandle());
            if (0 != handler->open())
            {
                CWX_ERROR(("Failure to register handler[%d]", handler->getHandle()));
                delete handler;
                break;
            }
        } while(0);
        CwxMsgBlockAlloc::free(block);
        block = NULL;
    }
    if (queue->isDeactived()) return -1;
    return 0;
}

///分发mq channel的线程函数，arg为app对象
void* CwxMqApp::MqThreadMain(CwxTss* , CwxMsgQueue* queue, void* arg)
{
    CwxMqApp* app = (CwxMqApp*) arg;
    if (0 != app->getMqChannel()->open())
    {
        CWX_ERROR(("Failure to open mq channel"));
        return NULL;
    }
    while(1)
    {
        //获取队列中的消息并处理
        if (0 != MqThreadDoQueue(queue, app,  app->getMqChannel())) break;
        if (-1 == app->getMqChannel()->dispatch(1))
        {
            CWX_ERROR(("Failure to invoke mq channel CwxAppChannel::dispatch()"));
            sleep(1);
        }
    }
    app->getMqChannel()->stop();
    app->getMqChannel()->close();
    if (!app->isStopped())
    {
        CWX_INFO(("Stop app for mq channel thread stopped."));
        app->stop();
    }
    return NULL;

}
///分发mq channel的队列消息函数。返回值：0：正常；-1：队列停止
int CwxMqApp::MqThreadDoQueue(CwxMsgQueue* queue, CwxMqApp* app, CwxAppChannel* channel)
{
    int iRet = 0;
    CwxMsgBlock* block = NULL;
    CwxAppHandler4Channel* handler = NULL;
    while (!queue->isEmpty())
    {
        do 
        {
            iRet = queue->dequeue(block);
            if (-1 == iRet) return -1;
            if ((block->event().getEvent() == CwxEventInfo::CONN_CREATED) &&
                (block->event().getSvrId() == SVR_TYPE_FETCH))
            {
                if (channel->isRegIoHandle(block->event().getIoHandle()))
                {
                    CWX_ERROR(("Handler[%] is register", block->event().getIoHandle()));
                    break;
                }
                if (block->event().getSvrId() == SVR_TYPE_FETCH)
                {
                    handler = new CwxMqBinFetchHandler(app, channel);
                }
                else
                {
                    CWX_ERROR(("Invalid svr_type[%d], close handle[%d]", block->event().getSvrId(), block->event().getIoHandle()));
                    ::close(block->event().getIoHandle());
                }
                handler->setHandle(block->event().getIoHandle());
                if (0 != handler->open())
                {
                    CWX_ERROR(("Failure to register handler[%d]", handler->getHandle()));
                    delete handler;
                    break;
                }
            }
            else
            {
                CWX_ASSERT(block->event().getEvent() == CwxEventInfo::TIMEOUT_CHECK);
                CWX_ASSERT(block->event().getSvrId() == SVR_TYPE_FETCH);
                app->getQueueMgr()->checkTimeout(time(NULL));
                app->commit_mq();
            }
        } while(0);
        CwxMsgBlockAlloc::free(block);
        block = NULL;
    }
    if (queue->isDeactived()) return -1;
    return 0;
}

///设置master recv连接的属性
int CwxMqApp::setMasterRecvSockAttr(CWX_HANDLE handle, void* arg)
{
    CwxMqApp* app = (CwxMqApp*)arg;
    if (app->getConfig().getMaster().m_recv.isKeepAlive())
    {
        if (0 != CwxSocket::setKeepalive(handle,
            true,
            CWX_APP_DEF_KEEPALIVE_IDLE,
            CWX_APP_DEF_KEEPALIVE_INTERNAL,
            CWX_APP_DEF_KEEPALIVE_COUNT))
        {
            CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
                app->getConfig().getMaster().m_recv.getHostName().c_str(),
                app->getConfig().getMaster().m_recv.getPort(),
                errno));
            return -1;
        }
    }

    int flags= 1;
    if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
            app->getConfig().getMaster().m_recv.getHostName().c_str(),
            app->getConfig().getMaster().m_recv.getPort(),
            errno));
        return -1;
    }
    struct linger ling= {0, 0};
    if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
            app->getConfig().getMaster().m_recv.getHostName().c_str(),
            app->getConfig().getMaster().m_recv.getPort(),
            errno));
        return -1;
    }
    return 0;

}
///设置master dispatch连接的属性
int CwxMqApp::setMasterDispatchSockAttr(CWX_HANDLE handle, void* arg)
{
    CwxMqApp* app = (CwxMqApp*)arg;
    if (app->getConfig().getCommon().m_uiSockBufSize)
    {
        int iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023)/1024;
        iSockBuf *= 1024;
        while (setsockopt(handle, SOL_SOCKET, SO_SNDBUF, (void*)&iSockBuf, sizeof(iSockBuf)) < 0)
        {
            iSockBuf -= 1024;
            if (iSockBuf <= 1024) break;
        }
        iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023)/1024;
        iSockBuf *= 1024;
        while(setsockopt(handle, SOL_SOCKET, SO_RCVBUF, (void *)&iSockBuf, sizeof(iSockBuf)) < 0)
        {
            iSockBuf -= 1024;
            if (iSockBuf <= 1024) break;
        }
    }

    if (app->getConfig().getMaster().m_async.isKeepAlive())
    {
        if (0 != CwxSocket::setKeepalive(handle,
            true,
            CWX_APP_DEF_KEEPALIVE_IDLE,
            CWX_APP_DEF_KEEPALIVE_INTERNAL,
            CWX_APP_DEF_KEEPALIVE_COUNT))
        {
            CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
                app->getConfig().getMaster().m_async.getHostName().c_str(),
                app->getConfig().getMaster().m_async.getPort(),
                errno));
            return -1;
        }
    }

    int flags= 1;
    if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
            app->getConfig().getMaster().m_async.getHostName().c_str(),
            app->getConfig().getMaster().m_async.getPort(),
            errno));
        return -1;
    }
    struct linger ling= {0, 0};
    if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
            app->getConfig().getMaster().m_async.getHostName().c_str(),
            app->getConfig().getMaster().m_async.getPort(),
            errno));
        return -1;
    }
    return 0;
}
///设置slave dispatch连接的属性
int CwxMqApp::setSlaveDispatchSockAttr(CWX_HANDLE handle, void* arg)
{
    CwxMqApp* app = (CwxMqApp*)arg;
    if (app->getConfig().getCommon().m_uiSockBufSize)
    {
        int iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023)/1024;
        iSockBuf *= 1024;
        while (setsockopt(handle, SOL_SOCKET, SO_SNDBUF, (void*)&iSockBuf, sizeof(iSockBuf)) < 0)
        {
            iSockBuf -= 1024;
            if (iSockBuf <= 1024) break;
        }
        iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023)/1024;
        iSockBuf *= 1024;
        while(setsockopt(handle, SOL_SOCKET, SO_RCVBUF, (void *)&iSockBuf, sizeof(iSockBuf)) < 0)
        {
            iSockBuf -= 1024;
            if (iSockBuf <= 1024) break;
        }
    }

    if (app->getConfig().getSlave().m_async.isKeepAlive())
    {
        if (0 != CwxSocket::setKeepalive(handle,
            true,
            CWX_APP_DEF_KEEPALIVE_IDLE,
            CWX_APP_DEF_KEEPALIVE_INTERNAL,
            CWX_APP_DEF_KEEPALIVE_COUNT))
        {
            CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
                app->getConfig().getSlave().m_async.getHostName().c_str(),
                app->getConfig().getSlave().m_async.getPort(),
                errno));
            return -1;
        }
    }

    int flags= 1;
    if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
            app->getConfig().getSlave().m_async.getHostName().c_str(),
            app->getConfig().getSlave().m_async.getPort(),
            errno));
        return -1;
    }
    struct linger ling= {0, 0};
    if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
            app->getConfig().getSlave().m_async.getHostName().c_str(),
            app->getConfig().getSlave().m_async.getPort(),
            errno));
        return -1;
    }
    return 0;
}

///设置slave report连接的熟悉
int CwxMqApp::setSlaveReportSockAttr(CWX_HANDLE handle, void* arg)
{
    CwxMqApp* app = (CwxMqApp*)arg;
    if (app->getConfig().getCommon().m_uiSockBufSize)
    {
        int iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023)/1024;
        iSockBuf *= 1024;
        while (setsockopt(handle, SOL_SOCKET, SO_SNDBUF, (void*)&iSockBuf, sizeof(iSockBuf)) < 0)
        {
            iSockBuf -= 1024;
            if (iSockBuf <= 1024) break;
        }
        iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023)/1024;
        iSockBuf *= 1024;
        while(setsockopt(handle, SOL_SOCKET, SO_RCVBUF, (void *)&iSockBuf, sizeof(iSockBuf)) < 0)
        {
            iSockBuf -= 1024;
            if (iSockBuf <= 1024) break;
        }
    }

    if (app->getConfig().getSlave().m_master.isKeepAlive())
    {
        if (0 != CwxSocket::setKeepalive(handle,
            true,
            CWX_APP_DEF_KEEPALIVE_IDLE,
            CWX_APP_DEF_KEEPALIVE_INTERNAL,
            CWX_APP_DEF_KEEPALIVE_COUNT))
        {
            CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
                app->getConfig().getSlave().m_master.getHostName().c_str(),
                app->getConfig().getSlave().m_master.getPort(),
                errno));
            return -1;
        }
    }

    int flags= 1;
    if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
            app->getConfig().getSlave().m_master.getHostName().c_str(),
            app->getConfig().getSlave().m_master.getPort(),
            errno));
        return -1;
    }
    struct linger ling= {0, 0};
    if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
            app->getConfig().getSlave().m_master.getHostName().c_str(),
            app->getConfig().getSlave().m_master.getPort(),
            errno));
        return -1;
    }
    return 0;

}
///设置mq连接的熟悉
int CwxMqApp::setMqSockAttr(CWX_HANDLE handle, void* arg)
{
    CwxMqApp* app = (CwxMqApp*)arg;

    if (app->getConfig().getMq().m_mq.isKeepAlive())
    {
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

    int flags= 1;
    if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
            app->getConfig().getMq().m_mq.getHostName().c_str(),
            app->getConfig().getMq().m_mq.getPort(),
            errno));
        return -1;
    }
    struct linger ling= {0, 0};
    if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
            app->getConfig().getMq().m_mq.getHostName().c_str(),
            app->getConfig().getMq().m_mq.getPort(),
            errno));
        return -1;
    }
    return 0;
}
