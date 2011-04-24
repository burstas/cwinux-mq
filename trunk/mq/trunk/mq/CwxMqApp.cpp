#include "CwxMqApp.h"
#include "CwxDate.h"
#include "linux/tcp.h"
///构造函数
CwxMqApp::CwxMqApp()
{
    m_bFirstBinLog =  true;
    m_ttLastCommitTime = 0;
    m_uiUnCommitLogNum = 0;
    m_ttMqLastCommitTime = 0;
    m_uiMqUnCommitLogNum = 0;
    m_uiCurSid = 0;
    m_pBinLogMgr = NULL;
    m_pMasterHandler = NULL;
    m_pBinRecvHandler = NULL;
    m_pMcRecvHandler = NULL;
    m_sysFile = NULL;
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
    setLogLevel(CwxLogger::LEVEL_DEBUG|CwxLogger::LEVEL_ERROR|CwxLogger::LEVEL_INFO|CwxLogger::LEVEL_WARNING);
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
        if (m_config.getMaster().m_recv_bin.getHostName().length())
        {
            m_pBinRecvHandler = new CwxMqBinRecvHandler(this);
            getCommander().regHandle(SVR_TYPE_RECV_BIN, m_pBinRecvHandler);
        }
        if (m_config.getMaster().m_recv_mc.getHostName().length())
        {
            m_pMcRecvHandler = new CwxMqMcRecvHandler(this);
            getCommander().regHandle(SVR_TYPE_RECV_MC, m_pMcRecvHandler);
        }
    }else{
        ///注册slave的master数据接收handler
        m_pMasterHandler = new CwxMqMasterHandler(this);
        getCommander().regHandle(SVR_TYPE_MASTER_BIN, m_pMasterHandler);
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
        if (m_config.getMaster().m_async_bin.getHostName().length() ||
            m_config.getMaster().m_async_mc.getHostName().length())
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
        if (m_config.getSlave().m_async_bin.getHostName().length() ||
            m_config.getSlave().m_async_mc.getHostName().length())
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
    }
    //创建mq线程池
    updateAppRunState();
    return 0;
}

///时钟函数
void CwxMqApp::onTime(CwxTimeValue const& current)
{
    ///调用基类的onTime函数
    CwxAppFramework::onTime(current);
    ///检查超时
    static time_t ttLastTime = time(NULL);
    if (m_config.getCommon().m_bMaster && (time(NULL) >= ttLastTime + 1))
    {
        ttLastTime = time(NULL);
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(SVR_TYPE_RECV_BIN);
        pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
        //将超时检查事件，放入事件队列
        m_pRecvThreadPool->append(pBlock);
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
    if ((SVR_TYPE_ASYNC_BIN == uiSvrId) || (SVR_TYPE_ASYNC_MC == uiSvrId))
    {
        m_pAsyncDispThreadPool->append(msg);
    }
    else if ((SVR_TYPE_FETCH_BIN == uiSvrId) || (SVR_TYPE_FETCH_MC == uiSvrId))
    {
        m_pMqThreadPool->append(msg);
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
    if ((SVR_TYPE_RECV_BIN == conn.getConnInfo().getSvrId()) ||
        (SVR_TYPE_RECV_MC == conn.getConnInfo().getSvrId()) ||
        (SVR_TYPE_MASTER_BIN == conn.getConnInfo().getSvrId()))
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
    else if (SVR_TYPE_MONITOR != conn.getConnInfo().getSvrId())
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
    if ((SVR_TYPE_RECV_BIN == conn.getConnInfo().getSvrId()) ||
        (SVR_TYPE_RECV_MC == conn.getConnInfo().getSvrId()) ||
        (SVR_TYPE_MASTER_BIN == conn.getConnInfo().getSvrId()))
    {
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(conn.getConnInfo().getSvrId());
        pBlock->event().setHostId(conn.getConnInfo().getHostId());
        pBlock->event().setConnId(conn.getConnInfo().getConnId());
        ///设置事件类型
        pBlock->event().setEvent(CwxEventInfo::CONN_CLOSED);
        m_pRecvThreadPool->append(pBlock);
    }
    else if (SVR_TYPE_MONITOR != conn.getConnInfo().getSvrId())
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
    if ((SVR_TYPE_RECV_BIN == conn.getConnInfo().getSvrId()) ||
        (SVR_TYPE_MASTER_BIN == conn.getConnInfo().getSvrId()))
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
    if(SVR_TYPE_RECV_MC == conn.getConnInfo().getSvrId())
    {
        return 0;
    }else if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId())
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
    if (m_pMcRecvHandler)
    {
        delete m_pMcRecvHandler;
        m_pMcRecvHandler = NULL;
    }
    if (m_pBinLogMgr)
    {
        m_pBinLogMgr->commit();
        delete m_pBinLogMgr;
        m_pBinLogMgr = NULL;
    }
    if (m_sysFile)
    {
        m_sysFile->commit();
        delete m_sysFile;
        m_sysFile = NULL;
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
        m_pBinLogMgr = new CwxBinLogMgr(m_config.getBinLog().m_strBinlogPath.c_str(),
            m_config.getBinLog().m_strBinlogPrex.c_str(),
            ullBinLogSize,
            m_config.getBinLog().m_bDelOutdayLogFile);
        if (0 != m_pBinLogMgr->init(m_config.getBinLog().m_uiMgrMaxDay,
            CWX_TSS_2K_BUF))
        {///<如果失败，则返回-1
            CWX_ERROR(("Failure to init binlog manager, error:%s", CWX_TSS_2K_BUF));
            return -1;
        }
        m_bFirstBinLog = true;
        m_ttLastCommitTime = time(NULL);
        m_uiUnCommitLogNum = 0;
        ///提取sid
        if (m_config.getBinLog().m_uiFlushNum > MAX_SKIP_SID_COUNT)
        {
            m_uiCurSid = m_pBinLogMgr->getMaxSid() + MAX_SKIP_SID_COUNT;
        }
        else if (m_config.getBinLog().m_uiFlushNum < MIN_SKIP_SID_COUNT)
        {
            m_uiCurSid = m_pBinLogMgr->getMaxSid() + MIN_SKIP_SID_COUNT;
        }
        else
        {
            m_uiCurSid = m_pBinLogMgr->getMaxSid() + m_config.getBinLog().m_uiFlushNum;
        }
    }
    //初始化MQ
    if (m_config.getMq().m_queues.size())
    {
        ///初始化mq分发的系统文件
        {
            m_ttMqLastCommitTime = time(NULL);
            m_uiMqUnCommitLogNum = 0;
            if (m_sysFile) delete m_sysFile;
            set<string> queues;
            map<string, CwxMqConfigQueue>::const_iterator iter =  m_config.getMq().m_queues.begin();
            while(iter != m_config.getMq().m_queues.end())
            {
                queues.insert(iter->first);
                iter ++;
            }
            m_sysFile = new CwxMqSysFile(100, m_config.getBinLog().m_strBinlogPath + ".cwx_mq_fetch_pos.dat");
            if (0 != m_sysFile->init(queues))
            {
                CWX_ERROR(("Failure to init sys file, err:%s", m_sysFile->getErrMsg()));
                return -1;
            }
        }
        //初始化队列管理器
        if (m_queueMgr) delete m_queueMgr;
        m_queueMgr = new CwxMqQueueMgr();
        if (0 != m_queueMgr->init(m_pBinLogMgr,
            m_sysFile->getQueues(),
            m_config.getMq().m_queues))
        {
            CWX_ERROR(("Failure to init mq queue manager"));
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
        if (m_config.getMaster().m_recv_bin.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_RECV_BIN, 
                m_config.getMaster().m_recv_bin.getHostName().c_str(),
                m_config.getMaster().m_recv_bin.getPort(),
                false,
                m_config.getMaster().m_recv_bin.isKeepAlive()))
            {
                CWX_ERROR(("Can't register the recv tcp accept listen: addr=%s, port=%d",
                    m_config.getMaster().m_recv_bin.getHostName().c_str(),
                    m_config.getMaster().m_recv_bin.getPort()));
                return -1;
            }
        }
        if (m_config.getMaster().m_recv_bin.getUnixDomain().length())
        {
            if (0 > this->noticeLsockListen(SVR_TYPE_RECV_BIN,
                m_config.getMaster().m_recv_bin.getUnixDomain().c_str(),
                false,
                m_config.getMaster().m_recv_bin.isKeepAlive()))
            {
                CWX_ERROR(("Can't register the recv unix-domain accept listen: path-file=%s",
                    m_config.getMaster().m_recv_bin.getUnixDomain().c_str()));
                return -1;
            }
        }

        ///打开mc协议消息接收的listen
        if (m_config.getMaster().m_recv_mc.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_RECV_MC, 
                m_config.getMaster().m_recv_mc.getHostName().c_str(),
                m_config.getMaster().m_recv_mc.getPort(),
                true,
                m_config.getMaster().m_recv_mc.isKeepAlive()))
            {
                CWX_ERROR(("Can't register the recv tcp accept listen: addr=%s, port=%d",
                    m_config.getMaster().m_recv_mc.getHostName().c_str(),
                    m_config.getMaster().m_recv_mc.getPort()));
                return -1;
            }
        }
        if (m_config.getMaster().m_recv_mc.getUnixDomain().length())
        {
            if (0 > this->noticeLsockListen(SVR_TYPE_RECV_MC,
                m_config.getMaster().m_recv_mc.getUnixDomain().c_str(),
                true,
                m_config.getMaster().m_recv_mc.isKeepAlive()))
            {
                CWX_ERROR(("Can't register the recv unix-domain accept listen: path-file=%s",
                    m_config.getMaster().m_recv_mc.getUnixDomain().c_str()));
                return -1;
            }
        }

        ///打开bin协议异步分发
        if (m_config.getMaster().m_async_bin.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_ASYNC_BIN, 
                m_config.getMaster().m_async_bin.getHostName().c_str(),
                m_config.getMaster().m_async_bin.getPort(),
                true,
                m_config.getMaster().m_async_bin.isKeepAlive(),
                CWX_APP_EVENT_MODE,
                m_config.getCommon().m_uiSockBufSize * 1024,
                m_config.getCommon().m_uiSockBufSize * 1024
                ))
            {
                CWX_ERROR(("Can't register the async-dispatch tcp accept listen: addr=%s, port=%d",
                    m_config.getMaster().m_async_bin.getHostName().c_str(),
                    m_config.getMaster().m_async_bin.getPort()));
                return -1;
            }
        }
        if (m_config.getMaster().m_async_bin.getUnixDomain().length())
        {
            if (0 > this->noticeLsockListen(SVR_TYPE_ASYNC_BIN,
                m_config.getMaster().m_async_bin.getUnixDomain().c_str(),
                true,
                m_config.getMaster().m_async_bin.isKeepAlive(),
                CWX_APP_EVENT_MODE))
            {
                CWX_ERROR(("Can't register the async-dispatch unix-domain accept listen: path-file=%s",
                    m_config.getMaster().m_async_bin.getUnixDomain().c_str()));
                return -1;
            }
        }

        ///打开mc协议异步分发
        if (m_config.getMaster().m_async_mc.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_ASYNC_MC, 
                m_config.getMaster().m_async_mc.getHostName().c_str(),
                m_config.getMaster().m_async_mc.getPort(),
                true,
                m_config.getMaster().m_async_mc.isKeepAlive(),
                CWX_APP_EVENT_MODE,
                m_config.getCommon().m_uiSockBufSize * 1024,
                m_config.getCommon().m_uiSockBufSize * 1024
                ))
            {
                CWX_ERROR(("Can't register the async-dispatch tcp accept listen: addr=%s, port=%d",
                    m_config.getMaster().m_async_mc.getHostName().c_str(),
                    m_config.getMaster().m_async_mc.getPort()));
                return -1;
            }
        }
        if (m_config.getMaster().m_async_mc.getUnixDomain().length())
        {
            if (0 > this->noticeLsockListen(SVR_TYPE_ASYNC_MC,
                m_config.getMaster().m_async_mc.getUnixDomain().c_str(),
                true,
                m_config.getMaster().m_async_mc.isKeepAlive(),
                CWX_APP_EVENT_MODE))
            {
                CWX_ERROR(("Can't register the async-dispatch unix-domain accept listen: path-file=%s",
                    m_config.getMaster().m_async_mc.getUnixDomain().c_str()));
                return -1;
            }
        }
    }
    else
    {
        ///连接bin协议master
        if (m_config.getSlave().m_master_bin.getUnixDomain().length())
        {
            if (0 > this->noticeLsockConnect(SVR_TYPE_MASTER_BIN, 0, 
                m_config.getSlave().m_master_bin.getUnixDomain().c_str(),
                false,
                true,
                1,
                2))
            {
                CWX_ERROR(("Can't register the master unix-domain connect: path-file=%s",
                    m_config.getSlave().m_master_bin.getUnixDomain().c_str()));
                return -1;
            }
        }
        else if (m_config.getSlave().m_master_bin.getHostName().length())
        {
            if (0 > this->noticeTcpConnect(SVR_TYPE_MASTER_BIN, 0, 
                m_config.getSlave().m_master_bin.getHostName().c_str(),
                m_config.getSlave().m_master_bin.getPort(),
                false,
                m_config.getSlave().m_master_bin.isKeepAlive(),
                1,
                2,
                m_config.getCommon().m_uiSockBufSize * 1024,
                m_config.getCommon().m_uiSockBufSize * 1024))
            {
                CWX_ERROR(("Can't register the master tcp connect: addr=%s, port=%d",
                    m_config.getSlave().m_master_bin.getHostName().c_str(),
                    m_config.getSlave().m_master_bin.getPort()));
                return -1;
            }
        }
        else
        {
            CWX_ERROR(("Not config master dispatch"));
            return -1;
        }
        ///bin协议异步分发
        if (m_config.getSlave().m_async_bin.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_ASYNC_BIN, 
                m_config.getSlave().m_async_bin.getHostName().c_str(),
                m_config.getSlave().m_async_bin.getPort(),
                true,
                true,
                CWX_APP_EVENT_MODE,
                m_config.getCommon().m_uiSockBufSize * 1024,
                m_config.getCommon().m_uiSockBufSize * 1024))
            {
                CWX_ERROR(("Can't register the async-dispatch tcp accept listen: addr=%s, port=%d",
                    m_config.getSlave().m_async_bin.getHostName().c_str(),
                    m_config.getSlave().m_async_bin.getPort()));
                return -1;
            }
        }
        if (m_config.getSlave().m_async_bin.getUnixDomain().length())
        {
            if (0 > this->noticeLsockListen(SVR_TYPE_ASYNC_BIN,
                m_config.getSlave().m_async_bin.getUnixDomain().c_str(),
                true,
                false,
                CWX_APP_EVENT_MODE))
            {
                CWX_ERROR(("Can't register the async-dispatch unix-domain accept listen: path-file=%s",
                    m_config.getSlave().m_async_bin.getUnixDomain().c_str()));
                return -1;
            }
        }
        ///mc协议异步分发
        if (m_config.getSlave().m_async_mc.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_ASYNC_MC, 
                m_config.getSlave().m_async_mc.getHostName().c_str(),
                m_config.getSlave().m_async_mc.getPort(),
                true,
                true,
                CWX_APP_EVENT_MODE,
                m_config.getCommon().m_uiSockBufSize * 1024,
                m_config.getCommon().m_uiSockBufSize * 1024))
            {
                CWX_ERROR(("Can't register the async-dispatch tcp accept listen: addr=%s, port=%d",
                    m_config.getSlave().m_async_mc.getHostName().c_str(),
                    m_config.getSlave().m_async_mc.getPort()));
                return -1;
            }
        }
        if (m_config.getSlave().m_async_mc.getUnixDomain().length())
        {
            if (0 > this->noticeLsockListen(SVR_TYPE_ASYNC_MC,
                m_config.getSlave().m_async_mc.getUnixDomain().c_str(),
                true,
                false,
                CWX_APP_EVENT_MODE))
            {
                CWX_ERROR(("Can't register the async-dispatch unix-domain accept listen: path-file=%s",
                    m_config.getSlave().m_async_mc.getUnixDomain().c_str()));
                return -1;
            }
        }
    }
    //打开bin mq获取的监听端口
    if (m_config.getMq().m_binListen.getHostName().length())
    {
        if (0 > this->noticeTcpListen(SVR_TYPE_FETCH_BIN, 
            m_config.getMq().m_binListen.getHostName().c_str(),
            m_config.getMq().m_binListen.getPort(),
            true,
            m_config.getMq().m_binListen.isKeepAlive(),
            CWX_APP_EVENT_MODE,
            m_config.getCommon().m_uiSockBufSize * 1024,
            m_config.getCommon().m_uiSockBufSize * 1024))
        {
            CWX_ERROR(("Can't register the mq-fetch tcp accept listen: addr=%s, port=%d",
                m_config.getMq().m_binListen.getHostName().c_str(),
                m_config.getMq().m_binListen.getPort()));
            return -1;
        }
    }
    if (m_config.getMq().m_binListen.getUnixDomain().length())
    {
        if (0 > this->noticeLsockListen(SVR_TYPE_FETCH_BIN,
            m_config.getMq().m_binListen.getUnixDomain().c_str(),
            true,
            false,
            CWX_APP_EVENT_MODE))
        {
            CWX_ERROR(("Can't register the mq-fetch unix-domain accept listen: path-file=%s",
                m_config.getMq().m_binListen.getUnixDomain().c_str()));
            return -1;
        }
    }
    //打开mc mq获取的监听端口
    if (m_config.getMq().m_mcListen.getHostName().length())
    {
        if (0 > this->noticeTcpListen(SVR_TYPE_FETCH_BIN, 
            m_config.getMq().m_mcListen.getHostName().c_str(),
            m_config.getMq().m_mcListen.getPort(),
            true,
            m_config.getMq().m_mcListen.isKeepAlive(),
            CWX_APP_EVENT_MODE,
            m_config.getCommon().m_uiSockBufSize * 1024,
            m_config.getCommon().m_uiSockBufSize * 1024))
        {
            CWX_ERROR(("Can't register the mq-fetch tcp accept listen: addr=%s, port=%d",
                m_config.getMq().m_mcListen.getHostName().c_str(),
                m_config.getMq().m_mcListen.getPort()));
            return -1;
        }
    }
    if (m_config.getMq().m_mcListen.getUnixDomain().length())
    {
        if (0 > this->noticeLsockListen(SVR_TYPE_FETCH_BIN,
            m_config.getMq().m_mcListen.getUnixDomain().c_str(),
            true,
            false,
            CWX_APP_EVENT_MODE))
        {
            CWX_ERROR(("Can't register the mq-fetch unix-domain accept listen: path-file=%s",
                m_config.getMq().m_mcListen.getUnixDomain().c_str()));
            return -1;
        }
    }

    return 0;
}

int CwxMqApp::commit_mq(char* szErr2K)
{
    int iRet = 0;
    CWX_INFO(("Begin flush mq sys file......."));
    if (getMqUncommitNum())
    {
        iRet = getSysFile()->commit();
        if ((0 != iRet) && szErr2K) strcpy(szErr2K, getSysFile()->getErrMsg());
        setMqUncommitNum(0);
        setMqLastCommitTime(time(NULL));
    }
    CWX_INFO(("End flush mq sys file......."));
    return iRet;
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
                return -1;
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
        CWX_UINT64 ullMqSid = 0;
        CWX_UINT64 ullMqNum = 0;
        CwxMqQueue* pQueue = 0;
        char szSid1[64];
        char szSid2[64];
        map<string, CwxMqConfigQueue>::const_iterator iter = getConfig().getMq().m_queues.begin();
        while(iter != getConfig().getMq().m_queues.end())
        {
            pQueue = getQueueMgr()->getQueue(iter->first);
            ullMqSid = pQueue->getCurSid(); 
            ullMqNum = pQueue->getMqNum();
            CwxCommon::toString(ullMqSid, szSid1, 10);
            CwxCommon::toString(ullMqNum, szSid2, 10);
            CwxCommon::snprintf(szLine, 4096, "STAT mq name(%s):sid(%s):mq_num(%s)\r\n",
                iter->first.c_str(), szSid1, szSid2);
            MQ_MONITOR_APPEND();
            iter++;
        }
    }
    while(0);
    strcpy(m_szBuf + uiPos, "END\r\n");
    return strlen(m_szBuf);

}

///分发channel的线程函数，arg为app对象
void* CwxMqApp::DispatchThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg)
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
        if (-1 == app->getAsyncDispChannel()->dispatch(5))
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
            CWX_ASSERT((block->event().getSvrId() == SVR_TYPE_ASYNC_BIN) ||
                (block->event().getSvrId() == SVR_TYPE_ASYNC_MC));

            if (channel->isRegIoHandle(block->event().getIoHandle()))
            {
                CWX_ERROR(("Handler[%] is register", block->event().getIoHandle()));
                break;
            }
            if (block->event().getSvrId() == SVR_TYPE_ASYNC_BIN)
            {
                handler = new CwxMqBinAsyncHandler(app, channel);
            }
            else if (block->event().getSvrId() == SVR_TYPE_ASYNC_MC)
            {
                handler = new CwxMqMcAsyncHandler(app, channel);
            }
            else
            {
                CWX_ERROR(("Invalid svr_type[%d], close handle[%d]", block->event().getSvrId(), block->event().getIoHandle()));
                ::close(block->event().getIoHandle());
            }
            handler->setHandle(block->event().getIoHandle());
            if (0 != channel->registerHandler(handler->getHandle(),
                handler,
                CwxAppHandler4Base::PREAD_MASK))
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
void* CwxMqApp::MqThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg)
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
        if (-1 == app->getMqChannel()->dispatch(5))
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
            CWX_ASSERT(block->event().getEvent() == CwxEventInfo::CONN_CREATED);
            CWX_ASSERT((block->event().getSvrId() == SVR_TYPE_FETCH_BIN) ||
                       (block->event().getSvrId() == SVR_TYPE_FETCH_MC));

            if (channel->isRegIoHandle(block->event().getIoHandle()))
            {
                CWX_ERROR(("Handler[%] is register", block->event().getIoHandle()));
                break;
            }
            if (block->event().getSvrId() == SVR_TYPE_FETCH_BIN)
            {
                handler = new CwxMqBinFetchHandler(app, channel);
            }
            else if (block->event().getSvrId() == SVR_TYPE_FETCH_MC)
            {
                handler = new CwxMqMcFetchHandler(app, channel);
            }
            else
            {
                CWX_ERROR(("Invalid svr_type[%d], close handle[%d]", block->event().getSvrId(), block->event().getIoHandle()));
                ::close(block->event().getIoHandle());
            }
            handler->setHandle(block->event().getIoHandle());
            if (0 != channel->registerHandler(handler->getHandle(),
                handler,
                CwxAppHandler4Base::PREAD_MASK))
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
