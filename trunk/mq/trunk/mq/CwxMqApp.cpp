#include "CwxMqApp.h"
#include "CwxDate.h"
#include "linux/tcp.h"
///构造函数
CwxMqApp::CwxMqApp()
{
    m_uiCurSid = 1; ///<最小的sid为1
    m_pBinLogMgr = NULL;
    m_pAsyncHandler = NULL;
    m_pMasterHandler = NULL;
    m_pRecvHandler = NULL;
    m_pFetchHandler = NULL;
    m_pWriteThreadPool = NULL;
    m_sysFile = NULL;
    m_queueMgr = NULL;
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
    setLogLevel(CwxAppLogger::LEVEL_DEBUG|CwxAppLogger::LEVEL_ERROR|CwxAppLogger::LEVEL_INFO|CwxAppLogger::LEVEL_WARNING);
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
        m_pRecvHandler = new CwxMqRecvHandler(this);
        getCommander().regHandle(SVR_TYPE_RECV, m_pRecvHandler);
    }else{
        ///注册slave的master数据接收handler
        m_pMasterHandler = new CwxMqMasterHandler(this);
        getCommander().regHandle(SVR_TYPE_MASTER, m_pMasterHandler);
    }
    ///创建异步分发的handler
    m_pAsyncHandler = new CwxMqAsyncHandler(this);
    getCommander().regHandle(SVR_TYPE_ASYNC, m_pAsyncHandler);
    ///创建mq的获取handler
    m_pFetchHandler = new CwxMqFetchHandler(this);
    getCommander().regHandle(SVR_TYPE_FETCH, m_pFetchHandler);

    ///启动网络连接与监听
    if (0 != startNetwork()) return -1;

    ///创建recv线程池对象，此线程池中线程的group-id为THREAD_GROUP_USER_START，
    ///线程池的线程数量为1。
    m_pWriteThreadPool = new CwxAppThreadPool(this,
        CwxAppFramework::THREAD_GROUP_USER_START,
        1);
    ///创建线程的tss对象
    CwxAppTss** pTss = new CwxAppTss*[1];
    pTss[0] = new CwxMqTss();
    ((CwxMqTss*)pTss[0])->init();
    ///启动线程
    if ( 0 != m_pWriteThreadPool->start(pTss))
    {
        CWX_ERROR(("Failure to start recv thread pool"));
        return -1;
    }
    ///更新服务的状态
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
        pBlock->event().setSvrId(SVR_TYPE_RECV);
        pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
        //将超时检查事件，放入事件队列
        m_pWriteThreadPool->append(pBlock);
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

///连接建立
int CwxMqApp::onConnCreated(CwxAppHandler4Msg& conn, bool& , bool& )
{
    if (SVR_TYPE_MONITOR != conn.getConnInfo().getSvrId())
    {
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(conn.getConnInfo().getSvrId());
        pBlock->event().setHostId(conn.getConnInfo().getHostId());
        pBlock->event().setConnId(conn.getConnInfo().getConnId());
        ///设置事件类型
        pBlock->event().setEvent(CwxEventInfo::CONN_CREATED);
        ///将事件添加到消息队列
        m_pWriteThreadPool->append(pBlock);
        ///消息包连续发送数量
        if (SVR_TYPE_ASYNC == conn.getConnInfo().getSvrId())
        {
            conn.getConnInfo().setContinueSendNum(m_config.getCommon().m_uiDispatchWindowSize);
        }
        else if (SVR_TYPE_MASTER == conn.getConnInfo().getSvrId())
        {
            conn.getConnInfo().setContinueSendNum(m_config.getCommon().m_uiFromMasterWindowSize);
        }
    }
    else
    {///如果是监控的连接建立，则建立一个string的buf，用于缓存不完整的命令
        string* buf = new string();
        conn.getConnInfo().setUserData(buf);
    }
    return 0;
}
///连接关闭
int CwxMqApp::onConnClosed(CwxAppHandler4Msg& conn)
{
    if (SVR_TYPE_MONITOR != conn.getConnInfo().getSvrId())
    {
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(conn.getConnInfo().getSvrId());
        pBlock->event().setHostId(conn.getConnInfo().getHostId());
        pBlock->event().setConnId(conn.getConnInfo().getConnId());
        ///设置事件类型
        pBlock->event().setEvent(CwxEventInfo::CONN_CLOSED);
        m_pWriteThreadPool->append(pBlock);
    }
    else
    {///若是监控的连接关闭，则必须释放先前所创建的string对象。
        if (conn.getConnInfo().getUserData())
        {
            delete (string*)conn.getConnInfo().getUserData();
            conn.getConnInfo().setUserData(NULL);
        }
    }
    return 0;
}

///收到消息
int CwxMqApp::onRecvMsg(CwxMsgBlock* msg,
                        CwxAppHandler4Msg& conn,
                        CwxMsgHead const& header,
                        bool& )
{
    if (SVR_TYPE_MONITOR != conn.getConnInfo().getSvrId())
    {
        msg->event().setSvrId(conn.getConnInfo().getSvrId());
        msg->event().setHostId(conn.getConnInfo().getHostId());
        msg->event().setConnId(conn.getConnInfo().getConnId());
        ///保存消息头
        msg->event().setMsgHeader(header);
        ///设置事件类型
        msg->event().setEvent(CwxEventInfo::RECV_MSG);
        ///将消息放到线程池队列中，有内部的线程调用其处理handle来处理
        m_pWriteThreadPool->append(msg);
        if (SVR_TYPE_ASYNC == conn.getConnInfo().getSvrId())
        {
            //        if (conn.getConnInfo().getContinueRecvNum()) CWX_INFO(("Async continue %u", conn.getConnInfo().getContinueRecvNum()));
            return conn.getConnInfo().getContinueRecvNum() < m_config.getCommon().m_uiDispatchWindowSize?1:0;
        }
        else if (SVR_TYPE_MASTER == conn.getConnInfo().getSvrId())
        {
            //        if (conn.getConnInfo().getContinueRecvNum()) CWX_INFO(("Master continue %u", conn.getConnInfo().getContinueRecvNum()));
            return conn.getConnInfo().getContinueRecvNum() < m_config.getCommon().m_uiFromMasterWindowSize?1:0;
        }
    }
    else
    {
        ///监控消息
        return monitorStats(msg, conn);
    }

    return 0;
}

///消息发送完毕
CWX_UINT32 CwxMqApp::onEndSendMsg(CwxMsgBlock*& msg,
                                  CwxAppHandler4Msg& conn)
{
    if ((SVR_TYPE_ASYNC == conn.getConnInfo().getSvrId()) ||
        (SVR_TYPE_FETCH == conn.getConnInfo().getSvrId()))
    { ///<只有ASYNC、SVR_TYPE_FETCH类型，才关心EndSendMsg的事件
        msg->event().setSvrId(conn.getConnInfo().getSvrId());
        msg->event().setHostId(conn.getConnInfo().getHostId());
        msg->event().setConnId(conn.getConnInfo().getConnId());
        msg->event().setEvent(CwxEventInfo::END_SEND_MSG);
        m_pWriteThreadPool->append(msg);
        ///清空msg，防止释放
        msg = NULL;
    }
    return 0;
}

///消息发送失败
void CwxMqApp::onFailSendMsg(CwxMsgBlock*& msg)
{
    if ((SVR_TYPE_ASYNC == msg->send_ctrl().getSvrId()) ||
        (SVR_TYPE_FETCH == msg->send_ctrl().getSvrId()))
    { ///<只有ASYNC、SVR_TYPE_FETCH类型，才关心EndSendMsg的事件
        msg->event().setSvrId(msg->send_ctrl().getSvrId());
        msg->event().setHostId(msg->send_ctrl().getHostId());
        msg->event().setConnId(msg->send_ctrl().getConnId());
        msg->event().setEvent(CwxEventInfo::FAIL_SEND_MSG);
        m_pWriteThreadPool->append(msg);
        ///清空msg，防止释放
        msg = NULL;
    }
}


void CwxMqApp::destroy()
{
    if (m_pWriteThreadPool)
    {
        m_pWriteThreadPool->stop();
        delete m_pWriteThreadPool;
        m_pWriteThreadPool = NULL;
    }
    if (m_queueMgr)
    {
        delete m_queueMgr;
        m_queueMgr = NULL;
    }
    if (m_pAsyncHandler)
    {
        delete m_pAsyncHandler;
        m_pAsyncHandler = NULL;
    }
    if (m_pMasterHandler)
    {
        delete m_pMasterHandler;
        m_pMasterHandler = NULL;
    }
    if (m_pRecvHandler)
    {
        delete m_pRecvHandler;
        m_pRecvHandler = NULL;
    }
    if (m_pFetchHandler)
    {
        delete m_pFetchHandler;
        m_pFetchHandler = NULL;
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
            ullBinLogSize);
        if (0 != m_pBinLogMgr->init(m_config.getBinLog().m_uiMgrMaxDay,
            CWX_APP_TSS_2K_BUF))
        {///<如果失败，则返回-1
            CWX_ERROR(("Failure to init binlog manager, error:%s", CWX_APP_TSS_2K_BUF));
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
    if (0 > this->noticeTcpListen(SVR_TYPE_MONITOR,
        m_config.getCommon().m_monitor.getHostName().c_str(),
        m_config.getCommon().m_monitor.getPort(),
        true,
        1024))
    {
        CWX_ERROR(("Can't register the monitor tcp accept listen: addr=%s, port=%d",
            m_config.getCommon().m_monitor.getHostName().c_str(),
            m_config.getCommon().m_monitor.getPort()));
        return -1;
    }
    ///打开连接
    if (m_config.getCommon().m_bMaster)
    {
        ///打开接收的listen
        if (m_config.getMaster().m_recv.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_RECV, 
                m_config.getMaster().m_recv.getHostName().c_str(),
                m_config.getMaster().m_recv.getPort(),
                false,
                0,
                m_config.getMaster().m_recv.isKeepAlive()))
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
                false,
                0,
                m_config.getMaster().m_recv.isKeepAlive()))
            {
                CWX_ERROR(("Can't register the recv unix-domain accept listen: path-file=%s",
                    m_config.getMaster().m_recv.getUnixDomain().c_str()));
                return -1;
            }
        }
        ///打开异步分发
        if (m_config.getMaster().m_async.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_ASYNC, 
                m_config.getMaster().m_async.getHostName().c_str(),
                m_config.getMaster().m_async.getPort(),
                false,
                0,
                m_config.getMaster().m_async.isKeepAlive(),
                CWX_APP_MSG_MODE,
                256*1024,
                64*1024
                ))
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
                0,
                m_config.getMaster().m_async.isKeepAlive()))
            {
                CWX_ERROR(("Can't register the async-dispatch unix-domain accept listen: path-file=%s",
                    m_config.getMaster().m_async.getUnixDomain().c_str()));
                return -1;
            }
        }
    }
    else
    {
        ///连接master
        if (m_config.getSlave().m_master.getUnixDomain().length())
        {
            if (0 > this->noticeLsockConnect(SVR_TYPE_MASTER, 0, 
                m_config.getSlave().m_master.getUnixDomain().c_str(),
                false,
                0,
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
                0,
                m_config.getSlave().m_master.isKeepAlive(),
                1,
                2,
                64*1024,
                256*1024))
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
        ///异步分发
        if (m_config.getSlave().m_async.getHostName().length())
        {
            if (0 > this->noticeTcpListen(SVR_TYPE_ASYNC, 
                m_config.getSlave().m_async.getHostName().c_str(),
                m_config.getSlave().m_async.getPort(),
                false,
                0,
                m_config.getSlave().m_async.isKeepAlive()))
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
                0,
                m_config.getSlave().m_async.isKeepAlive()))
            {
                CWX_ERROR(("Can't register the async-dispatch unix-domain accept listen: path-file=%s",
                    m_config.getSlave().m_async.getUnixDomain().c_str()));
                return -1;
            }
        }
    }
    //打开mq获取的监听端口
    ///异步分发
    if (m_config.getMq().m_listen.getHostName().length())
    {
        if (0 > this->noticeTcpListen(SVR_TYPE_FETCH, 
            m_config.getMq().m_listen.getHostName().c_str(),
            m_config.getMq().m_listen.getPort(),
            false,
            0,
            m_config.getMq().m_listen.isKeepAlive()))
        {
            CWX_ERROR(("Can't register the mq-fetch tcp accept listen: addr=%s, port=%d",
                m_config.getMq().m_listen.getHostName().c_str(),
                m_config.getMq().m_listen.getPort()));
            return -1;
        }
    }
    if (m_config.getMq().m_listen.getUnixDomain().length())
    {
        if (0 > this->noticeLsockListen(SVR_TYPE_FETCH,
            m_config.getMq().m_listen.getUnixDomain().c_str(),
            false,
            0,
            m_config.getMq().m_listen.isKeepAlive()))
        {
            CWX_ERROR(("Can't register the mq-fetch unix-domain accept listen: path-file=%s",
                m_config.getMq().m_listen.getUnixDomain().c_str()));
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

///分发新的binlog
void CwxMqApp::dispathWaitingBinlog(CwxMqTss* pTss)
{
    m_pAsyncHandler->dispatch(pTss);
    m_pFetchHandler->dispatch(pTss);
}

int CwxMqApp::monitorStats(CwxMsgBlock* msg, CwxAppHandler4Msg& conn)
{
    string* strCmd = (string*)conn.getConnInfo().getUserData();
    strCmd->append(msg->rd_ptr(), msg->length());
    CwxMsgBlockAlloc::free(msg);
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

