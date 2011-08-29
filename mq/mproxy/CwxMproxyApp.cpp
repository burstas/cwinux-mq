#include "CwxMproxyApp.h"

///构造函数，初始化发送的echo数据内容
CwxMproxyApp::CwxMproxyApp()
:CwxAppFramework(CwxAppFramework::APP_MODE_TWIN, 1024 * 64)
{
    m_uiTaskId = 0;
    m_threadPool = NULL;
    m_pRecvHandle = NULL;
    m_pMqHandle = NULL;
    m_uiMqConnId = CWX_APP_INVALID_CONN_ID;
}
///析构函数
CwxMproxyApp::~CwxMproxyApp()
{
}

///初始化APP，加载配置文件
int CwxMproxyApp::init(int argc, char** argv)
{
    string strErrMsg;
    ///首先调用架构的init
    if (CwxAppFramework::init(argc, argv) == -1) return -1;
    ///若没有通过-f指定配置文件，则采用默认的配置文件
    if ((NULL == this->getConfFile()) || (strlen(this->getConfFile()) == 0))
    {
        this->setConfFile("svr_conf.xml");
    }
    ///加载配置文件
    if (0 != m_config.loadConfig(getConfFile()))
    {
        CWX_ERROR((m_config.getError()));
        return -1;
    }
    ///设置输出运行日志的level
    setLogLevel(CwxLogger::LEVEL_ERROR|CwxLogger::LEVEL_INFO|CwxLogger::LEVEL_WARNING);
    return 0;
}

//init the Enviroment before run.0:success, -1:failure.
int CwxMproxyApp::initRunEnv()
{
    ///设置时钟的刻度，最小为1ms，此为1s。
    this->setClick(1000);//1s
    //set work dir
    this->setWorkDir(this->m_config.m_strWorkDir.c_str());
    //Set log file
    this->setLogFileNum(LOG_FILE_NUM);
    this->setLogFileSize(LOG_FILE_SIZE*1024*1024);
    ///调用架构的initRunEnv，使设置的参数生效
    if (CwxAppFramework::initRunEnv() == -1 ) return -1;
    //set version
    this->setAppVersion(CWX_MPROXY_APP_VERSION);
    //set last modify date
    this->setLastModifyDatetime(CWX_MPROXY_MODIFY_DATE);
    //set compile date
    this->setLastCompileDatetime(CWX_COMPILE_DATE(_BUILD_DATE));
    ///设置启动时间
    CwxDate::getDateY4MDHMS2(time(NULL), m_strStartTime);

    //output config
    m_config.outputConfig();
    ///创建代理消息的处理handle
    m_pRecvHandle = new CwxMproxyRecvHandler(this);
    ///注册handle
    getCommander().regHandle(SVR_TYPE_RECV, m_pRecvHandle);
    ///创建mq消息的处理handle
    m_pMqHandle = new CwxMproxyMqHandler(this);
    getCommander().regHandle(SVR_TYPE_MQ, m_pMqHandle);
    ///打开monitor监听端口
    if (m_config.m_monitor.getHostName().length())
    {
        ///打开监听的服务器端口号
        if (0 > this->noticeTcpListen(SVR_TYPE_MONITOR,
            m_config.m_monitor.getHostName().c_str(),
            m_config.m_monitor.getPort(),
            true))
        {
            CWX_ERROR(("Can't register the monitor tcp accept listen: addr=%s, port=%d",
                m_config.m_monitor.getHostName().c_str(),
                m_config.m_monitor.getPort()));
            return -1;
        }
    }
    //打开代理消息的监听端口
    if (m_config.m_recv.getHostName().length())
    {
        if (-1 == noticeTcpListen(SVR_TYPE_RECV,
            m_config.m_recv.getHostName().c_str(),
            m_config.m_recv.getPort(),
            false,
            CWX_APP_MSG_MODE,
            CwxMproxyApp::setRecvSockAttr,
            this
            ))
        {
            CWX_ERROR(("Failure to register proxy mq listen, ip=%s, port=%u",
                m_config.m_recv.getHostName().c_str(),
                m_config.m_recv.getPort()));
            return -1;
        }
    }
    if (m_config.m_recv.getUnixDomain().length())
    {
        if (-1 == noticeLsockListen(SVR_TYPE_RECV,
            m_config.m_recv.getUnixDomain().c_str()))
        {
            CWX_ERROR(("Failure to register proxy mq listen, unix-file%s",
                m_config.m_recv.getUnixDomain().c_str()));
            return -1;
        }
    }
    //连接mq
    if (m_config.m_mq.getUnixDomain().length())
    {
        if (0 > noticeLsockConnect(SVR_TYPE_MQ,
            0,
            m_config.m_mq.getUnixDomain().c_str(),
            false,
            1,
            2))
        {
            CWX_ERROR(("Failure to connect to mq, unix-file:%s",
                m_config.m_mq.getUnixDomain().c_str()));
            return -1;
        }
    }
    else if (m_config.m_mq.getHostName().length())
    {
        if (0 > noticeTcpConnect(SVR_TYPE_MQ,
            0,
            m_config.m_mq.getHostName().c_str(),
            m_config.m_mq.getPort(),
            false,
            1,
            2,
            CwxMproxyApp::setMqSockAttr,
            this))
        {
            CWX_ERROR(("Failure to connect to mq, ip=%s, port=%u",
                m_config.m_mq.getHostName().c_str(),
                m_config.m_mq.getPort()));
            return -1;
        }
    }
    else
    {
        CWX_ERROR(("Can't configure mq's address by ip or unix-file"));
        return -1;
    }
    m_uiMqConnId = CWX_APP_INVALID_CONN_ID;

    ///创建线程池对象，此线程池中线程的group-id为THREAD_GROUP_USER_START
    m_threadPool = new CwxThreadPoolEx(CwxAppFramework::THREAD_GROUP_USER_START,
        1,
        getThreadPoolMgr(),
        &getCommander());
    ///创建线程的tss对象
    CwxTss** pTss = new CwxTss*[1];
    pTss[0] = new CwxMqTss();
    ((CwxMqTss*)pTss[0])->init();
    ///启动线程。
    if ( 0 != m_threadPool->start(pTss))
    {
        CWX_ERROR(("Failure to start thread pool"));
        return -1;
    }
    return 0;
}

///时钟响应函数，什么也没有做
void CwxMproxyApp::onTime(CwxTimeValue const& current)
{
    static CWX_UINT64 ullLastTime = CwxDate::getTimestamp();
    CwxAppFramework::onTime(current);
    if (current.to_usec() > ullLastTime + 1000000)
    {//1s
        ///形成超时检查事件，由CwmCenterUiQuery的onTimeoutCheck的响应
        ullLastTime = current.to_usec();
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(SVR_TYPE_RECV);
        pBlock->event().setHostId(0);
        pBlock->event().setConnId(0);
        pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
        m_threadPool->append(pBlock, 0);
    }
}

///信号处理函数
void CwxMproxyApp::onSignal(int signum)
{
    switch(signum)
    {
    case SIGQUIT: 
        CWX_INFO(("Recv exit signal, exit right now."));
        this->stop();
        break;
    default:
        ///其他信号，忽略
        CWX_INFO(("Recv signal=%d, ignore it.", signum));
        break;
    }
}
//连接建立函数
int CwxMproxyApp::onConnCreated(CwxAppHandler4Msg& conn, bool& , bool& )
{
    if (SVR_TYPE_RECV == conn.getConnInfo().getSvrId())
    {
        bool* bAuth = new bool;
        *bAuth = false;
        conn.getConnInfo().setUserData((void*)bAuth);
    } 
    else if (SVR_TYPE_MQ == conn.getConnInfo().getSvrId())
    {
        m_uiMqConnId = conn.getConnInfo().getConnId();
    }
    else if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId())
    {
        string* buf = new string();
        conn.getConnInfo().setUserData(buf);
    }
    return 0;
}

int CwxMproxyApp::onRecvMsg(CwxMsgBlock* msg, CwxAppHandler4Msg & conn, CwxMsgHead const& header, bool& )
{

    msg->event().setSvrId(conn.getConnInfo().getSvrId());
    msg->event().setHostId(conn.getConnInfo().getHostId());
    msg->event().setConnId(conn.getConnInfo().getConnId());
    msg->event().setEvent(CwxEventInfo::RECV_MSG);
    msg->event().setMsgHeader(header);
    msg->event().setTimestamp(CwxDate::getTimestamp());
    msg->event().setConnUserData(conn.getConnInfo().getUserData());
    m_threadPool->append(msg, conn.getConnInfo().getConnId());
    return 0;
}
///收到消息的响应函数
int CwxMproxyApp::onRecvMsg(CwxAppHandler4Msg& conn,
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

int CwxMproxyApp::onConnClosed(CwxAppHandler4Msg& conn)
{
    if ((SVR_TYPE_MQ == conn.getConnInfo().getSvrId())||
        (SVR_TYPE_RECV == conn.getConnInfo().getSvrId()))
    {
        CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
        pBlock->event().setSvrId(conn.getConnInfo().getSvrId());
        pBlock->event().setHostId(conn.getConnInfo().getHostId());
        pBlock->event().setConnId(conn.getConnInfo().getConnId());
        pBlock->event().setEvent(CwxEventInfo::CONN_CLOSED);
        pBlock->event().setConnUserData(conn.getConnInfo().getUserData());
        m_threadPool->append(pBlock, conn.getConnInfo().getConnId());
    }
    else if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId())
    {
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

CWX_UINT32 CwxMproxyApp::onEndSendMsg(CwxMsgBlock*& msg,
                                CwxAppHandler4Msg & conn)
{
    if (SVR_TYPE_MQ == conn.getConnInfo().getSvrId())
    {
        msg->event().setSvrId(conn.getConnInfo().getSvrId());
        msg->event().setHostId(conn.getConnInfo().getHostId());
        msg->event().setConnId(conn.getConnInfo().getConnId());
        msg->event().setEvent(CwxEventInfo::END_SEND_MSG);
        m_threadPool->append(msg, conn.getConnInfo().getConnId());
        msg = NULL;
    }
    return 0;
}
void CwxMproxyApp::onFailSendMsg(CwxMsgBlock*& msg)
{
    if (SVR_TYPE_MQ == msg->send_ctrl().getSvrId())
    {
        msg->event().setSvrId(msg->send_ctrl().getSvrId());
        msg->event().setHostId(msg->send_ctrl().getHostId());
        msg->event().setConnId(msg->send_ctrl().getConnId());
        msg->event().setEvent(CwxEventInfo::FAIL_SEND_MSG);
        m_threadPool->append(msg, msg->send_ctrl().getConnId());
        msg = NULL;
    }
}


void CwxMproxyApp::destroy()
{
    if (m_threadPool){
        m_threadPool->stop();
        delete m_threadPool;
        m_threadPool = NULL;
    }
    if (m_pRecvHandle)
    {
        delete m_pRecvHandle;
        m_pRecvHandle = NULL;
    }
    if (m_pMqHandle)
    {
        delete m_pMqHandle;
        m_pMqHandle = NULL;
    }
    CwxAppFramework::destroy();
}


///设置recv连接的属性
int CwxMproxyApp::setRecvSockAttr(CWX_HANDLE handle, void* arg)
{
    CwxMproxyApp* app = (CwxMproxyApp*)arg;

    if (app->m_config.m_recv.isKeepAlive())
    {
        if (0 != CwxSocket::setKeepalive(handle,
            true,
            CWX_APP_DEF_KEEPALIVE_IDLE,
            CWX_APP_DEF_KEEPALIVE_INTERNAL,
            CWX_APP_DEF_KEEPALIVE_COUNT))
        {
            CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
                app->m_config.m_recv.getHostName().c_str(),
                app->m_config.m_recv.getPort(),
                errno));
            return -1;
        }
    }

    int flags= 1;
    if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
            app->m_config.m_recv.getHostName().c_str(),
            app->m_config.m_recv.getPort(),
            errno));
        return -1;
    }
    struct linger ling= {0, 0};
    if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
            app->m_config.m_recv.getHostName().c_str(),
            app->m_config.m_recv.getPort(),
            errno));
        return -1;
    }
    return 0;
}
///设置mq连接的属性
int CwxMproxyApp::setMqSockAttr(CWX_HANDLE handle, void* arg)
{
    CwxMproxyApp* app = (CwxMproxyApp*)arg;

    if (app->m_config.m_mq.isKeepAlive())
    {
        if (0 != CwxSocket::setKeepalive(handle,
            true,
            CWX_APP_DEF_KEEPALIVE_IDLE,
            CWX_APP_DEF_KEEPALIVE_INTERNAL,
            CWX_APP_DEF_KEEPALIVE_COUNT))
        {
            CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
                app->m_config.m_mq.getHostName().c_str(),
                app->m_config.m_mq.getPort(),
                errno));
            return -1;
        }
    }

    int flags= 1;
    if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
            app->m_config.m_mq.getHostName().c_str(),
            app->m_config.m_mq.getPort(),
            errno));
        return -1;
    }
    struct linger ling= {0, 0};
    if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
            app->m_config.m_mq.getHostName().c_str(),
            app->m_config.m_mq.getPort(),
            errno));
        return -1;
    }
    return 0;
}

///stats命令，-1：因为错误关闭连接；0：不关闭连接
int CwxMproxyApp::monitorStats(char const* buf, CWX_UINT32 uiDataLen, CwxAppHandler4Msg& conn)
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
    msg->send_ctrl().setSvrId(CwxMproxyApp::SVR_TYPE_MONITOR);
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

///形成监控内容，返回监控内容的长度
CWX_UINT32 CwxMproxyApp::packMonitorInfo()
{
    string strValue;
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
        //state
		CwxCommon::snprintf(szLine, 4096, "STAT mq %s\r\n", CWX_INVALID_HANDLE==(int)m_uiMqConnId?"closed":"connected");
        MQ_MONITOR_APPEND();
    }
    while(0);
    strcpy(m_szBuf + uiPos, "END\r\n");
    return strlen(m_szBuf);
}
