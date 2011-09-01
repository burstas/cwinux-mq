#include "CwxMqImportApp.h"

///构造函数，初始化发送的echo数据内容
CwxMqImportApp::CwxMqImportApp():m_uiSendNum(0)
{
    CWX_UINT32 i=0;
    for (i=0; i<100*1024; i++)
    {
        m_szBuf100K[i] = 'a' + i % 26;
    }
    m_szBuf100K[i] = 0x00;
    m_uiRecvNum = 0;
}
///析构函数
CwxMqImportApp::~CwxMqImportApp()
{

}

///初始化APP，加载配置文件
int CwxMqImportApp::init(int argc, char** argv)
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
int CwxMqImportApp::initRunEnv()
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

    //output config
    string strConfOut;
    m_config.outputConfig(strConfOut);
    CWX_INFO((strConfOut.c_str()));


    CWX_UINT16 i=0;
    //建立配置文件中设置的、与echo服务的连接
    for (i=0; i<m_config.m_unConnNum; i++)
    {
        if (m_config.m_bTcp)
        {
            //create  conn
            if (0 > this->noticeTcpConnect(SVR_TYPE_ECHO,
                0,
                this->m_config.m_listen.getHostName().c_str(),
                this->m_config.m_listen.getPort(),
                false,
                1,
                2,
                CwxMqImportApp::setSockAttr,
                this))
            {
                CWX_ERROR(("Can't connect the echo service: addr=%s, port=%d",
                    this->m_config.m_listen.getHostName().c_str(),
                    this->m_config.m_listen.getPort()));
                return -1;
            }
        }
        else
        {
            //create  conn
            if (0 > this->noticeLsockConnect(SVR_TYPE_ECHO,
                0,
                this->m_config.m_strUnixPathFile.c_str(),
                false,
                1,
                2))
            {
                CWX_ERROR(("Can't connect the echo service: addr=%s, port=%d",
                    this->m_config.m_listen.getHostName().c_str(),
                    this->m_config.m_listen.getPort()));
                return -1;
            }
        }

    }
    return 0;
}

///时钟响应函数，什么也没有做
void CwxMqImportApp::onTime(CwxTimeValue const& current)
{
    CwxAppFramework::onTime(current);
}

///信号处理函数
void CwxMqImportApp::onSignal(int signum)
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

///echo服务的连接建立响应函数
int CwxMqImportApp::onConnCreated(CwxAppHandler4Msg& conn, bool& , bool& )
{
    ///发送一个echo数据包
    sendNextMsg(conn.getConnInfo().getSvrId(),
        conn.getConnInfo().getHostId(),
        conn.getConnInfo().getConnId());
    return 0;
}

///echo回复的消息响应函数
int CwxMqImportApp::onRecvMsg(CwxMsgBlock* msg, CwxAppHandler4Msg& conn, CwxMsgHead const& header, bool& bSuspendConn)
{
    int flags = 1;
    struct linger ling= {0, 0};
    if (setsockopt(conn.getHandle(), SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
    {
        CWX_ERROR(("Failure to set SO_LINGER"));
    }

    if (setsockopt(conn.getHandle(), IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
    {
        CWX_ERROR(("Failure to set TCP_NODELAY"));
    }
    msg->event().setSvrId(conn.getConnInfo().getSvrId());
    msg->event().setHostId(conn.getConnInfo().getHostId());
    msg->event().setConnId(conn.getConnInfo().getConnId());
    msg->event().setEvent(CwxEventInfo::RECV_MSG);
    msg->event().setMsgHeader(header);
    msg->event().setTimestamp(CwxDate::getTimestamp());
    bSuspendConn = false;
    ///释放收到的数据包
    if (msg) CwxMsgBlockAlloc::free(msg);
    if (m_config.m_bLasting)
    {///如果是持久连接，则发送下一个echo请求数据包
        sendNextMsg(conn.getConnInfo().getSvrId(),
            conn.getConnInfo().getHostId(),
            conn.getConnInfo().getConnId());
    }
    else
    {
        noticeReconnect(conn.getConnInfo().getConnId());
    }
    ///收到的echo数据加1
    m_uiRecvNum++;
    ///若收到10000个数据包，则输出一条日志
    if (!(m_uiRecvNum%10000)){
        CWX_INFO(("Finish num=%u\n", m_uiRecvNum));
    }
    return 0;
}
//tss
CwxTss* CwxMqImportApp::onTssEnv()
{
    CwxMqTss* pTss = new CwxMqTss();
    pTss->init();
    return pTss;
}

///发送echo查询
void CwxMqImportApp::sendNextMsg(CWX_UINT32 uiSvrId, CWX_UINT32 uiHostId, CWX_UINT32 uiConnId)
{
    CwxMqTss* pTss = (CwxMqTss*)getAppTss();
    CwxMsgBlock* pBlock=NULL;
    CwxKeyValueItem data;
    data.m_szData = m_szBuf100K;
    data.m_uiDataLen = m_config.m_unDataSize;
    data.m_bKeyValue = false;
    if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packRecvData(pTss->m_pWriter,
        pBlock,
        0,
        data,
        m_config.m_uiGroup,
        m_config.m_uiType,
        m_config.m_strUser.c_str(),
        m_config.m_strPasswd.c_str(),
        NULL,
        false,
        pTss->m_szBuf2K))
    {
        CWX_ERROR(("Failure to pack send msg, err=%s", pTss->m_szBuf2K));
        this->stop();
        return;
    }
    ///设置消息的发送方式
    ///设置消息的svr-id
    pBlock->send_ctrl().setSvrId(uiSvrId);
    ///设置消息的host-id
    pBlock->send_ctrl().setHostId(uiHostId);
    ///设置消息发送的连接id
    pBlock->send_ctrl().setConnId(uiConnId);
    ///设置消息发送的user-data
    pBlock->send_ctrl().setUserData(NULL);
    ///设置消息发送阶段的行为，包括开始发送是否通知、发送完成是否通知、发送失败是否通知
    pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    ///发送echo请求
    if (0 != this->sendMsgByConn(pBlock)){
        CWX_ERROR(("Failure to send msg"));
        noticeCloseConn(uiConnId);
        return ;
    }
    ///发送数据数量+1
    m_uiSendNum++;
}

///设置连接的属性
int CwxMqImportApp::setSockAttr(CWX_HANDLE handle, void* arg)
{
    CwxMqImportApp* app = (CwxMqImportApp*)arg;

    if (app->m_config.m_listen.isKeepAlive())
    {
        if (0 != CwxSocket::setKeepalive(handle,
            true,
            CWX_APP_DEF_KEEPALIVE_IDLE,
            CWX_APP_DEF_KEEPALIVE_INTERNAL,
            CWX_APP_DEF_KEEPALIVE_COUNT))
        {
            CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
                app->m_config.m_listen.getHostName().c_str(),
                app->m_config.m_listen.getPort(),
                errno));
            return -1;
        }
    }

    int flags= 1;
    if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
            app->m_config.m_listen.getHostName().c_str(),
            app->m_config.m_listen.getPort(),
            errno));
        return -1;
    }
    struct linger ling= {0, 0};
    if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
    {
        CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
            app->m_config.m_listen.getHostName().c_str(),
            app->m_config.m_listen.getPort(),
            errno));
        return -1;
    }
    return 0;
}


