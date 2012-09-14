#ifndef __CWX_MPROXY_APP_H__
#define __CWX_MPROXY_APP_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxAppFramework.h"
#include "CwxMproxyConfig.h"
#include "CwxMproxyRecvHandler.h"
#include "CwxMproxyMqHandler.h"
#include "CwxMproxyTask.h"
#include "CwxThreadPoolEx.h"

#define CWX_MPROXY_APP_VERSION "2.2.10"
#define CWX_MPROXY_MODIFY_DATE "20110901102000"

CWINUX_USING_NAMESPACE;

///echo的压力测试app
class CwxMproxyApp : public CwxAppFramework
{
public:
    enum
    {
        MAX_MONITOR_REPLY_SIZE = 1024 * 1024,
        LOG_FILE_SIZE = 30, ///<每个循环运行日志文件的MBTYE
        LOG_FILE_NUM = 7,///<循环日志文件的数量
        SVR_TYPE_RECV = CwxAppFramework::SVR_TYPE_USER_START, ///<代理消息的接受
        SVR_TYPE_MQ = SVR_TYPE_RECV + 1,///<代理消息从mq的回复
        SVR_TYPE_MONITOR = SVR_TYPE_RECV + 2
    };
    ///构造函数
	CwxMproxyApp();
    ///析构函数
	virtual ~CwxMproxyApp();
    //初始化app, -1:failure, 0 success;
    virtual int init(int argc, char** argv);
public:
    //时钟响应函数
    virtual void onTime(CwxTimeValue const& current);
    //信号响应函数
    virtual void onSignal(int signum);
    //连接建立函数
    virtual int onConnCreated(CwxAppHandler4Msg& conn, bool& bSuspendConn, bool& bSuspendListen);
    //收到消息的响应函数
    virtual int onRecvMsg(CwxMsgBlock* msg, CwxAppHandler4Msg& conn, CwxMsgHead const& header, bool& bSuspendConn);
    ///收到消息的响应函数
    virtual int onRecvMsg(CwxAppHandler4Msg& conn,
        bool& bSuspendConn);
    //连接关闭
    virtual int onConnClosed(CwxAppHandler4Msg& conn);
    //消息发送完毕
    virtual CWX_UINT32 onEndSendMsg(CwxMsgBlock*& msg,
        CwxAppHandler4Msg& conn);
    //消息发送失败
    virtual void onFailSendMsg(CwxMsgBlock*& msg);
public:
    CwxMproxyConfig const& getConfig() const
    {
        return m_config;
    }

    CWX_UINT32 getNextTaskId()
    {
        CwxMutexGuard<CwxMutexLock>  lock(&m_lock);
        m_uiTaskId++;
        if (!m_uiTaskId) m_uiTaskId = 1;
        return m_uiTaskId;
    }

    CWX_UINT32 getMqConnId() const { return m_uiMqConnId; }
    void setMqConnId(CWX_UINT32 uiConnId) { m_uiMqConnId = uiConnId;}
protected:
    //init the Enviroment before run.0:success, -1:failure.
	virtual int initRunEnv();
    virtual void destroy();
    ///设置recv连接的属性
    static int setRecvSockAttr(CWX_HANDLE handle, void* arg);
    ///设置mq连接的属性
    static int setMqSockAttr(CWX_HANDLE handle, void* arg);
private:
    ///stats命令，-1：因为错误关闭连接；0：不关闭连接
    int monitorStats(char const* buf, CWX_UINT32 uiDataLen, CwxAppHandler4Msg& conn);
    ///形成监控内容，返回监控内容的长度
    CWX_UINT32 packMonitorInfo();
private:
    CwxMproxyConfig                m_config; ///<配置文件对象
    CwxMproxyRecvHandler*          m_pRecvHandle; ///处理接受消息的handler
    CwxMproxyMqHandler*            m_pMqHandle; ///<处理mq消息的handler
    CWX_UINT32                     m_uiTaskId; ///<发送给mq的消息的taskid
    CwxMutexLock                   m_lock; ///<m_uiTaskId的保护锁
    CwxThreadPoolEx*               m_threadPool;///<线程池对象
    CWX_UINT32                     m_uiMqConnId;
    char                           m_szBuf[MAX_MONITOR_REPLY_SIZE];///<监控消息的回复buf
    string                         m_strStartTime; ///<启动时间
};

#endif

