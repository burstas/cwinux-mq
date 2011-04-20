#ifndef __CWX_MQ_MC_ASYNC_HANDLER_H__
#define __CWX_MQ_MC_ASYNC_HANDLER_H__
/*
版权声明：
    本软件遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxCommander.h"
#include "CwxAppAioWindow.h"
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
#include "CwxMqDef.h"

class CwxMqApp;

///异步binlog分发的消息处理handler
class CwxMqMcAsyncHandler : public CwxCmdOp
{
public:
    ///构造函数
    CwxMqMcAsyncHandler(CwxMqApp* pApp);
    ///析构函数
    virtual ~CwxMqMcAsyncHandler();
public:
    ///连接建立后，需要维护连接上数据的分发
    virtual int onConnCreated(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///连接关闭后，需要清理环境
    virtual int onConnClosed(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///接收来自分发的回复信息及同步状态报告信息
    virtual int onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///消息发送完毕
    virtual int onEndSendMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///处理新消息与继续发送的消息
    virtual int onUserEvent(CwxMsgBlock*& msg, CwxTss* pThrEnv);
public:
    void dispatch(CwxMqTss* pTss);
    ///0：未完成状态；
    ///1：完成状态；
    ///-1：失败；
    static int sendBinLog(CwxMqApp* pApp,
        CwxMqDispatchConn* conn,
        CwxMqTss* pTss);
private:
    void noticeContinue(CwxMqTss* pTss, CwxMqDispatchConn* conn);
private:
    CwxMqApp*             m_pApp;  ///<app对象
    CwxMqDispatchConnSet* m_dispatchConns;
};

#endif 
