#ifndef __CWX_MQ_ASYNC_HANDLER_H__
#define __CWX_MQ_ASYNC_HANDLER_H__
/*
版权声明：
    本软件为个人所有，遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
但有以下例外：
    腾讯公司及与腾讯公司有直接业务与合作关系的公司不得使用此软件。原因可参考：
http://it.sohu.com/20100903/n274684530.shtml
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxAppCommander.h"
#include "CwxAppAioWindow.h"
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
#include "CwxMqDef.h"

class CwxMqApp;

///异步binlog分发的消息处理handler
class CwxMqAsyncHandler : public CwxAppCmdOp
{
public:
    ///构造函数
    CwxMqAsyncHandler(CwxMqApp* pApp);
    ///析构函数
    virtual ~CwxMqAsyncHandler();
public:
    ///连接建立后，需要维护连接上数据的分发
    virtual int onConnCreated(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
    ///连接关闭后，需要清理环境
    virtual int onConnClosed(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
    ///接收来自分发的回复信息及同步状态报告信息
    virtual int onRecvMsg(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
    ///处理新消息与继续发送的消息
    virtual int onUserEvent(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
public:
    void dispatch(CwxMqTss* pTss);
    ///0：未完成状态；
    ///1：完成状态；
    ///-1：失败；
    static int sendBinLog(CwxMqApp* pApp,
        CwxMqDispatchConn* conn,
        CwxMqTss* pTss,
        bool bNext=true);
private:
    void noticeContinue(CwxMqTss* pTss, CWX_UINT32 uiConnId);
private:
    CwxMqApp*             m_pApp;  ///<app对象
    CwxMqDispatchConnSet* m_dispatchConns;
};

#endif 
