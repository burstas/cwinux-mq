#ifndef __CWX_MQ_FETCH_HANDLER_H__
#define __CWX_MQ_FETCH_HANDLER_H__
/*
版权声明：
    本软件遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxAppCommander.h"
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
#include "CwxDTail.h"
#include "CwxSTail.h"
#include "CwxTypePoolEx.h"
#include "CwxBinLogMgr.h"
#include "CwxMqDef.h"
#include "CwxMqQueueMgr.h"

class CwxMqApp;



///Dispatch master处理收到的binlog handler
class CwxMqFetchHandler: public CwxAppCmdOp
{
public:
    ///构造函数
    CwxMqFetchHandler(CwxMqApp* pApp):m_pApp(pApp)
    {
        m_bHaveWaiting = false;
    }
    ///析构函数
    virtual ~CwxMqFetchHandler()
    {

    }
public:
    ///连接建立后，需要维护连接上数据的分发
    virtual int onConnCreated(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
    ///连接关闭后，需要清理环境
    virtual int onConnClosed(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
    ///接收来自分发的回复信息及同步状态报告信息
    virtual int onRecvMsg(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
    ///处理binlog发送完毕的消息
    virtual int onEndSendMsg(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
    ///处理发送失败的binlog
    virtual int onFailSendMsg(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
    ///处理继续发送的消息
    virtual int onUserEvent(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
public:
    void dispatch(CwxMqTss* pTss);
private:
    CwxMsgBlock* packErrMsg(CwxMqTss* pTss,
        int iRet,
        char const* szErrMsg
        );
    void reply(CwxMsgBlock* msg,
        CWX_UINT32 uiConnId,
        CwxMqQueue* pQueue,
        int ret,
        bool bClose=false);
    //将一个发送失败的消息，还回消息队列
    void back(CwxMsgBlock* msg);
    ///继续发送消息
    void noticeContinue(CwxMqTss* pTss, CWX_UINT32 uiConnId);
    ///发送消息
    void sentBinlog(CwxMqTss* pTss, CwxMqFetchConn * pConn);
private:
    CwxMqApp*     m_pApp;  ///<app对象
    CwxMqFetchConnSet     m_fetchConns; ///<mq fetch的连接集合
    bool          m_bHaveWaiting; ///<是否有等待的发送队列
};

#endif 
