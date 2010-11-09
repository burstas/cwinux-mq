#ifndef __CWX_MPROXY_RECV_HANDLER_H__
#define __CWX_MPROXY_RECV_HANDLER_H__
/*
版权声明：
    本软件为个人所有，遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
但有以下例外：
    腾讯公司及与腾讯公司有直接业务与合作关系的公司不得使用此软件。原因可参考：
http://it.sohu.com/20100903/n274684530.shtml
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxAppCommander.h"
#include "CwxAppTss.h"
#include "CwxGlobalMacro.h"
#include "CwxMqTss.h"

CWINUX_USING_NAMESPACE

class CwxMproxyApp;
///代理mq消息的接受handle
class CwxMproxyRecvHandler : public CwxAppCmdOp 
{
public:
    ///构造函数
    CwxMproxyRecvHandler(CwxMproxyApp* pApp):m_pApp(pApp)
    {
    }
    ///析构函数
    virtual ~CwxMproxyRecvHandler()
    {
    }
public:
    ///处理mq消息的函数
    virtual int onRecvMsg(CwxMsgBlock*& msg,  CwxAppTss* pThrEnv);
    //处理连接关闭的消息
    virtual int onConnClosed(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
    //超时坚持的消息
    virtual int onTimeoutCheck(CwxMsgBlock*& msg, CwxAppTss* pThrEnv);
public:
    //回复代理的mq消息
    static void reply(CwxMproxyApp* app, CwxMsgBlock* msg, CWX_UINT32 uiConnId);

private:
    CWX_UINT32 isAuth(CwxMqTss* pTss, CWX_UINT32 uiGroup, char const* szUser, char const* szPasswd);
private:
    CwxMproxyApp*     m_pApp;  ///<app对象
};

#endif 
