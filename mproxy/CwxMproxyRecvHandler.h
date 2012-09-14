#ifndef __CWX_MPROXY_RECV_HANDLER_H__
#define __CWX_MPROXY_RECV_HANDLER_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxCommander.h"
#include "CwxTss.h"
#include "CwxGlobalMacro.h"
#include "CwxMqTss.h"

CWINUX_USING_NAMESPACE

class CwxMproxyApp;
///代理mq消息的接受handle
class CwxMproxyRecvHandler : public CwxCmdOp 
{
public:
    ///构造函数
    CwxMproxyRecvHandler(CwxMproxyApp* pApp):m_pApp(pApp)
    {
        m_unzipBuf = NULL;
        m_uiBufLen = 0;
    }
    ///析构函数
    virtual ~CwxMproxyRecvHandler()
    {
        if (m_unzipBuf) delete [] m_unzipBuf;
    }
public:
    ///处理mq消息的函数
    virtual int onRecvMsg(CwxMsgBlock*& msg,  CwxTss* pThrEnv);
    //处理连接关闭的消息
    virtual int onConnClosed(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    //超时坚持的消息
    virtual int onTimeoutCheck(CwxMsgBlock*& msg, CwxTss* pThrEnv);
public:
    //回复代理的mq消息
    static void reply(CwxMproxyApp* app, CwxMsgBlock* msg, CWX_UINT32 uiConnId);

private:
    CWX_UINT32 isAuth(CwxMqTss* pTss, CWX_UINT32 uiGroup, char const* szUser, char const* szPasswd);
    //获取unzip的buf
    bool prepareUnzipBuf();
private:
    CwxMproxyApp*     m_pApp;  ///<app对象
    unsigned char*          m_unzipBuf; ///<解压的buffer
    CWX_UINT32              m_uiBufLen; ///<解压buffer的大小，其为trunk的20倍，最小为20M。
};

#endif 
