#ifndef __CWX_MQ_BIN_RECV_HANDLER_H__
#define __CWX_MQ_BIN_RECV_HANDLER_H__
/*
版权声明：
    本软件遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxCommander.h"
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
class CwxMqApp;


///Dispatch master处理收到的bin协议的binlog handler
class CwxMqBinRecvHandler: public CwxCmdOp
{
public:
    ///构造函数
    CwxMqBinRecvHandler(CwxMqApp* pApp):m_pApp(pApp)
    {
        m_uiUnSyncLogNum = 0; ///<上次形成sync记录以来的新记录数
        m_ttLastSyncTime = 0; ///<上一次形成sync记录的时间
    }
    ///析构函数
    virtual ~CwxMqBinRecvHandler()
    {

    }
public:
    ///连接建立后，需要维护连接上数据的分发
    virtual int onConnCreated(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///连接关闭后，需要清理环境
    virtual int onConnClosed(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///处理收到binlog的事件
    virtual int onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///对于同步dispatch，需要检查同步的超时
    virtual int onTimeoutCheck(CwxMsgBlock*& msg, CwxTss* pThrEnv);
private:
    ///-1:失败；0：成功
    int commit(char* szErr2K);
    ///-1:失败；0：无需形成log；1：形成一个log
    int checkSyncLog(bool bNew, char* szErr2K);
private:
    CWX_UINT32      m_uiUnSyncLogNum; ///<上次形成sync记录以来的新记录数
    time_t          m_ttLastSyncTime; ///<上一次形成sync记录的时间
    map<CWX_UINT32, bool>   m_clientMap; ///<连接认证的map
    CwxMqApp*       m_pApp;  ///<app对象
};

#endif 
