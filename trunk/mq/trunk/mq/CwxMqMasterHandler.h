#ifndef __CWX_MQ_MASTER_HANDLER_H__
#define __CWX_MQ_MASTER_HANDLER_H__
/*
版权声明：
    本软件遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxCommander.h"
#include "CwxMqMacro.h"
#include "CwxPackageReader.h"
#include "CwxPackageWriter.h"
#include "CwxMsgBlock.h"

class CwxMqApp;

///slave从master接收binlog的处理handle
class CwxMqMasterHandler : public CwxCmdOp
{
public:
    enum
    {
        RECONN_MASTER_DELAY_SECOND = 4
    };
public:
    ///构造函数
    CwxMqMasterHandler(CwxMqApp* pApp):m_pApp(pApp)
    {
        m_uiConnId = 0;
    }
    ///析构函数
    virtual ~CwxMqMasterHandler()
    {

    }
public:
    ///连接建立后，需要往master报告sid
    virtual int onConnCreated(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///master的连接关闭后，需要清理环境
    virtual int onConnClosed(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///接收来自master的消息
    virtual int onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
public:
    CWX_UINT32 getMasterConnId() const
    {
        return m_uiConnId;
    }
private:
    //0：成功；-1：失败
    int saveBinlog(CwxMqTss* pTss, char const* szBinLog, CWX_UINT32 uiLen, CWX_UINT64& ullSid);
private:
    CwxMqApp*     m_pApp;  ///<app对象
    CWX_UINT32          m_uiConnId; ///<master的连接ID
    CwxPackageReader      m_reader; 
};

#endif 
