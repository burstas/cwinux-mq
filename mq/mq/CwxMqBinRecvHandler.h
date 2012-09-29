#ifndef __CWX_MQ_BIN_RECV_HANDLER_H__
#define __CWX_MQ_BIN_RECV_HANDLER_H__
/*
 版权声明：
 本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
 联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
 */
#include "CwxCommander.h"
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
class CwxMqApp;

///Dispatch master处理收到的bin协议的binlog handler
class CwxMqBinRecvHandler : public CwxCmdOp {
  public:
    ///构造函数
    CwxMqBinRecvHandler(CwxMqApp* pApp) :
        m_pApp(pApp) {
      m_unzipBuf = NULL;
      m_uiBufLen = 0;
    }
    ///析构函数
    virtual ~CwxMqBinRecvHandler() {
      if (m_unzipBuf)
        delete[] m_unzipBuf;
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
    //获取unzip的buf
    bool prepareUnzipBuf();
  private:
    map<CWX_UINT32, bool> m_clientMap; ///<连接认证的map
    CwxMqApp* m_pApp;  ///<app对象
    unsigned char* m_unzipBuf; ///<解压的buffer
    CWX_UINT32 m_uiBufLen; ///<解压buffer的大小，其为trunk的20倍，最小为20M。
};

#endif 
