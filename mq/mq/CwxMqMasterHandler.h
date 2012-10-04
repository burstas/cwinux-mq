#ifndef __CWX_MQ_MASTER_HANDLER_H__
#define __CWX_MQ_MASTER_HANDLER_H__
/*
 版权声明：
 本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
 联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
 */
#include "CwxCommander.h"
#include "CwxMqMacro.h"
#include "CwxPackageReader.h"
#include "CwxPackageWriter.h"
#include "CwxMsgBlock.h"
#include "CwxMqTss.h"

class CwxMqApp;

///binlog同步的session
class CwxMqSyncSession {
  public:
    ///构造函数
    CwxMqSyncSession(CWX_UINT32 uiHostId) {
      m_ullSessionId = 0;
      m_ullNextSeq = 0;
      m_uiReportDatetime = 0;
      m_uiHostId = uiHostId;
    }
    ~CwxMqSyncSession() {
      map<CWX_UINT64/*seq*/, CwxMsgBlock*>::iterator iter = m_msg.begin();
      while (iter != m_msg.end()) {
        CwxMsgBlockAlloc::free(iter->second);
        iter++;
      }
    }
  public:
    ///接收新消息，返回已经收到的消息列表
    bool recv(CWX_UINT64 ullSeq,
        CwxMsgBlock* msg,
        list<CwxMsgBlock*>& finished)
    {
      map<CWX_UINT32, bool>::iterator conn_iter = m_conns.find(msg->event().getConnId());
      if ((conn_iter == m_conns.end()) || !conn_iter->second) return false;
      finished.clear();
      if (ullSeq == m_ullNextSeq) {
        finished.push_back(msg);
        m_ullNextSeq++;
        map<CWX_UINT64/*seq*/, CwxMsgBlock*>::iterator iter = m_msg.begin();
        while (iter != m_msg.end()) {
          if (iter->first == m_ullNextSeq) {
            finished.push_back(iter->second);
            m_ullNextSeq++;
            m_msg.erase(iter);
            iter = m_msg.begin();
            continue;
          }
          break;
        }
        return true;
      }
      m_msg[ullSeq] = msg;
      msg->event().setTimestamp((CWX_UINT32) time(NULL));
      return true;
    }

    //检测是否超时
    bool isTimeout(CWX_UINT32 uiTimeout) const {
      if (!m_msg.size()) return false;
      CWX_UINT32 uiNow = time(NULL);
      return m_msg.begin()->second->event().getTimestamp() + uiTimeout < uiNow;
    }
  public:
    CWX_UINT64        m_ullSessionId; ///<session id
    CWX_UINT64        m_ullNextSeq; ///<下一个待接收的sid
    CWX_UINT32        m_uiHostId; ///<host id
    map<CWX_UINT64/*seq*/, CwxMsgBlock*>    m_msg;   ///<等待排序的消息
    map<CWX_UINT32, bool/*是否已经report*/>  m_conns; ///<建立的连接
    CWX_UINT32                            m_uiReportDatetime; ///<报告的时间戳，若过了指定的时间没有回复，则关闭
};

///slave从master接收binlog的处理handle
class CwxMqMasterHandler : public CwxCmdOp {
  public:
    ///构造函数
    CwxMqMasterHandler(CwxMqApp* pApp) :
        m_pApp(pApp) {
      m_unzipBuf = NULL;
      m_uiBufLen = 0;
      m_uiCurHostId = 0;
      m_syncSession = NULL;
    }
    ///析构函数
    virtual ~CwxMqMasterHandler() {
      if (m_unzipBuf)
        delete[] m_unzipBuf;
    }
  public:
    ///master的连接关闭后，需要清理环境
    virtual int onConnClosed(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///接收来自master的消息
    virtual int onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    ///超时检查
    virtual int onTimeoutCheck(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  public:
    ///获取session
    CwxMqSyncSession* getSession() {
      return m_syncSession; ///<数据同步的session
    }

  private:
    //关闭已有连接
    void closeSession();
    ///创建与master同步的连接。返回值：0：成功；-1：失败
    int createSession(CwxMqTss* pTss); ///<tss对象
    ///收到一条消息的处理函数。返回值：0:成功；-1：失败
    int recvMsg(CwxMsgBlock*& msg, ///<收到的消息
        list<CwxMsgBlock*>& msgs ///<接收池中返回的可处理的消息。在list按照先后次序排序
        );
    ///处理Sync report的reply消息。返回值：0：成功；-1：失败
    int dealSyncReportReply(CwxMsgBlock*& msg, ///<收到的消息
        CwxMqTss* pTss ///<tss对象
        );
    ///处理收到的sync data。返回值：0：成功；-1：失败
    int dealSyncData(CwxMsgBlock*& msg, ///<收到的消息
        CwxMqTss* pTss ///<tss对象
        );
    //处理收到的chunk模式下的sync data。返回值：0：成功；-1：失败
    int dealSyncChunkData(CwxMsgBlock*& msg, ///<收到的消息
        CwxMqTss* pTss ///<tss对象
        );
    //处理错误消息。返回值：0：成功；-1：失败
    int dealErrMsg(CwxMsgBlock*& msg,  ///<收到的消息
        CwxMqTss* pTss ///<tss对象
        );
    //0：成功；-1：失败
    int saveBinlog(CwxMqTss* pTss, char const* szBinLog, CWX_UINT32 uiLen);
    bool checkSign(char const* data, CWX_UINT32 uiDateLen, char const* szSign,
        char const* sign);
    //获取unzip的buf
    bool prepareUnzipBuf();
  private:
    CwxMqApp* m_pApp;  ///<app对象
    CwxPackageReader m_reader; ///<解包的reader
    unsigned char* m_unzipBuf; ///<解压的buffer
    CWX_UINT32 m_uiBufLen; ///<解压buffer的大小，其为trunk的20倍，最小为20M。
    CwxMqSyncSession* m_syncSession; ///<数据同步的session
    CWX_UINT32 m_uiCurHostId; ///<当前的host id
};

#endif 
