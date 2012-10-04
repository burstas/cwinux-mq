#ifndef __CWX_MC_SYNC_HANDLER_H__
#define __CWX_MC_SYNC_HANDLER_H__

#include "CwxCommander.h"
#include "CwxMqMacro.h"
#include "CwxMsgBlock.h"
#include "CwxMqTss.h"
#include "CwxMcStore.h"

class CwxMcApp;
class CwxMcSyncHandler;

///binlog同步的session
class CwxMcSyncSession {
  public:
    ///构造函数
    CwxMcSyncSession() {
      m_ullSessionId = 0;
      m_ullNextSeq = 0;
      m_uiReportDatetime = 0;
      m_ullSid = 0;
      m_bClosed = true;
    }
    ~CwxMcSyncSession() {
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
    CWX_UINT64                         m_ullSessionId; ///<session id
    CWX_UINT64                         m_ullNextSeq; ///<下一个待接收的sid
    map<CWX_UINT64/*seq*/, CwxMsgBlock*> m_msg;   ///<等待排序的消息
    map<CWX_UINT32, CwxMcSyncHandler*>   m_conns; ///<建立的连接
    CWX_UINT32                         m_uiReportDatetime; ///<报告的时间戳，若过了指定的时间没有回复，则关闭
    CwxHostInfo                        m_syncHost;       ///<数据同步的主机
    CwxMcStore*                        m_store;          ///<存储对象
    CwxMcApp*                          m_pApp;           ///<app对象
    CWX_UINT64                         m_ullSid;         ///<当前同步的sid
    bool                              m_bClosed;        ///<session是否已经关闭
};

///从mq同步数据的处理handle
class CwxMcSyncHandler : public CwxAppHandler4Channel {
  public:
    ///构造函数
    CwxMcSyncHandler(CwxAppChannel* channel, CWX_UINT32 uiConnID) : CwxAppHandler4Channel(channel){
        m_uiConnId = uiConnID;
        m_uiRecvHeadLen = 0;
        m_uiRecvDataLen = 0;
        m_recvMsgData = NULL;
    }
    ///析构函数
    virtual ~CwxMcSyncHandler() {
        if (m_recvMsgData) CwxMsgBlockAlloc::free(m_recvMsgData);
    }
  public:
      /**
      @brief 连接可读事件，返回-1，close()会被调用
      @return -1：处理失败，会调用close()； 0：处理成功
      */
      virtual int onInput();
      /**
      @brief 通知连接关闭。
      @return 1：不从engine中移除注册；0：从engine中移除注册但不删除handler；-1：从engine中将handle移除并删除。
      */
      virtual int onConnClosed();
      /// 获取连接id
      CWX_UINT32 getConnId() const { return m_uiConnId;}
  private:
      ///接收消息，0：成功；-1：失败
      int recvMessage(CwxMqTss* pTss);
  private:
      CwxMsgHead              m_header; ///<消息头
      CWX_UINT32              m_uiConnId; ///<连接id
      char                    m_szHeadBuf[CwxMsgHead::MSG_HEAD_LEN + 1]; ///<消息头的buf
      CWX_UINT32              m_uiRecvHeadLen; ///<received msg header's byte number.
      CWX_UINT32              m_uiRecvDataLen; ///<received data's byte number.
      CwxMsgBlock*            m_recvMsgData; ///<the received msg data
};

#endif 
