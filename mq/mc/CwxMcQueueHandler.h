#ifndef __CWX_MC_QUEUE_HANDLER_H__
#define __CWX_MC_QUEUE_HANDLER_H__

#include "CwxMqMacro.h"
#include "CwxMqTss.h"
#include "CwxMqDef.h"
#include "CwxAppHandler4Channel.h"
#include "CwxAppChannel.h"

class CwxMcApp;

class CwxMcQueueHandler : public CwxAppHandler4Channel {
  public:
    ///构造函数
    CwxMcQueueHandler(CwxMcApp* pApp, CwxAppChannel* channel) : CwxAppHandler4Channel(channel)
    {
      m_pApp = pApp;
      m_uiRecvHeadLen = 0;
      m_uiRecvDataLen = 0;
      m_recvMsgData = NULL;
    }
    ///析构函数
    virtual ~CwxMcQueueHandler() {
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
    /**
     @brief Handler的redo事件，在每次dispatch时执行。
     @return -1：处理失败，会调用close()； 0：处理成功
     */
    virtual int onRedo();
  private:
    ///接收消息，0：成功；-1：失败
    int recvMessage(CwxMqTss* pTss);
    ///pack一个不包含mq消息体的消息包。
    CwxMsgBlock* packEmptyFetchMsg(CwxMqTss* pTss,
        int iRet, ///<状态码
        char const* szErrMsg ///<错误消息
        );
    ///发送消息，0：成功；-1：发送失败
    int replyFetchMq(CwxMqTss* pTss,
        CwxMsgBlock* msg,  ///<消息包
        bool bClose = false ///<是否发送完毕需要关闭连接
        );
    ///发送消息，0：没有消息发送；1：发送一个；-1：发送失败
    int sentMq(CwxMqTss* pTss);
    ///获取消息queue
    int fetchMq(CwxMqTss* pTss);
  private:
    CwxMcApp*               m_pApp;  ///<app对象
    CwxMqFetchConn           m_conn; ///<queue fetch的连接
    CwxMsgHead              m_header; ///<消息头
    char                    m_szHeadBuf[CwxMsgHead::MSG_HEAD_LEN + 1]; ///<消息头的buf
    CWX_UINT32              m_uiRecvHeadLen; ///<received msg header's byte number.
    CWX_UINT32              m_uiRecvDataLen; ///<received data's byte number.
    CwxMsgBlock*            m_recvMsgData; ///<the received msg data
};

#endif 
