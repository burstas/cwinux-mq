#ifndef __CWX_MQ_BIN_FETCH_HANDLER_H__
#define __CWX_MQ_BIN_FETCH_HANDLER_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
#include "CwxDTail.h"
#include "CwxSTail.h"
#include "CwxTypePoolEx.h"
#include "CwxBinLogMgr.h"
#include "CwxMqDef.h"
#include "CwxMqQueueMgr.h"
#include "CwxAppHandler4Channel.h"
#include "CwxAppChannel.h"

class CwxMqApp;

class CwxMqBinFetchHandler: public CwxAppHandler4Channel
{
public:
    ///构造函数
    CwxMqBinFetchHandler(CwxMqApp* pApp, CwxAppChannel* channel):CwxAppHandler4Channel(channel)
    {
        m_pApp = pApp;
        m_uiRecvHeadLen = 0;
        m_uiRecvDataLen = 0;
        m_recvMsgData = NULL;

    }
    ///析构函数
    virtual ~CwxMqBinFetchHandler()
    {
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
    /**
    @brief 通知连接完成一个消息的发送。<br>
    只有在Msg指定FINISH_NOTICE的时候才调用.
    @param [in,out] msg 传入发送完毕的消息，若返回NULL，则msg有上层释放，否则底层释放。
    @return 
    CwxMsgSendCtrl::UNDO_CONN：不修改连接的接收状态
    CwxMsgSendCtrl::RESUME_CONN：让连接从suspend状态变为数据接收状态。
    CwxMsgSendCtrl::SUSPEND_CONN：让连接从数据接收状态变为suspend状态
    */
    virtual CWX_UINT32 onEndSendMsg(CwxMsgBlock*& msg);

    /**
    @brief 通知连接上，一个消息发送失败。<br>
    只有在Msg指定FAIL_NOTICE的时候才调用.
    @param [in,out] msg 发送失败的消息，若返回NULL，则msg有上层释放，否则底层释放。
    @return void。
    */
    virtual void onFailSendMsg(CwxMsgBlock*& msg);
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
        bool bBinlog = true, ///<msg是否为binlog，若是在失败时需要回收
        bool bClose=false ///<是否发送完毕需要关闭连接
        );
    //将一个发送失败的消息，返还消息队列
    void backMq(CWX_UINT64 ullSid, CwxMqTss* pTss);
    ///发送消息，0：没有消息发送；1：发送一个；-1：发送失败
    int sentBinlog(CwxMqTss* pTss);
    ///fetch mq,返回值,0：成功；-1：失败
    int fetchMq(CwxMqTss* pTss);
    ///create queue,返回值,0：成功；-1：失败
    int createQueue(CwxMqTss* pTss);
    ///del queue,返回值,0：成功；-1：失败
    int delQueue(CwxMqTss* pTss);
private:
    CwxMqApp*              m_pApp;  ///<app对象
    CwxMqFetchConn         m_conn; ///<mq fetch的连接
    CwxMsgHead             m_header; ///<消息头
    char                   m_szHeadBuf[CwxMsgHead::MSG_HEAD_LEN + 1]; ///<消息头的buf
    CWX_UINT32             m_uiRecvHeadLen; ///<recieved msg header's byte number.
    CWX_UINT32             m_uiRecvDataLen; ///<recieved data's byte number.
    CwxMsgBlock*           m_recvMsgData; ///<the recieved msg data
};

#endif 
