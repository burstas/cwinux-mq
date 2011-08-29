#ifndef __CWX_MQ_BIN_ASYNC_HANDLER_H__
#define __CWX_MQ_BIN_ASYNC_HANDLER_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxCommander.h"
#include "CwxAppAioWindow.h"
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
#include "CwxMqDef.h"
#include "CwxAppHandler4Channel.h"
#include "CwxAppChannel.h"

class CwxMqApp;

///异步binlog分发的消息处理handler
class CwxMqBinAsyncHandler : public CwxAppHandler4Channel
{
public:
    ///构造函数
    CwxMqBinAsyncHandler(CwxMqApp* pApp, CwxAppChannel* channel);
    ///析构函数
    virtual ~CwxMqBinAsyncHandler();
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

public:
    ///0：未发送一条binlog；
    ///1：发送了一条binlog；
    ///-1：失败；
    ///2：窗口满了
    int sendBinLog(CwxMqTss* pTss);
    ///-1：失败，0：无效的消息；1：成功
    int packOneBinLog(CwxPackageReader* reader,
        CwxPackageWriter* writer,
        CwxMsgBlock*& block,
        char const* szData,
        CWX_UINT32  uiDataLen,
        char* szErr2K);
    ///-1：失败，0：无效的消息；1：成功
    int packMultiBinLog(CwxPackageReader* reader,
        CwxPackageWriter* writer,
        CwxPackageWriter* writer_item,
        char const* szData,
        CWX_UINT32  uiDataLen,
        CWX_UINT32&  uiLen,
        char* szErr2K);
    //1：发现记录；0：没有发现；-1：错误
    int seekToLog(CWX_UINT32& uiSkipNum, bool bSync=true);
    //1：成功；0：太大；-1：错误
    int seekToReportSid();

private:
    ///0：成功；-1：失败
    int recvMessage(CwxMqTss* pTss);
private:
    CwxMqApp*              m_pApp;  ///<app对象
    CwxMqDispatchConn      m_dispatch; ///<连接分发信息
    CwxMsgHead             m_header;
    char                   m_szHeadBuf[CwxMsgHead::MSG_HEAD_LEN];
    CWX_UINT32             m_uiRecvHeadLen; ///<recieved msg header's byte number.
    CWX_UINT32             m_uiRecvDataLen; ///<recieved data's byte number.
    CwxMsgBlock*           m_recvMsgData; ///<the recieved msg data

};

#endif 
