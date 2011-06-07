#ifndef __CWX_MQ_DEF_H__
#define __CWX_MQ_DEF_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
/**
@file CwxMqDef.h
@brief MQ系列服务的通用对象定义文件。
@author cwinux@gmail.com
@version 0.1
@date 2010-09-15
@warning
@bug
*/

#include "CwxMqMacro.h"
#include "CwxStl.h"
#include "CwxBinLogMgr.h"
#include "CwxMqPoco.h"
#include "CwxDTail.h"
#include "CwxSTail.h"
#include "CwxTypePoolEx.h"
#include "CwxAppHandler4Channel.h"

class CwxMqQueue;

///分发连接的session信息对象
class CwxMqDispatchConn
{
public:
    CwxMqDispatchConn();
    ~CwxMqDispatchConn();
public:
    CwxBinLogCursor*         m_pCursor; ///<binlog的读取cursor
    CWX_UINT32               m_uiChunk; ///<chunk大小
    CWX_UINT64               m_ullStartSid; ///<report的sid
    CWX_UINT64               m_ullSid; ///<当前发送到的sid
    bool                     m_bNext; ///<是否发送下一个消息
    bool                     m_bSync; ///<是否接受sync数据
    CwxMqSubscribe           m_subscribe; ///<消息订阅对象
    CWX_UINT32               m_uiWindow; ///<窗口的大小
    string                   m_strSign; ///<签名类型
    bool                     m_bZip; ///<是否压缩
    set<CWX_UINT64>          m_sendingSid; ///<发送中的sid
};


///mq的fetch连接的session对象
class CwxMqFetchConn
{
public:
    CwxMqFetchConn();
    ~CwxMqFetchConn();
public:
    void reset();
public:
    bool            m_bWaiting; ///<是否正在等在发送信息
    bool            m_bBlock; ///<是否为block连接
    bool            m_bCommit; ///<是否commit类型的queue
    CWX_UINT32      m_uiTimeout; ///<当前消息的timeout值
    CWX_UINT64      m_ullSendSid; ///<已经发送的sid
    bool            m_bSent;     ///<当前的消息是否发送完毕
    CWX_UINT32      m_uiTaskId; ///<连接的taskid
    string          m_strQueueName; ///<队列的名字
};

///mq信息对象
class CwxMqConfigQueue
{
public:
    CwxMqConfigQueue()
    {
        m_bCommit = false;
    }
    CwxMqConfigQueue(CwxMqConfigQueue const& item)
    {
        m_strName = item.m_strName;
        m_strUser = item.m_strUser;
        m_strPasswd = item.m_strPasswd;
        m_strSubScribe = item.m_strSubScribe;
        m_bCommit = item.m_bCommit;
    }
    CwxMqConfigQueue& operator=(CwxMqConfigQueue const& item)
    {
        if (this != &item)
        {
            m_strName = item.m_strName;
            m_strUser = item.m_strUser;
            m_strPasswd = item.m_strPasswd;
            m_strSubScribe = item.m_strSubScribe;
            m_bCommit = item.m_bCommit;
        }
        return *this;
    }
    bool operator==(CwxMqConfigQueue const& item) const
    {
        return m_strName == item.m_strName;
    };
public:
    string  m_strName; ///<队列的名字
    string  m_strUser; ///<队列的用户名
    string  m_strPasswd; ///<队列的口令
    string  m_strSubScribe; ///<队列的消息订阅
    bool    m_bCommit; ///<是否commit类型
};

///id 范围的比较对象
class CwxMqIdRange
{
public:
    CwxMqIdRange(CWX_UINT32 uiBegin, CWX_UINT32 uiEnd):m_uiBegin(uiBegin),m_uiEnd(uiEnd)
    {
    }
    CwxMqIdRange(CwxMqIdRange const& item)
    {
        m_uiBegin = item.m_uiBegin;
        m_uiEnd = item.m_uiEnd;
    }
    CwxMqIdRange& operator=(CwxMqIdRange const& item)
    {
        if (this != &item)
        {
            m_uiBegin = item.m_uiBegin;
            m_uiEnd = item.m_uiEnd;
        }
        return *this;
    }
    ///有重叠就相等
    bool operator == (CwxMqIdRange const& item) const
    {
         if (((m_uiBegin>=item.m_uiBegin)&&(m_uiBegin<=item.m_uiEnd)) ||
             ((m_uiEnd >= item.m_uiBegin)&&(m_uiEnd<=item.m_uiEnd)))
             return true;
         if (((item.m_uiBegin >= m_uiBegin)&& (item.m_uiBegin<=m_uiEnd)) ||
             ((item.m_uiEnd >= m_uiBegin)&&(item.m_uiEnd<=m_uiEnd)))
             return true;
         return false;
    }
    ///以begin为依据比较大小
    bool operator < (CwxMqIdRange const& item) const
    {
        if (*this == item) return false;
        return m_uiBegin<item.m_uiBegin;
    }

    inline CWX_UINT32 getBegin() const
    {
        return m_uiBegin;
    }

    inline CWX_UINT32 getEnd() const
    {
        return m_uiEnd;
    }
private:
    CWX_UINT32      m_uiBegin;
    CWX_UINT32      m_uiEnd;
};


///mq queue的信息对象
class CwxMqQueueInfo
{
public:
    CwxMqQueueInfo()
    {
        m_bCommit = false;
        m_uiDefTimeout = 0;
        m_uiMaxTimeout = 0;
        m_ullCursorSid = 0;
        m_ullLeftNum = 0;
        m_uiWaitCommitNum = 0;
        m_uiMemLogNum = 0;
        m_ucQueueState = CwxBinLogMgr::CURSOR_STATE_UNSEEK;
    }
public:
    CwxMqQueueInfo(CwxMqQueueInfo const& item)
    {
        m_strName = item.m_strName; ///<队列的名字
        m_strUser = item.m_strUser; ///<队列鉴权的用户名
        m_strPasswd = item.m_strPasswd; ///<队列的用户口令
        m_bCommit = item.m_bCommit; ///<是否commit类型的队列
        m_uiDefTimeout = item.m_uiDefTimeout; ///<缺省的timeout值
        m_uiMaxTimeout = item.m_uiMaxTimeout; ///<最大的timeout值
        m_strSubScribe = item.m_strSubScribe; ///<订阅规则
        m_ullCursorSid = item.m_ullCursorSid;
        m_ullLeftNum = item.m_ullLeftNum; ///<剩余消息的数量
        m_uiWaitCommitNum = item.m_uiWaitCommitNum; ///<等待commit的消息数量
        m_uiMemLogNum = item.m_uiMemLogNum;
        m_ucQueueState = item.m_ucQueueState;
        m_strQueueErrMsg = item.m_strQueueErrMsg;
    }
    CwxMqQueueInfo& operator=(CwxMqQueueInfo const& item)
    {
        if (this != &item)
        {
            m_strName = item.m_strName; ///<队列的名字
            m_strUser = item.m_strUser; ///<队列鉴权的用户名
            m_strPasswd = item.m_strPasswd; ///<队列的用户口令
            m_bCommit = item.m_bCommit; ///<是否commit类型的队列
            m_uiDefTimeout = item.m_uiDefTimeout; ///<缺省的timeout值
            m_uiMaxTimeout = item.m_uiMaxTimeout; ///<最大的timeout值
            m_strSubScribe = item.m_strSubScribe; ///<订阅规则
            m_ullCursorSid = item.m_ullCursorSid;
            m_ullLeftNum = item.m_ullLeftNum; ///<剩余消息的数量
            m_uiWaitCommitNum = item.m_uiWaitCommitNum; ///<等待commit的消息数量
            m_uiMemLogNum = item.m_uiMemLogNum;
            m_ucQueueState = item.m_ucQueueState;
            m_strQueueErrMsg = item.m_strQueueErrMsg;
        }
        return *this;
    }
public:
    string                           m_strName; ///<队列的名字
    string                           m_strUser; ///<队列鉴权的用户名
    string                           m_strPasswd; ///<队列的用户口令
    bool                             m_bCommit; ///<是否commit类型的队列
    CWX_UINT32                       m_uiDefTimeout; ///<缺省的timeout值
    CWX_UINT32                       m_uiMaxTimeout; ///<最大的timeout值
    string                           m_strSubScribe; ///<订阅规则
    CWX_UINT64                       m_ullCursorSid; ///<当前cursor的sid
    CWX_UINT64                       m_ullLeftNum; ///<剩余消息的数量
    CWX_UINT32                       m_uiWaitCommitNum; ///<等待commit的消息数量
    CWX_UINT32                       m_uiMemLogNum; ///<内存中消息的数量
    CWX_UINT8                        m_ucQueueState; ///<队列状态
    string                           m_strQueueErrMsg; //<队列的错误信息
};

#endif
