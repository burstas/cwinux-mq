#ifndef __CWX_MQ_QUEUE_MGR_H__
#define __CWX_MQ_QUEUE_MGR_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
/**
@file CwxMqMgr.h
@brief MQ系列服务的MQ管理器对象定义文件。
@author cwinux@gmail.com
@version 0.1
@date 2010-09-15
@warning
@bug
*/

#include "CwxMqMacro.h"
#include "CwxBinLogMgr.h"
#include "CwxMutexIdLocker.h"
#include "CwxMqPoco.h"
#include "CwxMsgBlock.h"
#include "CwxMqTss.h"
#include "CwxDTail.h"
#include "CwxSTail.h"
#include "CwxMqDef.h"
#include "CwxMinHeap.h"

class CwxMqQueueHeapItem
{
public:
    CwxMqQueueHeapItem()
    {
        m_msg = NULL;
        m_index = -1;
        m_ttTimestamp = 0;
        m_ullSid = 0;
    }
    ~CwxMqQueueHeapItem()
    {
        if (m_msg) CwxMsgBlockAlloc::free(m_msg);
    }
public:
    bool operator <(CwxMqQueueHeapItem const& item) const
    {
        return m_ttTimestamp < item.m_ttTimestamp;
    }

    CWX_INT32 index() const
    {
        return m_index;
    }
    void index(CWX_INT32 index)
    {
        m_index = index;
    }
    CwxMsgBlock* msg()
    {
        return m_msg;
    }
    void msg(CwxMsgBlock* msg)
    {
        m_msg = msg;
    }
    CWX_UINT32 timestamp() const
    {
        return m_ttTimestamp;
    }
    void timestamp(CWX_UINT32 uiTimestamp)
    {
        m_ttTimestamp = uiTimestamp;
    }
    CWX_UINT64 sid() const
    {
        return m_ullSid;
    }
    void sid(CWX_UINT64 ullSid)
    {
        m_ullSid = ullSid;
    }

private:
    CWX_INT32   m_index;
    CWX_UINT32  m_ttTimestamp;
    CWX_UINT64  m_ullSid;
    CwxMsgBlock*  m_msg;
};

class CwxMqQueue
{
public:
    CwxMqQueue(string strName,
        string strUser,
        string strPasswd,
        CWX_UINT64 ullStartSid,
        bool    bCommit,
        string strSubscribe,
        CWX_UINT32 uiDefTimeout,
        CWX_UINT32 uiMaxTimeout,
        CwxBinLogMgr* pBinlog);
    ~CwxMqQueue();
public:
    ///0:成功;-1：失败
    int init(string& strErrMsg);
    ///0：没有消息；
    ///1：获取一个消息；
    ///2：达到了搜索点，但没有发现消息；
    ///-1：失败；
    int getNextBinlog(CwxMqTss* pTss,
        CwxMsgBlock*&msg,
        CWX_UINT32 uiTimeout,
        int& err_num,
        char* szErr2K);

    ///对于非commit类型的队列，bCommit=true此表示消息已经写到socket buf，否则表示写失败。
    ///对于commit类型的队列，bCommit=true此表示已经收到对方的commit确认，否则写socket失败。
    ///返回值：0：不存在，1：成功.
    int commitBinlog(CWX_UINT64 ullSid, bool bCommit=true);
    ///检测commit类型队列超时的消息
    void checkTimeout();
    ///添加自start sid后，已经commit的消息sid
    void addCommitSid(CWX_UINT64 ullSid)
    {
        m_dispatchSid.insert(ullSid);
    }

    inline string const& getName() const
    {
        return m_strName;
    }
    inline string const& getUserName() const
    {
        return m_strUser;
    }
    inline string const& getPasswd() const
    {
        return m_strPasswd;
    }
    inline CwxMqSubscribe& getSubscribe()
    {
        return m_subscribe;
    }
    inline string const& getSubscribeRule() const
    {
        return m_strSubScribe;
    }
    inline CWX_UINT32 getDefTimeout() const
    {
        return m_uiDefTimeout;
    }
    inline CWX_UINT32 getMaxTimeout() const
    {
        return m_uiMaxTimeout;
    }
    inline CWX_UINT64 getCurSid() const
    {
        return m_cursor?m_cursor->getHeader().getSid():0;
    }
    CWX_UINT64 getMqNum();
private:
    ///0：没有消息；
    ///1：获取一个消息；
    ///2：达到了搜索点，但没有发现消息；
    ///-1：失败；
    int fetchNextBinlog(CwxMqTss* pTss,
        CwxMsgBlock*&msg,
        int& err_num,
        char* szErr2K);
private:
    string                           m_strName; ///<队列的名字
    string                           m_strUser; ///<队列鉴权的用户名
    string                           m_strPasswd; ///<队列鉴权的口令
    CWX_UINT64                       m_ullStartSid; ///<队列开始的sid
    bool                             m_bCommit; ///<是否commit类型的队列
    CWX_UINT32                       m_uiDefTimeout; ///<缺省的timeout值
    CWX_UINT32                       m_uiMaxTimeout; ///<最大的timeout值
    string                           m_strSubScribe; ///<订阅规则
    CwxBinLogMgr*                    m_binLog; ///<binlog
    CwxMinHeap<CwxMqQueueHeapItem>* m_pUncommitMsg; ///<commit队列中未commit的消息
    map<CWX_UINT64, void*>           m_uncommitMap; ///<commit队列中未commit的消息sid索引
    set<CWX_UINT64>                  m_dispatchSid; ///<已经分发的消息
    CwxBinLogCursor*                 m_cursor; ///<队列的游标
    CwxSTail<CwxMsgBlock>*           m_memMsgTail; ///<队列的数据
    CwxMqSubscribe                   m_subscribe; ///<订阅
};


class CwxMqQueueMgr
{
public:
    enum
    {
        QUEUE_DEF_TIMEOUT_SECOND = 10,
        QUEUE_MAX_TIMEOUT_SECOND = 300
    };
public:
    CwxMqQueueMgr(string const& strQueueFile,
        string const& strQueueLogFile);
    ~CwxMqQueueMgr();
public:
    int init(CwxBinLogMgr* binLog);
public:
    inline CwxMqQueue* getQueue(CWX_UINT32 uiQueueId) const
    {
        map<CWX_UINT32, CwxMqQueue*>::const_iterator iter = m_idQueues.find(uiQueueId);
        return iter == m_idQueues.end()?NULL:iter->second;
    }

    inline CwxMqQueue* getQueue(string const& strQueueName) const
    {
        map<string, CwxMqQueue*>::const_iterator iter = m_nameQueues.find(strQueueName);
        return iter == m_nameQueues.end()?NULL:iter->second;

    }
    inline CWX_UINT32 getQueueNum() const
    {
        return m_nameQueues.size();
    }
    inline map<string, CwxMqQueue*> const& getNameQueues() const
    {
        return m_nameQueues;
    }
    inline map<CWX_UINT32, CwxMqQueue*> const& getIdQueues() const
    {
        return m_idQueues;
    }

private:
    map<string, CwxMqQueue*>   m_nameQueues;
    CwxMutexLock
};


#endif
