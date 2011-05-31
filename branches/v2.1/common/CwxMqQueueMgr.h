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
#include "CwxMqDef.h"
#include "CwxMinHeap.h"
#include "CwxMqQueueLogFile.h"

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
        bool    bCommit,
        string strSubscribe,
        CWX_UINT32 uiDefTimeout,
        CWX_UINT32 uiMaxTimeout,
        CwxBinLogMgr* pBinlog);
    ~CwxMqQueue();
public:
    ///0:成功;-1：失败
    int init(CWX_UINT64 ullLastCommitSid,
        set<CWX_UINT64> const& uncommitSid,
        set<CWX_UINT64> const& commitSid,
        string& strErrMsg);
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
    void checkTimeout(CWX_UINT32 ttTimestamp);

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
    inline bool isCommit() const
    {
        return m_bCommit;
    }
    inline CWX_UINT64 getCurSid() const
    {
        return m_cursor?m_cursor->getHeader().getSid():0;
    }
    inline CWX_UINT32 getWaitCommitNum() const
    {
        return m_uncommitMap.size();
    }
    inline map<CWX_UINT64, void*>& getUncommitMap()
    {
        return m_uncommitMap; ///<commit队列中未commit的消息sid索引
    }
    inline map<CWX_UINT64, CwxMsgBlock*>& getMemMsgMap()
    {
        return m_memMsgMap;///<发送失败消息队列
    }

    inline CwxBinLogCursor* getCursor() 
    {
        return m_cursor;
    }
    inline CWX_UINT32 getMemSidNum() const
    {
        return m_memMsgMap.size();
    }
    inline CWX_UINT32 getUncommitSidNum() const
    {
        return m_uncommitMap.size();
    }
    inline CWX_UINT64 getCursorSid() const
    {
        if (m_cursor && (CwxBinLogMgr::CURSOR_STATE_READY == m_cursor->getSeekState()))
            return m_cursor->getHeader().getSid();
        return getStartSid();
    }
    ///获取cursor的起始sid
    inline CWX_UINT64 getStartSid()
    {
        if (m_lastUncommitSid.size())
        {
           return *m_lastUncommitSid.begin() - 1;
        }
        return m_ullLastCommitSid;
    }
    ///获取dump信息
    void getQueueDumpInfo(CWX_UINT64& ullLastCommitSid,
        set<CWX_UINT64>& uncommitSid,
        set<CWX_UINT64>& commitSid);
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
    bool                             m_bCommit; ///<是否commit类型的队列
    CWX_UINT32                       m_uiDefTimeout; ///<缺省的timeout值
    CWX_UINT32                       m_uiMaxTimeout; ///<最大的timeout值
    string                           m_strSubScribe; ///<订阅规则
    CwxBinLogMgr*                    m_binLog; ///<binlog
    CwxMinHeap<CwxMqQueueHeapItem>*  m_pUncommitMsg; ///<commit队列中未commit的消息
    map<CWX_UINT64, void*>           m_uncommitMap; ///<commit队列中未commit的消息sid索引
    map<CWX_UINT64, CwxMsgBlock*>    m_memMsgMap;///<发送失败消息队列
    CwxBinLogCursor*                 m_cursor; ///<队列的游标
    CwxMqSubscribe                   m_subscribe; ///<订阅

    CWX_UINT64                       m_ullLastCommitSid; ///<日志文件记录的cursor的sid
    set<CWX_UINT64>                  m_lastUncommitSid; ///<m_ullLastCommitSid之前未commit的binlog
    set<CWX_UINT64>                  m_lastCommitSid; ///<m_ullLastCommitSid之后commit的binlog
};


class CwxMqQueueMgr
{
public:
    enum
    {
        MQ_SWITCH_LOG_NUM = 100000
    };
public:
    CwxMqQueueMgr(string const& strQueueLogFile,
        CWX_UINT32 uiMaxFsyncNum);
    ~CwxMqQueueMgr();
public:
    int init(CwxBinLogMgr* binLog);
public:
    ///0：没有消息；
    ///1：获取一个消息；
    ///2：达到了搜索点，但没有发现消息；
    ///-1：失败；
    ///-2：队列不存在
    int getNextBinlog(CwxMqTss* pTss, ///<tss变量
        string const& strQueue, ///<队列的名字
        CwxMsgBlock*&msg, ///<消息
        CWX_UINT32 uiTimeout, ///<消息的超时时间
        int& err_num, ///<错误消息
        bool& bCommitType, ///<是否为commit类型的队列
        char* szErr2K=NULL);

    ///对于非commit类型的队列，bCommit=true此表示消息已经写到socket buf，否则表示写失败。
    ///对于commit类型的队列，bCommit=true此表示已经收到对方的commit确认，否则写socket失败。
    ///返回值：0：不存在；1：成功；-1：失败；-2：队列不存在
    int commitBinlog(string const& strQueue,
        CWX_UINT64 ullSid,
        bool bCommit=true,
        char* szErr2K=NULL);
    ///强行flush mq的log文件
    void commit();
    ///检测commit类型队列超时的消息
    void checkTimeout(CWX_UINT32 ttTimestamp);
    ///1：成功
    ///0：存在
    ///-1：其他错误
    int addQueue(string const& strQueue,
        CWX_UINT64 ullSid,
        bool bCommit,
        string const& strUser,
        string const& strPasswd,
        string const& strScribe,
        CWX_UINT32 uiDefTimeout,
        CWX_UINT32 uiMaxTimeout,
        char* szErr2K=NULL);
    ///1：成功
    ///0：不存在
    ///-1：其他错误
    int delQueue(string const& strQueue,
        char* szErr2K=NULL);

    void getQueuesInfo(list<CwxMqQueueInfo>& queues) const;

    inline bool isExistQueue(string const& strQueue) const
    {
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        return m_queues.find(strQueue) != m_queues.end();
    }
    //-1：失败；0：队列不存在；1：成功
    inline int authQueue(string const& strQueue, string const& user, string const& passwd) const
    {
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, CwxMqQueue*>::const_iterator iter = m_queues.find(strQueue);
        if (iter == m_queues.end()) return 0;
        if (iter->second.getUserName().length())
        {
            return ((user != iter->second.getUserName()) || (passwd != iter->second->getPasswd()))?-1:1;
        }
        return 1;
    }
    inline CWX_UINT32 getQueueNum() const
    {
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        return m_queues.size();
    }    
    inline bool isValid() const
    {
        return m_mqLogFile != NULL;
    }
    inline string const& getErrMsg() const
    {
        return m_strErrMsg;
    }

private:
    ///保存数据
    bool _save();

private:
    map<string, CwxMqQueue*>   m_queues;
    CwxRwLock                  m_lock;
    string                     m_strQueueLogFile;
    CWX_UINT32                 m_uiMaxFsyncNum;
    CwxMqQueueLogFile*         m_mqLogFile;
    CwxBinLogMgr*              m_binLog;
    string                     m_strErrMsg;
};


#endif
