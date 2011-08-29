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
        m_bSend = false;
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
    bool send() const
    {
        return m_bSend;
    }
    void send(bool bSend)
    {
        m_bSend = bSend;
    }

private:
    CWX_INT32   m_index;///<heap的index
    CWX_UINT32  m_ttTimestamp; ///<消息的时间戳
    CWX_UINT64  m_ullSid; ///<消息的sid
    bool        m_bSend; ///<消息是否发送完毕
    CwxMsgBlock*  m_msg; ///<消息
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

    ///用于commit类型的队列，提交commit消息。
    ///返回值：0：不存在，1：成功.
    int commitBinlog(CWX_UINT64 ullSid,
        bool bCommit=true,
        CWX_UINT32 uiDeley=0);
    ///消息发送完毕，bSend=true表示已经发送成功；false表示发送失败
    ///返回值：0：不存在，1：成功.
    int endSendMsg(CWX_UINT64 ullSid, bool bSend=true);
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
		return (m_cursor && (CwxBinLogMgr::CURSOR_STATE_READY == m_cursor->getSeekState()))?m_cursor->getHeader().getSid():0;
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
        ///如果cursor有效，则返回cursor的sid
        if (m_cursor && (CwxBinLogMgr::CURSOR_STATE_READY == m_cursor->getSeekState()))
            return m_cursor->getHeader().getSid();
        ///否则返回初始sid。
        return getStartSid();
    }
    ///获取cursor的起始sid
    inline CWX_UINT64 getStartSid() const
    {
        CWX_UINT64 ullSid = 0;
        //如果存在历史未commit的数据，则从历史未commit的数据中获取最小的sid
        if (m_lastUncommitSid.size())
        {
           ullSid =  *m_lastUncommitSid.begin();
           if (ullSid) ullSid --;
           if (ullSid > m_ullLastCommitSid) ullSid = m_ullLastCommitSid;
           return ullSid;
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
    CwxMinHeap<CwxMqQueueHeapItem>*  m_pUncommitMsg; ///<commit队列中未commit的消息，消息同时在m_uncommitMap中存在
    CwxMinHeap<CwxMqQueueHeapItem>*  m_pDelayMsg; ///<commit队列中delay的消息,delay的消息不会在m_uncommitMap中存在
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
        MQ_SWITCH_LOG_NUM = 100000,
        MQ_MAX_SWITCH_LOG_INTERNAL = 600
    };
public:
    CwxMqQueueMgr(string const& strQueueLogFilePath,
        CWX_UINT32 uiMaxFsyncNum);
    ~CwxMqQueueMgr();
public:
    //0:成功；-1：失败
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

    ///用于commit类型的队列，提交commit消息。
    ///返回值：0：不存在，1：成功，-1：失败；-2：队列不存在
    int commitBinlog(string const& strQueue,
        CWX_UINT64 ullSid,
        bool bCommit=true,
        CWX_UINT32 uiDeley=0, ///<若bCommit=false，则可设置消息多少秒后可被在此获取
        char* szErr2K=NULL);
    ///消息发送完毕，bSend=true表示已经发送成功；false表示发送失败
    ///返回值：0：不存在，1：成功，-1：失败，-2：队列不存在
    int endSendMsg(string const& strQueue,
        CWX_UINT64 ullSid,
        bool bSend=true,
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

    void getQueuesInfo(list<CwxMqQueueInfo>& queues);

    inline bool isExistQueue(string const& strQueue)
    {
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        return m_queues.find(strQueue) != m_queues.end();
    }
    //-1：权限失败；0：队列不存在；1：成功
    inline int authQueue(string const& strQueue, string const& user, string const& passwd)
    {
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >::const_iterator iter = m_queues.find(strQueue);
        if (iter == m_queues.end()) return 0;
        if (iter->second.first->getUserName().length())
        {
            return ((user != iter->second.first->getUserName()) || (passwd != iter->second.first->getPasswd()))?-1:1;
        }
        return 1;
    }
    inline CWX_UINT32 getQueueNum()
    {
        CwxReadLockGuard<CwxRwLock>  lock(&m_lock);
        return m_queues.size();
    }    
    inline bool isValid() const
    {
        return m_bValid;
    }
    inline string const& getErrMsg() const
    {
        return m_strErrMsg;
    }
	inline static bool isInvalidQueueName(char const* queue)
	{
		if (!queue) return false;
		CWX_UINT32 uiLen = strlen(queue);
		if (!uiLen) return false;
		for (CWX_UINT32 i=0; i<uiLen; i++)
		{
			if (queue[i]>='a' && queue[i]<='z') continue;
			if (queue[i]>='A' && queue[i]<='Z') continue;
			if (queue[i]>='0' && queue[i]<='9') continue;
			if (queue[i]=='-' || queue[i]=='_') continue;
			return false;
		}
		return true;
	}

private:
    ///保存数据
    bool _save(CwxMqQueue* queue, CwxMqQueueLogFile* logFile);
	bool _fetchLogFile(set<string/*queue name*/> & queues);
	bool _isQueueLogFile(string const& file, string& queue);
	string& _getQueueLogFile(string const& queue, string& strFile);
private:
    map<string, pair<CwxMqQueue*, CwxMqQueueLogFile*> >   m_queues; ///<队列
    CwxRwLock                  m_lock; ///<读写所
    string                     m_strQueueLogFilePath; ///<queue log文件的路径
    CWX_UINT32                 m_uiMaxFsyncNum; ///<flush硬盘的次数间隔
    CwxBinLogMgr*              m_binLog; ///<binlog driver
    string                     m_strErrMsg; ///<无效时的错误消息
	bool					   m_bValid; ///<是否有效
};


#endif
