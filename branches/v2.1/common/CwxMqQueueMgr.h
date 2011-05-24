#ifndef __CWX_MQ_QUEUE_MGR_H__
#define __CWX_MQ_QUEUE_MGR_H__
/*
��Ȩ������
    �������ѭGNU GPL V3��http://www.gnu.org/licenses/gpl.html����
    ��ϵ��ʽ��email:cwinux@gmail.com��΢��:http://t.sina.com.cn/cwinux
*/
/**
@file CwxMqMgr.h
@brief MQϵ�з����MQ�������������ļ���
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
#include "
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
    ///0:�ɹ�;-1��ʧ��
    int init(CWX_UINT64 ullLastCommitSid,
        set<CWX_UINT64>& uncommitSid,
        set<CWX_UINT64>& commitSid,
        string& strErrMsg);
    ///0��û����Ϣ��
    ///1����ȡһ����Ϣ��
    ///2���ﵽ�������㣬��û�з�����Ϣ��
    ///-1��ʧ�ܣ�
    int getNextBinlog(CwxMqTss* pTss,
        CwxMsgBlock*&msg,
        CWX_UINT32 uiTimeout,
        int& err_num,
        char* szErr2K);

    ///���ڷ�commit���͵Ķ��У�bCommit=true�˱�ʾ��Ϣ�Ѿ�д��socket buf�������ʾдʧ�ܡ�
    ///����commit���͵Ķ��У�bCommit=true�˱�ʾ�Ѿ��յ��Է���commitȷ�ϣ�����дsocketʧ�ܡ�
    ///����ֵ��0�������ڣ�1���ɹ�.
    int commitBinlog(CWX_UINT64 ullSid, bool bCommit=true);
    ///���commit���Ͷ��г�ʱ����Ϣ
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
        return m_uncommitMap; ///<commit������δcommit����Ϣsid����
    }
    inline map<CWX_UINT64, CwxMsgBlock*>& getMemMsgMap()
    {
        return m_memMsgMap;///<����ʧ����Ϣ����
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
    ///��ȡcursor����ʼsid
    inline CWX_UINT64 getStartSid()
    {
        if (m_lastUncommitSid.size())
        {
           return *m_lastUncommitSid.begin() - 1;
        }
        return m_ullLastCommitSid;
    }
    ///��ȡdump��Ϣ
    void getQueueDumpInfo(CWX_UINT64 ullLastCommitSid,
        set<CWX_UINT64>& uncommitSid,
        set<CWX_UINT64>& commitSid);
    CWX_UINT64 getMqNum();
private:
    ///0��û����Ϣ��
    ///1����ȡһ����Ϣ��
    ///2���ﵽ�������㣬��û�з�����Ϣ��
    ///-1��ʧ�ܣ�
    int fetchNextBinlog(CwxMqTss* pTss,
        CwxMsgBlock*&msg,
        int& err_num,
        char* szErr2K);
private:
    string                           m_strName; ///<���е�����
    string                           m_strUser; ///<���м�Ȩ���û���
    string                           m_strPasswd; ///<���м�Ȩ�Ŀ���
    bool                             m_bCommit; ///<�Ƿ�commit���͵Ķ���
    CWX_UINT32                       m_uiDefTimeout; ///<ȱʡ��timeoutֵ
    CWX_UINT32                       m_uiMaxTimeout; ///<����timeoutֵ
    string                           m_strSubScribe; ///<���Ĺ���
    CwxBinLogMgr*                    m_binLog; ///<binlog
    CwxMinHeap<CwxMqQueueHeapItem>*  m_pUncommitMsg; ///<commit������δcommit����Ϣ
    map<CWX_UINT64, void*>           m_uncommitMap; ///<commit������δcommit����Ϣsid����
    map<CWX_UINT64, CwxMsgBlock*>    m_memMsgMap;///<����ʧ����Ϣ����
    CwxBinLogCursor*                 m_cursor; ///<���е��α�
    CwxMqSubscribe                   m_subscribe; ///<����

    CWX_UINT64                       m_ullLastCommitSid; ///<��־�ļ���¼��cursor��sid
    set<CWX_UINT64>                  m_lastUncommitSid; ///<m_ullLastCommitSid֮ǰδcommit��binlog
    set<CWX_UINT64>                  m_lastCommitSid; ///<m_ullLastCommitSid֮��commit��binlog
};


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
        m_strName = item.m_strName; ///<���е�����
        m_strUser = item.m_strUser; ///<���м�Ȩ���û���
        m_bCommit = item.m_bCommit; ///<�Ƿ�commit���͵Ķ���
        m_uiDefTimeout = item.m_uiDefTimeout; ///<ȱʡ��timeoutֵ
        m_uiMaxTimeout = item.m_uiMaxTimeout; ///<����timeoutֵ
        m_strSubScribe = item.m_strSubScribe; ///<���Ĺ���
        m_ullCursorSid = item.m_ullCursorSid;
        m_ullLeftNum = item.m_ullLeftNum; ///<ʣ����Ϣ������
        m_uiWaitCommitNum = item.m_uiWaitCommitNum; ///<�ȴ�commit����Ϣ����
        m_uiMemLogNum = item.m_uiMemLogNum;
        m_ucQueueState = item.m_ucQueueState;
        m_strQueueErrMsg = item.m_strQueueErrMsg;
    }
    CwxMqQueueInfo& operator=CwxMqQueueInfo(CwxMqQueueInfo const& item)
    {
        if (this != &item)
        {
            m_strName = item.m_strName; ///<���е�����
            m_strUser = item.m_strUser; ///<���м�Ȩ���û���
            m_bCommit = item.m_bCommit; ///<�Ƿ�commit���͵Ķ���
            m_uiDefTimeout = item.m_uiDefTimeout; ///<ȱʡ��timeoutֵ
            m_uiMaxTimeout = item.m_uiMaxTimeout; ///<����timeoutֵ
            m_strSubScribe = item.m_strSubScribe; ///<���Ĺ���
            m_ullCursorSid = item.m_ullCursorSid;
            m_ullLeftNum = item.m_ullLeftNum; ///<ʣ����Ϣ������
            m_uiWaitCommitNum = item.m_uiWaitCommitNum; ///<�ȴ�commit����Ϣ����
            m_uiMemLogNum = item.m_uiMemLogNum;
            m_ucQueueState = item.m_ucQueueState;
            m_strQueueErrMsg = item.m_strQueueErrMsg;
        }
        return *this;
    }
public:
    string                           m_strName; ///<���е�����
    string                           m_strUser; ///<���м�Ȩ���û���
    bool                             m_bCommit; ///<�Ƿ�commit���͵Ķ���
    CWX_UINT32                       m_uiDefTimeout; ///<ȱʡ��timeoutֵ
    CWX_UINT32                       m_uiMaxTimeout; ///<����timeoutֵ
    string                           m_strSubScribe; ///<���Ĺ���
    CWX_UINT64                       m_ullCursorSid; ///<��ǰcursor��sid
    CWX_UINT64                       m_ullLeftNum; ///<ʣ����Ϣ������
    CWX_UINT32                       m_uiWaitCommitNum; ///<�ȴ�commit����Ϣ����
    CWX_UINT32                       m_uiMemLogNum; ///<�ڴ�����Ϣ������
    CWX_UINT8                        m_ucQueueState; ///<����״̬
    string                           m_strQueueErrMsg; //<���еĴ�����Ϣ
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
        string const& strQueueLogFile,
        string const& strQueuePosFile,
        CWX_UINT32 uiMaxFsyncNum,
        CWX_UINT32 uiMaxFsyncSecond);
    ~CwxMqQueueMgr();
public:
    int init(CwxBinLogMgr* binLog);
public:
    ///0��û����Ϣ��
    ///1����ȡһ����Ϣ��
    ///2���ﵽ�������㣬��û�з�����Ϣ��
    ///-1��ʧ�ܣ�
    ///-2�����в�����
    int getNextBinlog(CwxMqTss* pTss,
        string const& strQueue,
        CwxMsgBlock*&msg,
        CWX_UINT32 uiTimeout,
        int& err_num,
        char* szErr2K);

    ///���ڷ�commit���͵Ķ��У�bCommit=true�˱�ʾ��Ϣ�Ѿ�д��socket buf�������ʾдʧ�ܡ�
    ///����commit���͵Ķ��У�bCommit=true�˱�ʾ�Ѿ��յ��Է���commitȷ�ϣ�����дsocketʧ�ܡ�
    ///����ֵ��0�������ڣ�1���ɹ���-1��ʧ�ܣ�-2�����в�����
    int commitBinlog(string const& strQueue,
        CWX_UINT64 ullSid,
        bool bCommit=true);
    ///���commit���Ͷ��г�ʱ����Ϣ
    void checkTimeout(CWX_UINT32 ttTimestamp);
    ///1���ɹ�
    ///0������
    ///-1����������
    int addQueue(string const& strQueue,
        bool bCommit,
        string const& strUser,
        string const& strPasswd,
        string const& strScribe,
        CWX_UINT32 uiDefTimeout,
        CWX_UINT32 uiMaxTimeout,
        char* szErr2K=NULL);
    ///1���ɹ�
    ///0��������
    ///-1����������
    int delQueue(string const& strQueue,
        string const& strUser,
        string const& strPasswd,
        char* szErr2K=NULL);

    inline bool isExistQueue(string const& strQueue) const
    {
        CwxMutexGuard<CwxMutexLock>  lock;
        return m_queues.find(strQueue) != m_queues.end();
    }
    //-1��ʧ�ܣ�0�����в����ڣ�1���ɹ�
    inline int authQueue(string const& strQueue, string const& user, string const& passwd) const
    {
        CwxMutexGuard<CwxMutexLock>  lock;
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
        CwxMutexGuard<CwxMutexLock>  lock;
        return m_queues.size();
    }
    inline void getQueuesInfo(list<CwxMqQueueInfo>& queues) const
    {
        CwxMqQueueInfo info;
        map<string, CwxMqQueue*>::const_iterator iter = m_queues.begin();
        while(iter != m_queues.end())
        {
            info.m_strName = iter->second->getName();
            info.m_strUser = iter->second->getUserName();
            info.m_bCommit = iter->second->isCommit();
            info.m_uiDefTimeout = iter->second->getDefTimeout();
            info.m_uiMaxTimeout = iter->second->getMaxTimeout();
            info.m_strSubScribe = iter->second->getSubscribeRule();
            info.m_ullCursorSid = iter->second->getCursorSid();
            info.m_ullLeftNum = iter->second->getMqNum();
            info.m_uiWaitCommitNum = iter->second->getWaitCommitNum();
            info.m_uiMemLogNum = iter->second->getMemMsgMap().size();
            if (iter->second->getCursor())
            {
                info.m_ucQueueState = iter->second->getCursor()->getSeekState();
                if (CwxBinLogMgr::CURSOR_STATE_ERROR == info.m_ucQueueState)
                {
                    info.m_strQueueErrMsg = iter->second->getCursor()->getErrMsg();
                }
                else
                {
                    info.m_strQueueErrMsg = "";
                }
            }
            else
            {
                info.m_ucQueueState = CwxBinLogMgr::CURSOR_STATE_UNSEEK;
                info.m_strQueueErrMsg = "";
            }
            queues.push_back(info);
            iter++;
        }
    }

private:
    map<string, CwxMqQueue*>   m_queues;
    CwxMutexLock               m_lock;
    string                     m_strQueueFile;
    string                     m_strQueueLogFile;
    CWX_UINT32                 m_uiMaxFsyncNum;
    CWX_UINT32                 m_uiMaxFsyncSecond;
};


#endif
