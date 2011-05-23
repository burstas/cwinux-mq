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
    ///0:�ɹ�;-1��ʧ��
    int init(string& strErrMsg);
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
    void checkTimeout();
    ///�����start sid���Ѿ�commit����Ϣsid
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
    CWX_UINT64                       m_ullStartSid; ///<���п�ʼ��sid
    bool                             m_bCommit; ///<�Ƿ�commit���͵Ķ���
    CWX_UINT32                       m_uiDefTimeout; ///<ȱʡ��timeoutֵ
    CWX_UINT32                       m_uiMaxTimeout; ///<����timeoutֵ
    string                           m_strSubScribe; ///<���Ĺ���
    CwxBinLogMgr*                    m_binLog; ///<binlog
    CwxMinHeap<CwxMqQueueHeapItem>* m_pUncommitMsg; ///<commit������δcommit����Ϣ
    map<CWX_UINT64, void*>           m_uncommitMap; ///<commit������δcommit����Ϣsid����
    set<CWX_UINT64>                  m_dispatchSid; ///<�Ѿ��ַ�����Ϣ
    CwxBinLogCursor*                 m_cursor; ///<���е��α�
    CwxSTail<CwxMsgBlock>*           m_memMsgTail; ///<���е�����
    CwxMqSubscribe                   m_subscribe; ///<����
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
