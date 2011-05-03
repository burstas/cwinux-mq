#ifndef __CWX_MQ_QUEUE_MGR_H__
#define __CWX_MQ_QUEUE_MGR_H__
/*
版权声明：
    本软件遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
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

class CwxMqQueue
{
public:
    CwxMqQueue(CWX_UINT32 uiId,
        string strName,
        string strUser,
        string strPasswd,
        CWX_UINT64 ullStartSid,
        CwxBinLogMgr* pBinlog);
    ~CwxMqQueue();
public:
    ///0：没有消息；
    ///1：获取一个消息；
    ///2：达到了搜索点，但没有发现消息；
    ///-1：失败；
    int getNextBinlog(CwxMqTss* pTss, int& err_num, char* szErr2K);
    ///将一个发送失败的消息，送回queue。
    ///true：成功；false：失败。此时是内存分配错误
    bool backMsg(CwxBinLogHeader const& header, CwxKeyValueItem const& data);

    inline CWX_UINT32 getId() const
    {
        return m_uiId;
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
    inline CWX_UINT64 getCurSid() const
    {
        return m_cursor?m_cursor->getHeader().getSid():0;
    }
    inline CWX_UINT64 getMqNum()
    {
        if (!m_cursor)
        {
            if (m_ullStartSid < m_binLog->getMaxSid())
            {
                m_cursor = m_binLog->createCurser();
                if (!m_cursor)
                {
                    return 0;
                }
                int iRet = m_binLog->seek(m_cursor, m_ullStartSid);
                if (1 != iRet)
                {
                    m_binLog->destoryCurser(m_cursor);
                    m_cursor = NULL;
                    return 0;
                }
            }
            else
            {
                return 0;
            }
        }
        return m_binLog->leftLogNum(m_cursor) + m_memMsgTail->count();
    }
private:
    CWX_UINT32                       m_uiId; ///<队列的id
    string                           m_strName; ///<队列的名字
    string                           m_strUser; ///<队列鉴权的用户名
    string                           m_strPasswd; ///<队列鉴权的口令
    CWX_UINT64                       m_ullStartSid; ///<队列开始的sid
    CwxBinLogCursor*                 m_cursor; ///<队列的游标
    CwxSTail<CwxMsgBlock>*           m_memMsgTail; ///<队列的数据
    CwxBinLogMgr*                    m_binLog; ///<binlog
    CwxMqSubscribe                   m_subscribe; ///<订阅
};

class CwxMqQueueMgr
{
public:
    enum
    {
        QUEUE_ID_START = 1
    };
public:
    CwxMqQueueMgr();
    ~CwxMqQueueMgr();
public:
    int init(CwxBinLogMgr* binLog,
        map<string, CWX_UINT64> const& queueSid,
        map<string, CwxMqConfigQueue> const& queueInfo);
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
    map<CWX_UINT32, CwxMqQueue*> m_idQueues;
};


#endif
