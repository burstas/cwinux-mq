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
#include "CwxSidLogFile.h"

class CwxMqQueue {
public:
  CwxMqQueue(string strName,
    string strUser,
    string strPasswd,
    CwxBinLogMgr* pBinlog);
  ~CwxMqQueue();
public:
  ///0:成功;-1：失败
  int init(CWX_UINT64 ullSid, string& strErrMsg);
  ///0：没有消息；
  ///1：获取一个消息；
  ///2：达到了搜索点，但没有发现消息；
  ///-1：失败；
  int getNextBinlog(CwxMqTss* pTss,
    CwxMsgBlock*&msg,
    int& err_num,
    char* szErr2K);

  ///消息发送完毕，bSend=true表示已经发送成功；false表示发送失败
  void endSendMsg(CWX_UINT64 ullSid, bool bSend = true);
  ///获取队列的名字
  inline string const& getName() const {
    return m_strName;
  }
  ///获取队列的用户名
  inline string const& getUserName() const {
    return m_strUser;
  }
  ///获取队列的用户口令
  inline string const& getPasswd() const {
    return m_strPasswd;
  }
  ///获取cursor的起始sid
  inline CWX_UINT64 getCurSid() const {
    if (m_cursor)
      return m_cursor->getHeader().getSid();
    return 0;
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
  string m_strName; ///<队列的名字
  string m_strUser; ///<队列鉴权的用户名
  string m_strPasswd; ///<队列鉴权的口令
  CwxBinLogMgr* m_binLog; ///<binlog
  map<CWX_UINT64, CwxMsgBlock*> m_uncommitMap; ///<commit队列中未commit的消息sid索引
  map<CWX_UINT64, CwxMsgBlock*> m_memMsgMap; ///<发送失败消息队列
  CwxBinLogCursor* m_cursor; ///<队列的游标
};

class CwxMqQueueMgr {
public:
  CwxMqQueueMgr(string const& strQueuePath,
    CWX_UINT32 uiFlushNum,
    CWX_UINT32 uiFlushSecond);
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
    int& err_num, ///<错误消息
    char* szErr2K = NULL);

  ///消息发送完毕，bSend=true表示已经发送成功；false表示发送失败
  ///返回值：0：成功，-1：失败，-2：队列不存在
  int endSendMsg(string const& strQueue,
    CWX_UINT64 ullSid,
    bool bSend = true,
    char* szErr2K = NULL);

  ///强行flush mq的log文件
  void commit();
  // 时间commit检查；
  void timeout(CWX_UINT32 uiNow);
  ///添加队列
  int addQueue(string const& strQueue,
    CWX_UINT64 ullSid,
    string const& strUser,
    string const& strPasswd,
    char* szErr2K = NULL);
  ///1：成功
  ///0：不存在
  ///-1：其他错误
  int delQueue(string const& strQueue, char* szErr2K = NULL);

  void getQueuesInfo(list<CwxMqQueueInfo>& queues);

  inline bool isExistQueue(string const& strQueue) {
    CwxReadLockGuard<CwxRwLock> lock(&m_lock);
    return m_queues.find(strQueue) != m_queues.end();
  }
  //-1：权限失败；0：队列不存在；1：成功
  inline int authQueue(string const& strQueue,
    string const& user,
    string const& passwd)
  {
    CwxReadLockGuard<CwxRwLock> lock(&m_lock);
    map<string, pair<CwxMqQueue*, CwxSidLogFile*> >::const_iterator iter =
      m_queues.find(strQueue);
    if (iter == m_queues.end())
      return 0;
    if (iter->second.first->getUserName().length()) {
      return
        ((user != iter->second.first->getUserName())
        || (passwd != iter->second.first->getPasswd())) ? -1 : 1;
    }
    return 1;
  }
  inline CWX_UINT32 getQueueNum() {
    CwxReadLockGuard<CwxRwLock> lock(&m_lock);
    return m_queues.size();
  }
  inline bool isValid() const {
    return m_bValid;
  }
  inline string const& getErrMsg() const {
    return m_strErrMsg;
  }
  inline static bool isInvalidQueueName(char const* queue) {
    if (!queue)
      return false;
    CWX_UINT32 uiLen = strlen(queue);
    if (!uiLen)
      return false;
    for (CWX_UINT32 i = 0; i < uiLen; i++) {
      if (queue[i] >= 'a' && queue[i] <= 'z')
        continue;
      if (queue[i] >= 'A' && queue[i] <= 'Z')
        continue;
      if (queue[i] >= '0' && queue[i] <= '9')
        continue;
      if (queue[i] == '-' || queue[i] == '_')
        continue;
      return false;
    }
    return true;
  }

private:
  bool _fetchLogFile(set<string/*queue name*/> & queues);
  bool _isQueueLogFile(string const& file, string& queue);
  string& _getQueueLogFile(string const& queue, string& strFile);
private:
  map<string, pair<CwxMqQueue*, CwxSidLogFile*> > m_queues; ///<队列
  CwxRwLock           m_lock; ///<读写所
  string              m_strQueuePath; ///<queue log文件的路径
  CWX_UINT32          m_uiFlushNum; ///<flush硬盘的次数间隔
  CWX_UINT32          m_uiFlushSecond; ///<flush硬盘的时间间隔
  CwxBinLogMgr*       m_binLog; ///<binlog driver
  string              m_strErrMsg; ///<无效时的错误消息
  bool               m_bValid; ///<是否有效
};

#endif
