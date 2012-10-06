#ifndef __CWX_MC_QUEUE_H__
#define __CWX_MC_QUEUE_H__

#include "CwxMqMacro.h"
#include "CwxGlobalMacro.h"
#include "CwxCommon.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"
#include "CwxMutexLock.h"
#include "CwxLockGuard.h"

// 队列的数据项
struct CwxMcQueueItem{
public:
  enum{
    ALIGN_SIZE = 128  ///< 数据空间按照128对齐
  };
public:
  inline CWX_UINT64 getLogSid() const {
    return m_ullLogSid;
  }
  inline CWX_UINT32 getLogHostId() const {
    return m_uiLogHostId;
  }
  inline CWX_UINT32 getLogTimestamp() const{
    return m_uiLogTimestamp;
  }
  inline CWX_UINT32 getItemSize() const{
    return m_uiItemSize;
  }
  inline CWX_UINT32 getDataSize() const{
    return m_uiDataSize;
  }
  inline char const* getData() const{
    return m_szLogData;
  }
  bool operator < (CwxMcQueueItem const& item) const{
    if (m_uiLogTimestamp < item.m_uiLogTimestamp) return true;
    if (m_uiLogTimestamp > item.m_uiLogTimestamp) return false;
    if (m_uiLogHostId < item.m_uiLogHostId) return true;
    if (m_uiLogHostId > item.m_uiLogHostId) return false;
    return m_ullLogSid < item.m_ullLogSid;
  }
  ///创建对象
  static CwxMcQueueItem* createItem(CWX_UINT64 ullSid,
    CWX_UINT32 uiLogHostId, 
    CWX_UINT32 uiLogTimestamp,
    char const* szData,
    CWX_UINT32 uiDataLen);
  static void destoryItem(CwxMcQueueItem* item);
private:
  CwxMcQueueItem(){};
  ~CwxMcQueueItem(){}
private:
  CWX_UINT64     m_ullLogSid;     ///<日志的sid
  CWX_UINT32     m_uiLogHostId; ///<日志所属的host
  CWX_UINT32     m_uiLogTimestamp;  ///<日志的时间戳
  CWX_UINT32     m_uiItemSize;     ///<item对象的空间大小
  CWX_UINT32     m_uiDataSize;    ///<data的数据大小
  char           m_szLogData[0];     ///<data的数据
};


/* 此对象为一个queue对象，管理最新的日志数据。
**/
class CwxMcQueue {
public:
  CwxMcQueue(CWX_UINT32  uiTimeout, ///< 数据的超时时间
    CWX_UINT64   ullMaxSize  ///<管理数据的数量
    )
  {
    m_uiTimeout = uiTimeout;
    m_ullMaxSize = ullMaxSize;
    m_ullCurSize = 0;
    m_ullDiscardNum = 0;
  }

  ~CwxMcQueue() {
  }
public:
  ///添加数据
  void push(CwxMcQueueItem* log);
  ///pop数据，空表示没有数据
  CwxMcQueueItem* pop();
  ///检查超时
  void checkTimeout(CWX_UINT32 uiTime);
  ///获取日志的最大时间戳
  inline CWX_UINT32 getMaxTimestamp() {
    CwxMutexGuard<CwxMutexLock>  lock(&m_lock);
    if (m_queue.size()){
      CwxMcQueueItem* item = *m_queue.rbegin();
      return item->getLogTimestamp();
    }
    return time(NULL);
  }
  ///获取日志的最小时间戳
  inline CWX_UINT32 getMinTimestamp() {
    CwxMutexGuard<CwxMutexLock>  lock(&m_lock);
    if (m_queue.size()){
      CwxMcQueueItem* item = *m_queue.begin();
      return item->getLogTimestamp();
    }
    return time(NULL);
  }
  ///获取日志的数量
  inline CWX_UINT32 getCount() {
    CwxMutexGuard<CwxMutexLock>  lock(&m_lock);
    return m_queue.size();
  }
  ///获取空间大小
  inline CWX_UINT64 getSize() const{
    return m_ullCurSize;
  }
  ///获取丢弃的日志数量
  inline CWX_UINT64 getDiscardNum() {
    CwxMutexGuard<CwxMutexLock>  lock(&m_lock);
    return m_ullDiscardNum;
  }
private:
  CwxMutexLock               m_lock;
  set<CwxMcQueueItem*, CwxPointLess<CwxMcQueueItem> >  m_queue;
  CWX_UINT32                 m_uiTimeout; ///< 数据的超时时间
  CWX_UINT64                 m_ullMaxSize;  ///<管理数据的数量
  CWX_UINT64                 m_ullCurSize;  ///<当前的大小
  CWX_UINT64                 m_ullDiscardNum; ///<丢弃的日志数量
};

#endif
