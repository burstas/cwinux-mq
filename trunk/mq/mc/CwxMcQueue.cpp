#include "CwxMcQueue.h"
///创建对象
CwxMcQueueItem* CwxMcQueueItem::createItem(CWX_UINT64 ullSid,
                                           CWX_UINT32 uiLogHostId, 
                                           CWX_UINT32 uiLogTimestamp,
                                           char const* szData,
                                           CWX_UINT32 uiDataLen)
{
  CWX_UINT32 uiSize = sizeof(CwxMcQueueItem) + uiDataLen;
  uiSize = (uiSize + ALIGN_SIZE - 1)/ALIGN_SIZE;
  uiSize *= ALIGN_SIZE;
  CwxMcQueueItem* item = (CwxMcQueueItem*)malloc(uiSize);
  item->m_uiItemSize = uiSize;
  item->m_uiDataSize = uiDataLen;
  item->m_ullLogSid = ullSid;
  item->m_uiLogHostId = uiLogHostId;
  item->m_uiLogTimestamp = uiLogTimestamp;
  if (uiDataLen) memcpy(item->m_szLogData, szData, uiDataLen);
  return item;
}
void CwxMcQueueItem::destoryItem(CwxMcQueueItem* item){
  free(item);
}

///添加数据
void CwxMcQueue::push(CwxMcQueueItem* log){
  CwxMutexGuard<CwxMutexLock>  lock(&m_lock);
  m_queue.insert(log);
  m_ullCurSize += log->getItemSize();
  while(m_ullCurSize >  m_ullMaxSize){
    log = *m_queue.begin();
    m_ullCurSize -= log->getItemSize();
    m_ullDiscardNum ++;
    m_queue.erase(m_queue.begin());
    CwxMcQueueItem::destoryItem(log);
  }
}
///pop数据，空表示没有数据
CwxMcQueueItem* CwxMcQueue::pop(){
  CwxMutexGuard<CwxMutexLock>  lock(&m_lock);
  if (m_queue.size()){
    CwxMcQueueItem* log = *m_queue.begin();
    m_ullCurSize -= log->getItemSize();
    m_queue.erase(m_queue.begin());
    return log;
  }
  return NULL;

}
///检查超时
void CwxMcQueue::checkTimeout(CWX_UINT32 uiTime){
  CwxMcQueueItem* log;
  CwxMutexGuard<CwxMutexLock>  lock(&m_lock);
  if (m_queue.size()){
    log = *m_queue.rbegin();
    CWX_UINT32 uiTimeoutTime = log->getLogTimestamp();
    if (uiTimeoutTime > uiTime) uiTimeoutTime = uiTime;
    while(true){
      log = *m_queue.begin();
      if (log->getLogTimestamp() + m_uiTimeout >= uiTimeoutTime) break;
      m_queue.erase(m_queue.begin());
      m_ullCurSize -= log->getItemSize();
      CwxMcQueueItem::destoryItem(log);
    }
  }
}
