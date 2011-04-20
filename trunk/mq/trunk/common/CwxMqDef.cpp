#include "CwxMqDef.h"
#include "CwxMqQueueMgr.h"
#include "CwxMqPoco.h"

CwxMqDispatchConn::CwxMqDispatchConn(CWX_UINT32 uiSvrId,
                                     CWX_UINT32 uiHosId,
                                     CWX_UINT32 uiConnId,
                                     CWX_UINT32 uiWindowSize)
                                     :m_window(uiSvrId, uiHosId, uiConnId, uiWindowSize)
{
    m_bNext = false;
    m_bSync = false;
    m_prev = NULL;
    m_next = NULL;
}

CwxMqDispatchConn::~CwxMqDispatchConn()
{

}

CwxMqDispatchConnSet::CwxMqDispatchConnSet(CwxBinLogMgr* pBinlogMgr)
{
    m_pBinlogMgr = pBinlogMgr;
}

CwxMqDispatchConnSet::~CwxMqDispatchConnSet()
{
    map<CWX_UINT32, CwxMqDispatchConn*>::iterator iter = m_clientMap.begin();
    CwxBinLogCursor* pCursor = NULL;
    while (iter != m_clientMap.end())
    {
        if (iter->second->m_window.getHandle())
        {
            pCursor = (CwxBinLogCursor*)iter->second->m_window.getHandle();
            m_pBinlogMgr->destoryCurser(pCursor);
        }
        delete iter->second;
        iter++;
    }
}

CwxMqFetchConn::CwxMqFetchConn()
{
    m_uiConnId = 0;
    m_bBlock = false;
    m_bTail = false;
    m_uiTaskId = 0;
    m_pQueue = NULL;
    m_prev = NULL;
    m_next = NULL;
}

CwxMqFetchConn::~CwxMqFetchConn()
{

}

CwxMqFetchConnSet::CwxMqFetchConnSet()
{
    m_connPool = new CwxTypePoolEx<CwxMqFetchConn>(1024);
}

CwxMqFetchConnSet::~CwxMqFetchConnSet()
{
    if (m_connPool) delete m_connPool;
}


