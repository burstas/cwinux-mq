#include "CwxMqDef.h"
#include "CwxMqQueueMgr.h"
#include "CwxMqPoco.h"

CwxMqDispatchConn::CwxMqDispatchConn(CwxAppHandler4Channel handler)
{
    m_handler = handler;
    m_pCursor = NULL;
    m_ullStartSid = 0;
    m_ullSid = 0;
    m_uiChunk = 0;
    m_bNext = false;
    m_bSync = false;
}

CwxMqDispatchConn::~CwxMqDispatchConn()
{

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


