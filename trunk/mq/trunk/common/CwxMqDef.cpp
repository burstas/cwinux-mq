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
    m_bWaiting = false;
    m_bBlock = false;
    m_uiTaskId = 0;
    m_pQueue = NULL;
}

CwxMqFetchConn::~CwxMqFetchConn()
{

}

