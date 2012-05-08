#include "CwxMqDef.h"
#include "CwxMqQueueMgr.h"
#include "CwxMqPoco.h"

CwxMqDispatchConn::CwxMqDispatchConn()
{
    m_pCursor = NULL;
    m_ullStartSid = 0;
    m_uiChunk = 0;
    m_bNext = false;
    m_bSync = false;
    m_bZip = false;
    m_uiWindow = 1;
}

CwxMqDispatchConn::~CwxMqDispatchConn()
{

}


CwxMqFetchConn::CwxMqFetchConn()
{
    m_bWaiting = false;
    m_bBlock = false;
    m_bCommit = false;
    m_uiTimeout = 0;
    m_ullSendSid = 0;
    m_bSent = true;
    m_uiTaskId = 0;
}

CwxMqFetchConn::~CwxMqFetchConn()
{

}

void CwxMqFetchConn::reset()
{
    m_bWaiting = false;
    m_bBlock = false;
    m_bCommit = false;
    m_uiTimeout = 0;
    m_bSent = true;
    m_ullSendSid = 0;
    m_uiTaskId = 0;
}

bool mqParseHostPort(string const& strHostPort, CwxHostInfo& host){
    if ((strHostPort.find(':') == string::npos) || (0 == strHostPort.find(':'))) return false;
    host.setHostName(strHostPort.substr(0, strHostPort.find(':')));
    host.setPort(strtoul(strHostPort.substr(strHostPort.find(':')+1).c_str(), NULL, 10));
    return true;
}
