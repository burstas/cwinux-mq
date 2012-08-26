#include "CwxMqDef.h"
#include "CwxMqQueueMgr.h"
#include "CwxMqPoco.h"


CwxMqFetchConn::CwxMqFetchConn(){
    m_bWaiting = false;
    m_bBlock = false;
    m_uiTaskId = 0;
}

CwxMqFetchConn::~CwxMqFetchConn(){
}

void CwxMqFetchConn::reset()
{
    m_bWaiting = false;
    m_bBlock = false;
    m_uiTaskId = 0;
}

bool mqParseHostPort(string const& strHostPort, CwxHostInfo& host){
    if ((strHostPort.find(':') == string::npos) || (0 == strHostPort.find(':'))) return false;
    host.setHostName(strHostPort.substr(0, strHostPort.find(':')));
    host.setPort(strtoul(strHostPort.substr(strHostPort.find(':')+1).c_str(), NULL, 10));
    return true;
}
