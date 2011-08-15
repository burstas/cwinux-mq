#include "CwxMqFetchConfig.h"

int CwxMqFetchConfig::loadConfig(string const & strConfFile){
    CwxXmlFileConfigParser parser;
    char const* pValue;
    string value;
    //Ω‚Œˆ≈‰÷√Œƒº˛
    if (false == parser.parse(strConfFile)){
        snprintf(m_szError, 2047, "Failure to Load conf file.");
        return -1;
    }
    //load workdir svr_def:workdir{path}
    if ((NULL == (pValue=parser.getElementAttr("svr_def:workdir", "path"))) || !pValue[0]){
        snprintf(m_szError, 2047, "Must set [svr_def:workdir].");
        return -1;
    }
    value = pValue;
	if ('/' != value[value.length()-1]) value +="/";
    m_strWorkDir = value;

    // load echo connect num
    if ((NULL == (pValue=parser.getElementAttr("svr_def:conn", "num"))) || !pValue[0]){
        snprintf(m_szError, 2047, "Must set [svr_def:conn:num].");
        return -1;
    }
    m_unConnNum = strtoul(pValue, NULL, 0);
    // load query conn type
    if ((NULL == (pValue=parser.getElementAttr("svr_def:conn", "type"))) || !pValue[0]){
        snprintf(m_szError, 2047, "Must set [svr_def:conn:type].");
        return -1;
    }
    m_bTcp = strcasecmp("tcp", pValue)==0?true:false;

    // load query conn lasting
    if ((NULL == (pValue=parser.getElementAttr("svr_def:conn", "lasting"))) || !pValue[0]){
        snprintf(m_szError, 2047, "Must set [svr_def:conn:lasting].");
        return -1;
    }
    m_bLasting = strcasecmp("1", pValue)==0?true:false;
    // load query block
    if ((NULL == (pValue=parser.getElementAttr("svr_def:conn", "block"))) || !pValue[0]){
        snprintf(m_szError, 2047, "Must set [svr_def:conn:block].");
        return -1;
    }
    m_bBlock = strcasecmp("1", pValue)==0?true:false;

    //load listen
    if ((NULL == (pValue=parser.getElementAttr("svr_def:listen", "ip"))) || !pValue[0]){
        snprintf(m_szError, 2047, "Must set [svr_def:listen:ip].");
        return -1;
    }
    m_listen.setHostName(pValue);
    if ((NULL == (pValue=parser.getElementAttr("svr_def:listen", "port"))) || !pValue[0]){
        snprintf(m_szError, 2047, "Must set [svr_def:listen:port].");
        return -1;
    }
    m_listen.setPort(strtoul(pValue, NULL, 0));

    //load svr_def:unix{path}
    if ((NULL == (pValue=parser.getElementAttr("svr_def:unix", "path"))) || !pValue[0]){
        snprintf(m_szError, 2047, "Must set [svr_def:unix].");
        return -1;
    }
    m_strUnixPathFile = pValue;
    //load svr_def:auth{user}
    if ((NULL == (pValue=parser.getElementAttr("svr_def:auth", "user"))) || !pValue[0]){
        m_strUser.erase();
    }
    else
    {
        m_strUser = pValue;
    }
    //load svr_def:auth{passwd}
    if ((NULL == (pValue=parser.getElementAttr("svr_def:auth", "passwd"))) || !pValue[0]){
        m_strPasswd.erase();
    }
    else
    {
        m_strPasswd = pValue;
    }
    // load queue name
    if ((NULL == (pValue=parser.getElementAttr("svr_def:queue", "name"))) || !pValue[0]){
        snprintf(m_szError, 2047, "Must set [svr_def:queue:name].");
        return -1;
    }
    m_strQueue = pValue;


    return 0;
}

void CwxMqFetchConfig::outputConfig(string & strConfig){
    char szBuf[32];
	strConfig.clear();	
	strConfig += "*****************BEGIN CONFIG *******************";
    strConfig += "\nworkdir= " ;
    strConfig += m_strWorkDir;
    strConfig += "\nconn_num=";
    sprintf(szBuf, "%u", m_unConnNum);
    strConfig += szBuf;
	strConfig += "\nlisten: ip=";
    strConfig += m_listen.getHostName();
    strConfig += " port=";
    sprintf(szBuf, "%u", m_listen.getPort());
    strConfig += szBuf;
    strConfig += "\nauth: user=";
    strConfig += m_strUser;
    strConfig += "\nauth: passwd=";
    strConfig += m_strPasswd;
    strConfig += "\n*****************END   CONFIG *******************\n";   
}
