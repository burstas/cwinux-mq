#include "CwxMqFetchConfig.h"

int CwxMqFetchConfig::loadConfig(string const & strConfFile){
    CwxIniParse	 cnf;
    string value;
    //Ω‚Œˆ≈‰÷√Œƒº˛
    if (false == cnf.load(strConfFile))
    {
        CwxCommon::snprintf(m_szError, 2047, "Failure to Load conf file:%s. err:%s", strConfFile.c_str(), cnf.getErrMsg());
        return -1;
    }

    //load fetch:home
    if (!cnf.getAttr("fetch", "home", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:home].");
        return -1;
    }
    if ('/' != value[value.length()-1]) value +="/";
    m_strWorkDir = value;
    //load fetch:listen
    if (!cnf.getAttr("fetch", "listen", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:listen].");
        return -1;
    }
    if (!mqParseHostPort(value, m_listen)){
        snprintf(m_szError, 2047, "fetch:listen must be [host:port], [%s] is invalid.", value.c_str());
        return -1;
    }
    //load fetch:unix
    if (!cnf.getAttr("fetch", "unix", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:unix].");
        return -1;
    }
    m_strUnixPathFile = value;
    //load fetch:user
    if (!cnf.getAttr("fetch", "user", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:user].");
        return -1;
    }
    m_strUser = value;
    //load fetch:passwd
    if (!cnf.getAttr("fetch", "passwd", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:passwd].");
        return -1;
    }
    m_strPasswd = value;
    //load fetch:queue
    if (!cnf.getAttr("fetch", "queue", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:queue].");
        return -1;
    }
    m_strQueue = value;
    //load fetch:conn_type
    if (!cnf.getAttr("fetch", "conn_type", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:conn_type].");
        return -1;
    }
    m_bTcp = (value=="tcp"?true:false);
    //load fetch:conn_num
    if (!cnf.getAttr("fetch", "conn_num", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:conn_num].");
        return -1;
    }
    m_unConnNum = strtoul(value.c_str(), NULL, 10);
    if (!m_unConnNum) m_unConnNum = 1;
    //load fetch:conn_lasting
    if (!cnf.getAttr("fetch", "conn_lasting", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:conn_lasting].");
        return -1;
    }
    m_bLasting = (value == "1"?true:false);
    //load fetch:block
    if (!cnf.getAttr("fetch", "block", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [fetch:block].");
        return -1;
    }
    m_bBlock = (value == "1"?true:false);
    return 0;
}

void CwxMqFetchConfig::outputConfig(){
    CWX_INFO(("\n*****************BEGIN CONFIG*******************"));
    CWX_INFO(("home=%s", m_strWorkDir.c_str()));
    CWX_INFO(("listen=%s:%u", m_listen.getHostName().c_str(), m_listen.getPort()));
    CWX_INFO(("unix=%s", m_strUnixPathFile.c_str()));
    CWX_INFO(("user=%s", m_strUser.c_str()));
    CWX_INFO(("passwd=%s", m_strPasswd.c_str()));
    CWX_INFO(("queue=%s", m_strQueue.c_str()));
    CWX_INFO(("conn_type=%s", m_bTcp?"tcp":"unix"));
    CWX_INFO(("conn_num=%u", m_unConnNum));
    CWX_INFO(("conn_lasting=%u", m_bLasting?1:0));
    CWX_INFO(("block=%u", m_bBlock?1:0));
    CWX_INFO(("*****************END   CONFIG *******************"));
}
