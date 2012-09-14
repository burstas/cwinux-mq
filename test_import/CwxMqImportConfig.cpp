#include "CwxMqImportConfig.h"

int CwxMqImportConfig::loadConfig(string const & strConfFile){
    CwxIniParse	 cnf;
    string value;
    //解析配置文件
    if (false == cnf.load(strConfFile))
    {
        CwxCommon::snprintf(m_szError, 2047, "Failure to Load conf file:%s. err:%s", strConfFile.c_str(), cnf.getErrMsg());
        return -1;
    }

    //load import:home
    if (!cnf.getAttr("import", "home", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:home].");
        return -1;
    }
    if ('/' != value[value.length()-1]) value +="/";
    m_strWorkDir = value;
    //load import:listen
    if (!cnf.getAttr("import", "listen", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:listen].");
        return -1;
    }
    if (!mqParseHostPort(value, m_listen)){
        snprintf(m_szError, 2047, "import:listen must be [host:port], [%s] is invalid.", value.c_str());
        return -1;
    }
    //load import:unix
    if (!cnf.getAttr("import", "unix", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:unix].");
        return -1;
    }
    m_strUnixPathFile = value;
    //load import:user
    if (!cnf.getAttr("import", "user", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:user].");
        return -1;
    }
    m_strUser = value;
    //load import:passwd
    if (!cnf.getAttr("import", "passwd", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:passwd].");
        return -1;
    }
    m_strPasswd = value;
    //load import:conn_type
    if (!cnf.getAttr("import", "conn_type", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:conn_type].");
        return -1;
    }
    m_bTcp = (value=="tcp"?true:false);
    //load import:conn_num
    if (!cnf.getAttr("import", "conn_num", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:conn_num].");
        return -1;
    }
    m_unConnNum = strtoul(value.c_str(), NULL, 10);
    if (!m_unConnNum) m_unConnNum = 1;
    //load import:conn_lasting
    if (!cnf.getAttr("import", "conn_lasting", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:conn_lasting].");
        return -1;
    }
    m_bLasting = (value == "1"?true:false);
    //load import:data_group
    if (!cnf.getAttr("import", "data_group", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:data_group].");
        return -1;
    }
    m_uiGroup = strtoul(value.c_str(), NULL, 10);
    //load import:data_size
    if (!cnf.getAttr("import", "data_size", value) || !value.length()){
        snprintf(m_szError, 2047, "Must set [import:data_size].");
        return -1;
    }
    m_unDataSize = strtoul(value.c_str(), NULL, 10);
    return 0;
}

void CwxMqImportConfig::outputConfig(){
    CWX_INFO(("\n*****************BEGIN CONFIG*******************"));
    CWX_INFO(("home=%s", m_strWorkDir.c_str()));
    CWX_INFO(("listen=%s:%u", m_listen.getHostName().c_str(), m_listen.getPort()));
    CWX_INFO(("unix=%s", m_strUnixPathFile.c_str()));
    CWX_INFO(("user=%s", m_strUser.c_str()));
    CWX_INFO(("passwd=%s", m_strPasswd.c_str()));
    CWX_INFO(("conn_type=%s", m_bTcp?"tcp":"unix"));
    CWX_INFO(("conn_num=%u", m_unConnNum));
    CWX_INFO(("conn_lasting=%u", m_bLasting?1:0));
    CWX_INFO(("data_group=%u", m_uiGroup));
    CWX_INFO(("data_size=%u", m_unDataSize));
    CWX_INFO(("*****************END   CONFIG *******************"));
}
