#include "CwxMproxyConfig.h"
#include "CwxMproxyApp.h"

int CwxMproxyConfig::loadConfig(string const & strConfFile){
    CwxIniParse	cnf;
    string value;
    //解析配置文件
    if (false == cnf.load(strConfFile)){
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to Load conf file:%s. err:%s", strConfFile.c_str(), cnf.getErrMsg());
        return -1;
    }
    //load cmn:home
    if (!cnf.getAttr("cmn", "home", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [cmn:home].");
        return -1;
    }
    if ('/' != value[value.length()-1]) value +="/";
    m_strWorkDir = value;

    //load cmn:timeout
    if (!cnf.getAttr("cmn", "timeout", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [cmn:timeout].");
        return -1;
    }
    m_uiTimeout = strtoul(value.c_str(), NULL, 10);

    //load cmn:monitor
    if (!cnf.getAttr("cmn", "monitor", value) || !value.length()){
        m_monitor.reset();
    }else{
        if (!mqParseHostPort(value, m_monitor)){
            snprintf(m_szErrMsg, 2047, "cmn:monitor must be [host:port], [%s] is invalid.", value.c_str());
            return -1;
        }
    }

    //recv
    if (!fetchHost(cnf, "recv", m_recv)) return -1;
    //fetch passwd
    list<pair<string, string> > attrs;
    CwxMqConfigQueue passwd;
    list<pair<CWX_UINT32, CWX_UINT32> > ids;
    list<pair<CWX_UINT32, CWX_UINT32> >::iterator id_iter;
    //load passwd 
    m_groupPasswd.clear();
    if (cnf.getAttr("passwd", attrs)){
        list<pair<string, string> >::iterator pwd_iter = attrs.begin();
        while(pwd_iter != attrs.end()){
            if (!parsePasswd(pwd_iter->first, pwd_iter->second, passwd)) return -1;
            if (!parseIds(passwd.m_strSubScribe, ids)) return -1;
            id_iter = ids.begin();
            while(id_iter != ids.end())
            {
                CwxMqIdRange id(id_iter->first, id_iter->second);
                if (m_groupPasswd.find(id) != m_groupPasswd.end())
                {
                    snprintf(m_szErrMsg, 2047, "passwd for name=%s, group[%u,%u] for passwd is duplicate with passwd for name=%s，group[%u,%u]",
                        passwd.m_strName.c_str(),
                        id_iter->first,
                        id_iter->second,
                        m_groupPasswd.find(id)->second.m_strName.c_str(),
                        m_groupPasswd.find(id)->first.getBegin(),
                        m_groupPasswd.find(id)->first.getEnd());
                    return -1;
                }
                m_groupPasswd[id] = passwd;
                id_iter++;
            }
            pwd_iter++;
        }
    }

    //load allow
    m_allowGroup.clear();
    if (cnf.getAttr("allow", attrs)){
        list<pair<string, string> >::iterator allow_iter = attrs.begin();
        while(allow_iter != attrs.end()){
            if (!parseIds(allow_iter->second, ids)) return -1;
            id_iter = ids.begin();
            while(id_iter != ids.end()){
                CwxMqIdRange id(id_iter->first, id_iter->second);
                if (m_allowGroup.find(id) != m_allowGroup.end()){
                    snprintf(m_szErrMsg, 2047, "[allow:%s]'s[%u,%u] is duplicate with [allow:%s]",
                        allow_iter->first.c_str(),
                        id_iter->first,
                        id_iter->second,
                        m_allowGroup.find(id)->second.c_str());
                    return -1;
                }
                m_allowGroup[id] = allow_iter->first;
                id_iter++;
            }
            allow_iter++;
        }
    }
    //load deny
    m_denyGroup.clear();
    if (cnf.getAttr("deny", attrs)){
        list<pair<string, string> >::iterator deny_iter = attrs.begin();
        while(deny_iter != attrs.end()){
            if (!parseIds(deny_iter->second, ids)) return -1;
            id_iter = ids.begin();
            while(id_iter != ids.end()){
                CwxMqIdRange id(id_iter->first, id_iter->second);
                if (m_allowGroup.find(id) != m_allowGroup.end()){
                    snprintf(m_szErrMsg, 2047, "[deny:%s]'s[%u,%u] is duplicate with [deny:%s]",
                        deny_iter->first.c_str(),
                        id_iter->first,
                        id_iter->second,
                        m_allowGroup.find(id)->second.c_str());
                    return -1;
                }
                m_denyGroup[id] = deny_iter->first;
                id_iter++;
            }
            deny_iter++;
        }
    }
    //mq server
    if (!fetchHost(cnf, "mq", m_mq)) return -1;
    //load mq:sign
    if (!cnf.getAttr("mq", "sign", value) || !value.length()){
        m_mqSign = "";
    }else{
        m_mqSign = value;
        if ((m_mqSign  != CWX_MQ_MD5) && (m_mqSign != CWX_MQ_CRC32))
        {
            snprintf(m_szErrMsg, 2047, "Invalid mq sign[%s], it must be %s or %s",
                m_mqSign.c_str(),
                CWX_MQ_MD5,
                CWX_MQ_CRC32);
            return -1;
        }
    }
    //load mq:zip
    if (!cnf.getAttr("mq", "zip", value) || !value.length()){
        m_bzip = false;
    }else{
        if (value == "yes")
            m_bzip = true;
        else
            m_bzip = false;
    }
    return 0;
}

void CwxMproxyConfig::outputConfig()
{
    CWX_INFO(("*****************BEGIN CONFIG *******************"));
    CWX_INFO(("***************** cmn *******************"));
    CWX_INFO(("home=%s", m_strWorkDir.c_str()));
    CWX_INFO(("monitor=%s:%u", m_monitor.getHostName().c_str(), m_monitor.getPort()));
    CWX_INFO(("timeout=%u", m_uiTimeout));
    CWX_INFO(("***************** recv *******************"));
    CWX_INFO(("listen=%s:%u", m_recv.getHostName().c_str(), m_recv.getPort()));
    CWX_INFO(("user=%s", m_recv.getUser().c_str()));
    CWX_INFO(("passwd=%s", m_recv.getPasswd().c_str()));
    CWX_INFO(("unix=%s", m_recv.getUnixDomain().c_str()));
    CWX_INFO(("****************** passwd *****************"));
    {
        map<CwxMqIdRange, CwxMqConfigQueue>::iterator iter = m_groupPasswd.begin();
        while(iter != m_groupPasswd.end())
        {
            CWX_INFO(("%s=%s:%s:[%u,%u]",
                iter->second.m_strName.c_str(),
                iter->second.m_strUser.c_str(),
                iter->second.m_strPasswd.c_str(),
                iter->first.getBegin(),
                iter->first.getEnd()));
            iter++;
        }
    }
    CWX_INFO(("*********************** allow ************************"));
    {
        map<CwxMqIdRange, string>::iterator iter = m_allowGroup.begin();
        while(iter != m_allowGroup.end())
        {
            CWX_INFO(("%s=[%u,%u]",
                iter->second.c_str(),
                iter->first.getBegin(),
                iter->first.getEnd()));
            iter++;
        }
    }
    CWX_INFO(("******************** deny  ***********************"));
    {
        map<CwxMqIdRange, string>::iterator iter = m_denyGroup.begin();
        while(iter != m_denyGroup.end())
        {
            CWX_INFO(("%s=[%u,%u]",
                iter->second.c_str(),
                iter->first.getBegin(),
                iter->first.getEnd()));
            iter++;
        }
    }
    CWX_INFO(("************************* mq *************************"));
    CWX_INFO(("listen=%s:%u", m_mq.getHostName().c_str(), m_mq.getPort()));
    CWX_INFO(("user=%s", m_mq.getUser().c_str()));
    CWX_INFO(("passwd=%s", m_mq.getPasswd().c_str()));
    CWX_INFO(("unix=%s", m_mq.getUnixDomain().c_str()));
    CWX_INFO(("sign=%s", m_mqSign.c_str()));
    CWX_INFO(("zip=%s", m_bzip?"yes":"no"));
    CWX_INFO(("\n*****************END   CONFIG *******************\n"));   
}

bool CwxMproxyConfig::fetchHost(CwxIniParse& cnf,
                                string const& node,
                                CwxHostInfo& host,
                                bool bIpOnly)
{
    string value;
    host.reset();
    //get listen
    if (cnf.getAttr(node, "listen", value) && value.length()){
        if (!mqParseHostPort(value, host)){
            snprintf(m_szErrMsg, 2047, "%s:listen must be [host:port], [%s] is invalid.", node.c_str(), value.c_str());
            return false;
        }
    }
    //load keepalive
    if (cnf.getAttr(node, "keepalive", value) && value.length()){
        host.setKeepAlive(value=="yes"?true:false);
    }else{
        host.setKeepAlive(false);
    }
    //load user
    if (cnf.getAttr(node, "user", value) && value.length()){
        host.setUser(value);
    }else{
        host.setUser("");
    }
    //load passwd
    if (cnf.getAttr(node, "passwd", value) && value.length()){
        host.setPassword(value);
    }else{
        host.setPassword("");
    }
    //load path:unix
    if (cnf.getAttr(node, "unix", value) && value.length()){
        host.setUnixDomain(value);
    }else{
        host.setUnixDomain("");
    }
    if (!bIpOnly){
        if (!host.getHostName().length() && !host.getUnixDomain().length()){
            CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s]'s [listen] or [unix].", node.c_str());
            return false;
        }
    }else{
        if (!host.getHostName().length()){
            CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s]'s [listen].", node.c_str());
            return false;
        }
    }
    return true;
}

bool CwxMproxyConfig::parsePasswd(string const& strName, string const& strPasswd, CwxMqConfigQueue& passwd)
{
    passwd.m_strName = strName;
    list<string> items;
    list<string>::iterator iter;
    CwxCommon::split(strPasswd, items, ':');
    if (3 != items.size()){
        CwxCommon::snprintf(m_szErrMsg, 2047, "[passwd:%s] is invalid, must be in format for [user:passwd:group]", strName.c_str());
        return false;
    }
    iter = items.begin();
    passwd.m_strUser = *iter;
    CwxCommon::trim(passwd.m_strUser);
    iter++;
    passwd.m_strPasswd = *iter;
    CwxCommon::trim(passwd.m_strPasswd);
    iter++;
    passwd.m_strSubScribe = *iter;
    CwxCommon::trim(passwd.m_strSubScribe);
    return true;
}

bool CwxMproxyConfig::parseIds(string const& group, list<pair<CWX_UINT32, CWX_UINT32> >& ids)
{
    pair<CWX_UINT32, CWX_UINT32> item;
    ids.clear();
    list<string> ranges;
    list<string>::iterator iter;
    string strRange;
    string strFirst;
    CwxCommon::split(group, ranges, ',');
    iter = ranges.begin();
    while (iter != ranges.end())
    {
        strRange = *iter;
        CwxCommon::trim(strRange);
        if (strRange.length())
        {
            if (strRange.find('-') != string::npos)
            {//it's a range
                strFirst = strRange.substr(0, strRange.find('-'));
                item.first = strtoul(strFirst.c_str(), NULL, 10);
                item.second = strtoul(strRange.c_str() + strRange.find('-') + 1, NULL, 10);
            }
            else
            {
                item.first = item.second = strtoul(strRange.c_str(), NULL, 10);
            }
            if (item.first > item.second)
            {
                snprintf(m_szErrMsg, 2047, "group[%s]'s end[%u] is less than being[%u]",
                    group.c_str(),
                    item.second,
                    item.first);
                return false;
            }
            ids.push_back(item);
        }
        iter++;
    }
    return true;
}
