#include "CwxMproxyConfig.h"
#include "CwxMproxyApp.h"

int CwxMproxyConfig::loadConfig(string const & strConfFile)
{
    CwxXmlFileConfigParser parser;
    char const* pValue;
    //解析配置文件
    if (false == parser.parse(strConfFile))
    {
        snprintf(m_szErrMsg, 2047, "Failure to Load conf file.");
        return -1;
    }
    //load workdir mproxy:workdir{path}
    if ((NULL == (pValue=parser.getElementAttr("mproxy:workdir", "path"))) || !pValue[0])
    {
		snprintf(m_szErrMsg, 2047, "Must set [mproxy:workdir:path].");
        return -1;
    }
    m_strWorkDir = pValue;
	if ('/' != m_strWorkDir[m_strWorkDir.length()-1]) m_strWorkDir +="/";
    //timeout
    if ((NULL == (pValue=parser.getElementAttr("mproxy:timeout", "mili_second"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mproxy:timeout:mili_second].");
        return -1;
    }
    m_uiTimeout = strtoul(pValue, NULL, 0);
    //获取监控地址
    //load mproxy:monitor
    if (parser.getElementNode("mproxy:monitor"))
    {
        if (!fetchHost(parser, "mproxy:monitor", m_monitor, true)) return -1;
    }
    else
    {
        m_monitor.reset();
    }
    //recv
    if (!fetchHost(parser, "mproxy:recv:listen", m_recv)) return -1;
    CwxXmlTreeNode const* pNode = NULL;   
    CwxMqConfigQueue group;
    list<pair<CWX_UINT32, CWX_UINT32> > ids;
    list<pair<CWX_UINT32, CWX_UINT32> >::iterator id_iter;
    //load passwd 
    m_groupPasswd.clear();
    pNode = parser.getElementNode("mproxy:recv:passwd");
    if (pNode)
    {
        pNode = pNode->m_pChildHead;
        while(pNode)
        {
            if (strcmp(pNode->m_szElement, "group")==0)
            {
                if (!loadGroup("mproxy:recv:passwd", pNode, group)) return -1;
            }
            if (!parseIds(group.m_strSubScribe, ids)) return -1;
            id_iter = ids.begin();
            while(id_iter != ids.end())
            {
                CwxMqIdRange id(id_iter->first, id_iter->second);
                if (m_groupPasswd.find(id) != m_groupPasswd.end())
                {
                    snprintf(m_szErrMsg, 2047, "Group for name=%s, group[%u,%u] for passwd is duplicate with group for name=%s， group[%u,%u]",
                        group.m_strName.c_str(),
                        id_iter->first,
                        id_iter->second,
                        m_groupPasswd.find(id)->second.m_strName.c_str(),
                        m_groupPasswd.find(id)->first.getBegin(),
                        m_groupPasswd.find(id)->first.getEnd());
                    return -1;
                }
                m_groupPasswd[id] = group;
                id_iter++;
            }
            pNode = pNode->m_next;
        }
    }
    //load allow
    m_allowGroup.clear();
    pNode = parser.getElementNode("mproxy:recv:allow");
    if (pNode)
    {
        pNode = pNode->m_pChildHead;
        while(pNode)
        {
            if (strcmp(pNode->m_szElement, "group")==0)
            {
                if (!loadGroup("mproxy:recv:allow", pNode, group)) return -1;
            }
            if (!parseIds(group.m_strSubScribe, ids)) return -1;
            id_iter = ids.begin();
            while(id_iter != ids.end())
            {
                CwxMqIdRange id(id_iter->first, id_iter->second);
                if (m_allowGroup.find(id) != m_allowGroup.end())
                {
                    snprintf(m_szErrMsg, 2047, "group[name=%s, %u,%u] for allow is duplicate with group[%s]",
                        group.m_strName.c_str(),
                        id_iter->first,
                        id_iter->second,
                        m_allowGroup.find(id)->second.c_str());
                    return -1;
                }
                m_allowGroup[id] = group.m_strName;
                id_iter++;
            }
            pNode = pNode->m_next;
        }
    }
    //load deny
    m_denyGroup.clear();
    pNode = parser.getElementNode("mproxy:recv:deny");
    if (pNode)
    {
        pNode = pNode->m_pChildHead;
        while(pNode)
        {
            if (strcmp(pNode->m_szElement, "group")==0)
            {
                if (!loadGroup("mproxy:recv:deny", pNode, group)) return -1;
            }
            if (!parseIds(group.m_strSubScribe, ids)) return -1;
            id_iter = ids.begin();
            while(id_iter != ids.end())
            {
                CwxMqIdRange id(id_iter->first, id_iter->second);
                if (m_denyGroup.find(id) != m_denyGroup.end())
                {
                    snprintf(m_szErrMsg, 2047, "group[name=%s, %u,%u] for deny is duplicate with group[%s]",
                        group.m_strName.c_str(),
                        id_iter->first,
                        id_iter->second,
                        m_denyGroup.find(id)->second.c_str());
                    return -1;
                }
                m_denyGroup[id] = group.m_strName;
                id_iter++;
            }
            pNode = pNode->m_next;
        }
    }
    //mq server
    if (!fetchHost(parser, "mproxy:mq:mq", m_mq)) return -1;
    if ((NULL == (pValue=parser.getElementAttr("mproxy:mq:mq", "sign"))) || !pValue[0])
    {
        m_mqSign = "";
    }
	else
	{
		m_mqSign = pValue;
		if ((m_mqSign  != CWX_MQ_MD5) || (m_mqSign != CWX_MQ_CRC32))
		{
			snprintf(m_szErrMsg, 2047, "Invalid mq sign[%s], it must be %s or %s",
				m_mqSign.c_str(),
				CWX_MQ_MD5,
				CWX_MQ_CRC32);
			return -1;
		}
	}
    //load mproxy:mq:mq:zip
    if ((NULL == (pValue=parser.getElementAttr("mproxy:mq:mq", "zip"))) || !pValue[0])
    {
        m_bzip = false;
    }
    else
    {
        if (strcmp("yes", pValue) == 0)
            m_bzip = true;
        else
            m_bzip = false;
    }
    return 0;
}

void CwxMproxyConfig::outputConfig()
{
	CWX_INFO(("*****************BEGIN CONFIG *******************"));
    CWX_INFO(("workdir=%s", m_strWorkDir.c_str()));
    CWX_INFO(("monitor ip=%s  port=%u", m_monitor.getHostName().c_str(), m_monitor.getPort()));
    CWX_INFO(("timeout mili-second = %u", m_uiTimeout));
    CWX_INFO(("recv keep-alive=%s ip=%s  port=%u  user=%s  passwd=%s unix=%s",
        m_recv.isKeepAlive()?"true":"false",
        m_recv.getHostName().c_str(),
        m_recv.getPort(),
        m_recv.getUser().c_str(),
        m_recv.getPasswd().c_str(),
        m_recv.getUnixDomain().c_str()));
    CWX_INFO(("*************************Group Auth*************************"));
    {
        map<CwxMqIdRange, CwxMqConfigQueue>::iterator iter = m_groupPasswd.begin();
        while(iter != m_groupPasswd.end())
        {
            CWX_INFO(("Name=%s  group=[%u,%u]  user=%s  passwd=%s",
                iter->second.m_strName.c_str(),
                iter->first.getBegin(),
                iter->first.getEnd(),
                iter->second.m_strUser.c_str(),
                iter->second.m_strPasswd.c_str()));
            iter++;
        }
    }
    CWX_INFO(("*************************Permit Group*************************"));
    {

        map<CwxMqIdRange, string>::iterator iter = m_allowGroup.begin();
        while(iter != m_allowGroup.end())
        {
            CWX_INFO(("Name=%s: group[%u,%u]",
                iter->second.c_str(),
                iter->first.getBegin(),
                iter->first.getEnd()));
            iter++;
        }
    }
    CWX_INFO(("*************************Deny Group*************************"));
    {

        map<CwxMqIdRange, string>::iterator iter = m_denyGroup.begin();
        while(iter != m_denyGroup.end())
        {
            CWX_INFO(("Name=%s: group[%u,%u]",
                iter->second.c_str(),
                iter->first.getBegin(),
                iter->first.getEnd()));
            iter++;
        }
    }

    CWX_INFO(("Mq server:  keep-alive=%s ip=%s  port=%u  user=%s  passwd=%s unix=%s sign=%s",
        m_mq.isKeepAlive()?"true":"false",
        m_mq.getHostName().c_str(),
        m_mq.getPort(),
        m_mq.getUser().c_str(),
        m_mq.getPasswd().c_str(),
        m_mq.getUnixDomain().c_str(),
        m_mqSign.c_str()));
    CWX_INFO(("\n*****************END   CONFIG *******************\n"));   
}

bool CwxMproxyConfig::fetchHost(CwxXmlFileConfigParser& parser,
               string const& path,
               CwxHostInfo& host,
			   bool bIpOnly)
{
    char const* pValue;
    host.reset();
    pValue=parser.getElementAttr(path.c_str(), "ip");
    if (pValue && pValue[0])
    {
        host.setHostName(pValue);
        //load  path:port
        if ((NULL == (pValue=parser.getElementAttr(path.c_str(), "port"))) || !pValue[0])
        {
            CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s:port].", path.c_str());
            return false;
        }
        host.setPort(strtoul(pValue, NULL, 0));
    }
	else
	{
		host.setHostName("");
	}
    //load path:keep_alive
    pValue=parser.getElementAttr(path.c_str(), "keep_alive");
    if (pValue && pValue[0])
    {
        host.setKeepAlive(strcasecmp(pValue, "yes")==0?true:false);
    }
    else
    {
        host.setKeepAlive(false);
    }
    //load path:user
    pValue=parser.getElementAttr(path.c_str(), "user");
    if (pValue && pValue[0])
    {
        host.setUser(pValue);
    }
    else
    {
        host.setUser("");
    }
    //load path:passwd
    pValue=parser.getElementAttr(path.c_str(), "passwd");
    if (pValue && pValue[0])
    {
        host.setPassword(pValue);
    }
    else
    {
        host.setPassword("");
    }

    //load path:unix
    pValue=parser.getElementAttr(path.c_str(), "unix");
    if (pValue && pValue[0])
    {
        host.setUnixDomain(pValue);
    }
	else
	{
		host.setUnixDomain("");
	}
	if (!bIpOnly)
	{
		if (!host.getHostName().length() && !host.getUnixDomain().length())
		{
			CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s]'s ip or unix-domain file.", path.c_str());
			return false;
		}
	}
	else
	{
		if (!host.getHostName().length())
		{
			CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s]'s ip.", path.c_str());
			return false;
		}
	}

    return true;
}
bool CwxMproxyConfig::loadGroup(string const& path, CwxXmlTreeNode const* pGroup, CwxMqConfigQueue& group)
{
    pair<char*, char*> key;
    //find name
    if (CwxCommon::findKey(pGroup->m_lsAttrs, "name",  key) && strlen(key.second))
    {
        group.m_strName = key.second;
    }
    else
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [name] for [%s:group]", path.c_str());
        return false;
    }
    //find group
    if (CwxCommon::findKey(pGroup->m_lsAttrs, "group",  key) && strlen(key.second))
    {
        group.m_strSubScribe = key.second;
    }
    else
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [group] for [%s:group:%s]", path.c_str(), group.m_strName.c_str());
        return false;
    }
    //find user
    if (CwxCommon::findKey(pGroup->m_lsAttrs, "user",  key) && strlen(key.second))
    {
        group.m_strUser = key.second;
    }
    else
    {
        group.m_strUser = "";
    }
    //find passwd
    if (CwxCommon::findKey(pGroup->m_lsAttrs, "passwd",  key) && strlen(key.second))
    {
        group.m_strPasswd = key.second;
    }
    else
    {
        group.m_strPasswd = "";
    }
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
                item.first = strtoul(strFirst.c_str(), NULL, 0);
                item.second = strtoul(strRange.c_str() + strRange.find('-') + 1, NULL, 0);
            }
            else
            {
                item.first = item.second = strtoul(strRange.c_str(), NULL, 0);
            }
			if (item.first > item.second)
			{
				snprintf(m_szErrMsg, 2047, "group[%s]'s end[%u] is less than being[%u]",
					group.c_str(),
					item.end,
					item.first);
				return false;
			}
            ids.push_back(item);
        }
        iter++;
    }
    return true;
}
