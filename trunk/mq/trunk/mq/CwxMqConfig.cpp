#include "CwxMqConfig.h"
#include "CwxMqApp.h"

int CwxMqConfig::loadConfig(string const & strConfFile)
{
    CwxXmlFileConfigParser parser;
    char const* pValue;
    string value;
    string strErrMsg;
    //解析配置文件
    if (false == parser.parse(strConfFile))
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to Load conf file:%s.", strConfFile.c_str());
        return -1;
    }
    //load mq:common:workdir:path
    if ((NULL == (pValue=parser.getElementAttr("mq:common:workdir", "path"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:common:workdir:path].");
        return -1;
    }
    value = pValue;
	if ('/' != value[value.length()-1]) value +="/";
    m_common.m_strWorkDir = value;
    //load  mq:common:server:type
    if ((NULL == (pValue=parser.getElementAttr("mq:common:server", "type"))) || !pValue[0])
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [mq:common:server:type].");
        return -1;
    }
    if (strcasecmp(pValue, "master") == 0)
    {
        m_common.m_bMaster = true;
    }
    else if (strcasecmp(pValue, "slave") == 0)
    {
        m_common.m_bMaster = false;
    }
    else
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "[mq:common:server:type] must be [master] or [slave].");
        return -1;
    }
    //load mq:common:window:dispatch
    if ((NULL == (pValue=parser.getElementAttr("mq:common:window", "sock_buf_kbyte"))) || !pValue[0])
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [mq:common:window:sock_buf_kbyte].");
        return -1;
    }
    m_common.m_uiSockBufSize = strtoul(pValue, NULL, 0);
    if (m_common.m_uiSockBufSize < CwxMqConfigCmn::MIN_SOCK_BUF_KB)
    {
        m_common.m_uiSockBufSize = CwxMqConfigCmn::MIN_SOCK_BUF_KB;
    }
    if (m_common.m_uiSockBufSize > CwxMqConfigCmn::MAX_SOCK_BUF_KB)
    {
        m_common.m_uiSockBufSize = CwxMqConfigCmn::MAX_SOCK_BUF_KB;
    }
    //load mq:common:window:from_master
    if ((NULL == (pValue=parser.getElementAttr("mq:common:window", "max_trunk_kbyte"))) || !pValue[0])
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [mq:common:window:max_trunk_kbyte].");
        return -1;
    }
    m_common.m_uiTrunkSize = strtoul(pValue, NULL, 0);
    if (m_common.m_uiTrunkSize < CwxMqConfigCmn::MIN_TRUNK_SIZE_KB)
    {
        m_common.m_uiTrunkSize = CwxMqConfigCmn::MIN_TRUNK_SIZE_KB;
    }
    if (m_common.m_uiTrunkSize > CwxMqConfigCmn::MAX_TRUNK_BUF_KB)
    {
        m_common.m_uiTrunkSize = CwxMqConfigCmn::MAX_TRUNK_BUF_KB;
    }
    //load mq:common:monitor
    if (!fetchHost(parser, "mq:common:monitor", m_common.m_monitor)) return -1;

    //load mq:binlog:file:path
    if ((NULL == (pValue=parser.getElementAttr("mq:binlog:file", "path"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:binlog:file:path].");
        return -1;
    }
    value = pValue;
    if ('/' != value[value.length()-1]) value +="/";
    m_binlog.m_strBinlogPath  = value;
    //load mq:binlog:file:prefix
    if ((NULL == (pValue=parser.getElementAttr("mq:binlog:file", "prefix"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:binlog:file:prefix].");
        return -1;
    }
    m_binlog.m_strBinlogPrex = pValue;
    //load mq:binlog:file:max_mbyte
    if ((NULL == (pValue=parser.getElementAttr("mq:binlog:file", "max_mbyte"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:binlog:file:max_mbyte].");
        return -1;
    }
    m_binlog.m_uiBinLogMSize = strtoul(pValue, NULL, 0);
    if (m_binlog.m_uiBinLogMSize < CwxMqConfigBinLog::MIN_BINLOG_MSIZE)
    {
        m_binlog.m_uiBinLogMSize = CwxMqConfigBinLog::MIN_BINLOG_MSIZE;
    }
    if (m_binlog.m_uiBinLogMSize > CwxMqConfigBinLog::MAX_BINLOG_MSIZE)
    {
        m_binlog.m_uiBinLogMSize = CwxMqConfigBinLog::MAX_BINLOG_MSIZE;
    }
    //load mq:binlog:manage:max_day
    if ((NULL == (pValue=parser.getElementAttr("mq:binlog:manage", "max_day"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:binlog:manage:max_day].");
        return -1;
    }
    m_binlog.m_uiMgrMaxDay = strtoul(pValue, NULL, 0);
    if (m_binlog.m_uiMgrMaxDay < CwxBinLogMgr::MIN_MANAGE_DAY)
    {
        m_binlog.m_uiMgrMaxDay = CwxBinLogMgr::MIN_MANAGE_DAY;
    }
    if (m_binlog.m_uiMgrMaxDay > CwxBinLogMgr::MAX_MANAGE_DAY)
    {
        m_binlog.m_uiMgrMaxDay = CwxBinLogMgr::MAX_MANAGE_DAY;
    }
    //load mq:binlog:flush:log_num
    if ((NULL == (pValue=parser.getElementAttr("mq:binlog:flush", "log_num"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:binlog:flush:log_num].");
        return -1;
    }
    m_binlog.m_uiFlushNum = strtoul(pValue, NULL, 0);
    if (m_binlog.m_uiFlushNum < 1)
    {
        m_binlog.m_uiFlushNum = 1;
    }
    //load mq:binlog:flush:second
    if ((NULL == (pValue=parser.getElementAttr("mq:binlog:flush", "second"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:binlog:flush:second].");
        return -1;
    }
    m_binlog.m_uiFlushSecond = strtoul(pValue, NULL, 0);
    if (m_binlog.m_uiFlushSecond < 1)
    {
        m_binlog.m_uiFlushSecond = 1;
    }
    //load mq:binlog:mq_flush:fetch_num
    if ((NULL == (pValue=parser.getElementAttr("mq:binlog:mq_flush", "fetch_num"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:binlog:mq_flush:fetch_num].");
        return -1;
    }
    m_binlog.m_uiMqFetchFlushNum = strtoul(pValue, NULL, 0);
    if (m_binlog.m_uiMqFetchFlushNum < 1)
    {
        m_binlog.m_uiMqFetchFlushNum = 1;
    }
    //load mq:binlog:mq_flush:second
    if ((NULL == (pValue=parser.getElementAttr("mq:binlog:mq_flush", "second"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:binlog:mq_flush:second].");
        return -1;
    }
    m_binlog.m_uiMqFetchFlushSecond = strtoul(pValue, NULL, 0);
    if (m_binlog.m_uiMqFetchFlushSecond < 1)
    {
        m_binlog.m_uiMqFetchFlushSecond = 1;
    }

    //load master
    if (m_common.m_bMaster)
    {
        //load mq:master:recv
        if (!fetchHost(parser, "mq:master:recv", m_master.m_recv)) return -1;
        //load mq:master:async
        if (!fetchHost(parser, "mq:master:async", m_master.m_async)) return -1;
    }
    else
    {//slave
        //load mq:slave:master
        if (!fetchHost(parser, "mq:slave:master", m_slave.m_master)) return -1;
        //fetch mq:slave:master:subcribe
        if ((NULL == (pValue=parser.getElementAttr("mq:slave:master", "subcribe"))) || !pValue[0])
        {
            m_slave.m_strSubScribe = "";
        }
        else
        {
            m_slave.m_strSubScribe = pValue;
            if (!CwxMqPoco::isValidSubscribe(m_slave.m_strSubScribe, strErrMsg))
            {
                snprintf(m_szErrMsg,
                    2047,
                    "[mq:slave:master:subcribe]'s value [%s] is not valid subscribe, err:%s",
                    pValue,
                    strErrMsg.c_str());
                return -1;
            }
        }
        //load mq:slave:async
        if (!fetchHost(parser, "mq:slave:async", m_slave.m_async)) return -1;
    }
    //fetch mq:mq:listen
    if (parser.getElementNode("mq:mq"))
    {
        if (!fetchHost(parser, "mq:mq:listen", m_mq.m_listen)) return -1;
        //fetch queue
        CwxXmlTreeNode const* pNodeRoot = NULL;
        CwxXmlTreeNode const* node = NULL;
        CwxHostInfo host;
        pair<char*, char*> key;
        //load mq:mq:queues
        {
            CwxMqConfigQueue mq;
            pNodeRoot = parser.getElementNode("mq:mq:queues");
            if (!pNodeRoot || !pNodeRoot->m_pChildHead)
            {
                CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [mq:mq:queues].");
                return -1;
            }
            node = pNodeRoot->m_pChildHead;
            while(node)
            {
                if (strcmp(node->m_szElement, "queue") == 0)
                {
                    //find name
                    if (CwxCommon::findKey(node->m_lsAttrs, "name",  key) && strlen(key.second))
                    {
                        mq.m_strName = key.second;
                    }
                    else
                    {
                        CwxCommon::snprintf(m_szErrMsg, 2047, "[mq:mq:queues]'s queue must have name.");
                        return -1;
                    }
                    if (m_mq.m_queues.find(mq.m_strName) != m_mq.m_queues.end())
                    {
                        CwxCommon::snprintf(m_szErrMsg, 2047, "[mq:mq:queues]'s queue name[%s] is duplicate.", mq.m_strName.c_str());
                        return -1;
                    }
                    //find user
                    if (CwxCommon::findKey(node->m_lsAttrs, "user",  key) && strlen(key.second))
                    {
                        mq.m_strUser = key.second;
                    }
                    else
                    {
                        mq.m_strUser = "";
                    }
                    //find passwd
                    if (CwxCommon::findKey(node->m_lsAttrs, "passwd",  key) && strlen(key.second))
                    {
                        mq.m_strPasswd = key.second;
                    }
                    else
                    {
                        mq.m_strPasswd = "";
                    }
                    //find subcribe
                    if (CwxCommon::findKey(node->m_lsAttrs, "subcribe",  key) && strlen(key.second))
                    {
                        mq.m_strSubScribe = key.second;
                    }
                    else
                    {
                        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set queue[%s]'s [subcribe].", mq.m_strName.c_str());
                        return -1;
                    }
                    if (!CwxMqPoco::isValidSubscribe(mq.m_strSubScribe, strErrMsg))
                    {
                        CwxCommon::snprintf(m_szErrMsg, 2047, "queue[%s]'s subcribe[%s] is not valid, err:%s.", mq.m_strName.c_str(), strErrMsg.c_str());
                        return -1;
                    }
                    m_mq.m_queues[mq.m_strName] = mq;
                }
                node = node->m_next;
            }
        }
    }
    else
    {
        m_mq.m_listen.reset();
    }
    //fetch mq:mq:
    return 0;
}

bool CwxMqConfig::fetchHost(CwxXmlFileConfigParser& parser, 
                            string const& path,
                            CwxHostInfo& host)
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
    if (!host.getHostName().length() && !host.getUnixDomain().length())
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s]'s ip or unix-domain file.", path.c_str());
        return false;
    }

    return true;

}
void CwxMqConfig::outputConfig() const
{
    CWX_INFO(("\n*****************BEGIN CONFIG*******************"));
    CWX_INFO(("*****************common*******************"));
    CWX_INFO(("workdir=%s", m_common.m_strWorkDir.c_str()));
    CWX_INFO(("server type=%s", m_common.m_bMaster?"master":"slave"));
    CWX_INFO(("window size dispatch=%u  from-master=%u", m_common.m_uiDispatchWindowSize, m_common.m_uiFromMasterWindowSize));
    CWX_INFO(("*****************binlog*******************"));
    CWX_INFO(("file path=%s prefix=%s max-file-size(Mbyte)=%u", m_binlog.m_strBinlogPath.c_str(), m_binlog.m_strBinlogPrex.c_str(), m_binlog.m_uiBinLogMSize));
    CWX_INFO(("manager binlog file max_day=%u", m_binlog.m_uiMgrMaxDay));
    CWX_INFO(("binlog flush log_num=%u second=%u", m_binlog.m_uiFlushNum, m_binlog.m_uiFlushSecond));
    CWX_INFO(("mq-fetch flush log_num=%u second=%u", m_binlog.m_uiMqFetchFlushNum, m_binlog.m_uiMqFetchFlushSecond));
    if (m_common.m_bMaster){
        CWX_INFO(("*****************master*******************"));
        CWX_INFO(("recv keep_alive=%s user=%s passwd=%s ip=%s port=%u unix=%s",
            m_master.m_recv.isKeepAlive()?"yes":"no",
            m_master.m_recv.getUser().c_str(),
            m_master.m_recv.getPasswd().c_str(),
            m_master.m_recv.getHostName().c_str(),
            m_master.m_recv.getPort(),
            m_master.m_recv.getUnixDomain().c_str()));
        CWX_INFO(("async keep_alive=%s user=%s passwd=%s ip=%s port=%u unix=%s",
            m_master.m_async.isKeepAlive()?"yes":"no",
            m_master.m_async.getUser().c_str(),
            m_master.m_async.getPasswd().c_str(),
            m_master.m_async.getHostName().c_str(),
            m_master.m_async.getPort(),
            m_master.m_async.getUnixDomain().c_str()));
    }else{
        CWX_INFO(("*****************slave*******************"));
        CWX_INFO(("master keep_alive=%s user=%s passwd=%s subscribe=%s ip=%s port=%u unix=%s",
            m_slave.m_master.isKeepAlive()?"yes":"no",
            m_slave.m_master.getUser().c_str(),
            m_slave.m_master.getPasswd().c_str(),
            m_slave.m_strSubScribe.c_str(),
            m_slave.m_master.getHostName().c_str(),
            m_slave.m_master.getPort(),
            m_slave.m_master.getUnixDomain().c_str()));
        CWX_INFO(("async keep_alive=%s user=%s passwd=%s ip=%s port=%u unix=%s",
            m_slave.m_async.isKeepAlive()?"yes":"no",
            m_slave.m_async.getUser().c_str(),
            m_slave.m_async.getPasswd().c_str(),
            m_slave.m_async.getHostName().c_str(),
            m_slave.m_async.getPort(),
            m_master.m_async.getUnixDomain().c_str()));
    }
    CWX_INFO(("*****************mq-fetch*******************"));
    CWX_INFO(("listen keep_alive=%s  ip=%s port=%u unix=%s",
        m_mq.m_listen.isKeepAlive()?"yes":"no",
        m_mq.m_listen.getHostName().c_str(),
        m_mq.m_listen.getPort(),
        m_mq.m_listen.getUnixDomain().c_str()));
    map<string, CwxMqConfigQueue>::const_iterator iter = m_mq.m_queues.begin(); ///<消息分发的队列
    while(iter != m_mq.m_queues.end())
    {
        CWX_INFO(("queue name=%s\tuser=%s\tpasswd=%s\tsubscribe=%s",
            iter->second.m_strName.c_str(),
            iter->second.m_strUser.c_str(),
            iter->second.m_strPasswd.c_str(),
            iter->second.m_strSubScribe.c_str()));
        iter++;
    }
    CWX_INFO(("*****************END   CONFIG *******************"));
}
