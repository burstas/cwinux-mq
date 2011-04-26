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
    if ((NULL == (pValue=parser.getElementAttr("mq:common:window", "max_chunk_kbyte"))) || !pValue[0])
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [mq:common:window:max_chunk_kbyte].");
        return -1;
    }
    m_common.m_uiChunkSize = strtoul(pValue, NULL, 0);
    if (m_common.m_uiChunkSize < CwxMqConfigCmn::MIN_CHUNK_SIZE_KB)
    {
        m_common.m_uiChunkSize = CwxMqConfigCmn::MIN_CHUNK_SIZE_KB;
    }
    if (m_common.m_uiChunkSize > CwxMqConfigCmn::MAX_CHUNK_SIZE_KB)
    {
        m_common.m_uiChunkSize = CwxMqConfigCmn::MAX_CHUNK_SIZE_KB;
    }
    //load mq:common:monitor
    if (parser.getElementNode("mq:common:monitor"))
    {
        if (!fetchHost(parser, "mq:common:monitor", m_common.m_monitor)) return -1;
    }
    else
    {
        m_common.m_monitor.reset();
    }
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

    //load mq:binlog:manage:del_out_file
    if ((NULL == (pValue=parser.getElementAttr("mq:binlog:manage", "del_out_file"))) || !pValue[0])
    {
        snprintf(m_szErrMsg, 2047, "Must set [mq:binlog:manage:del_out_file].");
        return -1;
    }
    m_binlog.m_bDelOutdayLogFile = strcmp("yes", pValue)==0?true:false;

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
        //load mq:master:recv_bin
        if (parser.getElementNode("mq:master:recv_bin"))
        {
            if (!fetchHost(parser, "mq:master:recv_bin", m_master.m_recv_bin)) return -1;
        }
        else
        {
            m_master.m_recv_bin.reset();
        }
        //load mq:master:recv_mc
        if (parser.getElementNode("mq:master:recv_mc"))
        {
            if (!fetchHost(parser, "mq:master:recv_mc", m_master.m_recv_mc)) return -1;
        }
        else
        {
            m_master.m_recv_mc.reset();
        }
        if (!m_master.m_recv_mc.getHostName().length() &&
            !m_master.m_recv_bin.getHostName().length())
        {
            CWX_ERROR(("Must set [mq:master:recv_mc] or [mq:master:recv_bin]"));
            return -1;
        }
        //load mq:master:async_bin
        if (parser.getElementNode("mq:master:async_bin"))
        {
            if (!fetchHost(parser, "mq:master:async_bin", m_master.m_async_bin)) return -1;
        }
        else
        {
            m_master.m_async_bin.reset();
        }
        //load mq:master:async_mc
        if (parser.getElementNode("mq:master:async_mc"))
        {
            if (!fetchHost(parser, "mq:master:async_mc", m_master.m_async_mc)) return -1;
        }
        else
        {
            m_master.m_async_mc.reset();
        }
    }
    else
    {//slave
        //load mq:slave:master_bin
        if (!fetchHost(parser, "mq:slave:master_bin", m_slave.m_master_bin)) return -1;
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
        //load mq:slave:async_bin
        if (parser.getElementNode("mq:slave:async_bin"))
        {
            if (!fetchHost(parser, "mq:slave:async_bin", m_slave.m_async_bin)) return -1;
        }
        else
        {
            m_slave.m_master_bin.reset();
        }
        //load mq:slave:async_mc
        if (parser.getElementNode("mq:slave:m_async_mc"))
        {
            if (!fetchHost(parser, "mq:slave:async_mc", m_slave.m_async_mc)) return -1;
        }
        else
        {
            m_slave.m_async_mc.reset();
        }
    }
    //fetch mq:mq
    if (parser.getElementNode("mq:mq"))
    {
        if (!fetchMq(parser, "mq:mq", m_mq)) return -1;
    }
    else
    {
        m_mq.m_binListen.reset();
        m_mq.m_mcListen.reset();
    }

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

bool CwxMqConfig::fetchMq(CwxXmlFileConfigParser& parser,
             string const& path,
             CwxMqConfigMq& mq)
{
    string strErrMsg;
    string strPath = path + ":bin_listen";
    if (!fetchHost(parser, strPath.c_str(), mq.m_binListen))
    {
        mq.m_binListen.reset();
    }
    strPath = path + ":mc_listen";
    if (!fetchHost(parser, strPath.c_str(), mq.m_mcListen))
    {
        mq.m_mcListen.reset();
    }
    if (!mq.m_mcListen.getHostName().length() || !mq.m_binListen.getHostName().length())
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s:mc_listen] or [%s:bin_listen].",path.c_str(),path.c_str());
        return false;
    }
    //fetch queue
    CwxXmlTreeNode const* pNodeRoot = NULL;
    CwxXmlTreeNode const* node = NULL;
    CwxHostInfo host;
    pair<char*, char*> key;
    //load mq:mq:queues
    {
        CwxMqConfigQueue queue;
        strPath = path + ":queues";
        pNodeRoot = parser.getElementNode(strPath.c_str());
        if (!pNodeRoot || !pNodeRoot->m_pChildHead)
        {
            CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s:queues].",path.c_str());
            return false;
        }
        node = pNodeRoot->m_pChildHead;
        while(node)
        {
            if (strcmp(node->m_szElement, "queue") == 0)
            {
                //find name
                if (CwxCommon::findKey(node->m_lsAttrs, "name",  key) && strlen(key.second))
                {
                    queue.m_strName = key.second;
                }
                else
                {
                    CwxCommon::snprintf(m_szErrMsg, 2047, "[%:queues]'s queue must have name.", path.c_str());
                    return false;
                }
                if (mq.m_queues.find(queue.m_strName) != mq.m_queues.end())
                {
                    CwxCommon::snprintf(m_szErrMsg, 2047, "[%s:queues]'s queue name[%s] is duplicate.", path.c_str(), queue.m_strName.c_str());
                    return false;
                }
                //find user
                if (CwxCommon::findKey(node->m_lsAttrs, "user",  key) && strlen(key.second))
                {
                    queue.m_strUser = key.second;
                }
                else
                {
                    queue.m_strUser = "";
                }
                //find passwd
                if (CwxCommon::findKey(node->m_lsAttrs, "passwd",  key) && strlen(key.second))
                {
                    queue.m_strPasswd = key.second;
                }
                else
                {
                    queue.m_strPasswd = "";
                }
                //find subcribe
                if (CwxCommon::findKey(node->m_lsAttrs, "subcribe",  key) && strlen(key.second))
                {
                    queue.m_strSubScribe = key.second;
                }
                else
                {
                    CwxCommon::snprintf(m_szErrMsg, 2047, "Must set queue[%s]'s [subcribe].", queue.m_strName.c_str());
                    return false;
                }
                if (!CwxMqPoco::isValidSubscribe(queue.m_strSubScribe, strErrMsg))
                {
                    CwxCommon::snprintf(m_szErrMsg, 2047, "queue[%s]'s subcribe[%s] is not valid, err:%s.", queue.m_strName.c_str(), strErrMsg.c_str());
                    return false;
                }
                mq.m_queues[queue.m_strName] = queue;
            }
            node = node->m_next;
        }
    }
    return true;
}

void CwxMqConfig::outputConfig() const
{
    CWX_INFO(("\n*****************BEGIN CONFIG*******************"));
    CWX_INFO(("*****************common*******************"));
    CWX_INFO(("workdir=%s", m_common.m_strWorkDir.c_str()));
    CWX_INFO(("server type=%s", m_common.m_bMaster?"master":"slave"));
    CWX_INFO(("window sock_buf_kbyte=%u  trunk_kbyte=%u", m_common.m_uiSockBufSize, m_common.m_uiChunkSize));
    CWX_INFO(("*****************binlog*******************"));
    CWX_INFO(("file path=%s prefix=%s max-file-size(Mbyte)=%u", m_binlog.m_strBinlogPath.c_str(), m_binlog.m_strBinlogPrex.c_str(), m_binlog.m_uiBinLogMSize));
    CWX_INFO(("manager binlog file max_day=%u  del_outday_logfile=%s", m_binlog.m_uiMgrMaxDay, m_binlog.m_bDelOutdayLogFile?"yes":"no"));
    CWX_INFO(("binlog flush log_num=%u second=%u", m_binlog.m_uiFlushNum, m_binlog.m_uiFlushSecond));
    CWX_INFO(("mq-fetch flush log_num=%u second=%u", m_binlog.m_uiMqFetchFlushNum, m_binlog.m_uiMqFetchFlushSecond));
    if (m_common.m_bMaster){
        CWX_INFO(("*****************master*******************"));
        if (m_master.m_recv_bin.getHostName().length())
        {
            CWX_INFO(("recv_bin keep_alive=%s user=%s passwd=%s ip=%s port=%u unix=%s",
                m_master.m_recv_bin.isKeepAlive()?"yes":"no",
                m_master.m_recv_bin.getUser().c_str(),
                m_master.m_recv_bin.getPasswd().c_str(),
                m_master.m_recv_bin.getHostName().c_str(),
                m_master.m_recv_bin.getPort(),
                m_master.m_recv_bin.getUnixDomain().c_str()));
        }
        if (m_master.m_recv_mc.getHostName().length())
        {
            CWX_INFO(("recv_bin keep_alive=%s user=%s passwd=%s ip=%s port=%u unix=%s",
                m_master.m_recv_mc.isKeepAlive()?"yes":"no",
                m_master.m_recv_mc.getUser().c_str(),
                m_master.m_recv_mc.getPasswd().c_str(),
                m_master.m_recv_mc.getHostName().c_str(),
                m_master.m_recv_mc.getPort(),
                m_master.m_recv_mc.getUnixDomain().c_str()));
        }
        if (m_master.m_async_bin.getHostName().length())
        {
            CWX_INFO(("async_bin keep_alive=%s user=%s passwd=%s ip=%s port=%u unix=%s",
                m_master.m_async_bin.isKeepAlive()?"yes":"no",
                m_master.m_async_bin.getUser().c_str(),
                m_master.m_async_bin.getPasswd().c_str(),
                m_master.m_async_bin.getHostName().c_str(),
                m_master.m_async_bin.getPort(),
                m_master.m_async_bin.getUnixDomain().c_str()));
        }
        if (m_master.m_async_mc.getHostName().length())
        {
            CWX_INFO(("async_mc keep_alive=%s user=%s passwd=%s ip=%s port=%u unix=%s",
                m_master.m_async_mc.isKeepAlive()?"yes":"no",
                m_master.m_async_mc.getUser().c_str(),
                m_master.m_async_mc.getPasswd().c_str(),
                m_master.m_async_mc.getHostName().c_str(),
                m_master.m_async_mc.getPort(),
                m_master.m_async_mc.getUnixDomain().c_str()));
        }
    }else{
        CWX_INFO(("*****************slave*******************"));
        CWX_INFO(("master_bin keep_alive=%s user=%s passwd=%s subscribe=%s ip=%s port=%u unix=%s",
            m_slave.m_master_bin.isKeepAlive()?"yes":"no",
            m_slave.m_master_bin.getUser().c_str(),
            m_slave.m_master_bin.getPasswd().c_str(),
            m_slave.m_strSubScribe.c_str(),
            m_slave.m_master_bin.getHostName().c_str(),
            m_slave.m_master_bin.getPort(),
            m_slave.m_master_bin.getUnixDomain().c_str()));
        if (m_slave.m_async_bin.getHostName().length())
        {
            CWX_INFO(("async_bin keep_alive=%s user=%s passwd=%s ip=%s port=%u unix=%s",
                m_slave.m_async_bin.isKeepAlive()?"yes":"no",
                m_slave.m_async_bin.getUser().c_str(),
                m_slave.m_async_bin.getPasswd().c_str(),
                m_slave.m_async_bin.getHostName().c_str(),
                m_slave.m_async_bin.getPort(),
                m_master.m_async_bin.getUnixDomain().c_str()));
        }
        if (m_slave.m_async_mc.getHostName().length())
        {
            CWX_INFO(("async_mc keep_alive=%s user=%s passwd=%s ip=%s port=%u unix=%s",
                m_slave.m_async_mc.isKeepAlive()?"yes":"no",
                m_slave.m_async_mc.getUser().c_str(),
                m_slave.m_async_mc.getPasswd().c_str(),
                m_slave.m_async_mc.getHostName().c_str(),
                m_slave.m_async_mc.getPort(),
                m_master.m_async_mc.getUnixDomain().c_str()));
        }
    }

    if (m_mq.m_binListen.getHostName().length() || m_mq.m_mcListen.getHostName().length())
    {
        CWX_INFO(("*****************mq-fetch*******************"));
        if (m_mq.m_binListen.getHostName().length())
        {
            CWX_INFO(("listen keep_alive=%s  ip=%s port=%u unix=%s",
                m_mq.m_binListen.isKeepAlive()?"yes":"no",
                m_mq.m_binListen.getHostName().c_str(),
                m_mq.m_binListen.getPort(),
                m_mq.m_binListen.getUnixDomain().c_str()));
        }
        if (m_mq.m_mcListen.getHostName().length())
        {
            CWX_INFO(("listen keep_alive=%s  ip=%s port=%u unix=%s",
                m_mq.m_mcListen.isKeepAlive()?"yes":"no",
                m_mq.m_mcListen.getHostName().c_str(),
                m_mq.m_mcListen.getPort(),
                m_mq.m_mcListen.getUnixDomain().c_str()));
        }
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
    }


    CWX_INFO(("*****************END   CONFIG *******************"));
}
