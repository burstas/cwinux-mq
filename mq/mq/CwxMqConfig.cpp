#include "CwxMqConfig.h"
#include "CwxMqApp.h"

int CwxMqConfig::loadConfig(string const & strConfFile){
    CwxIniParse	 cnf;
    string value;
    string strErrMsg;
    //Ω‚Œˆ≈‰÷√Œƒº˛
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
    m_common.m_strWorkDir = value;

    //load  cmn:server_type
    if (!cnf.getAttr("cmn", "server_type", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [cmn:server_type].");
        return -1;
    }
    if (value == "master"){
        m_common.m_bMaster = true;
    }else if (value == "slave"){
        m_common.m_bMaster = false;
    }else{
        CwxCommon::snprintf(m_szErrMsg, 2047, "[cmn:server_type] must be [master] or [slave].");
        return -1;
    }

    //load cmn:sock_buf_kbyte
    if (!cnf.getAttr("cmn", "sock_buf_kbyte", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [cmn:sock_buf_kbyte].");
        return -1;
    }
    m_common.m_uiSockBufSize = strtoul(value.c_str(), NULL, 10);
    if (m_common.m_uiSockBufSize < CwxMqConfigCmn::MIN_SOCK_BUF_KB){
        m_common.m_uiSockBufSize = CwxMqConfigCmn::MIN_SOCK_BUF_KB;
    }
    if (m_common.m_uiSockBufSize > CwxMqConfigCmn::MAX_SOCK_BUF_KB){
        m_common.m_uiSockBufSize = CwxMqConfigCmn::MAX_SOCK_BUF_KB;
    }
    //load cmn:max_chunk_kbyte
    if (!cnf.getAttr("cmn", "max_chunk_kbyte", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [cmn:max_chunk_kbyte].");
        return -1;
    }
    m_common.m_uiChunkSize = strtoul(value.c_str(), NULL, 10);
    if (m_common.m_uiChunkSize < CwxMqConfigCmn::MIN_CHUNK_SIZE_KB){
        m_common.m_uiChunkSize = CwxMqConfigCmn::MIN_CHUNK_SIZE_KB;
    }
    if (m_common.m_uiChunkSize > CwxMqConfigCmn::MAX_CHUNK_SIZE_KB){
        m_common.m_uiChunkSize = CwxMqConfigCmn::MAX_CHUNK_SIZE_KB;
    }
    //load cmn:window
    if (!cnf.getAttr("cmn", "sync_conn_num", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [cmn:sync_conn_num].");
        return -1;
    }
    m_common.m_uiSyncConnNum = strtoul(value.c_str(), NULL, 10);
    if (m_common.m_uiSyncConnNum < CwxMqConfigCmn::MIN_SYNC_CONN_NUM){
        m_common.m_uiSyncConnNum = CwxMqConfigCmn::MIN_SYNC_CONN_NUM;
    }
    if (m_common.m_uiSyncConnNum > CwxMqConfigCmn::MAX_SYNC_CONN_NUM){
        m_common.m_uiSyncConnNum = CwxMqConfigCmn::MAX_SYNC_CONN_NUM;
    }
    //load cmn:monitor
    if (!cnf.getAttr("cmn", "monitor", value) || !value.length()){
        m_common.m_monitor.reset();
    }else{
        if (!mqParseHostPort(value, m_common.m_monitor)){
            snprintf(m_szErrMsg, 2047, "cmn:monitor must be [host:port], [%s] is invalid.", value.c_str());
            return -1;
        }
    }

    //load binlog:path
    if (!cnf.getAttr("binlog", "path", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [binlog:path].");
        return -1;
    }
    if ('/' != value[value.length()-1]) value +="/";
    m_binlog.m_strBinlogPath  = value;

    //load binlog:file_prefix
    if (!cnf.getAttr("binlog", "file_prefix", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [binlog:file_prefix].");
        return -1;
    }
    m_binlog.m_strBinlogPrex = value;

    //load binlog:file_max_mbyte
    if (!cnf.getAttr("binlog", "file_max_mbyte", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [binlog:file_max_mbyte].");
        return -1;
    }
    m_binlog.m_uiBinLogMSize = strtoul(value.c_str(), NULL, 10);
    if (m_binlog.m_uiBinLogMSize < CwxMqConfigBinLog::MIN_BINLOG_MSIZE){
        m_binlog.m_uiBinLogMSize = CwxMqConfigBinLog::MIN_BINLOG_MSIZE;
    }
    if (m_binlog.m_uiBinLogMSize > CwxMqConfigBinLog::MAX_BINLOG_MSIZE){
        m_binlog.m_uiBinLogMSize = CwxMqConfigBinLog::MAX_BINLOG_MSIZE;
    }
    //load binlog:max_file_num
    if (!cnf.getAttr("binlog", "max_file_num", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [binlog:max_file_num].");
        return -1;
    }
    m_binlog.m_uiMgrFileNum = strtoul(value.c_str(), NULL, 10);
    if (m_binlog.m_uiMgrFileNum < CwxBinLogMgr::MIN_MANAGE_FILE_NUM){
        m_binlog.m_uiMgrFileNum = CwxBinLogMgr::MIN_MANAGE_FILE_NUM;
    }
    if (m_binlog.m_uiMgrFileNum > CwxBinLogMgr::MAX_MANAGE_FILE_NUM){
        m_binlog.m_uiMgrFileNum = CwxBinLogMgr::MAX_MANAGE_FILE_NUM;
    }
    //load binlog:del_out_file
    if (!cnf.getAttr("binlog", "del_out_file", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [binlog:del_out_file].");
        return -1;
    }
    m_binlog.m_bDelOutdayLogFile = (value == "yes"?true:false);

    //load binlog:cache
    if (!cnf.getAttr("binlog", "cache", value) || !value.length()){
        m_binlog.m_bCache = true;
    }else{
        m_binlog.m_bCache = (value =="yes"?true:false);
    }
    //load binlog:flush_log_num
    if (!cnf.getAttr("binlog", "flush_log_num", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [binlog:flush_log_num].");
        return -1;
    }
    m_binlog.m_uiFlushNum = strtoul(value.c_str(), NULL, 10);
    if (m_binlog.m_uiFlushNum < 1){
        m_binlog.m_uiFlushNum = 1;
    }
    if (m_binlog.m_uiFlushNum > CWX_MQ_MAX_BINLOG_FLUSH_COUNT){
        m_binlog.m_uiFlushNum = CWX_MQ_MAX_BINLOG_FLUSH_COUNT;
    }
    //load binlog:flush_log_second
    if (!cnf.getAttr("binlog", "flush_log_second", value) || !value.length()){
        snprintf(m_szErrMsg, 2047, "Must set [binlog:flush_log_second].");
        return -1;
    }
    m_binlog.m_uiFlushSecond = strtoul(value.c_str(), NULL, 10);
    if (m_binlog.m_uiFlushSecond < 1){
        m_binlog.m_uiFlushSecond = 1;
    }

    //load master
    if (m_common.m_bMaster){
        //load recv
        if (cnf.isExistSection("recv")){
            if (!fetchHost(cnf, "recv", m_master.m_recv)){
                return -1;
            }
        }else{
            CWX_ERROR(("Must set [recv]"));
            return -1;
        }
        //load dispatch
        if (cnf.isExistSection("dispatch")){
            if (!fetchHost(cnf, "dispatch", m_master.m_async)){
                return -1;
            }
        }else{
            m_master.m_async.reset();
        }
    }else{//slave
        //load master
        if (!fetchHost(cnf, "master", m_slave.m_master)) return -1;
        //load master:zip
        if (!cnf.getAttr("master", "zip", value) || !value.length()){
            m_slave.m_bzip = false;
        }else{
            if (value == "yes")
                m_slave.m_bzip = true;
            else
                m_slave.m_bzip = false;
        }
        //load master:sign
        if (!cnf.getAttr("master", "sign", value) || !value.length()){
            m_slave.m_strSign = "";
        }else{
            if (value == CWX_MQ_CRC32)
                m_slave.m_strSign = CWX_MQ_CRC32;
            else if (value == CWX_MQ_MD5)
                m_slave.m_strSign = CWX_MQ_MD5;
            else
                m_slave.m_strSign = "";
        }
        //fetch master:subscribe
        if (!cnf.getAttr("master", "subscribe", value) || !value.length()){
            m_slave.m_strSubScribe = "";
        }else{
            m_slave.m_strSubScribe = value;
            if (!CwxMqPoco::isValidSubscribe(m_slave.m_strSubScribe, strErrMsg)){
                snprintf(m_szErrMsg,
                    2047,
                    "[master:subscribe]'s value [%s] is not valid subscribe, err:%s",
                    value.c_str(),
                    strErrMsg.c_str());
                return -1;
            }
        }
        //load dispatch
        if (cnf.isExistSection("dispatch")){
            if (!fetchHost(cnf, "dispatch", m_slave.m_async)){
                return -1;
            }
        }else{
            m_slave.m_async.reset();
        }
    }
    //fetch mq:mq
    if (cnf.isExistSection("mq")){
        if (!fetchHost(cnf, "mq", m_mq.m_mq)) return -1;
        //load mq:log_path
        if (!cnf.getAttr("mq", "log_path", value) || !value.length()){
            snprintf(m_szErrMsg, 2047, "Must set [mq:log_path].");
            return -1;
        }
        if ('/' != value[value.length()-1]) value +="/";
        m_mq.m_strLogFilePath = value;
        //load mq:log_flush_num
        if (!cnf.getAttr("mq", "log_flush_num", value) || !value.length()){
            snprintf(m_szErrMsg, 2047, "Must set [mq:log_flush_num].");
            return -1;
        }
        m_mq.m_uiFlushNum = strtoul(value.c_str(), NULL, 10);
        if (m_mq.m_uiFlushNum < 1){
            m_mq.m_uiFlushNum = 1;
        }
        //load mq:log_flush_num
        if (!cnf.getAttr("mq", "log_flush_second", value) || !value.length()){
            snprintf(m_szErrMsg, 2047, "Must set [mq:log_flush_second].");
            return -1;
        }
        m_mq.m_uiFlushSecond = strtoul(value.c_str(), NULL, 10);
        if (m_mq.m_uiFlushSecond < 1){
            m_mq.m_uiFlushSecond = 1;
        }
    }else{
        m_mq.m_mq.reset();
    }
    return 0;
}

bool CwxMqConfig::fetchHost(CwxIniParse& cnf,
                            string const& node,
                            CwxHostInfo& host)
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
    if (!host.getHostName().length()){
        CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s]'s [listen].", node.c_str());
        return false;
    }
    return true;
}


void CwxMqConfig::outputConfig() const
{
    CWX_INFO(("\n*****************BEGIN CONFIG*******************"));
    CWX_INFO(("*****************cmn*******************"));
    CWX_INFO(("home=%s", m_common.m_strWorkDir.c_str()));
    CWX_INFO(("server_type=%s", m_common.m_bMaster?"master":"slave"));
    CWX_INFO(("sock_buf_kbyte=%u", m_common.m_uiSockBufSize));
    CWX_INFO(("max_chunk_kbyte=%u", m_common.m_uiChunkSize));
    CWX_INFO(("sync_conn_num=%u", m_common.m_uiSyncConnNum));
    CWX_INFO(("monitor=%s:%u", m_common.m_monitor.getHostName().c_str(), m_common.m_monitor.getPort()));
    CWX_INFO(("*****************binlog*******************"));
    CWX_INFO(("path=%s", m_binlog.m_strBinlogPath.c_str()));
    CWX_INFO(("file_prefix=%s", m_binlog.m_strBinlogPrex.c_str()));
    CWX_INFO(("file_max_mbyte=%u", m_binlog.m_uiBinLogMSize));
    CWX_INFO(("max_file_num=%u", m_binlog.m_uiMgrFileNum));
    CWX_INFO(("del_out_file=%s", m_binlog.m_bDelOutdayLogFile?"yes":"no"));
    CWX_INFO(("cache=%s", m_binlog.m_bCache?"yes":"no"));
    CWX_INFO(("flush_log_num=%u", m_binlog.m_uiFlushNum));
    CWX_INFO(("flush_log_second=%u", m_binlog.m_uiFlushSecond));
    if (m_common.m_bMaster){
        CWX_INFO(("*****************dispatch*******************"));
        CWX_INFO(("user=%s", m_master.m_async.getUser().c_str()));
        CWX_INFO(("passwd=%s", m_master.m_async.getPasswd().c_str()));
        CWX_INFO(("listen=%s:%u", m_master.m_async.getHostName().c_str(), m_master.m_async.getPort()));
        CWX_INFO(("*****************recv*******************"));
        CWX_INFO(("user=%s", m_master.m_recv.getUser().c_str()));
        CWX_INFO(("passwd=%s", m_master.m_recv.getPasswd().c_str()));
        CWX_INFO(("listen=%s:%u", m_master.m_recv.getHostName().c_str(), m_master.m_recv.getPort()));
    }else{
        CWX_INFO(("*****************dispatch*******************"));
        CWX_INFO(("user=%s", m_slave.m_async.getUser().c_str()));
        CWX_INFO(("passwd=%s", m_slave.m_async.getPasswd().c_str()));
        CWX_INFO(("listen=%s:%u", m_slave.m_async.getHostName().c_str(), m_slave.m_async.getPort()));
        CWX_INFO(("*****************master*******************"));
        CWX_INFO(("user=%s", m_slave.m_master.getUser().c_str()));
        CWX_INFO(("passwd=%s", m_slave.m_master.getPasswd().c_str()));
        CWX_INFO(("listen=%s:%u",m_slave.m_master.getHostName().c_str(), m_slave.m_master.getPort()));
        CWX_INFO(("subscribe=%s", m_slave.m_strSubScribe.c_str()));
        CWX_INFO(("zip=%s", m_slave.m_bzip?"yes":"no"));
        CWX_INFO(("sign=%s", m_slave.m_strSign.c_str()));
    }
    {
        CWX_INFO(("*****************mq*******************"));
        CWX_INFO(("user=%s", m_mq.m_mq.getUser().c_str()));
        CWX_INFO(("passwd=%s", m_mq.m_mq.getPasswd().c_str()));
        CWX_INFO(("listen=%s:%u",m_mq.m_mq.getHostName().c_str(), m_mq.m_mq.getPort()));
        CWX_INFO(("log_path=%s", m_mq.m_strLogFilePath.c_str()));
        CWX_INFO(("log_flush_num=%u", m_mq.m_uiFlushNum));
        CWX_INFO(("log_flush_second=%u", m_mq.m_uiFlushSecond));
    }
    CWX_INFO(("*****************END   CONFIG *******************"));
}
