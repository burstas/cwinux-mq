#ifndef __CWX_MC_CONFIG_H__
#define __CWX_MC_CONFIG_H__

#include "CwxMqMacro.h"
#include "CwxGlobalMacro.h"
#include "CwxHostInfo.h"
#include "CwxCommon.h"
#include "CwxIniParse.h"
#include "CwxBinLogMgr.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"
#include "CwxMqDef.h"

CWINUX_USING_NAMESPACE

///配置文件的common参数对象
class CwxMcConfigCmn {
public:
  CwxMcConfigCmn() {
  }
public:
  string m_strWorkDir; ///<工作目录
  CwxHostInfo m_monitor; ///<监控监听
};

///配置文件的log参数对象
class CwxMcConfigLog {
public:
  enum {
    DEF_LOG_MSIZE = 1024, ///<缺省的binlog大小
    MIN_LOG_MSIZE = 1, ///<最小的binlog大小
    MAX_LOG_MSIZE = 2048 ///<最大的binlog大小
  };
public:
  CwxMcConfigLog() {
    m_uiLogMSize = DEF_LOG_MSIZE;
    m_uiSwitchSecond = 0;
    m_uiFlushNum = 100;
    m_uiFlushSecond = 30;
    m_uiReserveDay = 0;
    m_bAppendReturn = false;
  }
public:
  string          m_strPath; ///<log的目录
  CWX_UINT32      m_uiLogMSize; ///<log文件的最大大小，单位为M
  CWX_UINT32      m_uiSwitchSecond; ///<log文件切换的时间间隔，单位为s
  CWX_UINT32      m_uiFlushNum; ///<接收多少条记录后，flush binlog文件
  CWX_UINT32      m_uiFlushSecond; ///<间隔多少秒，必须flush binlog文件
  CWX_UINT32      m_uiReserveDay; ///<保留的天数，若是0表示不删除
  bool            m_bAppendReturn; ///<是否append return
};

///配置文件的mq对象
class CwxMcConfigMq {
public:
  CwxMcConfigMq() {
    m_uiCacheMSize = 100;
    m_uiCacheTimeout = 300;
  }
public:
  CwxHostInfo     m_mq; ///<mq的fetch的配置信息
  string          m_strName; ///<mq的名字
  CWX_UINT32      m_uiCacheMSize; ///<cache的MSize
  CWX_UINT32      m_uiCacheTimeout; ///<cache的数据时间
};

///数据接收的配置
class CwxMcConfigSync{
public:
  CwxMcConfigSync(){
    m_uiSockBufKByte = 64;
    m_uiChunkKBye = 64;
    m_uiConnNum = 8;
    m_bzip = false;
  }
public:
  string        m_strSource; ///<同步的source名字
  CWX_UINT32    m_uiSockBufKByte; ///<同步socket的buf大小
  CWX_UINT32    m_uiChunkKBye; ///<chunk的大小
  CWX_UINT32    m_uiConnNum; ///<同步的连接数量
  bool          m_bzip; ///<是否压缩
  string        m_strSign; ///<签名的类型，为crc32或md5
};

///分发的参数配置对象
class CwxMcConfigSyncHost {
public:
  CwxMcConfigSyncHost() {
  }
public:
  map<string, CwxHostInfo>  m_hosts;
};


///配置文件加载对象
class CwxMcConfig {
public:
  ///构造函数
  CwxMcConfig() {
    m_szErrMsg[0] = 0x00;
  }
  ///析构函数
  ~CwxMcConfig() {
  }
public:
  //加载配置文件.-1:failure, 0:success
  int loadConfig(string const & strConfFile);
  //加载sync的主机
  int loadSyncHost(string const& strSyncHostFile);
  //输出加载的配置文件信息
  void outputConfig() const;
  //输出host的配置
  void outputSyncHost() const;
public:
  ///获取common配置信息
  inline CwxMcConfigCmn const& getCommon() const {
    return m_common;
  }
  ///获取binlog配置信息
  inline CwxMcConfigLog const& getLog() const {
    return m_log;
  }
  inline CwxMcConfigMq const& getMq() const {
    return m_mq;
  }
  ///获取sync的信息
  CwxMcConfigSync const& getSync() const{
    return m_sync;
  }
  ///获取sync host的信息
  CwxMcConfigSyncHost const& getSyncHosts() const{
    return m_syncHosts;
  }
  ///获取配置文件加载的失败原因
  inline char const* getErrMsg() const {
    return m_szErrMsg;
  }

private:
  bool fetchHost(CwxIniParse& cnf, string const& node, CwxHostInfo& host);
private:
  CwxMcConfigCmn       m_common; ///<common的配置信息
  CwxMcConfigLog       m_log; ///<log的配置信息
  CwxMcConfigMq        m_mq; ///<mq的配置信息
  CwxMcConfigSync      m_sync; ///<sync的配置信息
  CwxMcConfigSyncHost  m_syncHosts; ///<sync host的配置信息
  char                m_szErrMsg[2048]; ///<错误消息的buf
};

#endif
