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
      DEF_BINLOG_MSIZE = 1024, ///<缺省的binlog大小
      MIN_BINLOG_MSIZE = 64, ///<最小的binlog大小
      MAX_BINLOG_MSIZE = 2048 ///<最大的binlog大小
    };
  public:
    CwxMcConfigLog() {
      m_uiLogMSize = DEF_BINLOG_MSIZE;
      m_uiSwitchSecond = 0;
      m_uiFlushNum = 100;
      m_uiFlushSecond = 30;
    }
  public:
    string m_strPath; ///<log的目录
    CWX_UINT32 m_uiLogMSize; ///<log文件的最大大小，单位为M
    CWX_UINT32 m_uiSwitchSecond; ///<log文件切换的时间间隔，单位为s
    CWX_UINT32 m_uiFlushNum; ///<接收多少条记录后，flush binlog文件
    CWX_UINT32 m_uiFlushSecond; ///<间隔多少秒，必须flush binlog文件
};

///配置文件的mq对象
class CwxMcConfigMq {
public:
    CwxMcConfigMq() {
        m_uiFlushNum = 1;
        m_uiFlushSecond = 30;
    }
public:
    CwxHostInfo     m_mq; ///<mq的fetch的配置信息
    CWX_UINT32      m_uiMSize; ///<cache的MSize
    CWX_UINT32      m_uiTimeout; ///<cache的数据时间
};

///数据接收的配置
class CwxMcConfigSync{
public:
    CwxMcConfigSync(){
        m_uiSockBufKByte = 64;
        m_uiChunkKBye = 64;
        m_uiConnNum = 8;
        m_bZip = false;
    }
public:
    string        m_strSource; ///<同步的source名字
    CWX_UINT32    m_uiSockBufKByte; ///<同步socket的buf大小
    CWX_UINT32    m_uiChunkKBye; ///<chunk的大小
    CWX_UINT32    m_uiConnNum; ///<同步的连接数量
    bool          m_bZip; ///<是否压缩
    string        m_strSign; ///<签名的类型，为crc32或md5
};


///分发的参数配置对象
class CwxMqConfigDispatch {
  public:
    CwxMqConfigDispatch() {
      m_uiFlushNum = 1;
      m_uiFlushSecond = 10;
    }
  public:
    CwxHostInfo m_async; ///<master bin协议异步分发端口信息
    string m_strSourcePath; ///<source的路径
    CWX_UINT32 m_uiFlushNum; ///<fetch多少条日志，必须flush获取点
    CWX_UINT32 m_uiFlushSecond; ///<多少秒必须flush获取点

};

///配置文件的recv参数对象
class CwxMqConfigRecv {
  public:
    CwxMqConfigRecv() {
    }
  public:
    CwxHostInfo m_recv; ///<master recieve消息的listen信息
};

///配置文件的Master参数对象
class CwxMqConfigMaster {
  public:
    CwxMqConfigMaster() {
      m_bzip = false;
    }
  public:
    CwxHostInfo m_master; ///<slave的master的连接信息
    bool m_bzip; ///<是否zip压缩
    string m_strSign; ///<签名类型
};


///配置文件加载对象
class CwxMqConfig {
  public:
    ///构造函数
    CwxMqConfig() {
      m_szErrMsg[0] = 0x00;
    }
    ///析构函数
    ~CwxMqConfig() {
    }
  public:
    //加载配置文件.-1:failure, 0:success
    int loadConfig(string const & strConfFile);
    //输出加载的配置文件信息
    void outputConfig() const;
  public:
    ///获取common配置信息
    inline CwxMqConfigCmn const& getCommon() const {
      return m_common;
    }
    ///获取binlog配置信息
    inline CwxMqConfigBinLog const& getBinLog() const {
      return m_binlog;
    }
    ///获取master配置信息
    inline CwxMqConfigMaster const& getMaster() const {
      return m_master;
    }
    ///获取分发的信息
    inline CwxMqConfigDispatch const& getDispatch() const {
      return m_dispatch;
    }
    ///获取消息接受的listen信息
    inline CwxMqConfigRecv const& getRecv() const {
      return m_recv;
    }
    inline CwxMqConfigMq const& getMq() const {
      return m_mq;
    }
    ///获取配置文件加载的失败原因
    inline char const* getErrMsg() const {
      return m_szErrMsg;
    }
    ;
  private:
    bool fetchHost(CwxIniParse& cnf, string const& node, CwxHostInfo& host);
  private:
    CwxMqConfigCmn    m_common; ///<common的配置信息
    CwxMqConfigBinLog m_binlog; ///<binlog的配置信息
    CwxMqConfigMaster m_master; ///<slave的master的数据同步配置信息
    CwxMqConfigRecv   m_recv; ///<master的数据接收listen信息
    CwxMqConfigDispatch m_dispatch; ///<dispatch的配置信息
    CwxMqConfigMq     m_mq; ///<mq的fetch的配置信息
    char              m_szErrMsg[2048]; ///<错误消息的buf
};

#endif
