#ifndef __CWX_MC_APP_H__
#define __CWX_MC_APP_H__
/*
* 版权声明：
* 本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
* 联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxMqMacro.h"
#include "CwxAppFramework.h"
#include "CwxMcConfig.h"
#include "CwxMqTss.h"
#include "CwxMqPoco.h"
#include "CwxThreadPool.h"
#include "CwxMcQueue.h"
#include "CwxMcQueueHandler.h"
#include "CwxMcSyncHandler.h"

///应用信息定义
#define CWX_MC_VERSION "0.1.0"
#define CWX_MC_MODIFY_DATE "20121006142000"

///MC服务的app对象
class CwxMcApp : public CwxAppFramework {
public:
  enum {
    // 监控的BUF空间大小
    MAX_MONITOR_REPLY_SIZE = 1024 * 1024,
    // 每个可循环使用日志文件的MByte
    LOG_FILE_SIZE = 30,
    // 可循环使用日志文件的数量
    LOG_FILE_NUM = 7,
    // 从sync binlog的数据接收的svr type
    SVR_TYPE_SYNC = CwxAppFramework::SVR_TYPE_USER_START,
    // queue消息获取svr type
    SVR_TYPE_QUEUE = CwxAppFramework::SVR_TYPE_USER_START + 1,
    // stats监听的服务类型
    SVR_TYPE_MONITOR = CwxAppFramework::SVR_TYPE_USER_START + 2,
    // event类型的定义
    // sync host信息改变的消息
    EVENT_TYPE_SYNC_CHANGE = CwxEventInfo::SYS_EVENT_NUM + 1

  };
  ///构造函数
  CwxMcApp();
  ///析构函数
  virtual ~CwxMcApp();
  ///重载初始化函数
  virtual int init(int argc, char** argv);
public:
  ///时钟响应函数
  virtual void onTime(CwxTimeValue const& current);
  ///signal响应函数
  virtual void onSignal(int signum);
  ///连接建立
  virtual int onConnCreated(CWX_UINT32 uiSvrId, // svr id
    CWX_UINT32 uiHostId, // host id
    CWX_HANDLE handle, // 连接handle
    bool& bSuspendListen // 是否suspend listen
    );
  ///连接建立
  virtual int onConnCreated(CwxAppHandler4Msg& conn, // 连接对象
    bool& bSuspendConn, // 是否suspend 连接消息的接收
    bool& bSuspendListen // 是否suspend 新连接的监听
    );
  ///连接关闭
  virtual int onConnClosed(CwxAppHandler4Msg& conn);
  ///收到消息的响应函数
  virtual int onRecvMsg(CwxAppHandler4Msg& conn,
    bool& bSuspendConn);
public:
  ///计算机的时钟是否回调
  static bool isClockBack(CWX_UINT32& uiLastTime,
    CWX_UINT32 uiNow) {
      if (uiLastTime > uiNow + 1) {
        uiLastTime = uiNow;
        return true;
      }
      uiLastTime = uiNow;
      return false;
  }
  ///获取配置信息对象
  inline CwxMcConfig const& getConfig() const {
    return m_config;
  }
  ///获取当前时间
  inline CWX_UINT32 getCurTime() const{
    return m_ttCurTime;
  }
  ///获取queue对象
  CwxMcQueue* getQueue() {
    return m_queue;
  }

protected:
  ///重载运行环境设置API
  virtual int initRunEnv();
  ///释放app资源
  virtual void destroy();
private:
  /// 停止sync。返回值，0：成功；-1：失败
  int stopSync(string const& strHostName);
  /// 启动sync。返回值，0：成功；-1：失败
  int startSync(CwxHostInfo const& hostInfo);
  /// 更新sync。返回值，0：成功；-1：失败
  int updateSync(CwxHostInfo const& hostInfo);
  /// 检查是否sync host发生了改变
  void checkSyncHostModify();
  /// 检查sync host文件的变化，若变化则加载。
  /// 返回值，-1：失败；1：变化并加载；0：没有变化
  int loadSyncHostForChange(bool bForceLoad=false);
  /// 启动网络，-1：失败；0：成功
  int startNetwork();
  ///stats命令，-1：因为错误关闭连接；0：不关闭连接
  int monitorStats(char const* buf,
    CWX_UINT32 uiDataLen,
    CwxAppHandler4Msg& conn);
  ///形成监控内容，返回监控内容的长度
  CWX_UINT32 packMonitorInfo();
  ///sync channel的线程函数，arg为app对象
  static void* syncThreadMain(CwxTss* tss,
    CwxMsgQueue* queue,
    void* arg);
  ///sync channel的队列消息函数。返回值：0：正常；-1：队列停止
  static int dealSyncThreadMsg(CwxMsgQueue* queue,
    CwxMcSyncSession* pSession,
    CwxAppChannel* channel);
  ///queue channel的线程函数，arg为app对象
  static void* queueThreadMain(CwxTss* tss,
    CwxMsgQueue* queue,
    void* arg);
  ///queue channel的队列消息函数。返回值：0：正常；-1：队列停止
  static int dealQueueThreadMsg(CwxMsgQueue* queue,
    CwxMcApp* app,
    CwxAppChannel* channel);
  ///设置queue的连接属性
  static int setQueueSockAttr(CWX_HANDLE handle, void* arg);
private:
  // 配置信息
  CwxMcConfig          m_config;
  // 队列对象
  CwxMcQueue*          m_queue;
  // sync host 文件的修改时间
  CWX_UINT32           m_uiSyncHostFileModifyTime;
  // 当前的host id
  CWX_UINT32           m_uiCurHostId;
  // 数据收集对象的map
  map<string, CwxMcSyncSession*>   m_syncs;
  // queue获取的线程池对象
  CwxThreadPool*       m_queueThreadPool;
  // queue获取的channel
  CwxAppChannel*       m_queueChannel;
  // 启动时间
  string              m_strStartTime;
  // 当前的时间
  volatile CWX_UINT32  m_ttCurTime;
  // 监控消息的回复buf
  char                 m_szBuf[MAX_MONITOR_REPLY_SIZE];
};
#endif

