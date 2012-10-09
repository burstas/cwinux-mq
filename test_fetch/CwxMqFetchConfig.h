#ifndef __CWX_MQ_FETCH_CONFIG_H__
#define __CWX_MQ_FETCH_CONFIG_H__
/*
版权声明：
本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxHostInfo.h"
#include "CwxCommon.h"
#include "CwxIniParse.h"
#include "CwxMqMacro.h"
#include "CwxMqDef.h"
#include "CwxLogger.h"

class CwxMqFetchConfig {
public:
  CwxMqFetchConfig() {
    m_bTcp = true;
    m_unConnNum = 0;
    m_bLasting = true;
    m_bBlock = true;
  }

  ~CwxMqFetchConfig() {
  }
public:
  //加载配置文件.-1:failure, 0:success
  int loadConfig(string const & strConfFile);
  //输出配置文件
  void outputConfig();
  //获取加载配置文件的失败错误信息
  char const* getError() {
    return m_szError;
  }
  ;

public:
  bool m_bTcp; ///<是否通过tcp进行连接
  string m_strUnixPathFile; ///<若采用unix domain连接，则为连接的path-file
  string m_strWorkDir; ///<工作目录
  CWX_UINT16 m_unConnNum; ///<连接的数量
  bool m_bLasting; ///<是否为持久连接，类似HTTP的keep-alive
  bool m_bBlock; ///<是否为阻塞获取
  string m_strUser; ///<fetch的用户名
  string m_strPasswd; ///<fetch的用户口令
  string m_strQueue; ///<fetch的队列的名字
  CwxHostInfo m_listen; ///<tcp连接的对方listen地址
  char m_szError[2048]; ///<错误消息buf
};

#endif
