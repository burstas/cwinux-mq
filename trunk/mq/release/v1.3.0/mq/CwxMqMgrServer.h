#ifndef __CWX_MQ_MGR_SERVER_H__
#define __CWX_MQ_MGR_SERVER_H__
/*
版权声明：
    本软件为个人所有，遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
但有以下例外：
    腾讯公司及与腾讯公司有直接业务与合作关系的公司不得使用此软件。原因可参考：
http://it.sohu.com/20100903/n274684530.shtml
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/

/**
@file CwxMqMgrServer.h
@brief 处理监控管理命令的CwxMqMgrServer类的定义
@author cwinux@gmail.com
@version 0.1
@date 2010-09-15
@warning
@bug
*/
#include "CwxMqMacro.h"
#include "CwxAppMgrServer.h"


class CwxMqMgrServer: public CwxAppMgrServer
{
public:
    ///构造函数
    CwxMqMgrServer(CwxAppFramework* pApp):CwxAppMgrServer(pApp)
    {

    }
    ///析构函数
    virtual ~CwxMqMgrServer()
    {

    }
public:
    /**
    @brief 获取服务详细运行信息命令的响应函数
    @param [in] msg msg命令数据包
    @param [in] pThrEnv 线程的Thread-env。
    @param [in] reply 回复的package等相关的信息。
    @return false：不回复，关闭连接； true：回复
    */
    virtual bool onCmdRunDetail(CwxMsgBlock*& msg,
        CwxAppTss* pThrEnv,
        CwxAppMgrReply& reply);
};


#endif 
