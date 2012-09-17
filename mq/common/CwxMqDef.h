#ifndef __CWX_MQ_DEF_H__
#define __CWX_MQ_DEF_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
/**
@file CwxMqDef.h
@brief MQ系列服务的通用对象定义文件。
@author cwinux@gmail.com
@version 0.1
@date 2010-09-15
@warning
@bug
*/

#include "CwxMqMacro.h"
#include "CwxStl.h"
#include "CwxBinLogMgr.h"
#include "CwxMqPoco.h"
#include "CwxDTail.h"
#include "CwxSTail.h"
#include "CwxTypePoolEx.h"
#include "CwxAppHandler4Channel.h"
#include "CwxHostInfo.h"

class CwxMqQueue;

///mq的fetch连接的session对象
class CwxMqFetchConn{
public:
    CwxMqFetchConn();
    ~CwxMqFetchConn();
public:
    void reset();
public:
    bool            m_bWaiting; ///<是否正在等在发送信息
    bool            m_bBlock; ///<是否为block连接
    CWX_UINT32      m_uiTaskId; ///<连接的taskid
    string          m_strQueueName; ///<队列的名字
};



///mq queue的信息对象
class CwxMqQueueInfo{
public:
    CwxMqQueueInfo(){
        m_ullCursorSid = 0;
    }
public:
    CwxMqQueueInfo(CwxMqQueueInfo const& item){
        m_strName = item.m_strName; ///<队列的名字
        m_strUser = item.m_strUser; ///<队列鉴权的用户名
        m_strPasswd = item.m_strPasswd; ///<队列的用户口令
        m_ullCursorSid = item.m_ullCursorSid;
        m_ullLeftNum = item.m_ullLeftNum;
    }

    CwxMqQueueInfo& operator=(CwxMqQueueInfo const& item){
        if (this != &item){
            m_strName = item.m_strName; ///<队列的名字
            m_strUser = item.m_strUser; ///<队列鉴权的用户名
            m_strPasswd = item.m_strPasswd; ///<队列的用户口令
            m_ullCursorSid = item.m_ullCursorSid;
            m_ullLeftNum = item.m_ullLeftNum;
        }
        return *this;
    }
public:
    string                           m_strName; ///<队列的名字
    string                           m_strUser; ///<队列鉴权的用户名
    string                           m_strPasswd; ///<队列的用户口令
    CWX_UINT64                       m_ullCursorSid; ///<当前cursor的sid
    CWX_UINT64                       m_ullLeftNum; ///<当前消息
};

bool mqParseHostPort(string const& strHostPort, CwxHostInfo& host);

#endif
