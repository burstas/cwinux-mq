#ifndef __CWX_MQ_APP_H__
#define __CWX_MQ_APP_H__
/*
版权声明：
    本软件为个人所有，遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
但有以下例外：
    腾讯公司及与腾讯公司有直接业务与合作关系的公司不得使用此软件。原因可参考：
http://it.sohu.com/20100903/n274684530.shtml
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxMqMacro.h"
#include "CwxAppFramework.h"
#include "CwxAppAioWindow.h"
#include "CwxBinLogMgr.h"
#include "CwxMqConfig.h"
#include "CwxMqMgrServer.h"
#include "CwxMqTss.h"
#include "CwxMqPoco.h"
#include "CwxMqAsyncHandler.h"
#include "CwxMqRecvHandler.h"
#include "CwxMqMasterHandler.h"
#include "CwxMqFetchHandler.h"
#include "CwxMqSysFile.h"
#include "CwxMqQueueMgr.h"

///应用信息定义
#define CWX_MQ_VERSION "1.3.1"
#define CWX_MQ_MODIFY_DATE "2010-11-07 11:30:00"

///MQ服务的app对象
class CwxMqApp : public CwxAppFramework
{
public:
    enum
    {
        LOG_FILE_SIZE = 30, ///<每个可循环使用日志文件的MByte
        LOG_FILE_NUM = 7, ///<可循环使用日志文件的数量
        SVR_TYPE_RECV = CwxAppFramework::SVR_TYPE_USER_START, ///<master接收的svr type
        SVR_TYPE_ASYNC = CwxAppFramework::SVR_TYPE_USER_START + 1, ///<master/slave 异步分发的svr type
        SVR_TYPE_MASTER = CwxAppFramework::SVR_TYPE_USER_START + 2, ///<slave 从master接收数据的svr type
        SVR_TYPE_FETCH = CwxAppFramework::SVR_TYPE_USER_START + 3
    };
    enum
    {
        MIN_SKIP_SID_COUNT = 50, ///<服务启动时，最小的skip sid数量
        MAX_SKIP_SID_COUNT = 10000 ///<服务启动时，最大的skip sid数量
    };
    enum
    {
        MQ_NEW_MSG_EVENT=CwxEventInfo::SYS_EVENT_NUM + 1, ///<binlog有新数据的事件
        MQ_CONTINUE_SEND_EVENT= MQ_NEW_MSG_EVENT + 1 ///<未完成发送的连接，继续发送
    };
    ///构造函数
	CwxMqApp();
    ///析构函数
	virtual ~CwxMqApp();
    ///重载初始化函数
    virtual int init(int argc, char** argv);
public:
    ///时钟响应函数
    virtual void onTime(CwxTimeValue const& current);
    ///signal响应函数
    virtual void onSignal(int signum);
    ///连接建立
    virtual int onConnCreated(CwxAppHandler4Msg& conn,
        bool& bSuspendConn,
        bool& bSuspendListen);
    ///连接关闭
    virtual int onConnClosed(CwxAppHandler4Msg const& conn);
    ///收到消息的响应函数
    virtual int onRecvMsg(CwxMsgBlock* msg,
                        CwxAppHandler4Msg const& conn,
                        CwxMsgHead const& header,
                        bool& bSuspendConn);
    ///消息发送完毕
    virtual CWX_UINT32 onEndSendMsg(CwxMsgBlock*& msg,
        CwxAppHandler4Msg const& conn);
    ///消息发送失败
    virtual void onFailSendMsg(CwxMsgBlock*& msg);
public:
    ///分发等待中的binlog
    void dispathWaitingBinlog(CwxMqTss* pTss);
    ///-1:失败；0：成功
    int commit_mq(char* szErr2K);
    ///获取分发线程池
    CwxAppThreadPool*  getWriteThreadPool() 
    {
        return m_pWriteThreadPool;///<消息接受的线程池对象
    }

    ///是否是第一条binlog
    inline bool isFirstBinLog() const
    {
        return m_bFirstBinLog;
    }
    ///设置为收到的第一条binlog
    inline void clearFirstBinLog()
    {
        m_bFirstBinLog = false;
    }
    ///获取上一次commit的时间
    inline time_t getLastCommitTime() const
    {
        return m_ttLastCommitTime;
    }
    ///设置上一次commit的时间
    inline void setLastCommitTime(time_t ttTime)
    {
        m_ttLastCommitTime = ttTime;
    }
    ///获取未commit的log数量
    inline CWX_UINT32 getUnCommitLogNum() const
    {
        return m_uiUnCommitLogNum;
    }
    ///增加未commit的log数量
    inline CWX_UINT32 incUnCommitLogNum()
    {
        return ++m_uiUnCommitLogNum;
    }
    ///将未commit的log数量归零
    inline void zeroUnCommitLogNum()
    {
        m_uiUnCommitLogNum = 0;
    }
    ///获取MQ上次commit的时间
    inline time_t getMqLastCommitTime() const
    {
        return m_ttMqLastCommitTime;
    }
    ///设置MQ上次commit的时间
    inline void setMqLastCommitTime(time_t ttTime)
    {
        m_ttMqLastCommitTime = ttTime;
    }
    ///获取MQ未commit的分发消息的数量
    inline CWX_UINT32 getMqUncommitNum() const
    {
        return m_uiMqUnCommitLogNum;
    }
    ///设置MQ未commit的分发消息的数量
    inline void setMqUncommitNum(CWX_UINT32 uiNum)
    {
        m_uiMqUnCommitLogNum = uiNum;
    }
    ///MQ未commit的分发消息数量加1
    inline void incMqUncommitNum()
    {
        m_uiMqUnCommitLogNum ++;
    }

    ///将当前的SID加1并返回，只有master才形成sid
    inline CWX_UINT64 nextSid()
    {
        return ++m_uiCurSid;
    }
    ///获取当前的sid
    inline CWX_UINT64 getCurSid()
    {
        return m_uiCurSid;
    }
    ///获取配置信息对象
    inline CwxMqConfig const& getConfig() const
    {
        return m_config;
    }
    ///获取binlog manager 对象指针
    inline CwxBinLogMgr* getBinLogMgr()
    {
        return m_pBinLogMgr;
    }
    ///获取mq队列管理器
    inline CwxMqQueueMgr* getQueueMgr()
    {
        return m_queueMgr;
    }
    ///获取异步binlog分发的handler对象
    inline CwxMqAsyncHandler* getAsyncHandler()
    {
        return m_pAsyncHandler;
    }
    ///获取slave从master同步binlog的handler对象
    inline CwxMqMasterHandler* getMasterHandler()
    {
        return m_pMasterHandler;
    }
    ///获取master接收binlog的handler对象
    inline CwxMqRecvHandler* getRecvHandler()
    {
        return m_pRecvHandler;
    }
    ///获取mq fetch handler对象
    inline CwxMqFetchHandler* getMqFetchHandler()
    {
        return m_pFetchHandler;
    }
    ///获取系统文件对象
    inline CwxMqSysFile* getSysFile()
    {
        return m_sysFile;
    }
    ///更新服务状态
    inline void updateAppRunState()
    {
        bool bValid = true;
        char const* szReason = "";
        do
        {
            if (m_pBinLogMgr->isInvalid())
            {
                bValid = false;
                szReason = m_pBinLogMgr->getInvalidMsg();
                break;
            }
            else if (m_pMasterHandler)
            {
                if (!m_pMasterHandler->getMasterConnId())
                {
                    bValid = false;
                    szReason = "Can't connect to master";
                }
            }
            else if (m_sysFile)
            {
                if (!m_sysFile->isValid())
                {
                    bValid = false;
                    szReason = m_sysFile->getErrMsg();
                }

            }

        }while(0);
        setAppRunValid(bValid);
        setAppRunFailReason(szReason);
    }
protected:
    ///重载运行环境设置API
    virtual int initRunEnv();
    virtual void destroy();
private:
    ///启动binlog管理器，-1：失败；0：成功
    int startBinLogMgr();
    int startNetwork();
private:
    bool                        m_bFirstBinLog; ///<服务启动后，收到的第一条binglog
    time_t                      m_ttLastCommitTime; ///<上一次commit的时候
    CWX_UINT32                  m_uiUnCommitLogNum; ///<自上一次commit以来，未commit的binlog数量
    time_t                      m_ttMqLastCommitTime; ///<消息分发sys文件上次commit的时候
    CWX_UINT32                  m_uiMqUnCommitLogNum; ///<消息分发sys文件未commit的数量
    CWX_UINT64                  m_uiCurSid; ///<当前的sid
    CwxMqConfig                 m_config; ///<配置文件
    CwxBinLogMgr*                m_pBinLogMgr; ///<binlog的管理对象
    CwxMqAsyncHandler*      m_pAsyncHandler; ///<异步分发handle
    CwxMqMasterHandler*     m_pMasterHandler; ///<从master接收消息的handle
    CwxMqRecvHandler*       m_pRecvHandler; ///<接收binlog的handle。
    CwxMqFetchHandler*      m_pFetchHandler; ///<mq获取的handle
    CwxMqSysFile*           m_sysFile; ///<mq分发的分发点记录文件
    CwxMqQueueMgr*          m_queueMgr; ///<队列管理器
    CwxAppThreadPool*       m_pWriteThreadPool;///<消息接受的线程池对象
//    CwxAppThreadPool*       m_pDispatchThreadPool; ///<消息分发的线程池对象
};
#endif

