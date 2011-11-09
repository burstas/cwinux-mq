#ifndef __CWX_MQ_APP_H__
#define __CWX_MQ_APP_H__
/*
��Ȩ������
    ��������ѭGNU GPL V3��http://www.gnu.org/licenses/gpl.html����
    ��ϵ��ʽ��email:cwinux@gmail.com��΢��:http://t.sina.com.cn/cwinux
*/
#include "CwxMqMacro.h"
#include "CwxAppFramework.h"
#include "CwxAppAioWindow.h"
#include "CwxBinLogMgr.h"
#include "CwxMqConfig.h"
#include "CwxMqTss.h"
#include "CwxMqPoco.h"
#include "CwxMqBinAsyncHandler.h"
#include "CwxMqBinRecvHandler.h"
#include "CwxMqMasterHandler.h"
#include "CwxMqBinFetchHandler.h"
#include "CwxMqQueueMgr.h"
#include "CwxThreadPool.h"

///Ӧ����Ϣ����
#define CWX_MQ_VERSION "2.2.6"
#define CWX_MQ_MODIFY_DATE "20110829173500"

///MQ�����app����
class CwxMqApp : public CwxAppFramework
{
public:
    enum
    {
        MAX_MONITOR_REPLY_SIZE = 1024 * 1024,
        LOG_FILE_SIZE = 30, ///<ÿ����ѭ��ʹ����־�ļ���MByte
        LOG_FILE_NUM = 7, ///<��ѭ��ʹ����־�ļ�������
        SVR_TYPE_RECV = CwxAppFramework::SVR_TYPE_USER_START, ///<master binЭ����յ�svr type
        SVR_TYPE_ASYNC = CwxAppFramework::SVR_TYPE_USER_START + 2, ///<master/slave binЭ���첽�ַ���svr type
        SVR_TYPE_MASTER = CwxAppFramework::SVR_TYPE_USER_START + 4, ///<slave ��master�������ݵ�svr type
        SVR_TYPE_FETCH = CwxAppFramework::SVR_TYPE_USER_START + 5, ///<mq binЭ����Ϣ��ȡ��������
        SVR_TYPE_MONITOR = CwxAppFramework::SVR_TYPE_USER_START + 7 ///<��ؼ����ķ�������
    };
    enum
    {
        MQ_NEW_MSG_EVENT=CwxEventInfo::SYS_EVENT_NUM + 1, ///<binlog�������ݵ��¼�
        MQ_CONTINUE_SEND_EVENT= MQ_NEW_MSG_EVENT + 1 ///<δ��ɷ��͵����ӣ���������
    };
    ///���캯��
	CwxMqApp();
    ///��������
	virtual ~CwxMqApp();
    ///���س�ʼ������
    virtual int init(int argc, char** argv);
public:
    ///ʱ����Ӧ����
    virtual void onTime(CwxTimeValue const& current);
    ///signal��Ӧ����
    virtual void onSignal(int signum);
    ///���ӽ���
    virtual int onConnCreated(CWX_UINT32 uiSvrId,
        CWX_UINT32 uiHostId,
        CWX_HANDLE handle,
        bool& bSuspendListen);
    ///���ӽ���
    virtual int onConnCreated(CwxAppHandler4Msg& conn,
        bool& bSuspendConn,
        bool& bSuspendListen);
    ///���ӹر�
    virtual int onConnClosed(CwxAppHandler4Msg& conn);
    ///�յ���Ϣ����Ӧ����
    virtual int onRecvMsg(CwxMsgBlock* msg,
                        CwxAppHandler4Msg& conn,
                        CwxMsgHead const& header,
                        bool& bSuspendConn);
    ///�յ���Ϣ����Ӧ����
    virtual int onRecvMsg(CwxAppHandler4Msg& conn,
           bool& bSuspendConn);
public:
    ///-1:ʧ�ܣ�0���ɹ�
    int commit_mq();
    ///�Ƿ��ǵ�һ��binlog
    inline bool isFirstBinLog() const
    {
        return m_bFirstBinLog;
    }
    ///����Ϊ�յ��ĵ�һ��binlog
    inline void clearFirstBinLog()
    {
        m_bFirstBinLog = false;
    }
    ///��ȡ��һ��commit��ʱ��
    inline CWX_UINT32 getLastCommitTime() const
    {
        return m_ttLastCommitTime;
    }

    ///������һ��commit��ʱ��
    inline void setLastCommitTime(CWX_UINT32 ttTime)
    {
        m_ttLastCommitTime = ttTime;
    }

    ///��ȡδcommit��log����
    inline CWX_UINT32 getUnCommitLogNum() const
    {
        return m_uiUnCommitLogNum;
    }

    ///����δcommit��log����
    inline CWX_UINT32 incUnCommitLogNum()
    {
        return ++m_uiUnCommitLogNum;
    }

    ///��δcommit��log��������
    inline void zeroUnCommitLogNum()
    {
        m_uiUnCommitLogNum = 0;
    }

    ///��ȡMQ�ϴ�commit��ʱ��
    inline CWX_UINT32 getMqLastCommitTime() const
    {
        return m_ttMqLastCommitTime;
    }

    ///����MQ�ϴ�commit��ʱ��
    inline void setMqLastCommitTime(CWX_UINT32 ttTime)
    {
        m_ttMqLastCommitTime = ttTime;
    }


    ///����ǰ��SID��1�����أ�ֻ��master���γ�sid
    inline CWX_UINT64 nextSid()
    {
        return ++m_uiCurSid;
    }

    ///��ȡ��ǰ��sid
    inline CWX_UINT64 getCurSid()
    {
        return m_uiCurSid;
    }

    ///��ȡ������Ϣ����
    inline CwxMqConfig const& getConfig() const
    {
        return m_config;
    }

    ///��ȡbinlog manager ����ָ��
    inline CwxBinLogMgr* getBinLogMgr()
    {
        return m_pBinLogMgr;
    }

    ///��ȡmq���й�����
    inline CwxMqQueueMgr* getQueueMgr()
    {
        return m_queueMgr;
    }

    ///��ȡslave��masterͬ��binlog��handler����
    inline CwxMqMasterHandler* getMasterHandler()
    {
        return m_pMasterHandler;
    }

    ///��ȡmaster����binlog��handler����
    inline CwxMqBinRecvHandler* getBinRecvHandler()
    {
        return m_pBinRecvHandler;
    }

    ///���·���״̬
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
                if (!m_pMasterHandler->isSync())
                {
                    bValid = false;
                    szReason = m_pMasterHandler->getMasterErr().c_str();
                }
            }
            else if (m_queueMgr)
            {
                if (!m_queueMgr->isValid())
                {
                    bValid = false;
                    szReason = m_queueMgr->getErrMsg().c_str();
                }

            }

        }while(0);
        setAppRunValid(bValid);
        setAppRunFailReason(szReason);
    }

    CwxAppChannel* getAsyncDispChannel()
    {
        return m_asyncDispChannel;
    }

    CwxAppChannel* getMqChannel()
    {
        return m_mqChannel;
    }

protected:
    ///�������л�������API
    virtual int initRunEnv();
    virtual void destroy();
private:
    ///����binlog��������-1��ʧ�ܣ�0���ɹ�
    int startBinLogMgr();
    int startNetwork();
    ///stats���-1����Ϊ����ر����ӣ�0�����ر�����
    int monitorStats(char const* buf, CWX_UINT32 uiDataLen, CwxAppHandler4Msg& conn);
    ///�γɼ�����ݣ����ؼ�����ݵĳ���
    CWX_UINT32 packMonitorInfo();
    ///�ַ�channel���̺߳�����argΪapp����
    static void* DispatchThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg);
    ///�ַ�channel�Ķ�����Ϣ����������ֵ��0��������-1������ֹͣ
    static int DispatchThreadDoQueue(CwxMsgQueue* queue, CwxMqApp* app, CwxAppChannel* channel);
    ///�ַ�mq channel���̺߳�����argΪapp����
    static void* MqThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg);
    ///�ַ�mq channel�Ķ�����Ϣ����������ֵ��0��������-1������ֹͣ
    static int MqThreadDoQueue(CwxMsgQueue* queue, CwxMqApp* app, CwxAppChannel* channel);
    ///����master recv���ӵ�����
    static int setMasterRecvSockAttr(CWX_HANDLE handle, void* arg);
    ///����master dispatch���ӵ�����
    static int setMasterDispatchSockAttr(CWX_HANDLE handle, void* arg);
    ///����slave dispatch���ӵ�����
    static int setSlaveDispatchSockAttr(CWX_HANDLE handle, void* arg);
    ///����slave report���ӵ�����
    static int setSlaveReportSockAttr(CWX_HANDLE handle, void* arg);
    ///����mq���ӵ�����
    static int setMqSockAttr(CWX_HANDLE handle, void* arg);
private:
    bool                        m_bFirstBinLog; ///<�����������յ��ĵ�һ��binglog
    CWX_UINT32                  m_ttLastCommitTime; ///<��һ��commit��ʱ��
    CWX_UINT32                  m_uiUnCommitLogNum; ///<����һ��commit������δcommit��binlog����
    CWX_UINT32                  m_ttMqLastCommitTime; ///<��Ϣ�ַ�sys�ļ��ϴ�commit��ʱ��
    CWX_UINT64                  m_uiCurSid; ///<��ǰ��sid
    CwxMqConfig                 m_config; ///<�����ļ�
    CwxBinLogMgr*               m_pBinLogMgr; ///<binlog�Ĺ�������
    CwxMqMasterHandler*         m_pMasterHandler; ///<��master������Ϣ��handle
    CwxMqBinRecvHandler*        m_pBinRecvHandler; ///<binЭ�����binlog��handle��
    CwxMqQueueMgr*              m_queueMgr; ///<���й�����
    CwxThreadPool*              m_pRecvThreadPool;///<��Ϣ���ܵ��̳߳ض���
    CwxThreadPool*              m_pAsyncDispThreadPool; ///<��Ϣ�첽�ַ����̳߳ض���
    CwxAppChannel*              m_asyncDispChannel; ///<��Ϣ�첽�ַ���channel
    CwxThreadPool*              m_pMqThreadPool;       ///<mq��ȡ���̳߳ض���
    CwxAppChannel*              m_mqChannel;           ///<mq��ȡ��channel
    string                      m_strStartTime; ///<����ʱ��
    char                        m_szBuf[MAX_MONITOR_REPLY_SIZE];///<�����Ϣ�Ļظ�buf

};
#endif
