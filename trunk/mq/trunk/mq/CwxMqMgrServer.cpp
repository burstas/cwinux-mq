#include "CwxMqMgrServer.h"
#include "CwxMqApp.h"

#define CWX_MQ_HOME  "home dir"
#define CWX_MQ_MIN_SID "min sid"
#define CWX_MQ_MAX_SID "max sid"
#define CWX_MQ_MIN_TIMESTAMP "min datetime"
#define CWX_MQ_MAX_TIMESTAMP "max datetime"
#define CWX_MQ_MIN_FILE "min file"
#define CWX_MQ_MAX_FILE "max file"
#define CWX_MQ_SERVER_TYPE "server type"
#define CWX_MQ_MQ_INFO  "mq info"

bool CwxMqMgrServer::onCmdRunDetail(CwxMsgBlock*& ,
                                    CwxAppTss* pThrEnv,
                                    CwxAppMgrReply& reply)
{
    reply.reset();
    reply.m_pRecord = new CwxPackageWriter(64 * 1024);
    CwxPackageWriter tPoolsPackage(60 * 1024);
    CwxPackageWriter tPoolPackage(60 * 1024);
    CwxPackageWriter threadPackage(4 * 1024);
    CwxMqApp* pApp=(CwxMqApp*)getApp();

    reply.m_pRecord->beginPack();
    if (!reply.m_pRecord->addKeyValue(CWX_MQ_HOME,
        pApp->getConfig().getCommon().m_strWorkDir.c_str(),
        pApp->getConfig().getCommon().m_strWorkDir.length()))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    if (!reply.m_pRecord->addKeyValue(CWX_APP_MGR_APP_VERSION, getApp()->getAppVersion().c_str(), getApp()->getAppVersion().length(), false))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    if (!reply.m_pRecord->addKeyValue(CWX_APP_MGR_APP_MODIFY_TIME, getApp()->getLastModifyDatetime().c_str(), getApp()->getLastModifyDatetime().length(), false))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    if (!reply.m_pRecord->addKeyValue(CWX_APP_MGR_COMPILE_TIME, getApp()->getLastCompileDatetime().c_str(), getApp()->getLastCompileDatetime().length(), false))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    if (!reply.m_pRecord->addKeyValue(CWX_MQ_MIN_SID , pApp->getBinLogMgr()->getMinSid()))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    if (!reply.m_pRecord->addKeyValue(CWX_MQ_MAX_SID , pApp->getBinLogMgr()->getMaxSid()))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    string strDatetime;
    if (!reply.m_pRecord->addKeyValue(CWX_MQ_MIN_TIMESTAMP ,
        CwxDate::getDate(pApp->getBinLogMgr()->getMinTimestamp(), strDatetime).c_str()))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    if (!reply.m_pRecord->addKeyValue(CWX_MQ_MAX_TIMESTAMP ,
        CwxDate::getDate(pApp->getBinLogMgr()->getMaxTimestamp(), strDatetime).c_str()))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    string strFileName ;
    pApp->getBinLogMgr()->getMinFile(strFileName);
    if (!reply.m_pRecord->addKeyValue(CWX_MQ_MIN_FILE ,
        strFileName.c_str(),
        strFileName.length()))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    pApp->getBinLogMgr()->getMaxFile(strFileName);
    if (!reply.m_pRecord->addKeyValue(CWX_MQ_MAX_FILE ,
        strFileName.c_str(),
        strFileName.length()))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    char const* szServerType = pApp->getConfig().getCommon().m_bMaster?"master":"slave";
    if (!reply.m_pRecord->addKeyValue(CWX_MQ_SERVER_TYPE ,
        szServerType,
        strlen(szServerType)))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    char szBuf[1024];
    CWX_UINT64 ullMqSid = 0;
    CWX_UINT64 ullMqNum = 0;
    CwxMqQueue* pQueue = 0;
    char szSid1[64];
    char szSid2[64];
    map<string, CwxMqConfigQueue>::const_iterator iter = pApp->getConfig().getMq().m_queues.begin();
    while(iter != pApp->getConfig().getMq().m_queues.end())
    {
        pQueue = pApp->getQueueMgr()->getQueue(iter->first);
        ullMqSid = pQueue->getCurSid(); 
        ullMqNum = pQueue->getMqNum();
        CwxCommon::toString(ullMqSid, szSid1, 10);
        CwxCommon::toString(ullMqNum, szSid2, 10);
        CwxCommon::snprintf(szBuf, 1023, " mq name=%s ; current sid=%s ; mq number=%s", iter->first.c_str(), szSid1, szSid2);
        if (!reply.m_pRecord->addKeyValue(CWX_MQ_MQ_INFO ,
            szBuf, strlen(szBuf)))
        {
            reply.m_status = -1;
            reply.m_strError = reply.m_pRecord->getErrMsg();
            return true;
        }
        iter++;
    }
    if (!getApp()->isAppRunValid())
    {
        reply.m_status = -1;
        reply.m_strError = getApp()->getAppRunFailReason();
    }
    else
    {
        reply.m_status = 0;
        reply.m_strError = "valid";
    }

    //patch thread info
    vector<vector<CwxAppTss*> > arrTss;
    CwxAppTss* pEnv = NULL;

    tPoolsPackage.beginPack();
    getApp()->getThreadPoolMgr()->getTss(arrTss);
    for (CWX_UINT32 i=0; i<arrTss.size(); i++)
    {
        tPoolPackage.beginPack();
        for (CWX_UINT32 j=0; j<arrTss[i].size(); j++)
        {
            pEnv = arrTss[i][j];
            if (!pEnv->packThreadInfo(threadPackage, pThrEnv->m_szBuf2K))return false;
            if (!tPoolPackage.addKeyValue(CWX_APP_MGR_THREAD, threadPackage.getMsg(), threadPackage.getMsgSize(), true))
            {
                reply.m_status = -1;
                reply.m_strError = tPoolPackage.getErrMsg();
                return true;
            }
        }
        if (!tPoolPackage.pack())
        {
            reply.m_status = -1;
            reply.m_strError = tPoolPackage.getErrMsg();
            return true;
        }
        if (!tPoolsPackage.addKeyValue(CWX_APP_MGR_THREAD_POOL, tPoolPackage.getMsg(), tPoolPackage.getMsgSize(), true))
        {
            reply.m_status = -1;
            reply.m_strError = tPoolsPackage.getErrMsg();
            return true;
        }
    }
    if (!reply.m_pRecord->addKeyValue(CWX_APP_MGR_THREAD_POOLS, tPoolsPackage.getMsg(), tPoolsPackage.getMsgSize(), true))
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    if (!reply.m_pRecord->pack())
    {
        reply.m_status = -1;
        reply.m_strError = reply.m_pRecord->getErrMsg();
        return true;
    }
    return true;
}
