#include "CwxMqMasterHandler.h"
#include "CwxMqApp.h"

///���ӽ�������Ҫ��master����sid
int CwxMqMasterHandler::onConnCreated(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    ///��������ID
    m_uiConnId = msg->event().getConnId();
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    ///������master����sid��ͨ�����ݰ�
    CwxMsgBlock* pBlock = NULL;
    int ret = CwxMqPoco::packReportData(pTss->m_pWriter,
        pBlock,
        0,
        m_pApp->getBinLogMgr()->getMaxSid(),
        false,
        m_pApp->getConfig().getCommon().m_uiChunkSize,
        m_pApp->getConfig().getCommon().m_uiWindowSize,
        m_pApp->getConfig().getSlave().m_strSubScribe.c_str(),
        m_pApp->getConfig().getSlave().m_master.getUser().c_str(),
        m_pApp->getConfig().getSlave().m_master.getPasswd().c_str(),
        pTss->m_szBuf2K);
    if (ret != CWX_MQ_SUCCESS)
    {///���ݰ�����ʧ��
        CWX_ERROR(("Failure to create report package, err:%s", pTss->m_szBuf2K));
        m_pApp->noticeCloseConn(m_uiConnId); ///�ر�����������ִ��ͬ��
        m_uiConnId = 0;
    }
    else
    {
        ///������Ϣ
        pBlock->send_ctrl().setConnId(m_uiConnId);
        pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MASTER);
        pBlock->send_ctrl().setHostId(0);
        pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
        if (0 != m_pApp->sendMsgByConn(pBlock))
        {//connect should be closed
            CWX_ERROR(("Failure to report sid to master"));
            CwxMsgBlockAlloc::free(pBlock);
        }
    }
    m_pApp->updateAppRunState();
    return 1;
}

///master�����ӹرպ���Ҫ��������
int CwxMqMasterHandler::onConnClosed(CwxMsgBlock*& , CwxTss* )
{
    CWX_ERROR(("Master is closed."));
    m_uiConnId = 0;
    m_pApp->updateAppRunState();
    return 1;
}

///��������master����Ϣ
int CwxMqMasterHandler::onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    int iRet = 0;
    CWX_UINT32 i = 0;
    if (!m_uiConnId) return 1;
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    //SID����Ļظ�����ʱ��һ���Ǳ���ʧ��
    if (CwxMqPoco::MSG_TYPE_SYNC_REPORT_REPLY == msg->event().getMsgHeader().getMsgType())
    {
        ///��ʱ���Զ˻�ر�����
        CWX_UINT64 ullSid = 0;
        char const* szMsg = NULL;
        int ret = 0;
        if (CWX_MQ_SUCCESS != CwxMqPoco::parseReportDataReply(pTss->m_pReader,
            msg,
            ret,
            ullSid,
            szMsg,
            pTss->m_szBuf2K))
        {
            CWX_ERROR(("Failure to parse report-reply msg from master, err=%s", pTss->m_szBuf2K));
            return 1;
        }
        CWX_ERROR(("Failure to sync message from master, ret=%d, err=%s", ret, szMsg));
        return 1;
    }
    else if (CwxMqPoco::MSG_TYPE_SYNC_DATA == msg->event().getMsgHeader().getMsgType())
    {///binlog����
        
        CWX_UINT64 ullSid;

        if (!m_pApp->getConfig().getCommon().m_uiChunkSize)
        {
            iRet = saveBinlog(pTss, msg->rd_ptr(), msg->length(), ullSid);
        }
        else
        {
            if (!m_reader.unpack(msg->rd_ptr(), msg->length(), false, true))
            {
                CWX_ERROR(("Failure to unpack master multi-binlog, err:%s", m_reader.getErrMsg()));
                iRet = -1;
            }
            for (i=0; i<m_reader.getKeyNum(); i++)
            {
                if(0 != strcmp(m_reader.getKey(i)->m_szKey, CWX_MQ_M))
                {
                    CWX_ERROR(("Master multi-binlog's key must be:%s, but:%s", CWX_MQ_M, m_reader.getKey(i)->m_szKey));
                    iRet = -1;
                }
                if (-1 == saveBinlog(pTss, m_reader.getKey(i)->m_szData, m_reader.getKey(i)->m_uiDataLen, ullSid))
                {
                    iRet = -1;
                    break;
                }
            }
            if (0 == i)
            {
                CWX_ERROR(("Master multi-binlog's key hasn't key"));
                iRet = -1;
            }
        }
        if (-1 == iRet)
        {
            m_pApp->updateAppRunState();
            m_pApp->noticeCloseConn(m_uiConnId);
            m_uiConnId = 0;
        }
        //�ظ�������
        CwxMsgBlock* reply_block = NULL;
        if (CWX_MQ_SUCCESS != CwxMqPoco::packSyncDataReply(pTss->m_pWriter,
            reply_block,
            msg->event().getMsgHeader().getTaskId(),
            ullSid,
            pTss->m_szBuf2K))
        {
            CWX_ERROR(("Failure to pack sync data reply, errno=%s", pTss->m_szBuf2K));
            m_pApp->updateAppRunState();
            m_pApp->noticeCloseConn(m_uiConnId);
            m_uiConnId = 0;
            return 1;
        }
        reply_block->send_ctrl().setConnId(m_uiConnId);
        reply_block->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MASTER);
        reply_block->send_ctrl().setHostId(0);
        reply_block->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
        if (0 != m_pApp->sendMsgByConn(reply_block))
        {//connect should be closed
            CWX_ERROR(("Failure to send sync data reply to master"));
            CwxMsgBlockAlloc::free(reply_block);
            m_pApp->updateAppRunState();
            m_pApp->noticeCloseConn(m_uiConnId);
            return 1;
        }
    }else{
        CWX_ERROR(("Unknow msg type[%u]", msg->event().getMsgHeader().getMsgType()));
        m_pApp->noticeReconnect(m_uiConnId, RECONN_MASTER_DELAY_SECOND * 1000);
        m_uiConnId = 0;
    }
    return 1;
}


//0���ɹ���-1��ʧ��
int CwxMqMasterHandler::saveBinlog(CwxMqTss* pTss, char const* szBinLog, CWX_UINT32 uiLen, CWX_UINT64& ullSid)
{
    CWX_UINT32 ttTimestamp;
    CWX_UINT32 uiGroup;
    CWX_UINT32 uiType;
    CWX_UINT32 uiAttr;
    CwxKeyValueItem const* data;
    ///��ȡbinlog������
    if (CWX_MQ_SUCCESS != CwxMqPoco::parseSyncData(pTss->m_pReader, 
        szBinLog,
        uiLen,
        ullSid,
        ttTimestamp,
        data,
        uiGroup,
        uiType,
        uiAttr,
        pTss->m_szBuf2K))
    {
        CWX_ERROR(("Failure to parse binlog from master, err=%s", pTss->m_szBuf2K));
        return -1;
    }
    //add to binlog
    pTss->m_pWriter->beginPack();
    pTss->m_pWriter->addKeyValue(CWX_MQ_DATA, data->m_szData, data->m_uiDataLen, data->m_bKeyValue);
    pTss->m_pWriter->pack();
    if (0 !=m_pApp->getBinLogMgr()->append(ullSid,
        (time_t)ttTimestamp,
        uiGroup,
        uiType,
        uiAttr,
        pTss->m_pWriter->getMsg(),
        pTss->m_pWriter->getMsgSize(),
        pTss->m_szBuf2K))
    {
        CWX_ERROR(("Failure to append binlog to binlog mgr, err=%s", pTss->m_szBuf2K));
        return -1;
    }
    return 0;
}
