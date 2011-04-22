#include "CwxMqMasterHandler.h"
#include "CwxMqApp.h"

///连接建立后，需要往master报告sid
int CwxMqMasterHandler::onConnCreated(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    ///保存连接ID
    m_uiConnId = msg->event().getConnId();
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    ///创建往master报告sid的通信数据包
    CwxMsgBlock* pBlock = NULL;
    int ret = CwxMqPoco::packReportData(pTss->m_pWriter,
        pBlock,
        0,
        m_pApp->getBinLogMgr()->getMaxSid(),
        false,
        m_pApp->getConfig().getCommon().m_uiChunkSize,
        m_pApp->getConfig().getSlave().m_strSubScribe.c_str(),
        m_pApp->getConfig().getSlave().m_master_bin.getUser().c_str(),
        m_pApp->getConfig().getSlave().m_master_bin.getPasswd().c_str(),
        pTss->m_szBuf2K);
    if (ret != CWX_MQ_SUCCESS)
    {///数据包创建失败
        CWX_ERROR(("Failure to create report package, err:%s", pTss->m_szBuf2K));
        m_pApp->noticeCloseConn(m_uiConnId); ///关闭练级，不再执行同步
        m_uiConnId = 0;
    }
    else
    {
        ///发送消息
        pBlock->send_ctrl().setConnId(m_uiConnId);
        pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MASTER_BIN);
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

///master的连接关闭后，需要清理环境
int CwxMqMasterHandler::onConnClosed(CwxMsgBlock*& , CwxTss* )
{
    CWX_ERROR(("Master is closed."));
    m_uiConnId = 0;
    m_pApp->updateAppRunState();
    return 1;
}

///接收来自master的消息
int CwxMqMasterHandler::onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    if (!m_uiConnId) return 1;
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    //SID报告的回复，此时，一定是报告失败
    if (CwxMqPoco::MSG_TYPE_SYNC_REPORT_REPLY == msg->event().getMsgHeader().getMsgType())
    {
        ///此时，对端会关闭连接
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
    {///binlog数据
        CWX_UINT64 ullSid;
        CWX_UINT32 ttTimestamp;
        CWX_UINT32 uiGroup;
        CWX_UINT32 uiType;
        CWX_UINT32 uiAttr;
        CwxKeyValueItem const* data;
        ///获取binlog的数据
        if (CWX_MQ_SUCCESS != CwxMqPoco::parseSyncData(pTss->m_pReader, 
            msg,
            ullSid,
            ttTimestamp,
            data,
            uiGroup,
            uiType,
            uiAttr,
            pTss->m_szBuf2K))
        {
            CWX_ERROR(("Failure to parse binlog from master, err=%s", pTss->m_szBuf2K));
            m_pApp->noticeReconnect(m_uiConnId, RECONN_MASTER_DELAY_SECOND * 1000);
            m_uiConnId = 0;
            return 1;
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
           m_pApp->updateAppRunState();
           m_pApp->noticeCloseConn(m_uiConnId);
           m_uiConnId = 0;
           return 1;
        }
        //回复发送者
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
        reply_block->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MASTER_BIN);
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


