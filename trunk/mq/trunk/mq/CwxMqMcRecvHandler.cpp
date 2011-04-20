#include "CwxMqMcRecvHandler.h"
#include "CwxMqApp.h"

///连接建立后，需要维护连接上数据的分发
int CwxMqMcRecvHandler::onConnCreated(CwxMsgBlock*& msg, CwxTss* )
{
    ///连接必须必须不存在
    CWX_ASSERT(m_clientMap.find(msg->event().getConnId()) == m_clientMap.end());
    m_clientMap[msg->event().getConnId()] = false;
    CWX_DEBUG(("Add recv conn for conn-id[%u]", msg->event().getConnId()));
    return 1;
}

///连接关闭后，需要清理环境
int CwxMqMcRecvHandler::onConnClosed(CwxMsgBlock*& msg, CwxTss* )
{
    map<CWX_UINT32, bool>::iterator iter = m_clientMap.find(msg->event().getConnId());
    ///连接必须存在
    CWX_ASSERT(iter != m_clientMap.end());
    m_clientMap.erase(iter);
    CWX_DEBUG(("remove recv conn for conn-id[%u]", msg->event().getConnId()));
    return 1;
}

///echo请求的处理函数
int CwxMqMcRecvHandler::onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    int iRet = CWX_MQ_SUCCESS;
    char const* user=NULL;
    char const* passwd=NULL;
    map<CWX_UINT32, bool>::iterator conn_iter = m_clientMap.find(msg->event().getConnId());
    CWX_ASSERT(conn_iter != m_clientMap.end());
    bool bAuth = conn_iter->second;
    do{
        ///binlog数据接收消息
        if (CwxMqPoco::MSG_TYPE_RECV_DATA == msg->event().getMsgHeader().getMsgType())
        {
            CWX_UINT32 uiGroup;
            CWX_UINT32 uiType;
            CWX_UINT32 uiAttr;
            CwxKeyValueItem const* pData;
            if (m_pApp->getBinLogMgr()->isInvalid())
            {
                ///如果binlog mgr无效，则停止接收
                iRet = CWX_MQ_BINLOG_INVALID;
                strcpy(pTss->m_szBuf2K, m_pApp->getBinLogMgr()->getInvalidMsg());
                break;
            }
            if (CWX_MQ_SUCCESS != CwxMqPoco::parseRecvData(pTss->m_pReader,
                msg,
                pData,
                uiGroup,
                uiType,
                uiAttr,
                user,
                passwd,
                pTss->m_szBuf2K))
            {
                //如果是无效数据，返回
                CWX_DEBUG(("Failure to parse the recieve msg, err=%s", pTss->m_szBuf2K));
                iRet = CWX_MQ_INVALID_MSG;
                break;
            }
            if (!bAuth && m_pApp->getConfig().getMaster().m_recv_bin.getUser().length())
            {
                if ((m_pApp->getConfig().getMaster().m_recv_bin.getUser() != user) ||
                    (m_pApp->getConfig().getMaster().m_recv_bin.getPasswd() != passwd))
                {
                    CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                    CWX_DEBUG((pTss->m_szBuf2K));
                    iRet = CWX_MQ_FAIL_AUTH;
                    break;
                }
                conn_iter->second = true;
            }
            pTss->m_pWriter->beginPack();
            pTss->m_pWriter->addKeyValue(CWX_MQ_DATA, pData->m_szData, pData->m_uiDataLen, pData->m_bKeyValue);
            pTss->m_pWriter->pack();
            if (0 != m_pApp->getBinLogMgr()->append(m_pApp->nextSid(),
                time(NULL),
                uiGroup,
                uiType,
                uiAttr,
                pTss->m_pWriter->getMsg(),
                pTss->m_pWriter->getMsgSize(),
                pTss->m_szBuf2K))
            {
                CWX_ERROR((pTss->m_szBuf2K));
                iRet = CWX_MQ_FAIL_ADD_BINLOG;
                ///更新服务的运行状态
                m_pApp->updateAppRunState();
                break;
            }
            ///增加未提交的binlog数量
            m_pApp->incUnCommitLogNum();
            ///auto commit
            if (m_pApp->isFirstBinLog() ||
                (m_pApp->getUnCommitLogNum() >= m_pApp->getConfig().getBinLog().m_uiFlushNum)||
                (time(NULL) > (time_t)(m_pApp->getLastCommitTime() + m_pApp->getConfig().getBinLog().m_uiFlushSecond)))
            {
                ///若达到提交的数量或第一次提交，则提交
                if (0 != commit(pTss->m_szBuf2K)){
                    CWX_ERROR((pTss->m_szBuf2K));
                    iRet = CWX_MQ_BINLOG_INVALID;
                    break;
                }
            }
            if (-1 == this->checkSyncLog(true, pTss->m_szBuf2K))
            {
                CWX_ERROR(("Failure to check sync log,err=%s", pTss->m_szBuf2K));
                iRet = CWX_MQ_BINLOG_INVALID;
                break;
            }
        }
        else if(CwxMqPoco::MSG_TYPE_RECV_COMMIT == msg->event().getMsgHeader().getMsgType())
        {
            if (CWX_MQ_SUCCESS != CwxMqPoco::parseCommit(pTss->m_pReader,
                msg,
                user,
                passwd,
                pTss->m_szBuf2K))
            {
                //如果是无效数据，返回
                CWX_DEBUG(("Failure to parse the commit msg, err=%s", pTss->m_szBuf2K));
                iRet = CWX_MQ_INVALID_MSG;
                break;
            }
            if (m_pApp->getConfig().getMaster().m_recv_bin.getUser().length())
            {
                if ((m_pApp->getConfig().getMaster().m_recv_bin.getUser() != user) ||
                    (m_pApp->getConfig().getMaster().m_recv_bin.getPasswd() != passwd))
                {
                    CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                    CWX_DEBUG((pTss->m_szBuf2K));
                    iRet = CWX_MQ_FAIL_AUTH;
                    break;
                }
            }

            if (0 != commit(pTss->m_szBuf2K))
            {
                CWX_ERROR(("Failure to commit the binlog, err=%s", pTss->m_szBuf2K));
                iRet = CWX_MQ_BINLOG_INVALID;
                break;
            }
        }
        else
        {
            CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u", msg->event().getMsgHeader().getMsgType());
            CWX_ERROR((pTss->m_szBuf2K));
            iRet = CWX_MQ_INVALID_MSG_TYPE;
            break;
        }
    }while(0);
    CwxMsgBlock* pBlock = NULL;
    if (CwxMqPoco::MSG_TYPE_RECV_COMMIT==msg->event().getMsgHeader().getMsgType())
    {
        if (CWX_MQ_SUCCESS != CwxMqPoco::packCommitReply(pTss,
            pBlock,
            msg->event().getMsgHeader().getTaskId(),
            iRet,
            pTss->m_szBuf2K,
            pTss->m_szBuf2K))
        {
            CWX_ERROR(("Failure to pack commit reply msg, err=%s", pTss->m_szBuf2K));
            m_pApp->noticeCloseConn(msg->event().getConnId());
            return 1;
        }
    }
    else
    {
        if (CWX_MQ_SUCCESS != CwxMqPoco::packRecvDataReply(pTss,
            pBlock,
            msg->event().getMsgHeader().getTaskId(),
            iRet,
            m_pApp->getCurSid(),
            pTss->m_szBuf2K,
            pTss->m_szBuf2K))
        {
            CWX_ERROR(("Failure to pack mq reply msg, err=%s", pTss->m_szBuf2K));
            m_pApp->noticeCloseConn(msg->event().getConnId());
            return 1;
        }
    }
    pBlock->send_ctrl().setConnId(conn_iter->first);
    pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_RECV_BIN);
    pBlock->send_ctrl().setHostId(0);
    pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    if (0 != m_pApp->sendMsgByConn(pBlock))
    {
        CWX_ERROR(("Failure to reply error msg"));
        CwxMsgBlockAlloc::free(pBlock);
        m_pApp->noticeCloseConn(msg->event().getConnId());
    }
    return 1;
}

///对于同步dispatch，需要检查同步的超时
int CwxMqMcRecvHandler::onTimeoutCheck(CwxMsgBlock*& , CwxTss* pThrEnv)
{
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    ///flush binlog
    if (m_pApp->getUnCommitLogNum())
    {
        if ((m_pApp->getUnCommitLogNum()>=m_pApp->getConfig().getBinLog().m_uiFlushNum) ||
            (time(NULL) > (time_t)(m_pApp->getLastCommitTime() + m_pApp->getConfig().getBinLog().m_uiFlushSecond)))
        {
            if (0 != this->commit(pTss->m_szBuf2K))
            {
                CWX_ERROR(("Failure to flush binlog ,err=%s", pTss->m_szBuf2K));
            }
        }
    }
    if (m_pApp->getMqUncommitNum())
    {
        if ((m_pApp->getMqUncommitNum() >= m_pApp->getConfig().getBinLog().m_uiMqFetchFlushNum) ||
            (time(NULL) > (time_t)(m_pApp->getMqLastCommitTime() + m_pApp->getConfig().getBinLog().m_uiMqFetchFlushSecond)))
        {
            if (0 != m_pApp->commit_mq(pTss->m_szBuf2K))
            {
                CWX_ERROR(("Failure to commit sys file, err=%s", pTss->m_szBuf2K));
            }
        }
    }
    int iRet = this->checkSyncLog(false, pTss->m_szBuf2K);
    if (-1 == iRet)
    {
        CWX_ERROR(("Failure to check sync log,err=%s", pTss->m_szBuf2K));
    }
    return 1;
}


int CwxMqMcRecvHandler::commit(char* szErr2K)
{
    int iRet = 0;
    CWX_INFO(("Begin flush bin log......."));
    if (m_pApp->getUnCommitLogNum())
    {
        m_pApp->clearFirstBinLog();
        iRet = m_pApp->getBinLogMgr()->commit(szErr2K);
        m_pApp->zeroUnCommitLogNum();
        m_pApp->setLastCommitTime(time(NULL));
    }
    CWX_INFO(("End flush bin log......."));
    return iRet;
}

///-1:失败；0：成功
int CwxMqMcRecvHandler::checkSyncLog(bool bNew, char* szErr2K)
{
    if (bNew) m_uiUnSyncLogNum ++;
    if (CwxMqPoco::isNeedSyncRecord(m_uiUnSyncLogNum, m_ttLastSyncTime))
    {
        if (0 != m_pApp->getBinLogMgr()->append(m_pApp->nextSid(),
            time(NULL),
            CwxMqPoco::SYNC_GROUP_TYPE,
            0,
            0,
            CwxMqPoco::getSyncRecordData(),
            CwxMqPoco::getSyncRecordDataLen(),
            szErr2K))
        {
            CWX_ERROR(("Failure append sync binlog to binlog, err=%s", szErr2K));
            return -1;
        }
        m_ttLastSyncTime = time(NULL);
        m_uiUnSyncLogNum = 0;
        m_pApp->incUnCommitLogNum();
        return 1;
    }
    return 0;
}


