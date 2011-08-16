#include "CwxMproxyTask.h"
#include "CwxMproxyApp.h"
#include "CwxMqTss.h"
#include "CwxMqPoco.h"
///¹¹Ôìº¯Êý

void CwxMproxyTask::noticeTimeout(CwxTss* )
{
    setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
    m_bReplyTimeout = true;
    CWX_DEBUG(("Task is timeout , task_id=%u", getTaskId()));
}

void CwxMproxyTask::noticeRecvMsg(CwxMsgBlock*& msg, CwxTss* , bool& )
{
    CWX_ASSERT(m_uiSendConnId == msg->event().getConnId());
    m_mqReply = msg;
    msg = NULL;
    setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
}

void CwxMproxyTask::noticeFailSendMsg(CwxMsgBlock*& , CwxTss* )
{
    m_bFailSend = true;
    setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
}

void CwxMproxyTask::noticeEndSendMsg(CwxMsgBlock*& msg, CwxTss* , bool& )
{
    CWX_ASSERT(!m_uiSendConnId);
    m_uiSendConnId = msg->event().getConnId();
}

void CwxMproxyTask::noticeConnClosed(CWX_UINT32 , CWX_UINT32 , CWX_UINT32 uiConnId, CwxTss* )
{
    CWX_ASSERT(m_uiSendConnId == uiConnId);
    m_bFailSend = true;
    setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
}

int CwxMproxyTask::noticeActive(CwxTss*  )
{
    setTaskState(TASK_STATE_WAITING);
    if (0 != CwxMproxyMqHandler::sendMq(m_pApp, getTaskId(), m_sndMsg))
    {
        m_bFailSend = true;
        setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
    }
    return 0;
}

void CwxMproxyTask::execute(CwxTss* pThrEnv)
{
    if (CwxTaskBoardTask::TASK_STATE_INIT == getTaskState())
    {
        m_bReplyTimeout = false;
        m_bFailSend = false;
        CWX_UINT64 timeStamp = m_pApp->getConfig().m_uiTimeout;
        timeStamp *= 1000;
        timeStamp += CwxDate::getTimestamp();
        this->setTimeoutValue(timeStamp);
        getTaskBoard()->noticeActiveTask(this, pThrEnv);
    }
    if (CwxTaskBoardTask::TASK_STATE_FINISH == getTaskState())
    {
        reply(pThrEnv);
        delete this;
    }
 }

void CwxMproxyTask::reply(CwxTss* pThrEnv)
{
    CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
    CwxMsgBlock* replyMsg = NULL;
    int ret = CWX_MQ_ERR_SUCCESS;
    CWX_UINT64 ullSid = 0;
    char const* szErrMsg = NULL;
    if (m_mqReply)
    {
		if (!m_bCommit)
		{
			if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseRecvDataReply(pTss->m_pReader,
				m_mqReply,
				ret,
				ullSid,
				szErrMsg,
				pTss->m_szBuf2K))
			{
				CWX_ERROR(("Failure to parse mq's reply, err:%s", pTss->m_szBuf2K));
				ret = CWX_MQ_ERR_INNER_ERR;
				szErrMsg = "Failure to parse mq's rely";
			}
		}
		else
		{
			if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseCommitReply(pTss->m_pReader£¬
				m_mqReply,
				ret,
				szErrMsg,
				pTss->m_szBuf2K))
			{
				CWX_ERROR(("Failure to parse mq's reply, err:%s", pTss->m_szBuf2K));
				ret = CWX_MQ_ERR_INNER_ERR;
				szErrMsg = "Failure to parse mq's commit rely";
			}
		}
    }
    else if (m_bReplyTimeout)
    {
        ret = CWX_MQ_PROXY_TIMEOUT;
        szErrMsg = "Mq reply is timeout";
    }
    else if (m_bFailSend)
    {
        ret = CWX_MQ_PROXY_MQ_INVALID;
        szErrMsg = "Mq is not available";
    }
    else
    {
        CWX_ASSERT(0);
        ret = CWX_MQ_ERR_INNER_ERR;
        szErrMsg = "Unknown error";
    }
	if (!m_bCommit)
	{
		if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packRecvDataReply(pTss->m_pWriter,
			replyMsg,
			m_uiMsgTaskId,
			ret,
			ullSid,
			szErrMsg,
			pTss->m_szBuf2K
			))
		{
			CWX_ERROR(("Failure to pack mq reply, err:%s", pTss->m_szBuf2K));
			m_pApp->noticeCloseConn(m_uiReplyConnId);
		}
	}
	else
	{
		if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packCommitReply(pTss->m_pWriter,
			replyMsg,
			m_uiMsgTaskId,
			ret,
			szErrMsg,
			pTss->m_szBuf2K
			))
		{
			CWX_ERROR(("Failure to pack mq commit reply, err:%s", pTss->m_szBuf2K));
			m_pApp->noticeCloseConn(m_uiReplyConnId);
		}
	}
    CwxMproxyRecvHandler::reply(m_pApp, replyMsg, m_uiReplyConnId);
}


