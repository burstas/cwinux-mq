#include "CwxMproxyMqHandler.h"
#include "CwxMproxyApp.h"

///echo请求的处理函数
int CwxMproxyMqHandler::onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    CwxTaskBoardTask* pTask = NULL;
    m_pApp->getTaskBoard().noticeRecvMsg(msg->event().getMsgHeader().getTaskId(), msg, pThrEnv, pTask);
    if (pTask) pTask->execute(pThrEnv);
    msg = NULL;
    return 1;
}


int CwxMproxyMqHandler::onConnClosed(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    list<CwxTaskBoardTask*> tasks;
    m_pApp->getTaskBoard().noticeConnClosed(msg, pThrEnv, tasks);
    if (!tasks.empty())
    {
        list<CwxTaskBoardTask*>::iterator iter = tasks.begin();
        while(iter != tasks.end())
        {
            (*iter)->execute(pThrEnv);
            iter++;
        }
        tasks.clear();
    }
    m_pApp->setMqConnId(CWX_INVALID_HANDLE);
    return 1;
}

int CwxMproxyMqHandler::onEndSendMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    CwxTaskBoardTask* pTask = NULL;
    m_pApp->getTaskBoard().noticeEndSendMsg(msg->event().getTaskId(), msg, pThrEnv, pTask);
    if (pTask) pTask->execute(pThrEnv);
    return 1;
}

int CwxMproxyMqHandler::onFailSendMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    CwxTaskBoardTask* pTask = NULL;
    m_pApp->getTaskBoard().noticeFailSendMsg(msg->event().getTaskId(), msg, pThrEnv, pTask);
    if (pTask) pTask->execute(pThrEnv);
    return 1;
}

int CwxMproxyMqHandler::sendMq(CwxMproxyApp* app, CWX_UINT32 uiTaskId, CwxMsgBlock*& msg)
{
    msg->event().setTaskId(uiTaskId);
    msg->send_ctrl().setConnId(app->getMqConnId());
    msg->send_ctrl().setHostId(0);
    msg->send_ctrl().setSvrId(CwxMproxyApp::SVR_TYPE_MQ);
    msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::FAIL_FINISH_NOTICE);
    if (0 != app->sendMsgByConn(msg))
    {
        CWX_ERROR(("Failure to send msg to mq"));
        CwxMsgBlockAlloc::free(msg);
        msg = NULL;
        return -1;
    }
    msg = NULL;
    return 0;
}
