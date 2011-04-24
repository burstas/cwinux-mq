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
    return 1;
}



