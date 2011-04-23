#include "CwxMqMcAsyncHandler.h"
#include "CwxMqApp.h"
///构造函数
CwxMqMcAsyncHandler::CwxMqMcAsyncHandler(CwxMqApp* pApp, CwxAppChannel* channel):CwxAppHandler4Channel(channel)
{
    m_pApp=pApp;
}
///析构函数
CwxMqMcAsyncHandler::~CwxMqMcAsyncHandler()
{
}
