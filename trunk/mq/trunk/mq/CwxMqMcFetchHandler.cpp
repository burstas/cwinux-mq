#include "CwxMqMcFetchHandler.h"
#include "CwxMqApp.h"
///构造函数
CwxMqMcFetchHandler::CwxMqMcFetchHandler(CwxMqApp* pApp, CwxAppChannel* channel):CwxAppHandler4Channel(channel)
{
    m_pApp = pApp;
}
///析构函数
CwxMqMcFetchHandler::~CwxMqMcFetchHandler()
{

}
