#ifndef __CWX_MQ_MC_FETCH_HANDLER_H__
#define __CWX_MQ_MC_FETCH_HANDLER_H__
/*
版权声明：
    本软件遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/

#include "CwxMqMacro.h"
#include "CwxMqTss.h"
#include "CwxDTail.h"
#include "CwxSTail.h"
#include "CwxTypePoolEx.h"
#include "CwxBinLogMgr.h"
#include "CwxMqDef.h"
#include "CwxMqQueueMgr.h"
#include "CwxAppHandler4Channel.h"

class CwxMqApp;

///
class CwxMqMcFetchHandler: public CwxAppHandler4Channel
{
public:
    ///构造函数
    CwxMqMcFetchHandler(CwxMqApp* pApp, CwxAppChannel* channel):CwxAppHandler4Channel(channel)
    {
        m_pApp = pApp;
    }
    ///析构函数
    virtual ~CwxMqMcFetchHandler()
    {

    }
public:
private:
    CwxMqApp*     m_pApp;  ///<app对象
};

#endif 
