#ifndef __CWX_MQ_TSS_H__
#define __CWX_MQ_TSS_H__
/*
版权声明：
    本软件遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
/**
@file CwxMqTss.h
@brief MQ系列服务的TSS定义文件。
@author cwinux@gmail.com
@version 0.1
@date 2010-09-15
@warning
@bug
*/

#include "CwxMqMacro.h"
#include "CwxAppLogger.h"
#include "CwxAppTss.h"
#include "CwxPackageReader.h"
#include "CwxPackageWriter.h"

//mq的tss
class CwxMqTss:public CwxAppTss
{
public:
    enum
    {
        MAX_PACKAGE_SIZE = 10 * 1024 * 1024 ///<分发数据包的最大长度
    };
public:
    ///构造函数
    CwxMqTss():CwxAppTss(new CwxAppTssInfo)
    {
        m_pReader = NULL;
        m_pWriter = NULL;
        m_szDataBuf = NULL;
        m_uiDataBufLen = 0;
    }
    ///析构函数
    ~CwxMqTss();
public:
    ///tss的初始化，0：成功；-1：失败
    int init();
    ///获取package的buf，返回NULL表示失败
    inline char* getBuf(CWX_UINT32 uiSize)
    {
        if (m_uiDataBufLen < uiSize)
        {
            delete [] m_szDataBuf;
            m_szDataBuf = new char[uiSize];
            m_uiDataBufLen = uiSize;
        }
        return m_szDataBuf;
    }
public:
    CwxPackageReader*      m_pReader; ///<数据包的解包对象
    CwxPackageWriter*      m_pWriter; ///<数据包的pack对象
private:
    char*                  m_szDataBuf; ///<数据buf
    CWX_UINT32             m_uiDataBufLen; ///<数据buf的空间大小
};





#endif
