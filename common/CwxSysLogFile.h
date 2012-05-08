#ifndef __CWX_SYS_LOG_FILE_H__
#define __CWX_SYS_LOG_FILE_H__

#include "CwxMqMacro.h"
#include "CwxFile.h"
#include "CwxCommon.h"


/**
@file CwxSysLogFile.h
@brief 系统记录文件管理对象，确保系统文件的一致性修改。
@author cwinux@gmail.com
@version 1.0
@date 2010-09-23
@warning
@bug
*/

class CwxSysLogFile
{
public:
    enum
    {
        DEF_SWITCH_SYS_FILE_NUM = 100000 ///<同一个系统文件，写多少次切换文件
    };
public:
    CwxSysLogFile(char const* szFileName, CWX_UINT32 uiSwithFileNum=DEF_SWITCH_SYS_FILE_NUM);
    ~CwxSysLogFile();
public:
    ///初始化系统文件；0：成功；-1：失败
    int init();
public:
    ///提交系统文件；0：成功；-1：失败
    inline int commit()
    {
        if (-1 != m_fd)
        {
            if (!m_bSaved)
            {
                if (0 != saveFile())
                {
                    ::close(m_fd);
                    m_fd = -1;
                    return -1;
                }
            }
            if (0 != ::fsync(m_fd))
            {
                CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to commit sys file:%s, errno=%d",
                    m_strFileName.c_str(),
                    errno);
                ::close(m_fd);
                m_fd = -1;
                return -1;
            }
            return 0;
        }
        return -1;
    }
    ///保存系统文件
    inline int write(char const* szContent, CWX_UINT32 uiSize, bool bSaveFile=true)
    {
        if (-1 != m_fd)
        {
            if (!prepareBuf(uiSize))
            {
                CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to alloc buf, size=%u", uiSize);
                ::close(m_fd);
                m_fd = -1;
                return -1;
            }
            memcpy(m_szFileContentBuf, szContent, uiSize);
            m_uiContentSize = uiSize;
            m_szFileContentBuf[uiSize] = 0x00;
            m_bSaved = false;
            if (bSaveFile)
            {
                if (0 != saveFile())
                {
                    ::close(m_fd);
                    m_fd = -1;
                    return -1;
                }
            }
            return 0;
        }
        return -1;
    }
    inline char const* getFileContents() const
    {
        return m_szFileContentBuf;
    }
    ///获取content的大小
    inline CWX_UINT32 getFileContentSize() const
    {
        return m_uiContentSize;
    }
    ///获取系统文件的名字
    inline string const& getFileName() const
    {
        return m_strFileName;
    }
    ///获取错误信息
    inline char const* getErrMsg() const
    {
        return m_szErrMsg;
    }
    ///是否有效
    inline bool isValid() const
    {
        return -1 != m_fd;
    }
private:
    ///切换系统文件；0：成功；-1：失败
    int switchSysFile();
    ///将当前的内容保存到文件；0：成功；-1：失败
    int saveFile();
    ///获取指定大小的内存；true：成功；false：失败
    bool prepareBuf(CWX_UINT32 uiSize);
private:
    string          m_strFileName; ///<系统文件名字
    string          m_strOldFileName; ///<旧系统文件名字
    string          m_strNewFileName; ///<新系统文件的名字
    char*           m_szFileContentBuf; ///<文件内容的buf
    CWX_UINT32      m_uiBufLen;  ///<文件内容buf的长度
    CWX_UINT32      m_uiFileSize; ///<当前文件的字节数
    CWX_UINT32      m_uiContentSize; ///<当前buf中内容的字节数
    CWX_UINT32      m_uiSwitchLogFileCount; ///<切换系统文件的写次数
    int             m_fd; ///<当前文件handle，若为-1表示无效
    bool            m_bSaved; ///<当前的内容是否保存到文件
    CWX_UINT32      m_uiWriteCount; ///<当前文件写的次数
    char            m_szErrMsg[2048];
};


#endif
