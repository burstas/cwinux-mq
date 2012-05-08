#include "CwxSysLogFile.h"


CwxSysLogFile::CwxSysLogFile(char const* szFileName, CWX_UINT32 uiSwithFileNum)
{
    m_strFileName = szFileName;
    m_strOldFileName = m_strFileName + ".old";
    m_strNewFileName = m_strFileName + ".new";
    m_szFileContentBuf = NULL;
    m_uiBufLen = 0;
    m_uiFileSize = 0;
    m_uiContentSize = 0;
    m_uiSwitchLogFileCount = uiSwithFileNum;
    m_fd = -1;
    m_uiWriteCount = 0;
    m_bSaved = true;
    strcpy(m_szErrMsg, "Sys log file doesn't initialize.");
}

CwxSysLogFile::~CwxSysLogFile()
{
    if (m_fd)
    {
        saveFile();
        commit();
        CwxFile::unlock(m_fd);
        ::close(m_fd);
    }
    if (m_szFileContentBuf) free(m_szFileContentBuf);
}

int CwxSysLogFile::init()
{
    bool bExistOld = CwxFile::isFile(m_strOldFileName.c_str());
    bool bExistCur = CwxFile::isFile(m_strFileName.c_str());
    bool bExistNew = CwxFile::isFile(m_strNewFileName.c_str());

    if (-1 != m_fd) ::close(m_fd);
    m_fd = -1;
    if (m_szFileContentBuf) free(m_szFileContentBuf);
    m_szFileContentBuf = NULL;
    m_uiBufLen = 0;
    m_uiFileSize = 0;
    m_uiContentSize = 0;
    m_bSaved =true;

    if (!bExistCur)
    {
        if (bExistOld)
        {//采用旧文件
            if (!CwxFile::moveFile(m_strOldFileName.c_str(), m_strFileName.c_str()))
            {
                CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to move old sys file[%s] to cur sys file:[%s], errno=%d",
                    2047,
                    m_strOldFileName.c_str(),
                    m_strFileName.c_str(),
                    errno);
                return -1;
            }
        }
        else if(bExistNew)
        {//采用新文件
            if (!CwxFile::moveFile(m_strNewFileName.c_str(), m_strFileName.c_str()))
            {
                CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to move new sys file[%s] to cur sys file:[%s], errno=%d",
                    2047,
                    m_strNewFileName.c_str(),
                    m_strFileName.c_str(),
                    errno);
                return -1;
            }
        }
        else
        {//创建空的当前文件
            int fd = ::open(m_strFileName.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            if (-1 == fd)
            {
                CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to create cur sys file:[%s], errno=%d",
                    2047,
                    m_strFileName.c_str(),
                    errno);
                return -1;
            }
            ::close(fd);
        }
    }
    //open file
    m_fd = ::open(m_strFileName.c_str(), O_RDWR);
    if (-1 == m_fd)
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to open sys file:[%s], errno=%d",
            m_strFileName.c_str(),
            errno);
        return -1;
    }
    int iRet = -1;
    do{
        if (!CwxFile::lock(m_fd))
        {
            CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to lock sys file:[%s], errno=%d",
                m_strFileName.c_str(),
                errno);
            break;
        }
        //load content
        off_t size = CwxFile::getFileSize(m_strFileName.c_str());
        if (-1 == size)
        {
            CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to get sys file:[%s] size, errno=%d",
                m_strFileName.c_str(),
                errno);
            break;
        }
        m_uiContentSize = m_uiFileSize = size;
        if (m_uiFileSize)
        {
            if (!prepareBuf(m_uiFileSize))
            {
                CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to alloc buf, size=%u", m_uiFileSize);
                break;
            }
            size  = ::pread(m_fd, m_szFileContentBuf, m_uiFileSize, 0);
            if ((CWX_UINT32)size != m_uiFileSize)
            {
                CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to read sys file:[%s], errno=%d",
                    m_strFileName.c_str(),
                    errno);
                break;
            }
            m_szFileContentBuf[m_uiFileSize] = 0x00;
        }
        iRet = 0;
    }while(0);
    if (-1 == iRet)
    {
        CwxFile::unlock(m_fd);
        ::close(m_fd);
        m_fd = -1;
        return -1;
    }
    return 0;
}

int CwxSysLogFile::switchSysFile()
{
    if (!m_fd) return -1; ///当前文件的fd
    if (!m_bSaved)
    {
        if (0 != saveFile())
        {
            ::close(m_fd);
            m_fd = -1;
            return -1;
        }
    }
    //flush当前文件的数据
    if (0 != ::fsync(m_fd))
    {
        CwxFile::unlock(m_fd);
        ::close(m_fd);
        m_fd = -1;
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to flush sys file:%s, errno=%d",
            m_strFileName.c_str(),
            errno);
        return -1;
    }
    //关闭当前系统文件
    CwxFile::unlock(m_fd);
    ::close(m_fd);
    m_fd = -1;
    //写到新文件
    int fd = ::open(m_strNewFileName.c_str(),  O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (-1 == fd)
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to open new sys file:%s, errno=%d",
            m_strNewFileName.c_str(),
            errno);
        return -1;
    }
    if ((int)m_uiContentSize != ::write(fd, m_szFileContentBuf?m_szFileContentBuf:"", m_uiContentSize))
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to write new sys file:%s, errno=%d",
            m_strNewFileName.c_str(),
            errno);
        close(fd);
        return -1;
    }
    if (0 != ::fsync(fd))
    {
        ::close(fd);
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to flush new sys file:%s, errno=%d",
            m_strNewFileName.c_str(),
            errno);
        return -1;
    }
    ::close(fd);
    //删除old文件
    CwxFile::rmFile(m_strOldFileName.c_str());
    //将当前文件，move为old文件
    if (!CwxFile::moveFile(m_strFileName.c_str(), m_strOldFileName.c_str()))
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to move sys file:[%s] to old sys file:[%s], errno=%d",
            m_strFileName.c_str(),
            m_strOldFileName.c_str(),
            errno);
        return -1;
    }
    //将新文件，move为当前文件
    if (!CwxFile::moveFile(m_strNewFileName.c_str(), m_strFileName.c_str()))
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to move new sys file:[%s] to sys file:[%s], errno=%d",
            m_strNewFileName.c_str(),
            m_strFileName.c_str(),
            errno);
        return -1;
    }
    //打开新文件
    m_fd = ::open(m_strFileName.c_str(), O_RDWR);
    if (-1 == m_fd)
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to open sys file:[%s], errno=%d",
            m_strFileName.c_str(),
            errno);
        return -1;
    }
    if (!CwxFile::lock(m_fd))
    {
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to lock sys file:[%s], errno=%d",
            m_strFileName.c_str(),
            errno);
        ::close(m_fd);
        m_fd = -1;
        return -1;
    }
    return 0;
}

///将当前的内容保存到文件；0：成功；-1：失败
int CwxSysLogFile::saveFile()
{
    CWX_UINT32 uiWriteSize = m_uiContentSize;
    if (m_bSaved) return 0;
    if (-1 == m_fd) return -1;
    if (m_uiContentSize < m_uiFileSize)
    {///以空格补齐
        for (CWX_UINT32 i=m_uiContentSize; i<m_uiFileSize; i++)
            m_szFileContentBuf[i] = 0x20;
        uiWriteSize = m_uiFileSize;
    }
    if (uiWriteSize)
    {
        if (::pwrite(m_fd, m_szFileContentBuf, uiWriteSize, 0) != (int)uiWriteSize)
        {
            m_szFileContentBuf[m_uiContentSize] = 0x00;
            CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to write sys file[%s], errno=%d", 
                m_strFileName.c_str(),
                errno);
            return -1;
        }
        m_szFileContentBuf[m_uiContentSize] = 0x00;
    }
    if (uiWriteSize > m_uiFileSize) m_uiFileSize = uiWriteSize;
    m_bSaved = true;
    m_uiWriteCount ++;
    if (m_uiWriteCount > m_uiSwitchLogFileCount)
    {
        if (0 != switchSysFile())
        {
            return -1;
        }
        m_uiWriteCount = 0;
    }
    return 0;
}

///获取指定大小的内存；true：成功；false：失败
bool CwxSysLogFile::prepareBuf(CWX_UINT32 uiSize)
{
    if (m_uiBufLen <= uiSize)
    {
        uiSize = ((uiSize/1024) + 1)*1024;
        if (m_szFileContentBuf)
        {
            m_szFileContentBuf = (char*)realloc(m_szFileContentBuf, uiSize);
        }
        else
        {
            m_szFileContentBuf = (char*) malloc(uiSize);
        }
        if (m_szFileContentBuf)
        {
            m_uiBufLen = uiSize;
            return true;
        }
        else
        {
            m_uiBufLen = 0;
            return false;
        }
    }
    return true;
}
