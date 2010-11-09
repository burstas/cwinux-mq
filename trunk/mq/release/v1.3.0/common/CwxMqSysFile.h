#ifndef __CWX_MQ_SYS_FILE_H__
#define __CWX_MQ_SYS_FILE_H__
/*
版权声明：
    本软件为个人所有，遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
但有以下例外：
    腾讯公司及与腾讯公司有直接业务与合作关系的公司不得使用此软件。原因可参考：
http://it.sohu.com/20100903/n274684530.shtml
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxMqMacro.h"
#include "CwxSysLogFile.h"
#include "CwxMutexLock.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"

/**
@file CwxMqSysFile.h
@brief MQ消息分发的分发点记录文件。
@author cwinux@gmail.com
@version 1.0
@date 2010-09-23
@warning
@bug
*/

class CwxMqSysFile
{
public:
    enum
    {
        SWITCH_SYS_FILE_NUM = 100000, ///<同一个系统文件，写多少次切换文件
        DEF_WRITE_DISK_INTERNAL = 100 ///<所有消息队列的sid多少次，写到数据文件。此只是write而不fsync
    };
public:
    CwxMqSysFile(CWX_UINT32 uiWriteDiskInternal, string const& strFileName);
    ~CwxMqSysFile();
public:
    ///初始化系统文件；0：成功；-1：失败
    int init(set<string> const& queues);
public:
    ///提交系统文件；0：成功；-1：失败
    inline int commit()
    {
        if (!m_bValid) return -1;
        {
            if (m_uiModifyCount)
            {
                if (!write()) return -1;
            }
            return m_pSysLogFile->commit();
        }
    }
    ///记录新的sid；1：成功；0：队列不存在；-1：失败
    inline int setSid(string const& strQueue, CWX_UINT64 ullSid, bool bMoreChange=true)
    {
        if (!m_bValid) return -1;
        {
            map<string, CWX_UINT64>::iterator iter=m_sids.find(strQueue);
            if (iter == m_sids.end()) return 0;
            if (bMoreChange && (iter->second>=ullSid)) return 1;
            m_uiModifyCount++;
            iter->second = ullSid;
            if (m_uiModifyCount > m_uiWriteDiskInternal)
            {
                if (!write()) return -1;
            }
        }
        return 1;
    }
    ///获取当前记录的sid
    inline bool getSid(string const& strQueue, CWX_UINT64& ullSid) const
    {
        map<string, CWX_UINT64>::const_iterator iter = m_sids.find(strQueue);
        if (iter == m_sids.end()) return false;
        ullSid = iter->second;
        return true;
    }
    ///获取队列的当前sid
    inline map<string, CWX_UINT64> const& getQueues() const
    {
        return m_sids;
    }
    ///获取系统文件的名字
    inline string const& getFileName() const
    {
        return m_strFileName;
    }
    ///获取错误信息
    inline char const* getErrMsg() const
    {
        return m_szErr2K;
    }
    ///是否有效
    inline bool isValid() const
    {
        return m_bValid;
    }
    ///是否存在队列
    inline bool isExistQueue(string const& strQueue)
    {
        return m_sids.find(strQueue) != m_sids.end();
    }
private:
    inline bool write()
    {
        m_uiModifyCount = 0;
        map<string, CWX_UINT64>::const_iterator iter = m_sids.begin();
        if (!m_szBuf)
        {
            CWX_UINT32 uiLen = 0;
            while(iter != m_sids.end())
            {
                uiLen = iter->first.length() + 64;
                iter++;
            }
            m_szBuf = new char[uiLen];
            if (!m_szBuf)
            {
                CwxCommon::snprintf(m_szErr2K, 2047, "Failure to malloc buf, size=%u", uiLen);
                return false;
            }
            iter = m_sids.begin();
        }
        CWX_UINT32 uiPos = 0;
        char sid[32];
        while (iter != m_sids.end())
        {
            CwxCommon::toString(iter->second, sid, 16);
            uiPos += sprintf(m_szBuf + uiPos, "%s=%s\n", iter->first.c_str(), sid);
            iter++;
        }
        if (0 != m_pSysLogFile->write(m_szBuf, uiPos, true))
        {
            m_bValid = false;
            strcpy(m_szErr2K, m_pSysLogFile->getErrMsg());
            return false;
        }
        return true;
    }

private:
    string          m_strFileName; ///<系统文件名字
    map<string, CWX_UINT64>  m_sids; ///<mq的map
    CwxSysLogFile*  m_pSysLogFile; ///<系统文件handle
    CWX_UINT32      m_uiWriteDiskInternal; ///<sid修改多少次写一次硬盘
    CWX_UINT32      m_uiModifyCount; ///<所有sid修改的次数
    char            m_szErr2K[2048]; ///<错误消息
    bool            m_bValid;  ///<是否有效
    char*           m_szBuf; ///<sid输出的buf。
};

#endif 
