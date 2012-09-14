#ifndef __CWX_MQ_QUEUE_LOG_FILE_H__
#define __CWX_MQ_QUEUE_LOG_FILE_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
#include "CwxMqMacro.h"
#include "CwxMutexLock.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"
#include "CwxCommon.h"
#include "CwxFile.h"
#include "CwxDate.h"
#include "CwxMqDef.h"

/**
@file CwxMqQueueLogFile.h
@brief MQ Queue 队列的log对象定义。
@author cwinux@gmail.com
@version 1.0
@date 2011-05-27
@warning
@bug
*/

class CwxMqQueueLogFile{
public:
    enum{
        SWITCH_FILE_LOG_NUM = 100000, ///<写入多少个Log记录，需要切换日志文件
    };
public:
    CwxMqQueueLogFile(CWX_UINT32 uiFsyncInternal, string const& strFileName);
    ~CwxMqQueueLogFile();
public:
    ///初始化系统文件；0：成功；-1：失败
    int init(CwxMqQueueInfo& queue,
        set<CWX_UINT64>& uncommitSets,
        set<CWX_UINT64>& commitSets);
    ///保存队列信息；0：成功；-1：失败
    int save(CwxMqQueueInfo const& queue,
        set<CWX_UINT64> const& uncommitSets,
        set<CWX_UINT64> const& commitSets);
    ///写commit记录；-1：失败；否则返回已经写入的log数量
    int log(CWX_UINT64 sid);
    ///强行fsync日志文件；0：成功；-1：失败
    int fsync();
public:
	///删除队列文件
	inline static void removeFile(string const& file){
		string strFile = file;
		CwxFile::rmFile(strFile.c_str());
		//remove old file
		strFile = file + ".old";
		CwxFile::rmFile(strFile.c_str());
		//remove new file
		strFile = file + ".new";
		CwxFile::rmFile(strFile.c_str());
	}
    ///获取系统文件的名字
    inline string const& getFileName() const{
        return m_strFileName;
    }
    ///获取错误信息
    inline char const* getErrMsg() const{
        return m_szErr2K;
    }
    ///是否有效
    inline bool isValid() const{
        return m_fd!=NULL;
    }
    inline CWX_UINT32 getCurLogCount() const{
        return m_uiCurLogCount;
    }
    inline CWX_UINT32 getTotalLogCount() const{
        return m_uiTotalLogCount;
    }
	inline CWX_UINT32 getLastSaveTime() const{
		return m_uiLastSaveTime;
	}
private:
    ///0：成功；-1：失败
    int prepare();
    ///0：成功；-1：失败
    int load( CwxMqQueueInfo& queues,
        set<CWX_UINT64>& uncommitSets,
        set<CWX_UINT64>& commitSets);
    ///0：成功；-1：失败
    int parseQueue(string const& line, CwxMqQueueInfo& queue);
    ///0：成功；-1：失败
    int parseSid(string const& line, CWX_UINT64& ullSid);
    ///关闭文件
    inline void closeFile(bool bSync=true){
        if (m_fd){
            if (bSync) ::fsync(fileno(m_fd));
            if (m_bLock){
                CwxFile::unlock(fileno(m_fd));
                m_bLock = false;
            }
            fclose(m_fd);
            m_fd = NULL;
        }
    }
private:
    string          m_strFileName; ///<系统文件名字
    string          m_strOldFileName; ///<旧系统文件名字
    string          m_strNewFileName; ///<新系统文件的名字
    FILE*           m_fd; ///<文件handle
    bool            m_bLock; ///<文件是否已经加锁
    CWX_UINT32      m_uiFsyncInternal; ///<flush硬盘的间隔
    CWX_UINT32      m_uiCurLogCount; ///<自上次fsync来，log记录的次数
    CWX_UINT32      m_uiTotalLogCount; ///<当前文件log的数量
    CWX_UINT32      m_uiLine; ///<读取文件的当前行数
	CWX_UINT32      m_uiLastSaveTime; ///<上一次log文件的保存时间
    char            m_szErr2K[2048]; ///<错误消息
};

#endif 
