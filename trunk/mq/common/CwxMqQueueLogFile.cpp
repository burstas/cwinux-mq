#include "CwxMqQueueLogFile.h"
#include "CwxDate.h"

CwxMqQueueLogFile::CwxMqQueueLogFile(CWX_UINT32 uiFsyncInternal,
                                     string const& strFileName)
{
    m_strFileName = strFileName; ///<系统文件名字
    m_strOldFileName = strFileName + ".old";///<旧系统文件名字
    m_strNewFileName = strFileName + ".new"; ///<新系统文件的名字
    m_fd = NULL;
    m_bLock = false;
    m_uiFsyncInternal = uiFsyncInternal; ///<flush硬盘的间隔
    m_uiCurLogCount = 0; ///<自上次fsync来，log记录的次数
    m_uiTotalLogCount = 0; ///<当前文件log的数量
	m_uiLastSaveTime = 0;
    strcpy(m_szErr2K, "No init");
}

CwxMqQueueLogFile::~CwxMqQueueLogFile(){
    closeFile(true);
}

///初始化系统文件；0：成功；-1：失败
int CwxMqQueueLogFile::init(CwxMqQueueInfo& queue)
{
    if (0 != prepare()){
        closeFile(false);
        return -1;
    }
    //清空数据
	queue.m_strName.erase();
    //加载数据
    if (0 != load(queue)){
        //若失败，清空数据
        closeFile(false);
        return -1;
    }
	m_uiLastSaveTime = time(NULL);
    return 0;
}

///保存队列信息；0：成功；-1：失败
int CwxMqQueueLogFile::save(CwxMqQueueInfo const& queue){
    if (!m_fd) return -1;
    //写新文件
    int fd = ::open(m_strNewFileName.c_str(),
        O_RDWR|O_CREAT|O_TRUNC,
        S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (-1 == fd){
        CwxCommon::snprintf(m_szErr2K, 2047,
            "Failure to open new sys file:%s, errno=%d",
            m_strNewFileName.c_str(),
            errno);
        closeFile(true);
        return -1;
    }
    //写入队列信息
    char line[1024];
    char szSid[64];
    ssize_t len = 0;
    //name=q1|sid=12345|u=u_q1|p=p_q1
	len = CwxCommon::snprintf(line, 
            1023,
            "name=%s|sid=%s|u=%s|p=%s\n",
            queue.m_strName.c_str(),
            CwxCommon::toString(queue.m_ullCursorSid, szSid, 10),
            queue.m_strUser.c_str(),
            queue.m_strPasswd.c_str());
	if (len != write(fd, line, len)){
		CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write new sys file:%s, errno=%d",
			m_strNewFileName.c_str(),
			errno);
		closeFile(true);
		return -1;
	}
    ::fsync(fd);
    ::close(fd);
    //确保旧文件删除
    if (CwxFile::isFile(m_strOldFileName.c_str()) &&
        !CwxFile::rmFile(m_strOldFileName.c_str()))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to rm old sys file:%s, errno=%d",
            m_strOldFileName.c_str(),
            errno);
        closeFile(true);
        return -1;
    }
    //关闭当前文件
    closeFile(true);
    //将当前文件move为old文件
    if (!CwxFile::moveFile(m_strFileName.c_str(), m_strOldFileName.c_str())){
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to move current sys file:%s to old sys file:%s, errno=%d",
            m_strFileName.c_str(),
            m_strOldFileName.c_str(),
            errno);
        return -1;
    }
    //将新文件移为当前文件
    if (!CwxFile::moveFile(m_strNewFileName.c_str(), m_strFileName.c_str())){
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to move new sys file:%s to current sys file:%s, errno=%d",
            m_strNewFileName.c_str(),
            m_strFileName.c_str(),
            errno);
        return -1;
    }
	//删除旧文件
	CwxFile::rmFile(m_strOldFileName.c_str());
    //打开当前文件，接受写
    //open file
    m_fd = ::fopen(m_strFileName.c_str(), "a+");
    if (!m_fd){
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to open sys file:[%s], errno=%d",
            m_strFileName.c_str(),
            errno);
        return -1;
    }
    if (!CwxFile::lock(fileno(m_fd))){
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to lock sys file:[%s], errno=%d",
            m_strFileName.c_str(),
            errno);
        return -1;
    }
    m_bLock = true;
    m_uiTotalLogCount = 0;
    m_uiCurLogCount = 0;
	m_uiLastSaveTime = time(NULL);
    return 0;
}

///写commit记录；-1：失败；否则返回已经写入的log数量
int CwxMqQueueLogFile::log(CWX_UINT64 sid){
    if (m_fd){
        char szBuf[1024];
        char szSid[64];
		size_t len = CwxCommon::snprintf(szBuf, 1023, "sid=%s\n", CwxCommon::toString(sid, szSid, 10));
        if (len != fwrite(szBuf, 1, len, m_fd)){
            closeFile(false);
            CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write log to file[%s], errno=%d",
                m_strFileName.c_str(),
                errno);
            return -1;
        }
        m_uiCurLogCount++;
        m_uiTotalLogCount++;
        if (m_uiCurLogCount >= m_uiFsyncInternal){
            if (0 != fsync()) return -1;
        }
        return m_uiTotalLogCount;
    }
    return -1;
}
///强行fsync日志文件；0：成功；-1：失败
int CwxMqQueueLogFile::fsync(){
    if (m_uiCurLogCount && m_fd){
        fflush(m_fd);
        if (0 != ::fsync(fileno(m_fd))){
            CwxCommon::snprintf(m_szErr2K, 2047, "Failure to fsync file[%s], errno=%d",
                m_strFileName.c_str(),
                errno);
            closeFile(false);
            return -1;
        }
        m_uiCurLogCount = 0;
    }
    return 0;
}


int CwxMqQueueLogFile::load(CwxMqQueueInfo& queue){
    bool bRet = true;
    string line;
    queue.m_strName.erase();
    //seek到文件头部
    fseek(m_fd, 0, SEEK_SET);
    //step
    int step = 0;
    m_uiLine = 1;
    string strQueue;
    CWX_UINT64 ullSid;
    //read queue, format:name=q1|sid=12345|u=u_q1|p=p_q1
    bRet = CwxFile::readTxtLine(m_fd, line);
    if (!bRet){
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to read queue file[%s], errno=%d",
            m_strFileName.c_str(),
            errno);
        return -1;
    }
    if (line.empty()) return 0;
    if (0 != parseQueue(line, queue)){
        queue.m_strName.erase();
        return 0;
    }
    //read sid
    while((bRet = CwxFile::readTxtLine(m_fd, line))){
        if (line.empty()) break;
        m_uiLine++;
        //format: sid=xx
        if (0 != parseSid(line, ullSid)){
            continue; ///数据可能不完整
        }
        queue.m_ullCursorSid = ullSid;
        m_uiTotalLogCount++;
    }
    if (!bRet){
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to read queue file[%s], errno=%d",
            m_strFileName.c_str(),
            errno);
        return -1;
    }
    return 0;
}


int CwxMqQueueLogFile::parseQueue(string const& line, CwxMqQueueInfo& queue){
    list<pair<string, string> > items;
    pair<string, string> item;
    CwxCommon::split(line, items, '|');
    //get name
    if (!CwxCommon::findKey(items, CWX_MQ_NAME, item)){
        CwxCommon::snprintf(m_szErr2K, 2047, "queue has no [%s] key, line:%u", 
            CWX_MQ_NAME,
            m_uiLine);
        return -1;
    }
    queue.m_strName = item.second;
    //get sid
    if (!CwxCommon::findKey(items, CWX_MQ_SID, item)){
        CwxCommon::snprintf(m_szErr2K, 2047, "queue[%s] has no [%s] key, line:%u",
            queue.m_strName.c_str(),
            CWX_MQ_SID,
            m_uiLine);
        return -1;
    }
    queue.m_ullCursorSid = strtoull(item.second.c_str(), NULL, 10);
    //get user
    if (!CwxCommon::findKey(items, CWX_MQ_U, item)){
        CwxCommon::snprintf(m_szErr2K, 2047, "queue[%s] has no [%s] key, line:%u",
            queue.m_strName.c_str(),
            CWX_MQ_U,
            m_uiLine);
        return -1;
    }
    queue.m_strUser = item.second;
    //get passwd
    if (!CwxCommon::findKey(items, CWX_MQ_P, item)){
        CwxCommon::snprintf(m_szErr2K, 2047, "queue[%s] has no [%s] key, line:%u",
            queue.m_strName.c_str(),
            CWX_MQ_P,
            m_uiLine);
        return -1;
    }
    queue.m_strPasswd = item.second;
    return 0;
}

int CwxMqQueueLogFile::parseSid(string const& line, CWX_UINT64& ullSid){
    pair<string, string> item;
    if (!CwxCommon::keyValue(line, item)){
		CwxCommon::snprintf(m_szErr2K, 2047, "Not find [%s] key, line:%u", 
			CWX_MQ_SID,
			m_uiLine);
		return -1;
	}
	if (item.first != CWX_MQ_SID){
		CwxCommon::snprintf(m_szErr2K, 2047, "Not find [%s] key, line:%u", 
			CWX_MQ_SID,
			m_uiLine);
		return -1;
	}
    //get sid
    ullSid = strtoull(item.second.c_str(), NULL, 10);
    return 0;
}


int CwxMqQueueLogFile::prepare(){
    bool bExistOld = CwxFile::isFile(m_strOldFileName.c_str());
    bool bExistCur = CwxFile::isFile(m_strFileName.c_str());
    bool bExistNew = CwxFile::isFile(m_strNewFileName.c_str());

    if (m_fd){
        closeFile(true);
    }
    m_fd = NULL;
    m_uiCurLogCount = 0; ///<自上次fsync来，log记录的次数
    m_uiTotalLogCount = 0; ///<当前文件log的数量
    strcpy(m_szErr2K, "No init");

    if (!bExistCur){
        if (bExistOld){//采用旧文件
            if (!CwxFile::moveFile(m_strOldFileName.c_str(), m_strFileName.c_str())){
                CwxCommon::snprintf(m_szErr2K, 2047, "Failure to move old sys file[%s] to cur sys file:[%s], errno=%d",
                    m_strOldFileName.c_str(),
                    m_strFileName.c_str(),
                    errno);
                return -1;
            }
        }else if(bExistNew){//采用新文件
            if (!CwxFile::moveFile(m_strNewFileName.c_str(), m_strFileName.c_str())){
                CwxCommon::snprintf(m_szErr2K, 2047, "Failure to move new sys file[%s] to cur sys file:[%s], errno=%d",
                    2047,
                    m_strNewFileName.c_str(),
                    m_strFileName.c_str(),
                    errno);
                return -1;
            }
        }else{//创建空的当前文件
            int fd = ::open(m_strFileName.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            if (-1 == fd){
                CwxCommon::snprintf(m_szErr2K, 2047, "Failure to create cur sys file:[%s], errno=%d",
                    m_strFileName.c_str(),
                    errno);
                return -1;
            }
            ::close(fd);
        }
    }
    //open file
    m_fd = ::fopen(m_strFileName.c_str(), "a+");
    if (!m_fd){
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to open sys file:[%s], errno=%d",
            m_strFileName.c_str(),
            errno);
        return -1;
    }
    if (!CwxFile::lock(fileno(m_fd))){
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to lock sys file:[%s], errno=%d",
            m_strFileName.c_str(),
            errno);
        return -1;
    }
	//删除旧文件
	CwxFile::rmFile(m_strOldFileName.c_str());
	//删除新文件
	CwxFile::rmFile(m_strNewFileName.c_str());
    m_bLock = true;
    return 0;
}
