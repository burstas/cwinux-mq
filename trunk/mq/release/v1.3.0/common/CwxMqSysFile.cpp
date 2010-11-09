#include "CwxMqSysFile.h"
#include "CwxDate.h"

CwxMqSysFile::CwxMqSysFile(CWX_UINT32 uiWriteDiskInternal,
                           string const& strFileName)
{
    m_uiWriteDiskInternal = uiWriteDiskInternal;
    m_uiModifyCount = 0;
    m_strFileName = strFileName;
    m_pSysLogFile = NULL;
    strcpy(m_szErr2K, "No init");
    m_bValid = false;
    m_szBuf = NULL;

}

CwxMqSysFile::~CwxMqSysFile()
{
    if (m_pSysLogFile)
    {
        delete m_pSysLogFile;
    }
    if (m_szBuf) delete [] m_szBuf;

}

int CwxMqSysFile::init(set<string> const& queues)
{
    if (m_pSysLogFile) delete m_pSysLogFile;
    m_pSysLogFile = new CwxSysLogFile(m_strFileName.c_str(), 
        SWITCH_SYS_FILE_NUM);
    m_sids.clear();
    m_uiModifyCount = 0;
    strcpy(m_szErr2K, "No init");
    m_bValid = false;
    if (m_szBuf) delete [] m_szBuf;
    m_szBuf = NULL;

    if (0 != m_pSysLogFile->init())
    {
        strcpy(m_szErr2K, m_pSysLogFile->getErrMsg());
        return -1;
    }
    bool bRemoveQueue=false;
    if(m_pSysLogFile->getFileContents())
    {
        string sid(m_pSysLogFile->getFileContents(), m_pSysLogFile->getFileContentSize());
        list< pair<string,string> > keys;
        pair<string,string> key;
        CwxCommon::split(sid, keys, '\n');
        list< pair<string,string> >::iterator iter = keys.begin();
        while(iter != keys.end())
        {
            if (queues.find(iter->first) != queues.end())
            {
                m_sids[iter->first] = strtoull(iter->second.c_str(), NULL, 16);
            }
            else
            {
                bRemoveQueue = true;
            }
            iter++;
        }
    }
    {//add new queue
        set<string>::const_iterator iter = queues.begin();
        while(iter != queues.end())
        {
            if (m_sids.find(*iter) == m_sids.end())
            {
                m_sids[*iter] = 0;
            }
            iter++;
        }
    }
    if (bRemoveQueue)
    {///备份旧文件
        string strDatetime;
        CwxDate::getDate(strDatetime);
        string strBakFile = m_strFileName + ".bak." + strDatetime.substr(0,10);
        FILE* fd = fopen(strBakFile.c_str(), "w+");
        if (!fd)
        {
            CwxCommon::snprintf(m_szErr2K, 2047, "Failure to open queue-sys-back-file:%s", strBakFile.c_str());
            return -1;
        }
        fwrite(m_pSysLogFile->getFileContents(), 1, m_pSysLogFile->getFileContentSize(), fd);
        fclose(fd);
    }
    if (!write()) return -1;

    m_bValid = true;
    m_szErr2K[0] = 0x00;
    return 0;
}
