#include "CwxMcStore.h"
#include "CwxFile.h"
#include "CwxDate.h"
/// 存储初始化。0：成功；-1：失败
int CwxMcStore::init(){
  // 确保初始信息的正确
  if (m_fd){
    ::fclose(m_fd);
    m_fd = NULL;
  }
  m_uiCurFileSeq = 0;
  m_offCurFileSize = 0;
  m_uiCurFileStartTime = 0;
  m_uiCurFileEndTime = 0;
  m_uiCurUnflushLogNum = 0;
  m_strCurFileName.clear();
  m_historyFiles.clear();

  // 如果目录不存在，创建目录
  if (!CwxFile::isDir(m_strPath.c_str())){
    if (!CwxFile::createDir(m_strPath.c_str())){
      CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to create path:%s", m_strPath.c_str());
      return -1;
    }
  }
  // load 目录下的文件，加载时间最大的文件
  list < string > files;
  if (!CwxFile::getDirFile(m_strPath, files)) {
    CwxCommon::snprintf(m_szErrMsg, 2047,
      "Failure to get load file in path:%s, errno=%d", m_strPath.c_str(),
      errno);
    return -1;
  }
  // 获取最大的文件
  CWX_UINT32 uiFileStartTime = 0;
  CWX_UINT32 uiFileSeq = 0;
  CWX_UINT64 ullFileDateSeq = 0;
  string     strFilePathName;
  list < string >::iterator iter = files.begin();
  while(iter != files.end()){
    if (isLogFile(*iter, uiFileStartTime, uiFileSeq)){
      // 检查是否是最大的文件号
      if (uiFileStartTime > m_uiCurFileStartTime){
        m_uiCurFileStartTime = uiFileStartTime;
        m_uiCurFileSeq = uiFileSeq;
        m_strCurFileName = m_strPath + "/" + *iter;
      }else if (uiFileStartTime == m_uiCurFileStartTime){
        if (m_uiCurFileSeq < uiFileSeq){
          m_uiCurFileSeq = uiFileSeq;
        }
        m_strCurFileName = m_strPath + "/" + *iter;
      }
      if (m_uiReserveDay > 0){
        ullFileDateSeq = getFileTimeSeq(uiFileStartTime, uiFileSeq);
        strFilePathName = m_strPath + "/" + *iter;
        m_historyFiles[ullFileDateSeq] =  strFilePathName;
      }
    }
    ++iter;
  }
  // 打开文件
  if (m_strCurFileName.length()){
    m_offCurFileSize = CwxFile::getFileSize(m_strCurFileName.c_str());
    if (-1 == m_offCurFileSize){
      CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to get file size:%s, errno=%d",
        m_strCurFileName.c_str(), errno);
      return -1;
    }
    // 打开文件
    m_fd = ::fopen(m_strCurFileName.c_str(), "a+");
    if (!m_fd){
      CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to open file:%s, errno=%d",
        m_strCurFileName.c_str(), errno);
      return -1;
    }
  }
  return 0;
}

/// 写入数据。0：成功；-1：失败
int CwxMcStore::append(CWX_UINT32 uiTime, char const* szData, CWX_UINT32 uiDataLen){
  if (isCreateNewFile(uiTime, uiDataLen)){
    if (!createLogFile(uiTime)) return -1;
  }
  if (uiDataLen != ::fwrite(szData, 1, uiDataLen, m_fd)){
    CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to write file:%s, errno=%d",
      m_strCurFileName.c_str(), errno);
    return -1;
  }
  m_offCurFileSize += uiDataLen;
  if (m_bAppendReturn){
    if (1 != ::fwrite("\n", 1, 1, m_fd)){
      CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to write file:%s, errno=%d",
        m_strCurFileName.c_str(), errno);
      return -1;
    }
  }
  m_offCurFileSize++;
  m_uiCurUnflushLogNum++;
  if (m_uiMaxUnflushLogNum && (m_uiCurUnflushLogNum >= m_uiMaxUnflushLogNum)){
    flush();
  }
  return 0;
}

/// flush数据
void CwxMcStore::flush() {
  if (m_fd) {
    m_uiCurUnflushLogNum = 0;
    m_uiCurFlushTimestamp = time(NULL);
    ::fsync(fileno(m_fd));
  }
}
/// 检查flush的时间间隔
void CwxMcStore::timeout(CWX_UINT32 uiTimeout){
  if ((uiTimeout < m_uiCurFlushTimestamp) || 
    (m_uiCurFlushTimestamp + m_uiMaxUnflushSecond < uiTimeout))
  {
    flush();
    m_uiCurFlushTimestamp = uiTimeout;
  }
}

/// 判断是否是合法的日志文件
bool CwxMcStore::isLogFile(string const& strFile, CWX_UINT32& uiFileDate, CWX_UINT32& seq){
  if (strFile.length() < m_strPrefix.length()) return false;
  string strTmp = strFile.substr(m_strPrefix.length() + 1); // 加1是为了skip 【_】字符
  list<string> items;
  CwxCommon::split(strTmp, items, '.');
  if (3 != items.size()) return false;
  list<string>::iterator iter = items.begin();
  if (iter->length() != 14) return false;
  uiFileDate = CwxDate::getDateY4MDHMS2(*iter);
  ++iter;
  seq = strtoul(iter->c_str(), NULL, 10);
  ++iter;
  if (*iter != "log") return false;
  return true;
}
/// 创建日志文件
bool CwxMcStore::createLogFile(CWX_UINT32 uiTime) {
  // 如果当前的文件打开，则关闭文件
  if (m_fd){
    flush();
    ::fclose(m_fd);
    m_fd = NULL;
  }
  // 将当前文件加入到文件列表中
  if (m_uiReserveDay){
    CWX_UINT64 ullFileDateSeq = getFileTimeSeq(m_uiCurFileStartTime, m_uiCurFileSeq);
    m_historyFiles[ullFileDateSeq] = m_strCurFileName;
  }
  uiTime /= m_uiMaxFileSecond;
  if (uiTime <= m_uiCurFileStartTime){
    // 还是当前的时间，文件序号加1
    m_uiCurFileSeq ++;
  }else{
    m_uiCurFileSeq = 0;
    m_uiCurFileStartTime = uiTime;
  }
  // 设置文件的结束时间
  m_uiCurFileEndTime = m_uiCurFileEndTime + m_uiMaxFileSecond;
  string strFileTime;
  char szTmp[32];
  CwxDate::getDateY4MDHMS2(m_uiCurFileStartTime, strFileTime);
  sprintf(szTmp, "%u", m_uiCurFileSeq);
  m_strCurFileName = m_strPath + "/" + m_strPrefix + "_" + strFileTime + "." + szTmp + ".log";
  // 打开文件
  m_fd = ::fopen(m_strCurFileName.c_str(), "w+");
  if (!m_fd){
    CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to create file:%s, errno=%d",
      m_strCurFileName.c_str(), errno);
    return false;
  }
  m_offCurFileSize = 0;
  // 删除超过日期范围的文件
  if (m_uiReserveDay){
    CWX_UINT32 uiFileDate;
    map<CWX_UINT64, string>::iterator iter = m_historyFiles.begin();
    while(iter != m_historyFiles.end()){
      uiFileDate = (iter->first >> 32);
      //必须大于m_uiReserveDay，因为若按天切换的话，前一天文件的开始时间是一样的。
      if ((m_uiCurFileStartTime -  uiFileDate)/(24 * 3600) > m_uiReserveDay){
        CwxFile::rmFile(iter->second.c_str());
        m_historyFiles.erase(iter);
        iter = m_historyFiles.begin();
        continue;
      }
      break;
    }
  }
  return true;

}

