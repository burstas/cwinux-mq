#ifndef __CWX_MC_STORE_H__
#define __CWX_MC_STORE_H__

#include "CwxMqMacro.h"
#include "CwxGlobalMacro.h"
#include "CwxCommon.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"

/* 注意：由于日志的文件名包含时间信息，日志的时间不能回退。 
* 数据存储对象，需要指定存储的path、文件的前缀及切换信息。
* 文件的存储路径为 strPath/prefix/prefix_yyyymmddhhmmss.seq.log
* 对于文件名部分，yyyymmddhhmmss为时间分割对应的时间边界，若以3600秒分割，
* 则yyyymmddhhmmss就是yyyymmddhh0000。
* 而seq则是一个时间段内，若文件大小超过限定的大小，则形成新文件。
* seq从0开始编码。
**/
class CwxMcStore {
public:
  enum{
    MIN_LOG_FILE_MSIZE = 32, ///<最小的文件大小
    MAX_LOG_FILE_MSIZE = 64 * 1024 ///< 最大的log文件大小
  };
public:
  CwxMcStore(string const& strPath, ///< 文件的路径，文件
    string const& strPrefix,
    CWX_UINT32 uiMaxFileMSize,
    CWX_UINT32 uiMaxFileSecond,
    CWX_UINT32 uiMaxUnflushLogNum,
    CWX_UINT32 uiMaxUnflushSecond,
    CWX_UINT32 uiReserveDay, ///<保存日志文件的天数，0表示全部保存
    bool bAppendReturn ///<是否在日志后append回车
    )
  {
    m_strPrefix = strPrefix;
    if (!m_strPrefix.length()){
      m_strPrefix = "log";
    }
    if ('/' == strPath[strPath.length() - 1]){
      m_strPath = strPath + "/" + m_strPrefix;
    }else{
      m_strPath = strPath + m_strPrefix;
    }
    if (uiMaxFileMSize < MIN_LOG_FILE_MSIZE) {
      uiMaxFileMSize = MIN_LOG_FILE_MSIZE;
    } else if (uiMaxFileMSize > MAX_LOG_FILE_MSIZE){
      uiMaxFileMSize = MAX_LOG_FILE_MSIZE;
    }
    m_offMaxFileSize = uiMaxFileMSize;
    m_offMaxFileSize *= 1024 * 1024;
    m_uiMaxFileSecond = uiMaxFileSecond;
    if (!m_uiMaxFileSecond || (m_uiMaxFileSecond > (24 * 3600))){
      m_uiMaxFileSecond = 24 * 3600;
    }
    m_uiMaxUnflushLogNum = uiMaxUnflushLogNum;
    m_uiMaxUnflushSecond = uiMaxUnflushSecond;
    m_uiReserveDay = uiReserveDay;
    m_bAppendReturn = bAppendReturn;
    m_uiCurFileSeq = 0;
    m_offCurFileSize = 0;
    m_uiCurFileStartTime = 0;
    m_uiCurFileEndTime = 0;
    m_uiCurUnflushLogNum = 0;
    m_uiCurFlushTimestamp = 0;
    m_fd = NULL;
    m_szErrMsg[0] = 0x00;
  }

  ~CwxMcStore() {
    if (m_fd) {
      flush();
      ::fclose(m_fd);
      m_fd = NULL;
    }
  }
public:
  /// 存储初始化。0：成功；-1：失败
  int init();
  /// 写入数据。0：成功；-1：失败
  int append(CWX_UINT32 uiTime, char const* szData, CWX_UINT32 uiDataLen);
  /// flush数据
  void flush();
  /// 检查flush的时间间隔
  void timeout(CWX_UINT32 uiTimeout);
  /// 获取当前错误的错误信息
  char const* getErrMsg() const {
    return m_szErrMsg;
  }
private:
  ///是否需要创建新的日志文件
  inline bool isCreateNewFile(CWX_UINT32 uiTime, CWX_UINT32 uiDateSize){
    if (!m_fd ||
      (uiTime > m_uiCurFileEndTime) ||
      (m_offCurFileSize + uiDateSize > m_offMaxFileSize))
    {
      return true;
    }
    return false;
  }
  /// 判断是否是合法的日志文件
  bool isLogFile(string const& strFile, CWX_UINT32& uiFileDate, CWX_UINT32& seq);
  /// 创建日志文件
  bool createLogFile(CWX_UINT32 uiTime);
  /// 获取文件的日期-序号值
  inline CWX_UINT64 getFileTimeSeq(CWX_UINT32 uiTime, CWX_UINT32 seq){
    CWX_UINT64 ullFileDateSeq = uiTime;
    ullFileDateSeq <<= 32;
    ullFileDateSeq += seq;
    return ullFileDateSeq;
  }
public:
  string      m_strPath; ///<文件存储的位置
  string      m_strPrefix; ///<文件名的前缀
  off_t       m_offMaxFileSize; ///<文件的最大大小
  off_t       m_uiMaxFileSecond; ///<文件的切换时间
  CWX_UINT32  m_uiMaxUnflushLogNum; ///<最大flush的log的数量
  CWX_UINT32  m_uiMaxUnflushSecond; ///<日志的flush时间间隔
  CWX_UINT32   m_uiReserveDay; ///<保存日志文件的天数
  bool        m_bAppendReturn; ///<是否增加回车符
  string      m_strCurFileName;  ///<当前的文件名
  CWX_UINT32  m_uiCurFileSeq; ///<当前的文件序号
  off_t       m_offCurFileSize; ///<当前的文件大小
  CWX_UINT32  m_uiCurFileStartTime; ///<当前文件的开始时间
  CWX_UINT32  m_uiCurFileEndTime; ///<当前文件的结束时间
  CWX_UINT32  m_uiCurUnflushLogNum; ///<当前未flush的log数量
  CWX_UINT32  m_uiCurFlushTimestamp; ///<当前flush的时间
  map<CWX_UINT64, string>  m_historyFiles; ///<历史文件信息
  FILE*       m_fd;           ///<当前文件的句柄
  char        m_szErrMsg[2048]; ///<当前的操作错误
};

#endif
