#ifndef __MQ_MEMCACHE_H__
#define __MQ_MEMCACHE_H__

#include "MqDispMacro.h"
#include "CwxSockStream.h"

class MqMemcache
{
public:
    enum
    {
        MEMCACHE_MAX_LINE=1023, ///<memcache 命令或状态行的最大长度
        MEMCACHE_MAX_DATA = 1024 * 1024, ///<memcache的key的data最大长度
        MEMCACHE_LINE_BUF = 4096 ///<读取line的时候，read的buf长度
    };
    enum
    {
        MEMCACHE_ERR_SUCCESS = 0,
        MEMCACHE_ERR_CLOSED = 1,
        MEMCACHE_ERR_NOT_STORE = 2, ///add存在或replace不存在
        MEMCACHE_ERR_EXISTS = 3, ///CAS的key已经变更
        MEMCACHE_ERR_NOT_FOUND = 4, ///CAS的key不存在或delete的key不存在
        MEMCACHE_ERR_ERROR = 10, ///memcache的"ERROR\r\n"
        MEMCACHE_ERR_CERROR = 11, ///memcache的"CLIENT_ERROR\r\n"
        MEMCACHE_ERR_SERROR = 12, ///memcache的"SERVER_ERROR\r\n"
        MEMCACHE_ERR_NOCONN = 20, ///未连接
        MEMCACHE_ERR_UNKNOWN = 99 ///其他错误
    };
public :
    ///构造函数
    MqMemcache(char const* szIp, CWX_UINT16 unPort);
    ///析构函数
    ~MqMemcache();
public:
    //0：成功；
    //-1：失败
    int  connect(CWX_UINT32 uiMiliTimeout=0);
    //关闭连接
    void close();
public:
    ///MEMCACHE_ERR_SUCCESS：成功
    ///MEMCACHE_ERR_CLOSED: 读写失败而关闭连接
    int stats(CWX_UINT32 uiMiliTimeout);
    ///MEMCACHE_ERR_SUCCESS：成功
    ///MEMCACHE_ERR_CLOSED：读写失败而关闭连接
    ///MEMCACHE_ERR_NOT_STORE
    int add(char const* szKey,
        CWX_UINT32 uiFlag,
        CWX_UINT32 uiExpire,
        char const* szData,
        CWX_UINT32 uiDataLen,
        CWX_UINT32 uiMiliTimeout = 0);
    ///MEMCACHE_ERR_SUCCESS：成功
    ///MEMCACHE_ERR_CLOSED：读写失败而关闭连接
    int set(char const* szKey,
        CWX_UINT32 uiFlag,
        CWX_UINT32 uiExpire,
        char const* szData,
        CWX_UINT32 uiDataLen,
        CWX_UINT32 uiMiliTimeout = 0);
    ///MEMCACHE_ERR_SUCCESS：成功
    ///MEMCACHE_ERR_CLOSED：读写失败而关闭连接
    ///MEMCACHE_ERR_EXISTS：key的cas改变而无法修改
    ///MEMCACHE_ERR_NOT_FOUND：key不存在
    int cas(char const* szKey,
        CWX_UINT32 uiFlag,
        CWX_UINT32 uiExpire,
        char const* cas,
        char const* szData,
        CWX_UINT32 uiDataLen,
        CWX_UINT32 uiMiliTimeout = 0);
    ///MEMCACHE_ERR_SUCCESS：成功
    ///MEMCACHE_ERR_NOT_FOUND：没有发现
    ///MEMCACHE_ERR_CLOSED：获取失败，连接关闭
    int get(char const* szKey, CWX_UINT32 uiMiliTimeout = 0);
    ///MEMCACHE_ERR_SUCCESS：成功
    ///MEMCACHE_ERR_NOT_FOUND：没有发现
    ///MEMCACHE_ERR_CLOSED：获取失败，连接关闭
    int gets(char const* szKey, CWX_UINT32 uiMiliTimeout = 0);
public:
    ///是否连接
    bool isConn() const
    {
        return m_stream.getHandle() != CWX_INVALID_HANDLE;
    }
    ///获取连接句柄
    CWX_HANDLE getHandle() const
    {
        return m_stream.getHandle();
    }
    ///获取get或gets返回的flags
    CWX_UINT32 getFlags() const
    {
        return m_uiFlags;
    }
    ///获取gets返回的cas
    char const* getCas() const
    {
        return m_szCas;
    }
    ///获取get或gets或stats返回的value长度。
    CWX_UINT32 getDataLen() const
    {
        return m_uiDataLen;
    }
    ///获取get或gets或stats返回的value
    char const* getData() const
    {
        return m_szData;
    }
    ///获取出错的错误消息
    char const* getErrMsg() const
    {
        return m_szErr2K;
    }
private:
    ///获取缓存的读取内容大小
    CWX_UINT32 getCachedReadBufSize() const
    {
        return m_uiReadBufEnd>m_uiReadBufStart?m_uiReadBufEnd - m_uiReadBufStart:0;
    }
    ///获取缓存的读取内容
    char const* getCachedReadBuf() const
    {
        return m_szReadBuf + m_uiReadBufStart;
    }
    //读取一行，不能超过MEMCACHE_MAX_LINE，包括最后的\r\n。
    //返回值 -1：失败，否则返回行的字节数。
    int readLine(CwxTimeouter* timer);
    //读取数据
    //返回值 -1：失败，否则返回字节数。
    int readData(CwxTimeouter* timer);
    //判断当前读取的memcache返回行，是否为"END\r\n"
    bool isEndLine(CWX_UINT32 uiLen)
    {
        return memcmp(m_szLine, "END\r\n", uiLen)==0;
    }
    //判断当前读取的memcache返回行，是否为"STORED\r\n"
    bool isStoredLine(CWX_UINT32 uiLen)
    {
        return memcmp(m_szLine, "STORED\r\n", uiLen)==0;
    }
    //判断当前读取的memcache返回行，是否为"NOT_STORED\r\n"
    bool isNotStoredLine(CWX_UINT32 uiLen)
    {
        return memcmp(m_szLine, "NOT_STORED\r\n", uiLen)==0;
    }
    //判断当前读取的memcache返回行，是否为"EXISTS\r\n"
    bool isExistLine(CWX_UINT32 uiLen)
    {
        return memcmp(m_szLine, "EXISTS\r\n", uiLen) == 0;
    }
    //判断当前读取的memcache返回行，是否为"NOT_FOUND\r\n"
    bool isNotFoundLine(CWX_UINT32 uiLen)
    {
        return memcmp(m_szLine, "NOT_FOUND\r\n", uiLen) == 0;
    }
    //判读当前读取的memcache返回行，是否为"ERROR\r\n"
    bool isErrorLine(CWX_UINT32 uiLen)
    {
        return memcmp(m_szLine, "ERROR\r\n", uiLen) == 0;
    }
    //判断当前读取的memcache返回行，是否为"CLIENT_ERROR"
    bool isClientErrorLine()
    {
        return memcmp(m_szLine, "CLIENT_ERROR", strlen("CLIENT_ERROR")) == 0;
    }
    //判断当前读取的memcache返回行，是否为"SERVER_ERROR"
    bool isServerErrorLine()
    {
        return memcmp(m_szLine, "SERVER_ERROR", strlen("SERVER_ERROR"))==0;
    }
    //判断当前读取的memcache返回行，是否为"VALUE" 行
    bool isValueLine()
    {
        return memcmp(m_szLine, "VALUE ", strlen("VALUE ")) ==0;
    }
private:
    CwxSockStream   m_stream; ///<memcache的连接stream
    CWX_UINT16      m_unPort; ///<memcache的端口号
    string          m_Ip;     ///<memcache的ip地址
    CWX_UINT32      m_uiFlags; ///<读取key的flags
    char            m_szCas[64]; ///<读取key的cas值
    CWX_UINT32      m_uiDataLen; ///<读取key的data或stat返回的长度
    char            m_szData[MEMCACHE_MAX_DATA]; ///<key的data或stat返回的内容
    char            m_szLine[MEMCACHE_MAX_LINE + 1]; ///<读取的行的内容
    char            m_szReadBuf[MEMCACHE_LINE_BUF]; ///<read line时的buf。
    CWX_UINT32      m_uiReadBufStart;               ///<m_szReadBuf有效内容的开始位置
    CWX_UINT32      m_uiReadBufEnd;                 ///<m_szReadBuf有效内容的结束位置
    char            m_szErr2K[2048]; ///<错误信息
};

#endif

