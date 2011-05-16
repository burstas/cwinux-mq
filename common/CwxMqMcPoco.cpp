#include "MqMemcache.h"
#include "CwxSockConnector.h"
#include "CwxINetAddr.h"
#include "CwxCommon.h"
#include "linux/tcp.h"

MqMemcache::MqMemcache(char const* szIp, CWX_UINT16 unPort)
{
    m_unPort = unPort;
    m_Ip = szIp;
    m_uiFlags = 0;
    m_uiDataLen = 0;
    m_szCas[0] = 0;
    m_uiReadBufStart = 0;
    m_uiReadBufEnd = 0;
    m_szErr2K[0] = 0;
}

MqMemcache::~MqMemcache()
{
    m_stream.close();
}

//0：成功；
//-1：失败
int MqMemcache::connect(CWX_UINT32 uiMiliTimeout)
{
    CwxINetAddr addr(m_unPort, m_Ip.c_str(), AF_INET);
    CwxSockConnector connector;
    CwxTimeValue timeout(uiMiliTimeout/1000, (uiMiliTimeout%1000)*1000);
    CwxTimeouter timer(&timeout);
    m_stream.close();
    m_uiReadBufEnd = 0;
    m_uiReadBufStart = 0;
    int ret = connector.connect(m_stream,
        addr,
        CwxAddr::sap_any,
        uiMiliTimeout?&timer:NULL);
    if (0 == ret)
    {
        int nodelay =1;
        setsockopt(m_stream.getHandle(), IPPROTO_TCP, TCP_NODELAY, (char *)&nodelay, sizeof(nodelay));
        return 0;
    }
    ///确保stream关闭
    m_stream.close();
    CwxCommon::snprintf(m_szErr2K, 2047, "Failure to connect [%s:%u], errno=%d", m_Ip.c_str(), m_unPort, errno);
    return -1;
}

//关闭连接
void MqMemcache::close()
{
    m_uiReadBufEnd = 0;
    m_uiReadBufStart = 0;
    m_stream.close();
}

///MEMCACHE_ERR_SUCCESS：成功
///MEMCACHE_ERR_CLOSED: 读写失败而关闭连接
int MqMemcache::stats(CWX_UINT32 uiMiliTimeout)
{
    CwxTimeValue timeout(uiMiliTimeout/1000, (uiMiliTimeout%1000)*1000);
    CwxTimeouter timer(&timeout);
    strcpy(m_szLine, "stats\r\n");
    int ret = m_stream.write_n(m_szLine,
        strlen(m_szLine),
        uiMiliTimeout?&timer:NULL);
    if (ret != (int)strlen(m_szLine))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s], errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    m_uiDataLen = 0;
    while(1)
    {
        ret = readLine(uiMiliTimeout?&timer:NULL);
        if (-1 == ret)
        {
            close();
            return MEMCACHE_ERR_CLOSED;
        }
        if (m_uiDataLen + ret <= MEMCACHE_MAX_DATA)
        {
            if (isEndLine(ret)) break;
            memcpy(m_szData + m_uiDataLen, m_szLine, ret);
            m_uiDataLen += ret;
        }
        else
        {
            close();
            CwxCommon::snprintf(m_szErr2K, 2047, "stats's return is more than %d", MEMCACHE_MAX_DATA);
            return MEMCACHE_ERR_CLOSED;
        }
    }
    m_szData[m_uiDataLen] = 0x00;
    return MEMCACHE_ERR_SUCCESS;

}

///MEMCACHE_ERR_SUCCESS：成功
///MEMCACHE_ERR_CLOSED：读写失败而关闭连接
///MEMCACHE_ERR_NOT_STORE
int MqMemcache::add(char const* szKey,
        CWX_UINT32 uiFlag,
        CWX_UINT32 uiExpire,
        char const* szData,
        CWX_UINT32 uiDataLen,
        CWX_UINT32 uiMiliTimeout)
{
    CwxTimeValue timeout(uiMiliTimeout/1000, (uiMiliTimeout%1000)*1000);
    CwxTimeouter timer(&timeout);
    int ret = 0;
    memset(m_szLine, 0x00, MEMCACHE_MAX_LINE);
    CwxCommon::snprintf(m_szLine, MEMCACHE_MAX_LINE, "add %s %u %u %u \r\n",
        szKey, uiFlag, uiExpire,uiDataLen);
    ret = strlen(m_szLine);
    //write the head line
    if (ret != m_stream.write_n(m_szLine,
        ret,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s], errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    //write the data
    if (uiDataLen != (CWX_UINT32)m_stream.write_n(szData,
        uiDataLen,
         uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s]'s data, errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    //write "\r\n"
    if (2 != (CWX_UINT32)m_stream.write_n("\r\n",
        2,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s]'s data, errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    //read reply
    ret = readLine(uiMiliTimeout?&timer:NULL);
    if (-1 == ret)
    {
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    if (isStoredLine(ret)) return MEMCACHE_ERR_SUCCESS;
    if (isNotStoredLine(ret)) return MEMCACHE_ERR_NOT_STORE;
    close();
    CwxCommon::snprintf(m_szErr2K, 2047, "Unknown memcache [add] reply:%s, close connection", m_szLine);
    return MEMCACHE_ERR_CLOSED;
}

///MEMCACHE_ERR_SUCCESS：成功
///MEMCACHE_ERR_CLOSED：读写失败而关闭连接
int MqMemcache::set(char const* szKey,
        CWX_UINT32 uiFlag,
        CWX_UINT32 uiExpire,
        char const* szData,
        CWX_UINT32 uiDataLen,
        CWX_UINT32 uiMiliTimeout)
{
//    this->close();
//    this->connect(0);
        
    CwxTimeValue timeout(uiMiliTimeout/1000, (uiMiliTimeout%1000)*1000);
    CwxTimeouter timer(&timeout);
    int ret = 0;
    memset(m_szLine, 0x00, MEMCACHE_MAX_LINE);
    CwxCommon::snprintf(m_szLine, MEMCACHE_MAX_LINE, "set %s %u %u %u \r\n",
        szKey, uiFlag, uiExpire,uiDataLen);
    ret = strlen(m_szLine);
    //write the head line
    if (ret != m_stream.write_n(m_szLine,
        ret,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s], errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    //write the data
    if (uiDataLen != (CWX_UINT32)m_stream.write_n(szData,
        uiDataLen,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s]'s data, errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    //write "\r\n"
    if (2 != (CWX_UINT32)m_stream.write_n("\r\n",
        2,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s]'s data, errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }

    //read reply
    ret = readLine(uiMiliTimeout?&timer:NULL);
    if (-1 == ret)
    {
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    if (isStoredLine(ret)) return MEMCACHE_ERR_SUCCESS;
    close();
    CwxCommon::snprintf(m_szErr2K, 2047, "Unknown memcache [set] reply:%s, close connection", m_szLine);
    return MEMCACHE_ERR_CLOSED;
}

///MEMCACHE_ERR_SUCCESS：成功
///MEMCACHE_ERR_CLOSED：读写失败而关闭连接
///MEMCACHE_ERR_EXISTS：key的cas改变而无法修改
///MEMCACHE_ERR_NOT_FOUND：key不存在
int MqMemcache::cas(char const* szKey,
        CWX_UINT32 uiFlag,
        CWX_UINT32 uiExpire,
        char const* cas,
        char const* szData,
        CWX_UINT32 uiDataLen,
        CWX_UINT32 uiMiliTimeout)
{
    CwxTimeValue timeout(uiMiliTimeout/1000, (uiMiliTimeout%1000)*1000);
    CwxTimeouter timer(&timeout);
    int ret = 0;
    memset(m_szLine, 0x00, MEMCACHE_MAX_LINE);
    CwxCommon::snprintf(m_szLine, MEMCACHE_MAX_LINE, "cas %s %u %u %u %s\r\n",
        szKey, uiFlag, uiExpire, uiDataLen, cas);
    ret = strlen(m_szLine);
    //write the head line
    if (ret != m_stream.write_n(m_szLine,
        ret,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s], errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    //write the data
    if (uiDataLen != (CWX_UINT32)m_stream.write_n(szData,
        uiDataLen,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s]'s data, errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    //write "\r\n"
    if (2 != (CWX_UINT32)m_stream.write_n("\r\n",
        2,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s]'s data, errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }

    //read reply
    ret = readLine(uiMiliTimeout?&timer:NULL);
    if (-1 == ret)
    {
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    if (isStoredLine(ret)) return MEMCACHE_ERR_SUCCESS;
    if (isExistLine(ret)) return MEMCACHE_ERR_EXISTS;
    if (isNotFoundLine(ret)) return MEMCACHE_ERR_NOT_FOUND;
    close();
    CwxCommon::snprintf(m_szErr2K, 2047, "Unknown memcache [cas] reply:%s, close connection", m_szLine);
    return MEMCACHE_ERR_CLOSED;
}

///MEMCACHE_ERR_SUCCESS：成功
///MEMCACHE_ERR_NOT_FOUND：没有发现
///MEMCACHE_ERR_CLOSED：获取失败，连接关闭
int MqMemcache::get(char const* szKey, CWX_UINT32 uiMiliTimeout)
{
    CwxTimeValue timeout(uiMiliTimeout/1000, (uiMiliTimeout%1000)*1000);
    CwxTimeouter timer(&timeout);
    int ret = 0;
    memset(m_szLine, 0x00, MEMCACHE_MAX_LINE);
    CwxCommon::snprintf(m_szLine, MEMCACHE_MAX_LINE, "get %s \r\n", szKey);
    ret = strlen(m_szLine);
    //write the head line
    if (ret != m_stream.write_n(m_szLine,
        ret,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s], errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    //read reply
    ret = readLine(uiMiliTimeout?&timer:NULL);
    if (-1 == ret)
    {
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    if (isValueLine())
    {
        char const* pos = NULL;
        do
        {
            //skip value
            pos = strchr(m_szLine, ' ');
            if (!pos) break;
            while(*pos == ' ')pos ++;

            //skip key
            pos = strchr(pos, ' ');
            if (!pos) break;
            while(*pos == ' ')pos ++;

            //get flag
            m_uiFlags = strtoul(pos, NULL, 0);
            //get bytes
            pos = strchr(pos, ' ');
            while(*pos == ' ') pos++;
            m_uiDataLen = strtoul(pos, NULL, 0);
        }while(0);
        if (!pos)
        {
            CwxCommon::snprintf(m_szErr2K, 2047, "invalid get reply [%s]", m_szLine);
            close();
            return MEMCACHE_ERR_CLOSED;
        }
        ret = readData(uiMiliTimeout?&timer:NULL);
        if (-1 == ret)
        {
            close();
            return MEMCACHE_ERR_CLOSED;
        }
        return MEMCACHE_ERR_SUCCESS;
    }
    if (isEndLine(ret)) return  MEMCACHE_ERR_NOT_FOUND;
    close();
    CwxCommon::snprintf(m_szErr2K, 2047, "Unknown memcache [get] reply:%s, close connection", m_szLine);
    return MEMCACHE_ERR_CLOSED;

}

///MEMCACHE_ERR_SUCCESS：成功
///MEMCACHE_ERR_NOT_FOUND：没有发现
///MEMCACHE_ERR_CLOSED：获取失败，连接关闭
int MqMemcache::gets(char const* szKey, CWX_UINT32 uiMiliTimeout)
{
    CwxTimeValue timeout(uiMiliTimeout/1000, (uiMiliTimeout%1000)*1000);
    CwxTimeouter timer(&timeout);
    int ret = 0;
    memset(m_szLine, 0x00, MEMCACHE_MAX_LINE);
    CwxCommon::snprintf(m_szLine, MEMCACHE_MAX_LINE, "gets %s \r\n", szKey);
    ret = strlen(m_szLine);
    //write the head line
    if (ret != m_stream.write_n(m_szLine,
        ret,
        uiMiliTimeout?&timer:NULL))
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure to write command[%s], errno=%d", m_szLine, errno);
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    //read reply
    ret = readLine(uiMiliTimeout?&timer:NULL);
    if (-1 == ret)
    {
        close();
        return MEMCACHE_ERR_CLOSED;
    }
    if (isValueLine())
    {
        char const* pos = NULL;
        char const* pos_end = NULL;
        do
        {
            //skip value
            pos = strchr(m_szLine, ' ');
            if (!pos) break;
            while(*pos == ' ')pos ++;

            //skip key
            pos = strchr(pos, ' ');
            if (!pos) break;
            while(*pos == ' ')pos ++;

            //get flag
            m_uiFlags = strtoul(pos, NULL, 0);
            //get bytes
            pos = strchr(pos, ' ');
            while(*pos == ' ') pos++;
            m_uiDataLen = strtoul(pos, NULL, 0);
            //get cas
            pos = strchr(pos, ' ');
            while(*pos == ' ') pos++;
            pos_end = strchr(pos, '\r');
            if (!pos_end) break;
            if (pos_end - pos > 63)
            {
                CwxCommon::snprintf(m_szErr2K, 2047, "gets reply [%s]'s cas is too long,exceeding 63 byte", m_szLine);
                m_stream.close();
                return MEMCACHE_ERR_CLOSED;
            }
            memcpy(m_szCas, pos, pos_end - pos);
            m_szCas[pos_end - pos] = 0;
        }while(0);
        if (!pos)
        {
            CwxCommon::snprintf(m_szErr2K, 2047, "invalid gets reply [%s]", m_szLine);
            close();
            return MEMCACHE_ERR_CLOSED;
        }
        ret = readData(uiMiliTimeout?&timer:NULL);
        if (-1 == ret)
        {
            close();
            return MEMCACHE_ERR_CLOSED;
        }
        return MEMCACHE_ERR_SUCCESS;
    }
    if (isEndLine(ret)) return MEMCACHE_ERR_NOT_FOUND;
    close();
    CwxCommon::snprintf(m_szErr2K, 2047, "Unknown memcache [gets] reply:%s, close connection", m_szLine);
    return MEMCACHE_ERR_CLOSED;
}

//>0：成功；-1：失败
int MqMemcache::readLine(CwxTimeouter* timer)
{
    int ret = 0;
    CWX_UINT32 i = 0;
    size_t n=0;
    for (i=0; i<MEMCACHE_MAX_LINE; )
    {
        if (getCachedReadBufSize())
        {
            m_szLine[i] = m_szReadBuf[m_uiReadBufStart++];
            if (m_szLine[i] == '\n')
            {
                m_szLine[i+1] = 0x00;
                return i+1;
            }
            i++;
            continue;
        }
        //not buf
        m_uiReadBufStart = 0;
        n = MEMCACHE_LINE_BUF;
        ret = CwxSocket::recv(m_stream.getHandle(),
            (void*)m_szReadBuf,
            n,
            timer);
        if (ret <= 0) break;
        m_uiReadBufEnd = ret;
    }
    if (MEMCACHE_MAX_LINE <= i)
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Line is too long, max is %u, connect[%s:%u].", MEMCACHE_MAX_LINE, m_Ip.c_str(), m_unPort);
    }
    else if (0 == ret)
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure read for connect[%s:%u]. it is closed.", m_Ip.c_str(), m_unPort);
    }
    else
    {
        CwxCommon::snprintf(m_szErr2K, 2047, "Failure read for connect[%s:%u]. errno=%d", m_Ip.c_str(), m_unPort, errno);
    }
    return -1;
}

int MqMemcache::readData(CwxTimeouter* timer)
{
    int ret = 0;
    CWX_UINT32 uiCopySize = 0;
    if (getCachedReadBufSize())
    {
        uiCopySize = getCachedReadBufSize() > m_uiDataLen? m_uiDataLen:getCachedReadBufSize();
        memcpy(m_szData, getCachedReadBuf(), uiCopySize);
        m_uiReadBufStart += uiCopySize;
    }
    if (uiCopySize < m_uiDataLen)
    {
        ret = m_stream.read_n(m_szData + uiCopySize,
            m_uiDataLen - uiCopySize,
            timer);
        if (ret != (int)(m_uiDataLen - uiCopySize))
        {
            CwxCommon::snprintf(m_szErr2K, 2047, "Failure to read data, errno=%d", errno);
            return -1;
        }
    }
    //read \r\n
    if (-1 == readLine(timer)) return -1;
    //read END\r\n
    if (-1 == readLine(timer)) return -1;
    m_szData[m_uiDataLen] = 0x00;
    return m_uiDataLen;
}
