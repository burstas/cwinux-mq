#include "CwxMqQueueMgr.h"

CwxMqQueue::CwxMqQueue(CWX_UINT32 uiId,
                       string strName,
                       string strUser,
                       string strPasswd,
                       CWX_UINT64 ullStartSid,
                       CwxBinLogMgr* pBinlog)
{
    m_uiId = uiId;
    m_strName = strName;
    m_strUser = strUser;
    m_strPasswd = strPasswd;
    m_ullStartSid = ullStartSid;
    m_cursor = NULL;
    m_memMsgTail = new CwxSTail<CwxMsgBlock>;
    m_binLog = pBinlog;
}

CwxMqQueue::~CwxMqQueue()
{
    if (m_cursor) m_binLog->destoryCurser(m_cursor);
    if (m_memMsgTail->count())
    {
        CwxMsgBlock* msg = NULL;
        while((msg=m_memMsgTail->pop_head()))
        {
            CwxMsgBlockAlloc::free(msg);
        }
    }
    delete m_memMsgTail;
}

///0：没有消息；
///1：获取一个消息；
///2：达到了搜索点，但没有发现消息；
///-1：失败；
int CwxMqQueue::getNextBinlog(CwxMqTss* pTss, int& err_num, char* szErr2K)
{
    CwxMsgBlock* msg;
    int iRet = 0;
    if (m_memMsgTail && m_memMsgTail->count())
    {
        msg = m_memMsgTail->pop_head();
        memcpy(&pTss->m_header, msg->rd_ptr(), sizeof(pTss->m_header));
        pTss->m_kvData.m_bKeyValue = *(msg->rd_ptr() + sizeof(pTss->m_header))=='1'?true:false;
        pTss->m_kvData.m_uiDataLen = msg->length() - sizeof(pTss->m_header) - 1;
        pTss->m_kvData.m_szData = pTss->getBuf(pTss->m_kvData.m_uiDataLen );
        if (!pTss->m_kvData.m_szData)
        {
            err_num = CWX_MQ_INNER_ERR;
            CwxCommon::snprintf(szErr2K, 2047, "Failure to malloc buf, size:%u", pTss->m_kvData.m_uiDataLen);
            CwxMsgBlockAlloc::free(msg);
            return -1;
        }
        memcpy(pTss->m_kvData.m_szData, msg->rd_ptr() + sizeof(pTss->m_header) + 1, pTss->m_kvData.m_uiDataLen);
        return 1;
    }
    if (!m_cursor)
    {
        if (m_ullStartSid < m_binLog->getMaxSid())
        {
            m_cursor = m_binLog->createCurser();
            if (!m_cursor)
            {
                err_num = CWX_MQ_INNER_ERR;
                strcpy(szErr2K, "Failure to create cursor");
                return -1;
            }
            iRet = m_binLog->seek(m_cursor, m_ullStartSid);
            if (1 != iRet)
            {
                if (-1 == iRet)
                {
                    strcpy(szErr2K, m_cursor->getErrMsg());
                }
                else
                {
                    strcpy(szErr2K, "Binlog's seek should return 1 but zero");
                }
                m_binLog->destoryCurser(m_cursor);
                m_cursor = NULL;
                err_num = CWX_MQ_INNER_ERR;
                return -1;
            }
            if (m_ullStartSid ==m_cursor->getHeader().getSid())
            {
                iRet = m_binLog->next(m_cursor);
                if (0 == iRet) return 0; ///<到了尾部
                if (-1 == iRet)
                {///<失败
                    strcpy(szErr2K, m_cursor->getErrMsg());
                    err_num = CWX_MQ_INNER_ERR;
                    return -1;
                }
            }
        }
        else
        {
            return 0;
        }
    }
    else
    {
        iRet = m_binLog->next(m_cursor);
        if (0 == iRet) return 0; ///<到了尾部
        if (-1 == iRet)
        {///<失败
            strcpy(szErr2K, m_cursor->getErrMsg());
            err_num = CWX_MQ_INNER_ERR;
            return -1;
        }
    }
    do 
    {
        CWX_UINT32 uiSkipNum = 0;
        while(!CwxMqPoco::isSubscribe(m_subscribe,
            false,
            m_cursor->getHeader().getGroup(),
            m_cursor->getHeader().getType()))
        {
            iRet = m_binLog->next(m_cursor);
            if (0 == iRet) return 0; ///<到了尾部
            if (-1 == iRet)
            {///<失败
                strcpy(szErr2K, m_cursor->getErrMsg());
                err_num = CWX_MQ_INNER_ERR;
                return -1;
            }
            uiSkipNum ++;
            if (!CwxMqPoco::isContinueSeek(uiSkipNum)) return 2;
            continue;
        }
        //fetch data
        ///获取binlog的data长度
        CWX_UINT32 uiDataLen = m_cursor->getHeader().getLogLen();
        ///准备data读取的buf
        char* pBuf = pTss->getBuf(uiDataLen);        
        ///读取data
        iRet = m_binLog->fetch(m_cursor, pBuf, uiDataLen);
        if (-1 == iRet)
        {//读取失败
            strcpy(szErr2K, m_cursor->getErrMsg());
            err_num = CWX_MQ_INNER_ERR;
            return -1;
        }
        ///unpack data的数据包
        if (pTss->m_pReader->unpack(pBuf, uiDataLen, false, true))
        {
            ///获取CWX_MQ_DATA的key，此为真正data数据
            CwxKeyValueItem const* pItem = pTss->m_pReader->getKey(CWX_MQ_DATA);
            if (pItem)
            {
                pTss->m_kvData.m_szData = (char*)pItem->m_szData;
                pTss->m_kvData.m_uiDataLen = pItem->m_uiDataLen;
                pTss->m_kvData.m_bKeyValue = pItem->m_bKeyValue;
                pTss->m_header = m_cursor->getHeader();
                return 1;
            }
            else
            {///读取的数据无效
                char szBuf[64];
                CWX_ERROR(("Can't find key[%s] in binlog, sid=%s", CWX_MQ_DATA,
                    CwxCommon::toString(m_cursor->getHeader().getSid(), szBuf)));
            }            
        }
        else
        {///binlog的数据格式错误，不是kv
            char szBuf[64];
            CWX_ERROR(("Can't unpack binlog, sid=%s",
                CwxCommon::toString(m_cursor->getHeader().getSid(), szBuf)));
        }
        uiSkipNum ++;
        if (!CwxMqPoco::isContinueSeek(uiSkipNum)) return 2;
        iRet = m_binLog->next(m_cursor);
        if (0 == iRet) return 0; ///<到了尾部
        if (-1 == iRet)
        {///<失败
            strcpy(szErr2K, m_cursor->getErrMsg());
            err_num = CWX_MQ_INNER_ERR;
            return -1;
        }
    }while(1);

    return 0;
}


bool CwxMqQueue::backMsg(CwxBinLogHeader const& header, CwxKeyValueItem const& data)
{
    CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(header) + 1 + data.m_uiDataLen);
    if (!msg) return false;
    memcpy(msg->wr_ptr(), &header, sizeof(header));
    if (data.m_bKeyValue)
        *(msg->wr_ptr() + sizeof(header)) = '1';
    else
        *(msg->wr_ptr() + sizeof(header)) = '0';

    memcpy(msg->wr_ptr() + sizeof(header) + 1, data.m_szData, data.m_uiDataLen);
    msg->wr_ptr(sizeof(header) + data.m_uiDataLen + 1);
    m_memMsgTail->push_head(msg);
    return true;
}


CwxMqQueueMgr::CwxMqQueueMgr()
{
}

CwxMqQueueMgr::~CwxMqQueueMgr()
{
    map<string, CwxMqQueue*>::iterator iter =  m_nameQueues.begin();
    while(iter != m_nameQueues.end())
    {
        delete iter->second;
        iter++;
    }
    m_nameQueues.clear();
    m_idQueues.clear();
}

int CwxMqQueueMgr::init(CwxBinLogMgr* binLog,
                        map<string, CWX_UINT64> const& queueSid,
                        map<string, CwxMqConfigQueue> const& queueInfo)
{
    CWX_UINT32 uiId = QUEUE_ID_START;
    CwxMqQueue* mq = NULL;
    map<string, CWX_UINT64>::const_iterator iter=queueSid.begin();
    map<string, CwxMqConfigQueue>::const_iterator iter_info ;
    string errMsg;
    while(iter != queueSid.end())
    {
        iter_info = queueInfo.find(iter->first);
        CWX_ASSERT(iter_info != queueInfo.end());
        mq = new CwxMqQueue(uiId,
            iter->first,
            iter_info->second.m_strUser,
            iter_info->second.m_strPasswd,
            iter->second,
            binLog);
        if (!CwxMqPoco::parseSubsribe(iter_info->second.m_strSubScribe, mq->getSubscribe(), errMsg))
        {
            delete mq;
            return -1;
        }
        m_nameQueues[iter->first] = mq;
        m_idQueues[uiId++] = mq;
        iter++;
    }
    return 0;
}
