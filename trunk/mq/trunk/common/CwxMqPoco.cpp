#include "CwxMqPoco.h"

CwxPackageWriter* CwxMqPoco::m_pWriter =NULL;
///初始化协议。返回值，CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::init(char* )
{
    char const* data="This is binlog sync record.";
    if (!m_pWriter) m_pWriter = new CwxPackageWriter();
    m_pWriter->beginPack();
    m_pWriter->addKeyValue(CWX_MQ_DATA, data, strlen(data), false);
    m_pWriter->pack();
    return CWX_MQ_SUCCESS;
}
///释放协议。
void CwxMqPoco::destory()
{
    if (m_pWriter) delete m_pWriter;
    m_pWriter = NULL;
}

///返回值，CWX_MQ_SUCCESS：成功；其他都是失败

int CwxMqPoco::packRecvData(CwxPackageWriter* writer,
                        CwxMsgBlock*& msg,
                        CWX_UINT32 uiTaskId,
                        CwxKeyValueItem const& data,
                        CWX_UINT32 group,
                        CWX_UINT32 type,
                        CWX_UINT32 attr,
                        char const* user,
                        char const* passwd,
                        char* szErr2K
                        )
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_DATA, data.m_szData, data.m_uiDataLen, data.m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_GROUP, group))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_TYPE, type))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_ATTR, attr))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (user && !writer->addKeyValue(CWX_MQ_USER, user, strlen(user)))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (passwd && !writer->addKeyValue(CWX_MQ_PASSWD, passwd, strlen(passwd)))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_DATA, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}


///返回值，CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvData(CwxPackageReader* reader,
                         CwxMsgBlock const* msg,
                         CwxKeyValueItem const*& data,
                         CWX_UINT32& group,
                         CWX_UINT32& type,
                         CWX_UINT32& attr,
                         char const*& user,
                         char const*& passwd,
                         char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get data
    data = reader->getKey(CWX_MQ_DATA);
    if (!data)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_DATA);
        return CWX_MQ_NO_KEY_DATA;
    }
    if (data->m_bKeyValue)
    {
        if (!CwxPackage::isValidPackage(data->m_szData, data->m_uiDataLen))
        {
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "key[%s] is key/value, but it's format is not valid..", CWX_MQ_DATA);
            return CWX_MQ_INVALID_DATA_KV;
        }
    }
    //get group
    if (!reader->getKey(CWX_MQ_GROUP, group))
    {
        group = 0;
    }
    //get type
    if (!reader->getKey(CWX_MQ_TYPE, type))
    {
        type = 0;
    }
    if (SYNC_GROUP_TYPE == type)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "mq's type can't be [%X], it's binlog sync type.", SYNC_GROUP_TYPE);
        return CWX_MQ_INVALID_BINLOG_TYPE;
    }
    //get attr
    if (!reader->getKey(CWX_MQ_ATTR, attr))
    {
        attr = 0;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get user
    if (!(pItem = reader->getKey(CWX_MQ_USER)))
    {
        user = "";
    }
    else
    {
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = reader->getKey(CWX_MQ_PASSWD)))
    {
        passwd = "";
    }
    else
    {
        passwd = pItem->m_szData;
    }
    return CWX_MQ_SUCCESS;
}



///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::packRecvDataReply(CwxPackageWriter* writer,
                             CwxMsgBlock*& msg,
                             CWX_UINT32 uiTaskId,
                             int ret,
                             CWX_UINT64 ullSid,
                             char const* szErrMsg,
                             char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (CWX_MQ_SUCCESS != ret)
    {

        if (!writer->addKeyValue(CWX_MQ_ERR,
            szErrMsg?szErrMsg:"",
            szErrMsg?strlen(szErrMsg):0))
        {
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
    }
    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_DATA_REPLY, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvDataReply(CwxPackageReader* reader,
                                  CwxMsgBlock const* msg,
                                  int& ret,
                                  CWX_UINT64& ullSid,
                                  char const*& szErrMsg,
                                  char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_NO_RET;
    }
    //get sid
    if (!reader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    //get err
    if (CWX_MQ_SUCCESS != ret)
    {
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = reader->getKey(CWX_MQ_ERR)))
        {
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
            return CWX_MQ_NO_ERR;
        }
        szErrMsg = pItem->m_szData;
    }
    else
    {
        szErrMsg = "";
    }
    return CWX_MQ_SUCCESS;

}

///返回值，CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::packCommit(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        char const* user,
        char const* passwd,
        char* szErr2K
        )
{
    writer->beginPack();
    if (user)
    {
        if (!writer->addKeyValue(CWX_MQ_USER, user, strlen(user)))
        {
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
        if (passwd)
        {
            if (!writer->addKeyValue(CWX_MQ_PASSWD, passwd, strlen(passwd)))
            {
                if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
                return CWX_MQ_INNER_ERR;
            }
        }
    }
    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_COMMIT, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}
    ///返回值，CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseCommit(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        char const*& user,
        char const*& passwd,
        char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get user
    if (!(pItem = reader->getKey(CWX_MQ_USER)))
    {
        user = "";
    }
    else
    {
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = reader->getKey(CWX_MQ_PASSWD)))
    {
        passwd = "";
    }
    else
    {
        passwd = pItem->m_szData;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::packCommitReply(CwxPackageWriter* writer,
                           CwxMsgBlock*& msg,
                           CWX_UINT32 uiTaskId,
                           int ret,
                           char const* szErrMsg,
                           char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (CWX_MQ_SUCCESS != ret)
    {

        if (!writer->addKeyValue(CWX_MQ_ERR,
            szErrMsg?szErrMsg:"",
            szErrMsg?strlen(szErrMsg):0))
        {
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
    }
    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_COMMIT_REPLY, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseCommitReply(CwxPackageReader* reader,
                            CwxMsgBlock const* msg,
                            int& ret,
                            char const*& szErrMsg,
                            char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_NO_RET;
    }
    //get err
    if (CWX_MQ_SUCCESS != ret)
    {
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = reader->getKey(CWX_MQ_ERR)))
        {
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
            return CWX_MQ_NO_ERR;
        }
        szErrMsg = pItem->m_szData;
    }
    else
    {
        szErrMsg = "";
    }
    return CWX_MQ_SUCCESS;
}

int CwxMqPoco::packReportData(CwxPackageWriter* writer,
                          CwxMsgBlock*& msg,
                          CWX_UINT32 uiTaskId,
                          CWX_UINT64 ullSid,
                          bool      bNewly,
                          CWX_UINT32  uiChunkSize,
                          char const* subscribe,
                          char const* user,
                          char const* passwd,
                          char* szErr2K)
{
    writer->beginPack();
    if (!bNewly)
    {
        if (!writer->addKeyValue(CWX_MQ_SID, ullSid))
        {
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
    }
    if (uiChunkSize && writer->addKeyValue(CWX_MQ_CHUNK, uiChunkSize))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (subscribe && !writer->addKeyValue(CWX_MQ_SUBSCRIBE, subscribe, strlen(subscribe)))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (user && !writer->addKeyValue(CWX_MQ_USER, user, strlen(user)))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (passwd && !writer->addKeyValue(CWX_MQ_PASSWD, passwd, strlen(passwd)))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }

    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_REPORT, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseReportData(CwxPackageReader* reader,
                           CwxMsgBlock const* msg,
                           CWX_UINT64& ullSid,
                           bool& bNewly,
                           CWX_UINT32&  uiChunkSize,
                           char const*& subscribe,
                           char const*& user,
                           char const*& passwd,
                           char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get sid
    if (!reader->getKey(CWX_MQ_SID, ullSid))
    {
        bNewly = true;
    }
    else
    {
        bNewly = false;
    }
    if (!reader->getKey(CWX_MQ_CHUNK, uiChunkSize))
    {
        uiChunkSize = 0;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get subscribe
    if (!(pItem = reader->getKey(CWX_MQ_SUBSCRIBE)))
    {
        subscribe = "";
    }
    else
    {
        subscribe = pItem->m_szData;
    }
    //get user
    if (!(pItem = reader->getKey(CWX_MQ_USER)))
    {
        user = "";
    }
    else
    {
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = reader->getKey(CWX_MQ_PASSWD)))
    {
        passwd = "";
    }
    else
    {
        passwd = pItem->m_szData;
    }

    return CWX_MQ_SUCCESS;

}
///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::packReportDataReply(CwxPackageWriter* writer,
                               CwxMsgBlock*& msg,
                               CWX_UINT32 uiTaskId,
                               int ret,
                               CWX_UINT64 ullSid,
                               char const* szErrMsg,
                               char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_ERR,
        szErrMsg?szErrMsg:"",
        szErrMsg?strlen(szErrMsg):0))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_REPORT_REPLY, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}


///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseReportDataReply(CwxPackageReader* reader,
                                CwxMsgBlock const* msg,
                                int& ret,
                                CWX_UINT64& ullSid,
                                char const*& szErrMsg,
                                char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_NO_RET;
    }
    //get sid
    if (!reader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    //get err
    if (CWX_MQ_SUCCESS != ret)
    {
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = reader->getKey(CWX_MQ_ERR)))
        {
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
            return CWX_MQ_NO_ERR;
        }
        szErrMsg = pItem->m_szData;
    }
    else
    {
        szErrMsg = "";
    }
    return CWX_MQ_SUCCESS;
}


///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::packSyncData(CwxPackageWriter* writer,
                        CwxMsgBlock*& msg,
                        CWX_UINT32 uiTaskId,
                        CWX_UINT64 ullSid,
                        CWX_UINT32 uiTimeStamp,
                        CwxKeyValueItem const& data,
                        CWX_UINT32 group,
                        CWX_UINT32 type,
                        CWX_UINT32 attr,
                        char* szErr2K)
{
    writer->beginPack();
    int ret = packSyncDataItem(writer,
        ullSid,
        uiTimeStamp,
        data,
        group,
        type,
        attr,
        szErr2K);
    if (CWX_MQ_SUCCESS != ret) return ret;

    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_DATA, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}

int packSyncDataItem(CwxPackageWriter* writer,
                            CWX_UINT64 ullSid,
                            CWX_UINT32 uiTimeStamp,
                            CwxKeyValueItem const& data,
                            CWX_UINT32 group,
                            CWX_UINT32 type,
                            CWX_UINT32 attr,
                            char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_DATA, data.m_szData, data.m_uiDataLen, data.m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_GROUP, group))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_TYPE, type))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_ATTR, attr))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    writer->pack();
    return CWX_MQ_SUCCESS;
}

int CwxMqPoco::packMultiSyncData(
                                     CWX_UINT32 uiTaskId,
                                     char const* szData,
                                     CWX_UINT32 uiDataLen,
                                     CwxMsgBlock*& msg,
                                     char* szErr2K
                                 )
{
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_DATA, uiTaskId, uiDataLen);
    msg = CwxMsgBlockAlloc::pack(head, szData, uiDataLen);
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", uiDataLen);
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}


int CwxMqPoco::parseSyncData(CwxPackageReader* reader,
                         CwxMsgBlock const* msg,
                         CWX_UINT64& ullSid,
                         CWX_UINT32& uiTimeStamp,
                         CwxKeyValueItem const*& data,
                         CWX_UINT32& group,
                         CWX_UINT32& type,
                         CWX_UINT32& attr,
                         char* szErr2K)
{
    return parseSyncData(reader,
        msg->rd_ptr(),
        msg->length(),
        ullSid,
        uiTimeStamp,
        data,
        group,
        type,
        attr,
        szErr2K);
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseSyncData(CwxPackageReader* reader,
                         char const* szData,
                         CWX_UINT32 uiDataLen,
                         CWX_UINT64& ullSid,
                         CWX_UINT32& uiTimeStamp,
                         CwxKeyValueItem const*& data,
                         CWX_UINT32& group,
                         CWX_UINT32& type,
                         CWX_UINT32& attr,
                         char* szErr2K)
{
    if (!reader->unpack(szData, uiDataLen, false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get SID
    if (!reader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    //get timestamp
    if (!reader->getKey(CWX_MQ_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_TIMESTAMP);
        return CWX_MQ_NO_TIMESTAMP;
    }
    //get data
    if (!(data=reader->getKey(CWX_MQ_DATA)))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_DATA);
        return CWX_MQ_NO_KEY_DATA;
    }
    //get group
    if (!reader->getKey(CWX_MQ_GROUP, group))
    {
        group = 0;
    }
    //get type
    if (!reader->getKey(CWX_MQ_TYPE, type))
    {
        type = 0;
    }
    //get attr
    if (!reader->getKey(CWX_MQ_ATTR, attr))
    {
        type = 0;
    }
    return CWX_MQ_SUCCESS;

}


///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::packSyncDataReply(CwxPackageWriter* writer,
                            CwxMsgBlock*& msg,
                            CWX_UINT32 uiTaskId,
                            CWX_UINT64 ullSid,
                            char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_DATA_REPLY, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}


int CwxMqPoco::parseSyncDataReply(CwxPackageReader* reader,
                             CwxMsgBlock const* msg,
                             CWX_UINT64& ullSid,
                             char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get SID
    if (!reader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::packFetchMq(CwxPackageWriter* writer,
                       CwxMsgBlock*& msg,
                       bool bBlock,
                       char const* queue_name,
                       char const* user,
                       char const* passwd,
                       char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_BLOCK, bBlock))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (queue_name && !writer->addKeyValue(CWX_MQ_QUEUE, queue_name, strlen(queue_name)))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (user && !writer->addKeyValue(CWX_MQ_USER, user, strlen(user)))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (passwd && !writer->addKeyValue(CWX_MQ_PASSWD, passwd, strlen(passwd)))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_FETCH_DATA, 0, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;

}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseFetchMq(CwxPackageReader* reader,
                        CwxMsgBlock const* msg,
                        bool& bBlock,
                        char const*& queue_name,
                        char const*& user,
                        char const*& passwd,
                        char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get block
    if (!reader->getKey(CWX_MQ_BLOCK, bBlock, false))
    {
        bBlock = false;
    }

    CwxKeyValueItem const* pItem = NULL;
    //get queue
    if (!(pItem = reader->getKey(CWX_MQ_QUEUE)))
    {
        queue_name = "";
    }
    else
    {
        queue_name = pItem->m_szData;
    }
    //get user
    if (!(pItem = reader->getKey(CWX_MQ_USER)))
    {
        user = "";
    }
    else
    {
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = reader->getKey(CWX_MQ_PASSWD)))
    {
        passwd = "";
    }
    else
    {
        passwd = pItem->m_szData;
    }
    return CWX_MQ_SUCCESS;

}

int CwxMqPoco::packFetchMqReply(CwxPackageWriter* writer,
                            CwxMsgBlock*& msg,
                            int  ret,
                            char const* szErrMsg,
                            CWX_UINT64 ullSid,
                            CWX_UINT32 uiTimeStamp,
                            CwxKeyValueItem const& data,
                            CWX_UINT32 group,
                            CWX_UINT32 type,
                            CWX_UINT32 attr,
                            char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if(CWX_MQ_SUCCESS != ret)
    {
        if (!szErrMsg) szErrMsg="";
        if (!writer->addKeyValue(CWX_MQ_ERR, szErrMsg, strlen(szErrMsg)))
        {
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
    }
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_DATA, data.m_szData, data.m_uiDataLen, data.m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_GROUP, group))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_TYPE, type))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->addKeyValue(CWX_MQ_ATTR, attr))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_FETCH_DATA_REPLY, 0, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;

}


int CwxMqPoco::parseFetchMqReply(CwxPackageReader* reader,
                    CwxMsgBlock const* msg,
                    int&  ret,
                    char const*& szErrMsg,
                    CWX_UINT64& ullSid,
                    CWX_UINT32& uiTimeStamp,
                    CwxKeyValueItem const* data,
                    CWX_UINT32& group,
                    CWX_UINT32& type,
                    CWX_UINT32& attr,
                    char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_NO_RET;
    }
    if (CWX_MQ_SUCCESS != ret)
    {
        //get err
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = reader->getKey(CWX_MQ_ERR)))
        {
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
            return CWX_MQ_NO_ERR;
        }
        szErrMsg = pItem->m_szData;
    }
    else
    {
        szErrMsg = "";
    }
    //get SID
    if (!reader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    //get timestamp
    if (!reader->getKey(CWX_MQ_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_TIMESTAMP);
        return CWX_MQ_NO_TIMESTAMP;
    }
    //get data
    if (!(data=reader->getKey(CWX_MQ_DATA)))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_DATA);
        return CWX_MQ_NO_KEY_DATA;
    }
    //get group
    if (!reader->getKey(CWX_MQ_GROUP, group))
    {
        type = 0;
    }
    //get type
    if (!reader->getKey(CWX_MQ_TYPE, type))
    {
        type = 0;
    }
    //get attr
    if (!reader->getKey(CWX_MQ_ATTR, attr))
    {
        type = 0;
    }
    return CWX_MQ_SUCCESS;

}

///是否为有效地消息订阅语法
bool CwxMqPoco::isValidSubscribe(string const& strSubscribe, string& strErrMsg)
{
    CwxMqSubscribe subscribe;
    return parseSubsribe(strSubscribe, subscribe, strErrMsg);
}

///解析订阅的语法
bool CwxMqPoco::parseSubsribe(string const& strSubscribe,
                              CwxMqSubscribe& subscribe, string& strErrMsg)
{
    list<string> groups;
    list<string>::iterator iter_group;
    pair<CwxMqSubscribeItem/*group*/, CwxMqSubscribeItem/*type*/> rule;
    bool bAll;
    string strGroup=strSubscribe;
    subscribe.m_bAll = false;
    subscribe.m_subscribe.clear();
    CwxCommon::trim(strGroup);
    if (!strGroup.length() || !strcmp("*",strGroup.c_str()))
    {
        subscribe.m_bAll = true;
        return true;
    }
    //split strSubscribe by [;]
    CwxCommon::split(strSubscribe, groups, ';');
    iter_group = groups.begin();
    while(iter_group != groups.end())
    {
        strGroup = *iter_group;
        CwxCommon::trim(strGroup);
        if (strGroup.length())
        {//it must be group_express:type_express
            if (!parseSubsribeRule(strGroup, rule, bAll, strErrMsg)) return false;
            if (bAll)
            {
                subscribe.m_bAll = true;
                return true;
            }
            subscribe.m_subscribe.push_back(rule);
        }
        iter_group++;
    }
    if (!subscribe.m_subscribe.size()) subscribe.m_bAll = true;
    return true;
}

///解析一个订阅规则; format group_express:type_express
bool CwxMqPoco::parseSubsribeRule(string const& strSubsribeRule,
                              pair<CwxMqSubscribeItem/*group*/, CwxMqSubscribeItem/*type*/>& rule,
                              bool& bAll,
                              string& strErrMsg)
{
    list<string> express;
    list<string>::iterator iter_express;
    CwxCommon::split(strSubsribeRule,express, ':');
    string strGroupExpress;
    string strTypeExpress;
    if (express.size() != 2)
    {
        strErrMsg = "[";
        strErrMsg += strSubsribeRule + "] is not a valid 'group_express:type_express'";
        return false;
    }
    iter_express = express.begin();
    strGroupExpress = *iter_express;
    CwxCommon::trim(strGroupExpress);
    iter_express++;
    strTypeExpress = *iter_express;
    CwxCommon::trim(strTypeExpress);
    if (!parseSubsribeExpress(strGroupExpress, rule.first, strErrMsg)) return false;
    if (!parseSubsribeExpress(strTypeExpress, rule.second, strErrMsg)) return false;
    if (rule.first.m_bAll && rule.second.m_bAll)
    {
        bAll = true;
    }
    else
    {
        bAll = false;
    }
    return true;
}

///解析一个订阅表达式； [*]|[type_index%typte_num]|[begin-end,begin-group,...]
bool CwxMqPoco::parseSubsribeExpress(string const& strSubsribeExpress,
                                 CwxMqSubscribeItem& express,
                                 string& strErrMsg)
{
    express.m_bAll = false;
    express.m_bMod = false;
    express.m_uiModBase = 0;
    express.m_uiModIndex = 0;
    express.m_set.clear();
    if (!strSubsribeExpress.length() || !strcmp("*", strSubsribeExpress.c_str()))
    {
        express.m_bAll = true;
        return true;
    }
    if (strSubsribeExpress.find('%') != string::npos)
    {//mod表达式
        express.m_bMod = true;
        express.m_uiModIndex = strtoul(strSubsribeExpress.c_str(), NULL, 0);
        express.m_uiModBase = strtoul(strSubsribeExpress.c_str() + strSubsribeExpress.find('%') + 1, NULL, 0);
        if (!express.m_uiModBase)
        {
            strErrMsg = "[";
            strErrMsg += strSubsribeExpress + "]'s mod-base is zero";
            return false;
        }
        if (express.m_uiModBase <= express.m_uiModIndex)
        {
            strErrMsg = "[";
            strErrMsg += strSubsribeExpress + "]'s mod-base is not more than mod-index";
            return false;
        }
        return true;
    }
    //set表达式 begin-end,begin-end,..
    {
        list<string> items;
        list<string>::iterator iter;
        pair<CWX_UINT32, CWX_UINT32> range;
        string strValue;
        CwxCommon::split(strSubsribeExpress,items, ',');
        iter = items.begin();
        while(iter != items.end())
        {
            strValue = *iter;
            CwxCommon::trim(strValue);
            if (strValue.find('-') != string::npos)
            {//it's a range
                range.first = strtoul(strValue.c_str(), NULL, 0);
                range.second = strtoul(strValue.c_str() + strValue.find('-') + 1, NULL, 0);
            }
            else
            {
                range.first = range.second = strtoul(strValue.c_str(), NULL, 0);
            }
            if (range.first > range.second)
            {
                strErrMsg = "[";
                strErrMsg += strSubsribeExpress + "]'s begin is more than end.";
                return false;
            }
            express.m_set.push_back(range);
            iter++;
        }
    }
    return true;
}
