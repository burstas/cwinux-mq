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

int CwxMqPoco::packRecvData(CwxMqTss* pTss,
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
    pTss->m_pWriter->beginPack();
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_DATA, data.m_szData, data.m_uiDataLen, data.m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_GROUP, group))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_TYPE, type))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_ATTR, attr))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (user && !pTss->m_pWriter->addKeyValue(CWX_MQ_USER, user, strlen(user)))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (passwd && !pTss->m_pWriter->addKeyValue(CWX_MQ_PASSWD, passwd, strlen(passwd)))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_DATA, uiTaskId, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}


///返回值，CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvData(CwxMqTss* pTss,
                         CwxMsgBlock const* msg,
                         CwxKeyValueItem const*& data,
                         CWX_UINT32& group,
                         CWX_UINT32& type,
                         CWX_UINT32& attr,
                         char const*& user,
                         char const*& passwd,
                         char* szErr2K)
{
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get data
    data = pTss->m_pReader->getKey(CWX_MQ_DATA);
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
    if (!pTss->m_pReader->getKey(CWX_MQ_GROUP, group))
    {
        group = 0;
    }
    //get type
    if (!pTss->m_pReader->getKey(CWX_MQ_TYPE, type))
    {
        type = 0;
    }
    if (SYNC_GROUP_TYPE == type)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "mq's type can't be [%X], it's binlog sync type.", SYNC_GROUP_TYPE);
        return CWX_MQ_INVALID_BINLOG_TYPE;
    }
    //get attr
    if (!pTss->m_pReader->getKey(CWX_MQ_ATTR, attr))
    {
        attr = 0;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get user
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_USER)))
    {
        user = "";
    }
    else
    {
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_PASSWD)))
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
int CwxMqPoco::packRecvDataReply(CwxMqTss* pTss,
                             CwxMsgBlock*& msg,
                             CWX_UINT32 uiTaskId,
                             int ret,
                             CWX_UINT64 ullSid,
                             char const* szErrMsg,
                             char* szErr2K)
{
    pTss->m_pWriter->beginPack();
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (CWX_MQ_SUCCESS != ret)
    {

        if (!pTss->m_pWriter->addKeyValue(CWX_MQ_ERR,
            szErrMsg?szErrMsg:"",
            szErrMsg?strlen(szErrMsg):0))
        {
            if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
    }
    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_DATA_REPLY, uiTaskId, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvDataReply(CwxMqTss* pTss,
                                  CwxMsgBlock const* msg,
                                  int& ret,
                                  CWX_UINT64& ullSid,
                                  char const*& szErrMsg,
                                  char* szErr2K)
{
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get ret
    if (!pTss->m_pReader->getKey(CWX_MQ_RET, ret))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_NO_RET;
    }
    //get sid
    if (!pTss->m_pReader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    //get err
    if (CWX_MQ_SUCCESS != ret)
    {
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_ERR)))
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
int CwxMqPoco::packCommit(CwxMqTss* pTss,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        char const* user,
        char const* passwd,
        char* szErr2K
        )
{
    pTss->m_pWriter->beginPack();
    if (user)
    {
        if (!pTss->m_pWriter->addKeyValue(CWX_MQ_USER, user, strlen(user)))
        {
            if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
        if (passwd)
        {
            if (!pTss->m_pWriter->addKeyValue(CWX_MQ_PASSWD, passwd, strlen(passwd)))
            {
                if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
                return CWX_MQ_INNER_ERR;
            }
        }
    }
    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_COMMIT, uiTaskId, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}
    ///返回值，CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseCommit(CwxMqTss* pTss,
        CwxMsgBlock const* msg,
        char const*& user,
        char const*& passwd,
        char* szErr2K)
{
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get user
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_USER)))
    {
        user = "";
    }
    else
    {
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_PASSWD)))
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
int CwxMqPoco::packCommitReply(CwxMqTss* pTss,
                           CwxMsgBlock*& msg,
                           CWX_UINT32 uiTaskId,
                           int ret,
                           char const* szErrMsg,
                           char* szErr2K)
{
    pTss->m_pWriter->beginPack();
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (CWX_MQ_SUCCESS != ret)
    {

        if (!pTss->m_pWriter->addKeyValue(CWX_MQ_ERR,
            szErrMsg?szErrMsg:"",
            szErrMsg?strlen(szErrMsg):0))
        {
            if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
    }
    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_COMMIT_REPLY, uiTaskId, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseCommitReply(CwxMqTss* pTss,
                            CwxMsgBlock const* msg,
                            int& ret,
                            char const*& szErrMsg,
                            char* szErr2K)
{
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get ret
    if (!pTss->m_pReader->getKey(CWX_MQ_RET, ret))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_NO_RET;
    }
    //get err
    if (CWX_MQ_SUCCESS != ret)
    {
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_ERR)))
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

int CwxMqPoco::packReportData(CwxMqTss* pTss,
                          CwxMsgBlock*& msg,
                          CWX_UINT32 uiTaskId,
                          CWX_UINT64 ullSid,
                          bool      bNewly,
                          char const* subscribe,
                          char const* user,
                          char const* passwd,
                          char* szErr2K)
{
    pTss->m_pWriter->beginPack();
    if (!bNewly)
    {
        if (!pTss->m_pWriter->addKeyValue(CWX_MQ_SID, ullSid))
        {
            if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
    }
    if (subscribe && !pTss->m_pWriter->addKeyValue(CWX_MQ_SUBSCRIBE, subscribe, strlen(subscribe)))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (user && !pTss->m_pWriter->addKeyValue(CWX_MQ_USER, user, strlen(user)))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (passwd && !pTss->m_pWriter->addKeyValue(CWX_MQ_PASSWD, passwd, strlen(passwd)))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }

    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_REPORT, uiTaskId, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseReportData(CwxMqTss* pTss,
                           CwxMsgBlock const* msg,
                           CWX_UINT64& ullSid,
                           bool& bNewly,
                           char const*& subscribe,
                           char const*& user,
                           char const*& passwd,
                           char* szErr2K)
{
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get sid
    if (!pTss->m_pReader->getKey(CWX_MQ_SID, ullSid))
    {
        bNewly = true;
    }
    else
    {
        bNewly = false;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get subscribe
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_SUBSCRIBE)))
    {
        subscribe = "";
    }
    else
    {
        subscribe = pItem->m_szData;
    }
    //get user
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_USER)))
    {
        user = "";
    }
    else
    {
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_PASSWD)))
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
int CwxMqPoco::packReportDataReply(CwxMqTss* pTss,
                               CwxMsgBlock*& msg,
                               CWX_UINT32 uiTaskId,
                               int ret,
                               CWX_UINT64 ullSid,
                               char const* szErrMsg,
                               char* szErr2K)
{
    pTss->m_pWriter->beginPack();
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_ERR,
        szErrMsg?szErrMsg:"",
        szErrMsg?strlen(szErrMsg):0))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_REPORT_REPLY, uiTaskId, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}


///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseReportDataReply(CwxMqTss* pTss,
                                CwxMsgBlock const* msg,
                                int& ret,
                                CWX_UINT64& ullSid,
                                char const*& szErrMsg,
                                char* szErr2K)
{
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get ret
    if (!pTss->m_pReader->getKey(CWX_MQ_RET, ret))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_NO_RET;
    }
    //get sid
    if (!pTss->m_pReader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    //get err
    if (CWX_MQ_SUCCESS != ret)
    {
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_ERR)))
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
int CwxMqPoco::packSyncData(CwxMqTss* pTss,
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
    pTss->m_pWriter->beginPack();
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_DATA, data.m_szData, data.m_uiDataLen, data.m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_GROUP, group))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_TYPE, type))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_ATTR, attr))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_DATA, uiTaskId, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}

int CwxMqPoco::parseSyncData(CwxMqTss* pTss,
                         CwxMsgBlock const* msg,
                         CWX_UINT64& ullSid,
                         CWX_UINT32& uiTimeStamp,
                         CwxKeyValueItem const*& data,
                         CWX_UINT32& group,
                         CWX_UINT32& type,
                         CWX_UINT32& attr,
                         char* szErr2K)
{
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get SID
    if (!pTss->m_pReader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    //get timestamp
    if (!pTss->m_pReader->getKey(CWX_MQ_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_TIMESTAMP);
        return CWX_MQ_NO_TIMESTAMP;
    }
    //get data
    if (!(data=pTss->m_pReader->getKey(CWX_MQ_DATA)))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_DATA);
        return CWX_MQ_NO_KEY_DATA;
    }
    //get group
    if (!pTss->m_pReader->getKey(CWX_MQ_GROUP, group))
    {
        group = 0;
    }
    //get type
    if (!pTss->m_pReader->getKey(CWX_MQ_TYPE, type))
    {
        type = 0;
    }
    //get attr
    if (!pTss->m_pReader->getKey(CWX_MQ_ATTR, attr))
    {
        type = 0;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::packSyncDataReply(CwxMqTss* pTss,
                            CwxMsgBlock*& msg,
                            CWX_UINT32 uiTaskId,
                            CWX_UINT64 ullSid,
                            char* szErr2K)
{
    pTss->m_pWriter->beginPack();
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_DATA_REPLY, uiTaskId, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;
}


int CwxMqPoco::parseSyncDataReply(CwxMqTss* pTss,
                             CwxMsgBlock const* msg,
                             CWX_UINT64& ullSid,
                             char* szErr2K)
{
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get SID
    if (!pTss->m_pReader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    return CWX_MQ_SUCCESS;
}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::packFetchMq(CwxMqTss* pTss,
                       CwxMsgBlock*& msg,
                       bool bBlock,
                       char const* queue_name,
                       char const* user,
                       char const* passwd,
                       char* szErr2K)
{
    pTss->m_pWriter->beginPack();
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_BLOCK, bBlock))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (queue_name && !pTss->m_pWriter->addKeyValue(CWX_MQ_QUEUE, queue_name, strlen(queue_name)))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (user && !pTss->m_pWriter->addKeyValue(CWX_MQ_USER, user, strlen(user)))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (passwd && !pTss->m_pWriter->addKeyValue(CWX_MQ_PASSWD, passwd, strlen(passwd)))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_FETCH_DATA, 0, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;

}

///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseFetchMq(CwxMqTss* pTss,
                        CwxMsgBlock const* msg,
                        bool& bBlock,
                        char const*& queue_name,
                        char const*& user,
                        char const*& passwd,
                        char* szErr2K)
{
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get block
    if (!pTss->m_pReader->getKey(CWX_MQ_BLOCK, bBlock, false))
    {
        bBlock = false;
    }

    CwxKeyValueItem const* pItem = NULL;
    //get queue
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_QUEUE)))
    {
        queue_name = "";
    }
    else
    {
        queue_name = pItem->m_szData;
    }
    //get user
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_USER)))
    {
        user = "";
    }
    else
    {
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_PASSWD)))
    {
        passwd = "";
    }
    else
    {
        passwd = pItem->m_szData;
    }
    return CWX_MQ_SUCCESS;

}

int CwxMqPoco::packFetchMqReply(CwxMqTss* pTss,
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
    pTss->m_pWriter->beginPack();
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if(CWX_MQ_SUCCESS != ret)
    {
        if (!szErrMsg) szErrMsg="";
        if (!pTss->m_pWriter->addKeyValue(CWX_MQ_ERR, szErrMsg, strlen(szErrMsg)))
        {
            if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
            return CWX_MQ_INNER_ERR;
        }
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_DATA, data.m_szData, data.m_uiDataLen, data.m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_GROUP, group))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_TYPE, type))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->addKeyValue(CWX_MQ_ATTR, attr))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    if (!pTss->m_pWriter->pack())
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pWriter->getErrMsg());
        return CWX_MQ_INNER_ERR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_FETCH_DATA_REPLY, 0, pTss->m_pWriter->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", pTss->m_pWriter->getMsgSize());
        return CWX_MQ_INNER_ERR;
    }
    return CWX_MQ_SUCCESS;

}


int CwxMqPoco::parseFetchMqReply(CwxMqTss* pTss,
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
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, pTss->m_pReader->getErrMsg());
        return CWX_MQ_INVALID_MSG;
    }
    //get ret
    if (!pTss->m_pReader->getKey(CWX_MQ_RET, ret))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_NO_RET;
    }
    if (CWX_MQ_SUCCESS != ret)
    {
        //get err
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = pTss->m_pReader->getKey(CWX_MQ_ERR)))
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
    if (!pTss->m_pReader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_NO_SID;
    }
    //get timestamp
    if (!pTss->m_pReader->getKey(CWX_MQ_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_TIMESTAMP);
        return CWX_MQ_NO_TIMESTAMP;
    }
    //get data
    if (!(data=pTss->m_pReader->getKey(CWX_MQ_DATA)))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_DATA);
        return CWX_MQ_NO_KEY_DATA;
    }
    //get group
    if (!pTss->m_pReader->getKey(CWX_MQ_GROUP, group))
    {
        type = 0;
    }
    //get type
    if (!pTss->m_pReader->getKey(CWX_MQ_TYPE, type))
    {
        type = 0;
    }
    //get attr
    if (!pTss->m_pReader->getKey(CWX_MQ_ATTR, attr))
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
