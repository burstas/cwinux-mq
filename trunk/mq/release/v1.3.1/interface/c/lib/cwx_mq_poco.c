#ifdef __cplusplus
extern "C" {
#endif
#include "cwx_mq_poco.h"

static int cwx_mq_pack_msg(CWX_UINT16 unMsgType,
                           CWX_UINT32 uiTaskId,
                           char* buf,
                           CWX_UINT32* buf_len,
                           char const* szData,
                           CWX_UINT32 data_len)
{
    CWX_MSG_HEADER_S head;
    head.m_ucVersion = 0;
    head.m_ucAttr = 0;
    head.m_uiTaskId =uiTaskId;
    head.m_unMsgType = unMsgType;
    head.m_uiDataLen = data_len;

    if (*buf_len < CWX_MSG_HEAD_LEN + data_len) return -1;
    cwx_msg_pack_head(&head, buf);
    memcpy(buf + CWX_MSG_HEAD_LEN, szData, data_len);
    *buf_len = CWX_MSG_HEAD_LEN + data_len;
    return 0;
}

int cwx_mq_pack_mq(struct CWX_PG_WRITER * writer,
                   CWX_UINT32 uiTaskId,
                   char* buf,
                   CWX_UINT32* buf_len,
                   struct CWX_KEY_VALUE_ITEM_S const* data,
                   CWX_UINT32 group,
                   CWX_UINT32 type,
                   CWX_UINT32 attr,
                   char const* user,
                   char const* passwd,
                   char* szErr2K
                   )
{
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key(writer, CWX_MQ_KEY_DATA, data->m_szData, data->m_uiDataLen, data->m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_GROUP, group))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_TYPE, type))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_ATTR, attr))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (user && (0!=cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_USER, user)))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (passwd && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_PASSWD, passwd)))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if ( 0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }

    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_MQ,
        uiTaskId,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_parse_mq(struct CWX_PG_READER* reader,
                    char const* msg,
                    CWX_UINT32  msg_len,
                    struct CWX_KEY_VALUE_ITEM_S const** data,
                    CWX_UINT32* group,
                    CWX_UINT32* type,
                    CWX_UINT32* attr,
                    char const** user,
                    char const** passwd,
                    char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    //get data
    *data = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_DATA, 0);
    if (!(*data))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_DATA);
        return CWX_MQ_ERR_NO_KEY_DATA;
    }
    if ((*data)->m_bKeyValue)
    {
        if (!cwx_pg_is_valid((*data)->m_szData, (*data)->m_uiDataLen))
        {
            if (szErr2K) snprintf(szErr2K, 2047, "key[%s] is key/value, but it's format is not valid..", CWX_MQ_KEY_DATA);
            return CWX_MQ_ERR_INVALID_DATA_KV;
        }
    }
    //get group
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_GROUP, group, 0))
    {
        *group = 0;
    }
    if (CWX_MQ_GROUP_SYNC == *group)
    {
        if (szErr2K) snprintf(szErr2K, 2047, "mq's group can't be [%x], it's binlog sync group.", CWX_MQ_GROUP_SYNC);
        return CWX_MQ_ERR_INVALID_BINLOG_TYPE;
    }
    //get type
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_TYPE, type, 0))
    {
        *type = 0;
    }
    //get attr
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_ATTR, attr, 0))
    {
        *attr = 0;
    }
    struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
    //get user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_USER, 0)))
    {
        *user = 0;
    }
    else
    {
        *user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_PASSWD, 0)))
    {
        *passwd = 0;
    }
    else
    {
        *passwd = pItem->m_szData;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_pack_mq_reply(struct CWX_PG_WRITER * writer,
                         CWX_UINT32 uiTaskId,
                         char* buf,
                         CWX_UINT32* buf_len,
                         int ret,
                         CWX_UINT64 ullSid,
                         char const* szErrMsg,
                         char* szErr2K)
{
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (CWX_MQ_ERR_SUCCESS != ret)
    {

        if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_ERR, szErrMsg?szErrMsg:""))
        {
            if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
            return CWX_MQ_ERR_INNER_ERR;
        }
    }
    if (0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_MQ_REPLY,
        uiTaskId,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_parse_mq_reply(struct CWX_PG_READER* reader,
                          char const* msg,
                          CWX_UINT32 msg_len,
                          int* ret,
                          CWX_UINT64* ullSid,
                          char const** szErrMsg,
                          char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    //get ret
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_RET, ret, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_RET);
        return CWX_MQ_ERR_NO_RET;
    }
    //get sid
    if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_SID);
        return CWX_MQ_ERR_NO_SID;
    }
    //get err
    if (CWX_MQ_ERR_SUCCESS != *ret)
    {
        struct CWX_KEY_VALUE_ITEM_S const* pItem = NULL;
        if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_ERR, 0)))
        {
            if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_ERR);
            return CWX_MQ_ERR_NO_ERR;
        }
        *szErrMsg = pItem->m_szData;
    }
    else
    {
        *szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_pack_commit(struct CWX_PG_WRITER * writer,
                       CWX_UINT32 uiTaskId,
                       char* buf,
                       CWX_UINT32* buf_len,
                       char const* user,
                       char const* passwd,
                       char* szErr2K)
{
    cwx_pg_writer_begin_pack(writer);
    if (user)
    {
        if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_USER, user))
        {
            if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
            return CWX_MQ_ERR_INNER_ERR;
        }
        if (passwd)
        {
            if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_PASSWD, passwd))
            {
                if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
                return CWX_MQ_ERR_INNER_ERR;
            }
        }
    }
    if (0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_MQ_COMMIT,
        uiTaskId,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;

    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_parse_commit(struct CWX_PG_READER* reader,
                        char const* msg,
                        CWX_UINT32 msg_len,
                        char const** user,
                        char const** passwd,
                        char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
    //get user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_USER, 0)))
    {
        *user = "";
    }
    else
    {
        *user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_PASSWD, 0)))
    {
        *passwd = "";
    }
    else
    {
        *passwd = pItem->m_szData;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_pack_commit_reply(struct CWX_PG_WRITER * writer,
                             CWX_UINT32 uiTaskId,
                             char* buf,
                             CWX_UINT32* buf_len,
                             int ret,
                             char const* szErrMsg,
                             char* szErr2K)
{
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (CWX_MQ_ERR_SUCCESS != ret)
    {

        if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_ERR, szErrMsg?szErrMsg:""))
        {
            if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
            return CWX_MQ_ERR_INNER_ERR;
        }
    }
    if (0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_MQ_COMMIT_REPLY,
        uiTaskId,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_parse_commit_reply(struct CWX_PG_READER* reader,
                              char const* msg,
                              CWX_UINT32 msg_len,
                              int* ret,
                              char const** szErrMsg,
                              char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    //get ret
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_RET, ret, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_RET);
        return CWX_MQ_ERR_NO_RET;
    }
    //get err
    if (CWX_MQ_ERR_SUCCESS != *ret)
    {
        struct CWX_KEY_VALUE_ITEM_S const* pItem = NULL;
        if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_ERR, 0)))
        {
            if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_ERR);
            return CWX_MQ_ERR_NO_ERR;
        }
        *szErrMsg = pItem->m_szData;
    }
    else
    {
        *szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_pack_sync_report(struct CWX_PG_WRITER * writer,
                            CWX_UINT32 uiTaskId,
                            char* buf,
                            CWX_UINT32* buf_len,
                            CWX_UINT64 ullSid,
                            int      bNewly,
                            char const* subscribe,
                            char const* user,
                            char const* passwd,
                            char* szErr2K)
{
    cwx_pg_writer_begin_pack(writer);
    if (!bNewly)
    {
        if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid))
        {
            if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
            return CWX_MQ_ERR_INNER_ERR;
        }
    }
    if (subscribe && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_SUBSCRIBE, subscribe)))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (user && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_USER, user)))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (passwd && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_PASSWD, passwd)))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }

    if (0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_SYNC_REPORT,
        uiTaskId,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_parse_sync_report(struct CWX_PG_READER* reader,
                             char const* msg,
                             CWX_UINT32 msg_len,
                             CWX_UINT64* ullSid,
                             int*       bNewly,
                             char const** subscribe,
                             char const** user,
                             char const** passwd,
                             char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    //get sid
    if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0))
    {
        *bNewly = 1;
    }
    else
    {
        *bNewly = 0;
    }
    struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
    //get subscribe
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_SUBSCRIBE, 0)))
    {
        *subscribe = "";
    }
    else
    {
        *subscribe = pItem->m_szData;
    }
    //get user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_USER, 0)))
    {
        *user = "";
    }
    else
    {
        *user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_PASSWD, 0)))
    {
        *passwd = "";
    }
    else
    {
        *passwd = pItem->m_szData;
    }

    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_pack_sync_report_reply(struct CWX_PG_WRITER * writer,
                                  CWX_UINT32 uiTaskId,
                                  char* buf,
                                  CWX_UINT32* buf_len,
                                  int ret,
                                  CWX_UINT64 ullSid,
                                  char const* szErrMsg,
                                  char* szErr2K)
{
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_ERR, szErrMsg?szErrMsg:""))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_SYNC_REPORT_REPLY,
        uiTaskId,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_parse_sync_report_reply(struct CWX_PG_READER* reader,
                                   char const* msg,
                                   CWX_UINT32 msg_len,
                                   int* ret,
                                   CWX_UINT64* ullSid,
                                   char const** szErrMsg,
                                   char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    //get ret
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_RET, ret, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_RET);
        return CWX_MQ_ERR_NO_RET;
    }
    //get sid
    if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_SID);
        return CWX_MQ_ERR_NO_SID;
    }
    //get err
    if (CWX_MQ_ERR_SUCCESS != *ret)
    {
        struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
        if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_ERR, 0)))
        {
            if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_ERR);
            return CWX_MQ_ERR_NO_ERR;
        }
        *szErrMsg = pItem->m_szData;
    }
    else
    {
        *szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_pack_sync_data(struct CWX_PG_WRITER * writer,
                          CWX_UINT32 uiTaskId,
                          char* buf,
                          CWX_UINT32* buf_len,
                          CWX_UINT64 ullSid,
                          CWX_UINT32 uiTimeStamp,
                          struct CWX_KEY_VALUE_ITEM_S const* data,
                          CWX_UINT32 group,
                          CWX_UINT32 type,
                          CWX_UINT32 attr,
                          char* szErr2K)
{
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key(writer, CWX_MQ_KEY_DATA, data->m_szData, data->m_uiDataLen, data->m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_GROUP, group))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_TYPE, type))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_ATTR, attr))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_SYNC_DATA,
        uiTaskId,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_parse_sync_data(struct CWX_PG_READER* reader,
                           char const* msg,
                           CWX_UINT32 msg_len,
                           CWX_UINT64* ullSid,
                           CWX_UINT32* uiTimeStamp,
                           struct CWX_KEY_VALUE_ITEM_S const** data,
                           CWX_UINT32* group,
                           CWX_UINT32* type,
                           CWX_UINT32* attr,
                           char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    //get SID
    if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_SID);
        return CWX_MQ_ERR_NO_SID;
    }
    //get timestamp
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_TIMESTAMP, uiTimeStamp, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_TIMESTAMP);
        return CWX_MQ_ERR_NO_TIMESTAMP;
    }
    //get data
    *data=cwx_pg_reader_get_key(reader, CWX_MQ_KEY_DATA, 0);
    if (!(*data))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_DATA);
        return CWX_MQ_ERR_NO_KEY_DATA;
    }
    //get group
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_GROUP, group, 0))
    {
        *group = 0;
    }
    //get type
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_TYPE, type, 0))
    {
        *type = 0;
    }
    //get attr
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_ATTR, attr, 0))
    {
        *attr = 0;
    }
    return CWX_MQ_ERR_SUCCESS;
}


int cwx_mq_pack_sync_data_reply(struct CWX_PG_WRITER * writer,
                          CWX_UINT32 uiTaskId,
                          char* buf,
                          CWX_UINT32* buf_len,
                          CWX_UINT64 ullSid,
                          char* szErr2K)
{
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_SYNC_DATA_REPLY,
        uiTaskId,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_parse_sync_data_reply(struct CWX_PG_READER* reader,
                           char const* msg,
                           CWX_UINT32 msg_len,
                           CWX_UINT64* ullSid,
                           char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    //get SID
    if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_SID);
        return CWX_MQ_ERR_NO_SID;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_pack_fetch_mq(struct CWX_PG_WRITER * writer,
                         char* buf,
                         CWX_UINT32* buf_len,
                         int bBlock,
                         char const* queue_name,
                         char const* user,
                         char const* passwd,
                         char* szErr2K)
{
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_BLOCK, bBlock))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (queue_name && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_QUEUE, queue_name)))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (user && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_USER, user)))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (passwd && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_PASSWD, passwd)))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_FETCH_DATA,
        0,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    return CWX_MQ_ERR_SUCCESS;

}

int cwx_mq_parse_fetch_mq(struct CWX_PG_READER* reader,
                          char const* msg,
                          CWX_UINT32 msg_len,
                          int* bBlock,
                          char const** queue_name,
                          char const** user,
                          char const** passwd,
                          char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    //get block
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_BLOCK, bBlock, 0))
    {
        bBlock = 0;
    }

    struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
    //get queue
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_QUEUE, 0)))
    {
        *queue_name = "";
    }
    else
    {
        *queue_name = pItem->m_szData;
    }
    //get user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_USER, 0)))
    {
        *user = "";
    }
    else
    {
        *user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_PASSWD, 0)))
    {
        *passwd = "";
    }
    else
    {
        *passwd = pItem->m_szData;
    }
    return CWX_MQ_ERR_SUCCESS;
}


int cwx_mq_pack_fetch_mq_reply(struct CWX_PG_WRITER * writer,
                               char* buf,
                               CWX_UINT32* buf_len,
                               int  ret,
                               char const* szErrMsg,
                               CWX_UINT64 ullSid,
                               CWX_UINT32 uiTimeStamp,
                               struct CWX_KEY_VALUE_ITEM_S const* data,
                               CWX_UINT32 group,
                               CWX_UINT32 type,
                               CWX_UINT32 attr,
                               char* szErr2K)
{
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_int32(writer,CWX_MQ_KEY_RET, ret))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if(CWX_MQ_ERR_SUCCESS != ret)
    {
        if (!szErrMsg) szErrMsg="";
        if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_ERR, szErrMsg))
        {
            if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
            return CWX_MQ_ERR_INNER_ERR;
        }
    }
    if (0 !=cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_TIMESTAMP, uiTimeStamp))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key(writer, CWX_MQ_KEY_DATA, data->m_szData, data->m_uiDataLen, data->m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_GROUP, group))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_TYPE, type))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_ATTR, attr))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_pg_writer_pack(writer))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_FETCH_DATA_REPLY,
        0,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, 
            CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_INNER_ERR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int cwx_mq_parse_fetch_mq_reply(struct CWX_PG_READER* reader,
                                char const* msg,
                                CWX_UINT32 msg_len,
                                int*  ret,
                                char const** szErrMsg,
                                CWX_UINT64* ullSid,
                                CWX_UINT32* uiTimeStamp,
                                struct CWX_KEY_VALUE_ITEM_S const** data,
                                CWX_UINT32* group,
                                CWX_UINT32* type,
                                CWX_UINT32* attr,
                                char* szErr2K)
{
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1))
    {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_INVALID_MSG;
    }
    //get ret
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_RET, ret, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_RET);
        return CWX_MQ_ERR_NO_RET;
    }
    if (CWX_MQ_ERR_SUCCESS != *ret)
    {
        //get err
        struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
        if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_ERR, 0)))
        {
            if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_ERR);
            return CWX_MQ_ERR_NO_ERR;
        }
        *szErrMsg = pItem->m_szData;
    }
    else
    {
        *szErrMsg = "";
    }
    //get SID
    if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_SID);
        return CWX_MQ_ERR_NO_SID;
    }
    //get timestamp
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_TIMESTAMP, uiTimeStamp, 0))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_TIMESTAMP);
        return CWX_MQ_ERR_NO_TIMESTAMP;
    }
    //get data
    *data=cwx_pg_reader_get_key(reader, CWX_MQ_KEY_DATA, 0);
    if (!(*data))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_DATA);
        return CWX_MQ_ERR_NO_KEY_DATA;
    }
    //get group
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_GROUP, group, 0))
    {
        *group = 0;
    }
    //get type
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_TYPE, type, 0))
    {
        *type = 0;
    }
    //get attr
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_ATTR, attr, 0))
    {
        *attr = 0;
    }
    return CWX_MQ_ERR_SUCCESS;
}


#ifdef __cplusplus
}
#endif

