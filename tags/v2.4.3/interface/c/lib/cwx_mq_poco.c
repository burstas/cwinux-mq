#include "cwx_mq_poco.h"
#include "cwx_md5.h"
#include "cwx_crc32.h"
#include <zlib.h>

#ifdef __cplusplus
extern "C" {
#endif

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
    head.m_uiTaskId = uiTaskId;
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
    char const* user,
    char const* passwd,
    int zip, char* szErr2K)
  {
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key(writer,
      CWX_MQ_KEY_D,
      data->m_szData,
      data->m_uiDataLen,
      data->m_bKeyValue))
    {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (user && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_U, user))) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (passwd && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_P, passwd))) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (zip) {
      unsigned long ulDstLen = *buf_len - CWX_MSG_HEAD_LEN;
      if (Z_OK == compress2((unsigned char*) buf + CWX_MSG_HEAD_LEN,
        &ulDstLen,
        (const unsigned char*) cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer),
        Z_DEFAULT_COMPRESSION))
      {
        CWX_MSG_HEADER_S head;
        head.m_ucVersion = 0;
        head.m_ucAttr = CWX_MSG_ATTR_COMPRESS;
        head.m_uiTaskId = uiTaskId;
        head.m_unMsgType = CWX_MQ_MSG_TYPE_MQ;
        head.m_uiDataLen = ulDstLen;
        cwx_msg_pack_head(&head, buf);
      }
      *buf_len = CWX_MSG_HEAD_LEN + ulDstLen;
      return CWX_MQ_ERR_SUCCESS;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_MQ,
      uiTaskId,
      buf,
      buf_len,
      cwx_pg_writer_get_msg(writer),
      cwx_pg_writer_get_msg_size(writer)))
    {
      if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
        *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
      return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_mq(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    struct CWX_KEY_VALUE_ITEM_S const** data,
    char const** user,
    char const** passwd,
    char* szErr2K)
  {
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
      return CWX_MQ_ERR_ERROR;
    }
    //get data
    *data = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_D, 0);
    if (!(*data)) {
      if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_D);
      return CWX_MQ_ERR_ERROR;
    }
    if ((*data)->m_bKeyValue) {
      if (!cwx_pg_is_valid((*data)->m_szData, (*data)->m_uiDataLen)) {
        if (szErr2K)
          snprintf(szErr2K, 2047,
          "key[%s] is key/value, but it's format is not valid..",
          CWX_MQ_KEY_D);
        return CWX_MQ_ERR_ERROR;
      }
    }
    struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
    //get user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_U, 0))) {
      *user = 0;
    } else {
      *user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_P, 0))) {
      *passwd = 0;
    } else {
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
    if (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_RET, ret)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != ret) {

      if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_ERR,
        szErrMsg ? szErrMsg : ""))
      {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
      }
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_MQ_REPLY,
      uiTaskId,
      buf,
      buf_len,
      cwx_pg_writer_get_msg(writer),
      cwx_pg_writer_get_msg_size(writer)))
    {
      if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
        *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
      return CWX_MQ_ERR_ERROR;
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
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
      return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_RET, ret, 0)) {
      if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_RET);
      return CWX_MQ_ERR_ERROR;
    }
    //get sid
    if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0)) {
      if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_SID);
      return CWX_MQ_ERR_ERROR;
    }
    //get err
    if (CWX_MQ_ERR_SUCCESS != *ret) {
      struct CWX_KEY_VALUE_ITEM_S const* pItem = NULL;
      if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_ERR, 0))) {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_ERR);
        return CWX_MQ_ERR_ERROR;
      }
      *szErrMsg = pItem->m_szData;
    } else {
      *szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_pack_sync_report(struct CWX_PG_WRITER * writer,
    CWX_UINT32 uiTaskId,
    char* buf,
    CWX_UINT32* buf_len,
    CWX_UINT64 ullSid,
    int bNewly,
    CWX_UINT32 uiChunk,
    char const* source,
    char const* user,
    char const* passwd,
    int zip,
    char* szErr2K)
  {
    cwx_pg_writer_begin_pack(writer);
    if (!bNewly) {
      if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid)) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
      }
    }
    if (uiChunk && (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_CHUNK, uiChunk))) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
    }
    if (source && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_SOURCE, source))) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
    }
    if (user && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_U, user))) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (passwd && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_P, passwd))) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
    }
    if (zip && (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_ZIP, zip))) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_SYNC_REPORT,
      uiTaskId,
      buf,
      buf_len,
      cwx_pg_writer_get_msg(writer),
      cwx_pg_writer_get_msg_size(writer)))
    {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
          *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_sync_report(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    CWX_UINT64* ullSid,
    int* bNewly,
    CWX_UINT32* uiChunk,
    char const** source,
    char const** user,
    char const** passwd,
    int* zip,
    char* szErr2K)
  {
      if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_ERROR;
      }
      //get sid
      if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0)) {
        *bNewly = 1;
      } else {
        *bNewly = 0;
      }
      if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_CHUNK, uiChunk, 0)) {
        *uiChunk = 0;
      }
      struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
      //get subscribe
      if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_SOURCE, 0))) {
        *source = "";
      } else {
        *source = pItem->m_szData;
      }
      //get user
      if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_U, 0))) {
        *user = "";
      } else {
        *user = pItem->m_szData;
      }
      //get passwd
      if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_P, 0))) {
        *passwd = "";
      } else {
        *passwd = pItem->m_szData;
      }
      //get zip
      if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_ZIP, zip, 0)) {
        *zip = 0;
      }
      return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_pack_sync_report_reply(struct CWX_PG_WRITER * writer,
    CWX_UINT32 uiTaskId,
    char* buf,
    CWX_UINT32* buf_len,
    CWX_UINT64 ullSession,
    char const* szErrMsg,
    char* szErr2K)
  {
      cwx_pg_writer_begin_pack(writer);
      if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_SESSION, ullSession)) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
      }
      if (0 != cwx_pg_writer_pack(writer)) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
      }
      if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_SYNC_REPORT_REPLY,
        uiTaskId,
        buf,
        buf_len,
        cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer)))
      {
          if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
            *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
          return CWX_MQ_ERR_ERROR;
      }
      return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_sync_report_reply(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    CWX_UINT64* ullSession,
    char const** szErrMsg,
    char* szErr2K)
  {
      if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
        return CWX_MQ_ERR_ERROR;
      }
      //get session
      if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SESSION, ullSession, 0)) {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_SESSION);
        return CWX_MQ_ERR_ERROR;
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
    int zip,
    CWX_UINT64 ullSeq,
    char* szErr2K)
  {
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_add_key_uint32(writer, CWX_MQ_KEY_T, uiTimeStamp)) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_add_key(writer, CWX_MQ_KEY_D, data->m_szData,
      data->m_uiDataLen, data->m_bKeyValue))
    {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    CWX_MSG_HEADER_S head;
    head.m_ucVersion = 0;
    head.m_ucAttr = 0;
    head.m_uiTaskId = uiTaskId;
    head.m_unMsgType = CWX_MQ_MSG_TYPE_SYNC_DATA;
    if (zip) {
      unsigned long ulDstLen = *buf_len - CWX_MSG_HEAD_LEN - sizeof(ullSeq);
      if (Z_OK == compress2((unsigned char*) buf + CWX_MSG_HEAD_LEN + sizeof(ullSeq), &ulDstLen,
        (const unsigned char*) cwx_pg_writer_get_msg(writer),
        cwx_pg_writer_get_msg_size(writer), Z_DEFAULT_COMPRESSION))
      {
        head.m_ucAttr = CWX_MSG_ATTR_COMPRESS;
        head.m_uiDataLen = ulDstLen + sizeof(ullSeq);
        cwx_msg_pack_head(&head, buf);
        cwx_mq_set_seq(buf + CWX_MSG_HEAD_LEN, ullSeq);
        *buf_len = CWX_MSG_HEAD_LEN + ulDstLen + sizeof(ullSeq);
        return CWX_MQ_ERR_SUCCESS;
      }
    }
    head.m_uiDataLen = cwx_pg_writer_get_msg_size(writer) + sizeof(ullSeq);
    cwx_msg_pack_head(&head, buf);
    cwx_mq_set_seq(buf + CWX_MSG_HEAD_LEN, ullSeq);
    memcpy(buf + CWX_MSG_HEAD_LEN + sizeof(ullSeq), cwx_pg_writer_get_msg(writer), cwx_pg_writer_get_msg_size(writer));
    *buf_len = CWX_MSG_HEAD_LEN + sizeof(ullSeq) + cwx_pg_writer_get_msg_size(writer);
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_sync_data(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    CWX_UINT64* ullSid,
    CWX_UINT32* uiTimeStamp,
    struct CWX_KEY_VALUE_ITEM_S const** data,
    char* szErr2K)
  {
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
      return CWX_MQ_ERR_ERROR;
    }
    //get SID
    if (0 == cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0)) {
      if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_SID);
      return CWX_MQ_ERR_ERROR;
    }
    //get timestamp
    if (0 == cwx_pg_reader_get_uint32(reader, CWX_MQ_KEY_T, uiTimeStamp, 0)) {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_T);
        return CWX_MQ_ERR_ERROR;
    }
    //get data
    *data = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_D, 0);
    if (!(*data)) {
      if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_D);
      return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_pack_sync_data_reply(struct CWX_PG_WRITER * writer,
    CWX_UINT32 uiTaskId,
    char* buf,
    CWX_UINT32* buf_len,
    CWX_UINT64 ullSeq,
    char* szErr2K)
  {
    char seq_buf[9];
    cwx_mq_set_seq(seq_buf, ullSeq);
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_SYNC_DATA_REPLY, uiTaskId, buf,
      buf_len, seq_buf, sizeof(ullSeq)))
    {
      if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
        *buf_len, CWX_MSG_HEAD_LEN + sizeof(ullSeq));
      return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_sync_data_reply(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    CWX_UINT64* ullSeq,
    char* szErr2K)
  {
    *ullSeq = cwx_mq_get_seq(msg);
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
    if (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_B, bBlock)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (queue_name && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_Q, queue_name))) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
    }
    if (user && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_U, user))) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (passwd && (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_P, passwd))) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_FETCH_DATA, 0, buf, buf_len,
      cwx_pg_writer_get_msg(writer), cwx_pg_writer_get_msg_size(writer))) 
    {
      if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
        *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
      return CWX_MQ_ERR_ERROR;
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
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
      return CWX_MQ_ERR_ERROR;
    }
    //get block
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_B, bBlock, 0)) {
      bBlock = 0;
    }
    struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
    //get queue
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_Q, 0))) {
      *queue_name = "";
    } else {
      *queue_name = pItem->m_szData;
    }
    //get user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_U, 0))) {
      *user = "";
    } else {
      *user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_P, 0))) {
      *passwd = "";
    } else {
      *passwd = pItem->m_szData;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_pack_fetch_mq_reply(struct CWX_PG_WRITER * writer,
    char* buf,
    CWX_UINT32* buf_len,
    int ret,
    char const* szErrMsg,
    struct CWX_KEY_VALUE_ITEM_S const* data,
    char* szErr2K)
  {
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_RET, ret)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != ret) {
      if (!szErrMsg) szErrMsg = "";
      if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_ERR, szErrMsg)) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
      }
    }
    if (0 != cwx_pg_writer_add_key(writer, CWX_MQ_KEY_D, data->m_szData,
      data->m_uiDataLen, data->m_bKeyValue)) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_FETCH_DATA_REPLY, 0, buf, buf_len,
      cwx_pg_writer_get_msg(writer), cwx_pg_writer_get_msg_size(writer))) {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
          *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_fetch_mq_reply(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    int* ret,
    char const** szErrMsg,
    struct CWX_KEY_VALUE_ITEM_S const** data,
    char* szErr2K)
  {
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
      return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_RET, ret, 0)) {
      if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_RET);
      return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != *ret) {
      //get err
      struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
      if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_ERR, 0))) {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_ERR);
        return CWX_MQ_ERR_ERROR;
      }
      *szErrMsg = pItem->m_szData;
    } else {
      *szErrMsg = "";
    }
    //get data
    *data = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_D, 0);
    if (!(*data)) {
      if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_D);
      return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_pack_create_queue(struct CWX_PG_WRITER * writer,
    char* buf,
    CWX_UINT32* buf_len,
    char const* name,
    char const* user,
    char const* passwd,
    char const* auth_user,
    char const* auth_passwd,
    CWX_UINT64 ullSid,
    char* szErr2K)
  {
    cwx_pg_writer_begin_pack(writer);
    //add name
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_NAME, name)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    //add user
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_U, user)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    //add passwd
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_P, passwd)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    //add auth_user
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_AUTH_USER, auth_user)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    //add auth_passwd
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_AUTH_PASSWD,
      auth_passwd)) {
        if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
        return CWX_MQ_ERR_ERROR;
    }
    //add sid
    if (0 != cwx_pg_writer_add_key_uint64(writer, CWX_MQ_KEY_SID, ullSid)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_CREATE_QUEUE, 0, buf, buf_len,
      cwx_pg_writer_get_msg(writer), cwx_pg_writer_get_msg_size(writer))) {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
          *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_create_queue(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    char const** name,
    char const** user,
    char const** passwd,
    char const** auth_user,
    char const** auth_passwd,
    CWX_UINT64* ullSid,
    char* szErr2K)
  {
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
      return CWX_MQ_ERR_ERROR;
    }

    struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
    //get name
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_NAME, 0))) {
      *name = "";
    } else {
      *name = pItem->m_szData;
    }
    //get user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_U, 0))) {
      *user = "";
    } else {
      *user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_P, 0))) {
      *passwd = "";
    } else {
      *passwd = pItem->m_szData;
    }
    //get auth_user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_AUTH_USER, 0))) {
      *auth_user = "";
    } else {
      *auth_user = pItem->m_szData;
    }
    //get auth_passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_AUTH_PASSWD, 0))) {
      *auth_passwd = "";
    } else {
      *auth_passwd = pItem->m_szData;
    }
    //get sid
    if (!cwx_pg_reader_get_uint64(reader, CWX_MQ_KEY_SID, ullSid, 0)) {
      ullSid = 0;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_pack_create_queue_reply(struct CWX_PG_WRITER * writer,
    char* buf,
    CWX_UINT32* buf_len,
    int ret,
    char const* szErrMsg,
    char* szErr2K)
  {
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_RET, ret)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_ERR, szErrMsg)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_CREATE_QUEUE_REPLY, 0, buf, buf_len,
      cwx_pg_writer_get_msg(writer), cwx_pg_writer_get_msg_size(writer))) {
        if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
          *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_create_queue_reply(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    int* ret,
    char const** szErrMsg,
    char* szErr2K)
  {
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
      return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_RET, ret, 0)) {
      if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_RET);
      return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != *ret) {
      //get err
      struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
      if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_ERR, 0))) {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_ERR);
        return CWX_MQ_ERR_ERROR;
      }
      *szErrMsg = pItem->m_szData;
    } else {
      *szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_pack_del_queue(struct CWX_PG_WRITER * writer,
    char* buf,
    CWX_UINT32* buf_len,
    char const* name,
    char const* user,
    char const* passwd,
    char const* auth_user,
    char const* auth_passwd,
    char* szErr2K)
  {
    cwx_pg_writer_begin_pack(writer);
    //add name
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_NAME, name)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    //add user
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_U, user)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    //add passwd
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_P, passwd)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    //add auth_user
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_AUTH_USER, auth_user)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    //add auth_passwd
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_AUTH_PASSWD, auth_passwd))
    {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_DEL_QUEUE, 0, buf, buf_len,
      cwx_pg_writer_get_msg(writer), cwx_pg_writer_get_msg_size(writer)))
    {
      if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
        *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
      return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_del_queue(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    char const** name,
    char const** user,
    char const** passwd,
    char const** auth_user,
    char const** auth_passwd,
    char* szErr2K)
  {
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
      if (szErr2K)
        strcpy(szErr2K, cwx_pg_reader_get_error(reader));
      return CWX_MQ_ERR_ERROR;
    }

    struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
    //get name
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_NAME, 0))) {
      *name = "";
    } else {
      *name = pItem->m_szData;
    }
    //get user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_U, 0))) {
      *user = "";
    } else {
      *user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_P, 0))) {
      *passwd = "";
    } else {
      *passwd = pItem->m_szData;
    }
    //get auth_user
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_AUTH_USER, 0))) {
      *auth_user = "";
    } else {
      *auth_user = pItem->m_szData;
    }
    //get auth_passwd
    if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_AUTH_PASSWD, 0))) {
      *auth_passwd = "";
    } else {
      *auth_passwd = pItem->m_szData;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_pack_del_queue_reply(struct CWX_PG_WRITER * writer,
    char* buf,
    CWX_UINT32* buf_len,
    int ret,
    char const* szErrMsg,
    char* szErr2K)
  {
    cwx_pg_writer_begin_pack(writer);
    if (0 != cwx_pg_writer_add_key_int32(writer, CWX_MQ_KEY_RET, ret)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_add_key_str(writer, CWX_MQ_KEY_ERR, szErrMsg)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_pg_writer_pack(writer)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_writer_get_error(writer));
      return CWX_MQ_ERR_ERROR;
    }
    if (0 != cwx_mq_pack_msg(CWX_MQ_MSG_TYPE_DEL_QUEUE_REPLY, 0, buf, buf_len,
      cwx_pg_writer_get_msg(writer), cwx_pg_writer_get_msg_size(writer)))
    {
      if (szErr2K) snprintf(szErr2K, 2047, "msg buf is too small[%u], size[%u] is needed.",
        *buf_len, CWX_MSG_HEAD_LEN + cwx_pg_writer_get_msg_size(writer));
      return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  int cwx_mq_parse_del_queue_reply(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    int* ret,
    char const** szErrMsg,
    char* szErr2K)
  {
    if (0 != cwx_pg_reader_unpack(reader, msg, msg_len, 0, 1)) {
      if (szErr2K) strcpy(szErr2K, cwx_pg_reader_get_error(reader));
      return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (0 == cwx_pg_reader_get_int32(reader, CWX_MQ_KEY_RET, ret, 0)) {
      if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_RET);
      return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != *ret) {
      //get err
      struct CWX_KEY_VALUE_ITEM_S const* pItem = 0;
      if (!(pItem = cwx_pg_reader_get_key(reader, CWX_MQ_KEY_ERR, 0))) {
        if (szErr2K) snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_KEY_ERR);
        return CWX_MQ_ERR_ERROR;
      }
      *szErrMsg = pItem->m_szData;
    } else {
      *szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
  }

  ///设置数据同步包的seq号
  void cwx_mq_set_seq(char* szBuf, CWX_UINT64 ullSeq) {
    CWX_UINT32 byte4 = (CWX_UINT32) (ullSeq >> 32);
    byte4 = CWX_HTONL(byte4);
    memcpy(szBuf, &byte4, 4);
    byte4 = (CWX_UINT32) (ullSeq & 0xFFFFFFFF);
    byte4 = CWX_HTONL(byte4);
    memcpy(szBuf + 4, &byte4, 4);

  }
  ///获取数据同步包的seq号
  CWX_UINT64 cwx_mq_get_seq(char const* szBuf) {
    CWX_UINT64 ullSeq = 0;
    CWX_UINT32 byte4;
    memcpy(&byte4, szBuf, 4);
    ullSeq = CWX_NTOHL(byte4);
    memcpy(&byte4, szBuf + 4, 4);
    ullSeq <<= 32;
    ullSeq += CWX_NTOHL(byte4);
    return ullSeq;
  }

#ifdef __cplusplus
}
#endif

