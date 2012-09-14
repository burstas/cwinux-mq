#include "CwxMqPoco.h"
#include "CwxZlib.h"

CwxPackageWriter* CwxMqPoco::m_pWriter =NULL;
///初始化协议。返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::init(char* ){
    char const* data="This is binlog sync record.";
    if (!m_pWriter) m_pWriter = new CwxPackageWriter();
    m_pWriter->beginPack();
    m_pWriter->addKeyValue(CWX_MQ_D, data, strlen(data), false);
    m_pWriter->pack();
    return CWX_MQ_ERR_SUCCESS;
}

///释放协议。
void CwxMqPoco::destory(){
    if (m_pWriter) delete m_pWriter;
    m_pWriter = NULL;
}

///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packRecvData(CwxPackageWriter* writer,
                        CwxMsgBlock*& msg,
                        CWX_UINT32 uiTaskId,
                        CwxKeyValueItem const& data,
                        CWX_UINT32 group,
                        char const* user,
                        char const* passwd,
                        char const* sign,
                        bool        zip,
                        char* szErr2K
                        )
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_D, data.m_szData, data.m_uiDataLen, data.m_bKeyValue)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_G, group)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (user && !writer->addKeyValue(CWX_MQ_U, user, strlen(user))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (passwd && !writer->addKeyValue(CWX_MQ_P, passwd, strlen(passwd))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (sign){
        if (strcmp(sign, CWX_MQ_CRC32) == 0){//CRC32签名
            CWX_UINT32 uiCrc32 = CwxCrc32::value(writer->getMsg(), writer->getMsgSize());
            if (!writer->addKeyValue(CWX_MQ_CRC32, (char*)&uiCrc32, sizeof(uiCrc32))){
                if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
                return CWX_MQ_ERR_ERROR;
            }
        }else if (strcmp(sign, CWX_MQ_MD5) == 0){//md5签名
            CwxMd5 md5;
            unsigned char szMd5[16];
            md5.update((unsigned char*)writer->getMsg(), writer->getMsgSize());
            md5.final(szMd5);
            if (!writer->addKeyValue(CWX_MQ_MD5, (char*)szMd5, 16)){
                if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
                return CWX_MQ_ERR_ERROR;
            }
        }
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_DATA, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::malloc(CwxMsgHead::MSG_HEAD_LEN + writer->getMsgSize() + CWX_MQ_ZIP_EXTRA_BUF);
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    unsigned long ulDestLen = writer->getMsgSize() + CWX_MQ_ZIP_EXTRA_BUF;
    if (zip){
        if (!CwxZlib::zip((unsigned char*)msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN, ulDestLen, (unsigned char const*)writer->getMsg(), writer->getMsgSize())){
            zip = false;
        }
    }
    if (zip){
        head.addAttr(CwxMsgHead::ATTR_COMPRESS);
        head.setDataLen(ulDestLen);
        memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
        msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + ulDestLen);
    }else{
        memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
        memcpy(msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN, writer->getMsg(), writer->getMsgSize());
        msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + writer->getMsgSize());        
    }
    return CWX_MQ_ERR_SUCCESS;
}


///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvData(CwxPackageReader* reader,
                         CwxMsgBlock const* msg,
                         CwxKeyValueItem const*& data,
                         CWX_UINT32& group,
                         char const*& user,
                         char const*& passwd,
                         char* szErr2K)
{
    return parseRecvData(reader,
        msg->rd_ptr(),
        msg->length(),
        data,
        group,
        user,
        passwd,
        szErr2K);
}

///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvData(CwxPackageReader* reader,
        char const* msg,
        CWX_UINT32  msg_len,
        CwxKeyValueItem const*& data,
        CWX_UINT32& group,
        char const*& user,
        char const*& passwd,
        char* szErr2K)
{
    if (!reader->unpack(msg, msg_len, false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get data
    data = reader->getKey(CWX_MQ_D);
    if (!data){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_D);
        return CWX_MQ_ERR_ERROR;
    }
    if (data->m_bKeyValue){
        if (!CwxPackage::isValidPackage(data->m_szData, data->m_uiDataLen)){
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "key[%s] is key/value, but it's format is not valid..", CWX_MQ_D);
            return CWX_MQ_ERR_ERROR;
        }
    }
    //get group
    if (!reader->getKey(CWX_MQ_G, group)){
        group = 0;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get user
    if (!(pItem = reader->getKey(CWX_MQ_U))){
        user = "";
    }else{
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = reader->getKey(CWX_MQ_P))){
        passwd = "";
    }else{
        passwd = pItem->m_szData;
    }
    //get crc32
    if ((pItem = reader->getKey(CWX_MQ_CRC32))){
        CWX_UINT32 uiOrgCrc32 = 0;
        memcpy(&uiOrgCrc32, pItem->m_szData, sizeof(uiOrgCrc32));
        CWX_UINT32 uiCrc32 = CwxCrc32::value(msg, pItem->m_szKey - msg - CwxPackage::getKeyOffset());
        if (uiCrc32 != uiOrgCrc32){
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "CRC32 signture error. recv signture:%x, local signture:%x", uiOrgCrc32, uiCrc32);
            return CWX_MQ_ERR_ERROR;
        }
    }
    //get md5
    if ((pItem = reader->getKey(CWX_MQ_MD5))){
        unsigned char szMd5[16];
        CwxMd5 md5;
        md5.update((unsigned char*)msg, pItem->m_szKey - msg - CwxPackage::getKeyOffset());
        md5.final(szMd5);
        if (memcmp(szMd5, pItem->m_szData, 16) != 0){
            if (szErr2K){
                char szTmp1[33];
                char szTmp2[33];
                CWX_UINT32 i=0;
                for (i=0; i<16; i++){
                    sprintf(szTmp1 + i*2, "%2.2x", (unsigned char)pItem->m_szData[i]);
                    sprintf(szTmp2 + i*2, "%2.2x", szMd5[i]);
                }
                CwxCommon::snprintf(szErr2K, 2047, "MD5 signture error. recv signture:%s, local signture:%s", szTmp1, szTmp2);
            }
            return CWX_MQ_ERR_ERROR;
        }
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packRecvDataReply(CwxPackageWriter* writer,
                             CwxMsgBlock*& msg,
                             CWX_UINT32 uiTaskId,
                             int ret,
                             CWX_UINT64 ullSid,
                             char const* szErrMsg,
                             char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, ret)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != ret){
        if (!writer->addKeyValue(CWX_MQ_ERR,
            szErrMsg?szErrMsg:"",
            szErrMsg?strlen(szErrMsg):0))
        {
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_ERR_ERROR;
        }
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_DATA_REPLY, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvDataReply(CwxPackageReader* reader,
                                  CwxMsgBlock const* msg,
                                  int& ret,
                                  CWX_UINT64& ullSid,
                                  char const*& szErrMsg,
                                  char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_ERR_ERROR;
    }
    //get sid
    if (!reader->getKey(CWX_MQ_SID, ullSid)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_ERR_ERROR;
    }
    //get err
    if (CWX_MQ_ERR_SUCCESS != ret){
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = reader->getKey(CWX_MQ_ERR))){
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
            return CWX_MQ_ERR_ERROR;
        }
        szErrMsg = pItem->m_szData;
    }else{
        szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packCommit(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        char const* user,
        char const* passwd,
        char* szErr2K
        )
{
    writer->beginPack();
    if (user){
        if (!writer->addKeyValue(CWX_MQ_U, user, strlen(user))){
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_ERR_ERROR;
        }
        if (passwd){
            if (!writer->addKeyValue(CWX_MQ_P, passwd, strlen(passwd))){
                if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
                return CWX_MQ_ERR_ERROR;
            }
        }
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_COMMIT, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}
    ///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseCommit(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        char const*& user,
        char const*& passwd,
        char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get user
    if (!(pItem = reader->getKey(CWX_MQ_U))){
        user = "";
    }else{
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = reader->getKey(CWX_MQ_P))){
        passwd = "";
    }else{
        passwd = pItem->m_szData;
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packCommitReply(CwxPackageWriter* writer,
                           CwxMsgBlock*& msg,
                           CWX_UINT32 uiTaskId,
                           int ret,
                           char const* szErrMsg,
                           char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, ret)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != ret){
        if (!writer->addKeyValue(CWX_MQ_ERR,
            szErrMsg?szErrMsg:"",
            szErrMsg?strlen(szErrMsg):0))
        {
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_ERR_ERROR;
        }
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_RECV_COMMIT_REPLY, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseCommitReply(CwxPackageReader* reader,
                            CwxMsgBlock const* msg,
                            int& ret,
                            char const*& szErrMsg,
                            char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_ERR_ERROR;
    }
    //get err
    if (CWX_MQ_ERR_SUCCESS != ret){
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = reader->getKey(CWX_MQ_ERR))){
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
            return CWX_MQ_ERR_ERROR;
        }
        szErrMsg = pItem->m_szData;
    }else{
        szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
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
                          char const* sign,
                          bool  zip,
                          char* szErr2K)
{
    writer->beginPack();
    if (!bNewly){
        if (!writer->addKeyValue(CWX_MQ_SID, ullSid)){
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_ERR_ERROR;
        }
    }
    if (uiChunkSize && !writer->addKeyValue(CWX_MQ_CHUNK, uiChunkSize)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (subscribe && !writer->addKeyValue(CWX_MQ_SUBSCRIBE, subscribe, strlen(subscribe))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (user && !writer->addKeyValue(CWX_MQ_U, user, strlen(user))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (passwd && !writer->addKeyValue(CWX_MQ_P, passwd, strlen(passwd))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (sign){
        if ((strcmp(sign, CWX_MQ_CRC32) == 0) || (strcmp(sign, CWX_MQ_MD5)==0)){
            if (!writer->addKeyValue(CWX_MQ_SIGN, sign, strlen(sign))){
                if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
                return CWX_MQ_ERR_ERROR;
            }
        }
    }
    if (zip){
        if (!writer->addKeyValue(CWX_MQ_ZIP,zip)){
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_ERR_ERROR;
        }
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_REPORT, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseReportData(CwxPackageReader* reader,
                           CwxMsgBlock const* msg,
                           CWX_UINT64& ullSid,
                           bool& bNewly,
                           CWX_UINT32&  uiChunkSize,
                           char const*& subscribe,
                           char const*& user,
                           char const*& passwd,
                           char const*& sign,
                           bool&        zip,
                           char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get sid
    if (!reader->getKey(CWX_MQ_SID, ullSid)){
        bNewly = true;
    }else{
        bNewly = false;
    }
    if (!reader->getKey(CWX_MQ_CHUNK, uiChunkSize)){
        uiChunkSize = 0;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get subscribe
    if (!(pItem = reader->getKey(CWX_MQ_SUBSCRIBE))){
        subscribe = "";
    }else{
        subscribe = pItem->m_szData;
    }
    //get user
    if (!(pItem = reader->getKey(CWX_MQ_U))){
        user = "";
    }else{
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = reader->getKey(CWX_MQ_P))){
        passwd = "";
    }else{
        passwd = pItem->m_szData;
    }
    //get sign
    if (!(pItem = reader->getKey(CWX_MQ_SIGN))){
        sign = "";
    }else{
        if (strcmp(pItem->m_szData, CWX_MQ_CRC32)==0){
            sign = CWX_MQ_CRC32;
        }else if (strcmp(pItem->m_szData, CWX_MQ_MD5)==0){
            sign = CWX_MQ_MD5;
        }else{
            sign = "";
        }
    }
    CWX_UINT32 uiValue = 0;
    if (!reader->getKey(CWX_MQ_ZIP, uiValue)){
        zip = false;
    }else{
        zip=uiValue;
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packReportDataReply(CwxPackageWriter* writer,
                               CwxMsgBlock*& msg,
                               CWX_UINT32 uiTaskId,
                               CWX_UINT64 ullSession,
                               char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_SESSION, ullSession)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_REPORT_REPLY, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}


///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseReportDataReply(CwxPackageReader* reader,
                                CwxMsgBlock const* msg,
                                CWX_UINT64& ullSession,
                                char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get sid
    if (!reader->getKey(CWX_MQ_SESSION, ullSession)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SESSION);
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}


///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packReportNewConn(CwxPackageWriter* writer,
                             CwxMsgBlock*& msg,
                             CWX_UINT32 uiTaskId,
                             CWX_UINT64 ullSession,
                             char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_SESSION, ullSession)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_SESSION_REPORT, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;

}
///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseReportNewConn(CwxPackageReader* reader,
                              CwxMsgBlock const* msg,
                              CWX_UINT64& ullSession,
                              char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get session
    if (!reader->getKey(CWX_MQ_SESSION, ullSession)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SESSION);
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packSyncData(CwxPackageWriter* writer,
                        CwxMsgBlock*& msg,
                        CWX_UINT32 uiTaskId,
                        CWX_UINT64 ullSid,
                        CWX_UINT32 uiTimeStamp,
                        CwxKeyValueItem const& data,
                        CWX_UINT32 group,
                        char const* sign,
                        bool       zip,
                        CWX_UINT64 ullSeq,
                        char* szErr2K)
{
    writer->beginPack();
    int ret = packSyncDataItem(writer,
        ullSid,
        uiTimeStamp,
        data,
        group,
        sign,
        szErr2K);
    if (CWX_MQ_ERR_SUCCESS != ret) return ret;

    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_DATA, uiTaskId, writer->getMsgSize() + sizeof(ullSeq));

    msg = CwxMsgBlockAlloc::malloc(CwxMsgHead::MSG_HEAD_LEN + writer->getMsgSize() + CWX_MQ_ZIP_EXTRA_BUF +  + sizeof(ullSeq));
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    unsigned long ulDestLen = writer->getMsgSize() + CWX_MQ_ZIP_EXTRA_BUF;
    if (zip){
        if (!CwxZlib::zip((unsigned char*)msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN + sizeof(ullSeq), 
            ulDestLen,
            (unsigned char const*)writer->getMsg(),
            writer->getMsgSize()))
        {
            zip = false;
        }
    }
    if (zip){
        head.addAttr(CwxMsgHead::ATTR_COMPRESS);
        head.setDataLen(ulDestLen + sizeof(ullSeq));
        memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
        msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + ulDestLen + sizeof(ullSeq));
    }else{
        memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
        memcpy(msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN + sizeof(ullSeq), writer->getMsg(), writer->getMsgSize());
        msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + writer->getMsgSize() + sizeof(ullSeq));        
    }
    //seq seq
    setSeq(msg->rd_ptr() + CwxMsgHead::MSG_HEAD_LEN, ullSeq);
    return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::packSyncDataItem(CwxPackageWriter* writer,
                            CWX_UINT64 ullSid,
                            CWX_UINT32 uiTimeStamp,
                            CwxKeyValueItem const& data,
                            CWX_UINT32 group,
                            char const* sign,
                            char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_T, uiTimeStamp))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_D, data.m_szData, data.m_uiDataLen, data.m_bKeyValue))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_G, group))
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (sign)
    {
        if (strcmp(sign, CWX_MQ_CRC32) == 0)//CRC32签名
        {
            CWX_UINT32 uiCrc32 = CwxCrc32::value(writer->getMsg(), writer->getMsgSize());
            if (!writer->addKeyValue(CWX_MQ_CRC32, (char*)&uiCrc32, sizeof(uiCrc32)))
            {
                if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
                return CWX_MQ_ERR_ERROR;
            }
        }
        else if (strcmp(sign, CWX_MQ_MD5) == 0)//md5签名
        {
            CwxMd5 md5;
            unsigned char szMd5[16];
            md5.update((char unsigned*)writer->getMsg(), writer->getMsgSize());
            md5.final(szMd5);
            if (!writer->addKeyValue(CWX_MQ_MD5, (char*)szMd5, 16))
            {
                if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
                return CWX_MQ_ERR_ERROR;
            }
        }
    }
    writer->pack();
    return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::packMultiSyncData(
                                 CWX_UINT32 uiTaskId,
                                 char const* szData,
                                 CWX_UINT32 uiDataLen,
                                 CwxMsgBlock*& msg,
                                 CWX_UINT64 ullSeq,
                                 bool  zip,
                                 char* szErr2K
                                 )
{
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_DATA_CHUNK, uiTaskId, uiDataLen+sizeof(ullSeq));
    msg = CwxMsgBlockAlloc::malloc(CwxMsgHead::MSG_HEAD_LEN + uiDataLen + CWX_MQ_ZIP_EXTRA_BUF + sizeof(ullSeq));
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", uiDataLen);
        return CWX_MQ_ERR_ERROR;
    }
    unsigned long ulDestLen = uiDataLen + CWX_MQ_ZIP_EXTRA_BUF;
    if (zip){
        if (!CwxZlib::zip((unsigned char*)(msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN) + sizeof(ullSeq),
            ulDestLen,
            (unsigned char const*)szData,
            uiDataLen))
        {
            zip = false;
        }
    }
    if (zip){
        head.addAttr(CwxMsgHead::ATTR_COMPRESS);
        head.setDataLen(ulDestLen + sizeof(ullSeq));
        memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
        msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + ulDestLen + sizeof(ullSeq));
    }else{
        memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
        memcpy(msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN + sizeof(ullSeq), szData, uiDataLen);
        msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + uiDataLen + sizeof(ullSeq));
    }
    //seq seq
    setSeq(msg->rd_ptr() + CwxMsgHead::MSG_HEAD_LEN, ullSeq);
    return CWX_MQ_ERR_SUCCESS;

}


int CwxMqPoco::parseSyncData(CwxPackageReader* reader,
                         CwxMsgBlock const* msg,
                         CWX_UINT64& ullSid,
                         CWX_UINT32& uiTimeStamp,
                         CwxKeyValueItem const*& data,
                         CWX_UINT32& group,
                         char* szErr2K)
{
    return parseSyncData(reader,
        msg->rd_ptr(),
        msg->length(),
        ullSid,
        uiTimeStamp,
        data,
        group,
        szErr2K);
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseSyncData(CwxPackageReader* reader,
                         char const* szData,
                         CWX_UINT32 uiDataLen,
                         CWX_UINT64& ullSid,
                         CWX_UINT32& uiTimeStamp,
                         CwxKeyValueItem const*& data,
                         CWX_UINT32& group,
                         char* szErr2K)
{
    if (!reader->unpack(szData, uiDataLen, false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get SID
    if (!reader->getKey(CWX_MQ_SID, ullSid))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_ERR_ERROR;
    }
    //get timestamp
    if (!reader->getKey(CWX_MQ_T, uiTimeStamp))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_T);
        return CWX_MQ_ERR_ERROR;
    }
    //get data
    if (!(data=reader->getKey(CWX_MQ_D)))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_D);
        return CWX_MQ_ERR_ERROR;
    }
    //get group
    if (!reader->getKey(CWX_MQ_G, group))
    {
        group = 0;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get crc32
    if ((pItem = reader->getKey(CWX_MQ_CRC32)))
    {
        CWX_UINT32 uiOrgCrc32 = 0;
        memcpy(&uiOrgCrc32, pItem->m_szData, sizeof(uiOrgCrc32));
        CWX_UINT32 uiCrc32 = CwxCrc32::value(szData, pItem->m_szKey - szData - CwxPackage::getKeyOffset());
        if (uiCrc32 != uiOrgCrc32)
        {
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "CRC32 signture error. recv signture:%x, local signture:%x", uiOrgCrc32, uiCrc32);
            return CWX_MQ_ERR_ERROR;
        }
    }
    //get md5
    if ((pItem = reader->getKey(CWX_MQ_MD5)))
    {
        unsigned char szMd5[16];
        CwxMd5 md5;
        md5.update((unsigned char*)szData, pItem->m_szKey - szData - CwxPackage::getKeyOffset());
        md5.final(szMd5);
        if (memcmp(szMd5, pItem->m_szData, 16) != 0)
        {
            if (szErr2K)
            {
                char szTmp1[33];
                char szTmp2[33];
                CWX_UINT32 i=0;
                for (i=0; i<16; i++)
                {
                    sprintf(szTmp1 + i*2, "%2.2x", (unsigned char)pItem->m_szData[i]);
                    sprintf(szTmp2 + i*2, "%2.2x", szMd5[i]);
                }
                CwxCommon::snprintf(szErr2K, 2047, "MD5 signture error. recv signture:%s, local signture:%s", szTmp1, szTmp2);
            }
            return CWX_MQ_ERR_ERROR;
        }

    }
    return CWX_MQ_ERR_SUCCESS;

}


///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packSyncDataReply(CwxPackageWriter* writer,
                            CwxMsgBlock*& msg,
                            CWX_UINT32 uiTaskId,
                            CWX_UINT16 unMsgType,
                            CWX_UINT64 ullSeq,
                            char* szErr2K)
{
    char szBuf[9];
    setSeq(szBuf, ullSeq);
    CwxMsgHead head(0, 0, unMsgType, uiTaskId, sizeof(ullSeq));
    msg = CwxMsgBlockAlloc::pack(head, szBuf, sizeof(ullSeq));
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}


int CwxMqPoco::parseSyncDataReply(CwxPackageReader* ,
                             CwxMsgBlock const* msg,
                             CWX_UINT64& ullSeq,
                             char* szErr2K)
{
    if (msg->length() < sizeof(ullSeq)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Data Length[%u] is too less, no seq id", msg->length());
        return CWX_MQ_ERR_ERROR;
    }
    ullSeq = getSeq(msg->rd_ptr());
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packFetchMq(CwxPackageWriter* writer,
                       CwxMsgBlock*& msg,
                       bool bBlock,
                       char const* queue_name,
                       char const* user,
                       char const* passwd,
                       char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_B, bBlock)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (queue_name && !writer->addKeyValue(CWX_MQ_Q, queue_name, strlen(queue_name))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (user && !writer->addKeyValue(CWX_MQ_U, user, strlen(user))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (passwd && !writer->addKeyValue(CWX_MQ_P, passwd, strlen(passwd))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_FETCH_DATA, 0, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;

}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseFetchMq(CwxPackageReader* reader,
                        CwxMsgBlock const* msg,
                        bool& bBlock,
                        char const*& queue_name,
                        char const*& user,
                        char const*& passwd,
                        char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get block
    CWX_UINT32 uiValue = 0;
    if (!reader->getKey(CWX_MQ_B, uiValue)){
        bBlock = false;
    }else{
        bBlock = uiValue;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get queue
    if (!(pItem = reader->getKey(CWX_MQ_Q))){
        queue_name = "";
    }else{
        queue_name = pItem->m_szData;
    }
    //get user
    if (!(pItem = reader->getKey(CWX_MQ_U))){
        user = "";
    }else{
        user = pItem->m_szData;
    }
    //get passwd
    if (!(pItem = reader->getKey(CWX_MQ_P))){
        passwd = "";
    }else{
        passwd = pItem->m_szData;
    }
    return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::packFetchMqReply(CwxPackageWriter* writer,
                            CwxMsgBlock*& msg,
                            int  ret,
                            char const* szErrMsg,
                            CWX_UINT64 ullSid,
                            CWX_UINT32 uiTimeStamp,
                            CwxKeyValueItem const& data,
                            CWX_UINT32 group,
                            char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, ret)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if(CWX_MQ_ERR_SUCCESS != ret){
        if (!szErrMsg) szErrMsg="";
        if (!writer->addKeyValue(CWX_MQ_ERR, szErrMsg, strlen(szErrMsg))){
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_ERR_ERROR;
        }
    }
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_T, uiTimeStamp)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_D, data.m_szData, data.m_uiDataLen, data.m_bKeyValue)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_G, group)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_FETCH_DATA_REPLY, 0, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}


int CwxMqPoco::parseFetchMqReply(CwxPackageReader* reader,
                    CwxMsgBlock const* msg,
                    int&  ret,
                    char const*& szErrMsg,
                    CWX_UINT64& ullSid,
                    CWX_UINT32& uiTimeStamp,
                    CwxKeyValueItem const*& data,
                    CWX_UINT32& group,
                    char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != ret){
        //get err
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = reader->getKey(CWX_MQ_ERR))){
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
            return CWX_MQ_ERR_ERROR;
        }
        szErrMsg = pItem->m_szData;
    }else{
        szErrMsg = "";
    }
    //get SID
    if (!reader->getKey(CWX_MQ_SID, ullSid)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_SID);
        return CWX_MQ_ERR_ERROR;
    }
    //get timestamp
    if (!reader->getKey(CWX_MQ_T, uiTimeStamp)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_T);
        return CWX_MQ_ERR_ERROR;
    }
    //get data
    if (!(data=reader->getKey(CWX_MQ_D))){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_D);
        return CWX_MQ_ERR_ERROR;
    }
    //get group
    if (!reader->getKey(CWX_MQ_G, group)){
        group = 0;
    }
    return CWX_MQ_ERR_SUCCESS;
}


///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseCreateQueue(CwxPackageReader* reader,
                            CwxMsgBlock const* msg,
                            char const*& name,
                            char const*& user,
                            char const*& passwd,
                            char const*& scribe,
                            char const*& auth_user,
                            char const*& auth_passwd,
                            CWX_UINT64&  ullSid,///< 0：当前最大值，若小于当前最小值，则采用当前最小sid值
                            char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true))
    {
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get name
    pItem = reader->getKey(CWX_MQ_NAME);
    if (!pItem){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv package.", CWX_MQ_NAME);
        return CWX_MQ_ERR_ERROR;
    }
    name = pItem->m_szData;
    //get user
    pItem = reader->getKey(CWX_MQ_U);
    if (!pItem){
        user = "";
    }else{
        user = pItem->m_szData;
    }
    //get passwd
    pItem = reader->getKey(CWX_MQ_P);
    if (!pItem){
        passwd = "";
    }else{
        passwd = pItem->m_szData;
    }
    //get subscribe
    pItem = reader->getKey(CWX_MQ_SUBSCRIBE);
    if (!pItem){
        scribe = "";
    }else{
        scribe = pItem->m_szData;
    }
    //get auth_user
    pItem = reader->getKey(CWX_MQ_AUTH_USER);
    if (!pItem){
        auth_user = "";
    }else{
        auth_user = pItem->m_szData;
    }
    //get auth_passwd
    pItem = reader->getKey(CWX_MQ_AUTH_PASSWD);
    if (!pItem){
        auth_passwd = "";
    }else{
        auth_passwd = pItem->m_szData;
    }
    //get sid
    if (!reader->getKey(CWX_MQ_SID, ullSid))
        ullSid = 0;
    return CWX_MQ_ERR_SUCCESS;
}
///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packCreateQueue(CwxPackageWriter* writer,
                           CwxMsgBlock*& msg,
                           char const* name,
                           char const* user,
                           char const* passwd,
                           char const* scribe,
                           char const* auth_user,
                           char const* auth_passwd,
                           CWX_UINT64  ullSid,///< 0：当前最大值，若小于当前最小值，则采用当前最小sid值
                           char* szErr2K)
{
    writer->beginPack();
    if (!name){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Name is null.");
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_NAME, name, strlen(name))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (user && !writer->addKeyValue(CWX_MQ_U, user, strlen(user))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (passwd && !writer->addKeyValue(CWX_MQ_P, passwd, strlen(passwd))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (scribe && !writer->addKeyValue(CWX_MQ_SUBSCRIBE, scribe, strlen(scribe))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (auth_user && !writer->addKeyValue(CWX_MQ_AUTH_USER, auth_user, strlen(auth_user))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (auth_passwd && !writer->addKeyValue(CWX_MQ_AUTH_PASSWD, auth_passwd, strlen(auth_passwd))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->pack())
    {
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_CREATE_QUEUE, 0, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg)
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseCreateQueueReply(CwxPackageReader* reader,
                                 CwxMsgBlock const* msg,
                                 int&  ret,
                                 char const*& szErrMsg,
                                 char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != ret){
        //get err
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = reader->getKey(CWX_MQ_ERR))){
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
            return CWX_MQ_ERR_ERROR;
        }
        szErrMsg = pItem->m_szData;
    }else{
        szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packCreateQueueReply(CwxPackageWriter* writer,
                                CwxMsgBlock*& msg,
                                int  ret,
                                char const* szErrMsg,
                                char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, ret)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }

    if (CWX_MQ_ERR_SUCCESS != ret){
        if (!szErrMsg) szErrMsg="";
        if (!writer->addKeyValue(CWX_MQ_ERR, szErrMsg, strlen(szErrMsg))){
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_ERR_ERROR;
        }
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_CREATE_QUEUE_REPLY, 0, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseDelQueue(CwxPackageReader* reader,
                         CwxMsgBlock const* msg,
                         char const*& name,
                         char const*& user,
                         char const*& passwd,
                         char const*& auth_user,
                         char const*& auth_passwd,
                         char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxKeyValueItem const* pItem = NULL;
    //get name
    pItem = reader->getKey(CWX_MQ_NAME);
    if (!pItem){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv package.", CWX_MQ_NAME);
        return CWX_MQ_ERR_ERROR;
    }
    name = pItem->m_szData;
    //get user
    pItem = reader->getKey(CWX_MQ_U);
    if (!pItem){
        user = "";
    }else{
        user = pItem->m_szData;
    }
    //get passwd
    pItem = reader->getKey(CWX_MQ_P);
    if (!pItem){
        passwd = "";
    }else{
        passwd = pItem->m_szData;
    }
    //get auth_user
    pItem = reader->getKey(CWX_MQ_AUTH_USER);
    if (!pItem){
        auth_user = "";
    }else{
        auth_user = pItem->m_szData;
    }
    //get auth_passwd
    pItem = reader->getKey(CWX_MQ_AUTH_PASSWD);
    if (!pItem){
        auth_passwd = "";
    }else{
        auth_passwd = pItem->m_szData;
    }
    return CWX_MQ_ERR_SUCCESS;
}
///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packDelQueue(CwxPackageWriter* writer,
                        CwxMsgBlock*& msg,
                        char const* name,
                        char const* user,
                        char const* passwd,
                        char const* auth_user,
                        char const* auth_passwd,
                        char* szErr2K)
{
    writer->beginPack();
    if (!name){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Name is null.");
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_NAME, name, strlen(name))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (user && !writer->addKeyValue(CWX_MQ_U, user, strlen(user))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (passwd && !writer->addKeyValue(CWX_MQ_P, passwd, strlen(passwd))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (auth_user && !writer->addKeyValue(CWX_MQ_AUTH_USER, auth_user, strlen(auth_user))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (auth_passwd && !writer->addKeyValue(CWX_MQ_AUTH_PASSWD, auth_passwd, strlen(auth_passwd))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_DEL_QUEUE, 0, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}


///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseDelQueueReply(CwxPackageReader* reader,
                              CwxMsgBlock const* msg,
                              int&  ret,
                              char const*& szErrMsg,
                              char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_ERR_ERROR;
    }
    if (CWX_MQ_ERR_SUCCESS != ret){
        //get err
        CwxKeyValueItem const* pItem = NULL;
        if (!(pItem = reader->getKey(CWX_MQ_ERR))){
            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
            return CWX_MQ_ERR_ERROR;
        }
        szErrMsg = pItem->m_szData;
    }else{
        szErrMsg = "";
    }
    return CWX_MQ_ERR_SUCCESS;
}
///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packDelQueueReply(CwxPackageWriter* writer,
                             CwxMsgBlock*& msg,
                             int  ret,
                             char const* szErrMsg,
                             char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, ret)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }

    if (CWX_MQ_ERR_SUCCESS != ret){
        if (!szErrMsg) szErrMsg="";
        if (!writer->addKeyValue(CWX_MQ_ERR, szErrMsg, strlen(szErrMsg))){
            if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
            return CWX_MQ_ERR_ERROR;
        }
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_DEL_QUEUE_REPLY, 0, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;
}

///返回值：UNISTOR_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packSyncErr(CwxPackageWriter* writer,
                             CwxMsgBlock*& msg,
                             CWX_UINT32 uiTaskId,
                             int ret,
                             char const* szErrMsg,
                             char* szErr2K)
{
    writer->beginPack();
    if (!writer->addKeyValue(CWX_MQ_RET, strlen(CWX_MQ_RET), ret)){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->addKeyValue(CWX_MQ_ERR, strlen(CWX_MQ_ERR), szErrMsg, strlen(szErrMsg))){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    if (!writer->pack()){
        if (szErr2K) strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    CwxMsgHead head(0, 0, MSG_TYPE_SYNC_ERR, uiTaskId, writer->getMsgSize());
    msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
    if (!msg){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u", writer->getMsgSize());
        return CWX_MQ_ERR_ERROR;
    }
    return CWX_MQ_ERR_SUCCESS;

}
///返回值：UNISTOR_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseSyncErr(CwxPackageReader* reader,
                              CwxMsgBlock const* msg,
                              int& ret,
                              char const*& szErrMsg,
                              char* szErr2K)
{
    if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)){
        if (szErr2K) strcpy(szErr2K, reader->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
    //get ret
    if (!reader->getKey(CWX_MQ_RET, ret)){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_RET);
        return CWX_MQ_ERR_ERROR;
    }
    //get err
    CwxKeyValueItem const* pItem = NULL;
    if (!(pItem = reader->getKey(CWX_MQ_ERR))){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_ERR);
        return CWX_MQ_ERR_ERROR;
    }
    szErrMsg = pItem->m_szData;
    return CWX_MQ_ERR_SUCCESS;
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
    CwxMqSubscribeItem/*group*/ rule;
    string strGroup=strSubscribe;
    subscribe.m_bAll = false;
    subscribe.m_subscribe.clear();
    CwxCommon::trim(strGroup);
    if (!strGroup.length() || !strcmp("*",strGroup.c_str())){
        subscribe.m_bAll = true;
        return true;
    }
    //split strSubscribe by [;]
    CwxCommon::split(strSubscribe, groups, ';');
    iter_group = groups.begin();
    while(iter_group != groups.end()){
        strGroup = *iter_group;
        CwxCommon::trim(strGroup);
        if (strGroup.length()){//it must be group_express
            if (!parseSubsribeExpress(strGroup, rule, strErrMsg)) return false;
            if (rule.m_bAll){
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
        express.m_uiModIndex = strtoul(strSubsribeExpress.c_str(), NULL, 10);
        express.m_uiModBase = strtoul(strSubsribeExpress.c_str() + strSubsribeExpress.find('%') + 1, NULL, 10);
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
        while(iter != items.end()){
            strValue = *iter;
            CwxCommon::trim(strValue);
            if (strValue.find('-') != string::npos){//it's a range
                range.first = strtoul(strValue.c_str(), NULL, 10);
                range.second = strtoul(strValue.c_str() + strValue.find('-') + 1, NULL, 10);
            }else{
                range.first = range.second = strtoul(strValue.c_str(), NULL, 10);
            }
            if (range.first > range.second){
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
