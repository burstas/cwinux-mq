#include "CwxSocket.h"
#include "cwx_msg_header.h"
#include "cwx_mq_poco.h"
#include "CwxINetAddr.h"
#include "CwxSockStream.h"
#include "CwxSockConnector.h"
using namespace cwinux;

int main(int argc ,char** argv)
{
    if (6 != argc)
    {
        printf("using %s  ip port queue user passwd\n", argv[0]);
        return 0;
    }
    CwxSockStream  stream;
    CwxINetAddr  addr(strtoul(argv[2], NULL, 0), argv[1]);
    CwxSockConnector conn;
    if (0 != conn.connect(stream, addr))
    {
        printf("failure to connect ip/port:%s/%s, errno=%d\n", argv[1], argv[2], errno);
        return 0;
    }
    //report
    CWX_PG_WRITER* writer=cwx_pg_writer_create(2048);
    CwxMsgHead head;
    CwxMsgBlock* msg=NULL;
    CWX_PG_READER* reader = cwx_pg_reader_create();
    int ret;
    CWX_UINT64 ullSid;
    CWX_KEY_VALUE_ITEM_S const*  data;
    char const* szErrMsg=NULL;
    CWX_UINT32 uiTimestamp;
    CWX_UINT32 group;
    CWX_UINT32 type;
    CWX_UINT32 attr;
    char szErr2K[2048];
    char szSendBuf[2048];
    CWX_UINT32 uiSendLen = 2048;
    if (CWX_MQ_ERR_SUCCESS != cwx_mq_pack_fetch_mq(writer,
        szSendBuf,
        &uiSendLen,
        1,
        argv[3],
        argv[4],
        argv[5],
        0,
        szErr2K))
    {
        printf("failure to pack fetch msg, err=%s", szErr2K);
        return 0;
    }
    while(1)
    {
        if (uiSendLen != CwxSocket::write_n(stream.getHandle(), szSendBuf, uiSendLen))
        {
            printf("failure to send  a message, errno=%d\n", errno);
            break;
        }
        //recv msg
        if (0 >= CwxSocket::read(stream.getHandle(), head, msg))
        {
            printf("failure to read the message reply, errno=%d\n", errno);
        }
        if (CWX_MQ_MSG_TYPE_FETCH_DATA_REPLY == head.getMsgType())
        {
            if (CWX_MQ_ERR_SUCCESS != cwx_mq_parse_fetch_mq_reply(reader,
                msg->rd_ptr(),
                msg->length(),
                &ret,
                &szErrMsg,
                &ullSid,
                &uiTimestamp,
                &data,
                &group,
                &type,
                &attr,
                szErr2K))
            {
                printf("failure to parse fetch reply, err=%s\n", szErr2K);
                break;
            }
            if (CWX_MQ_ERR_SUCCESS != ret)
            {
                printf("failure to fetch a mq message, err=%s\n", szErrMsg);
                break;
            }
            printf("fetch a message, ret=%d, sid=%"PRIx64" timestamp=%u group=%u type=%u attr=%d data=%s\n", ret, ullSid, uiTimestamp, group, type, attr, data->m_szData);
        }
        else if (head.isKeepAlive(false))
        {
            head.setMsgType(head.getMsgType() + 1);
            char const* szHead = head.toNet();
            if (CwxMsgHead::MSG_HEAD_LEN != CwxSocket::write_n(stream.getHandle(), szHead, CwxMsgHead::MSG_HEAD_LEN))
            {
                printf("Failure to send keep-alive reply, errno=%d\n", errno);
                break;
            }
            printf("success to parse report reply, ret=%d, err=%s\n", ret, szErrMsg);
        }
        else
        {
            printf("recv a unknow msg type, msg_attr=%u, msg_type=%u, data_len=%u\n", head.getAttr(), head.getMsgType(), head.getDataLen());
            break;
        }
        CwxMsgBlockAlloc::free(msg);
    }
    stream.close();
    return 0;
}
