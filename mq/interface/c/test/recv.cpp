#include "CwxSocket.h"
#include "cwx_msg_header.h"
#include "cwx_mq_poco.h"
#include "CwxINetAddr.h"
#include "CwxSockStream.h"
#include "CwxSockConnector.h"
using namespace cwinux;

int main(int argc ,char** argv)
{
    if (7 != argc)
    {
        printf("using %s  ip port user passwd sid subscribe\n", argv[0]);
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
    char buf[4096];
    CWX_UINT32 len=4096;
    if (CWX_MQ_ERR_SUCCESS != cwx_mq_pack_sync_report(writer,
        0,
        buf,
        &len,
        strtoull(argv[5], NULL, 0),
        0,
        0,
        argv[6],
        argv[3],
        argv[4],
        "",
        false,
        buf))
    {
        printf("failure to pack report package, err=%s\n", buf);
        return 0;
    }
    if (len != CwxSocket::write_n(stream.getHandle(), buf, len))
    {
        printf("failure to send report, err=%d\n", errno);
        return 0;
    }
    CwxMsgHead head;
    CwxMsgBlock* msg=NULL;
    CWX_PG_READER* reader = cwx_pg_reader_create();
    int ret;
    CWX_UINT64 ullSid;
    CWX_UINT32 uiTimestamp;
    CWX_KEY_VALUE_ITEM_S const* data;
    CWX_UINT32 group;
    CWX_UINT32 type;
    CWX_UINT32 attr;
    char const* szErrMsg=NULL;
    char szErr2K[2048];
    while(1)
    {
        fflush(NULL);
        if (0>=CwxSocket::recv(stream.getHandle(), head, msg))
        {
            printf("failure to recv a message, errno=%d", errno);
            break;
        }
        printf("recv msg: msg_attr=%u, msg_type=%u, data_len=%u\n", head.getAttr(), head.getMsgType(), head.getDataLen());
        if (CWX_MQ_MSG_TYPE_SYNC_DATA == head.getMsgType())
        {
            if (CWX_MQ_ERR_SUCCESS != cwx_mq_parse_sync_data(reader,
                msg->rd_ptr(),
                msg->length(),
                &ullSid,
                &uiTimestamp,
                &data,
                &group,
                &type,
                &attr,
                szErr2K))
            {
                printf("failure to parse sync data, err=%s\n", szErr2K);
                break;
            }
            printf("data:  sid=%"PRIu64", group=%u, type=%u, attr=%u, data_key=%s, data=%s, data_len=%u\n", ullSid, group, type, attr, data->m_szKey, data->m_szData, data->m_uiDataLen);
           //reply
           len = 4096;
           if (CWX_MQ_ERR_SUCCESS != cwx_mq_pack_sync_data_reply(writer, head.getTaskId(), buf, &len, ullSid, buf))
          {  
              printf("failure to pack sync relply, err=%s\n", buf);
              break;
          }
          if (len != CwxSocket::write_n(stream.getHandle(), buf, len))
          {
             printf("failure to send sync reply, err=%d\n", errno);
             break;
          }
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
            printf("recv a keepalive\n");
        }
        else if (CWX_MQ_MSG_TYPE_SYNC_REPORT_REPLY == head.getMsgType())
        {
            if (CWX_MQ_ERR_SUCCESS != cwx_mq_parse_sync_report_reply(reader, msg->rd_ptr(), msg->length(), &ret, &ullSid, &szErrMsg, szErr2K))
            {
                printf("failure to parse report reply, err=%s\n", szErr2K);
                return 0;
            }
            printf("success to parse report reply, ret=%d, err=%s\n", ret, szErrMsg);
            break;
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
