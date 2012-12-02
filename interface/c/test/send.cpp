#include "CwxSocket.h"
#include "cwx_msg_header.h"
#include "cwx_mq_poco.h"
#include "CwxINetAddr.h"
#include "CwxSockStream.h"
#include "CwxSockConnector.h"
using namespace cwinux;

int main(int argc, char** argv) {
  if (7 != argc) {
    printf("using %s  ip port group type user passwd\n", argv[0]);
    return 0;
  }
  CwxSockStream stream;
  CwxINetAddr addr(strtoul(argv[2], NULL, 0), argv[1]);
  CwxSockConnector conn;
  if (0 != conn.connect(stream, addr)) {
    printf("failure to connect ip/port:%s/%s, errno=%d\n", argv[1], argv[2],
        errno);
    return 0;
  }
  //report
  CWX_PG_WRITER* writer = cwx_pg_writer_create(2048);
  char buf[64001];
  CWX_UINT32 len = 1024;
  CwxMsgHead head;
  CwxMsgBlock* msg = NULL;
  CWX_PG_READER* reader = cwx_pg_reader_create();
  int ret;
  CWX_UINT64 ullSid;
  CWX_KEY_VALUE_ITEM_S data;
  char const* szErrMsg = NULL;
  char szErr2K[2048];
  char szSendBuf[164001];
  CWX_UINT32 uiSendLen = 164001;
  CWX_UINT32 i = 0;
  for (i = 0; i < len; i++) {
    buf[i] = 'a' + (i % 26);
  }
  buf[i] = 0x00;
  data.m_szData = buf;
  data.m_uiDataLen = 64000;
  data.m_bKeyValue = 0;
  if (CWX_MQ_ERR_SUCCESS
      != cwx_mq_pack_mq(writer, 0, szSendBuf, &uiSendLen, &data,
          strtoul(argv[3], NULL, 0), strtoul(argv[4], NULL, 0), 1000, argv[5],
          argv[6], "", szErr2K)) {
    printf("failure to pack send msg, err=%s", szErr2K);
    return 0;
  }
  while (1) {
    if (uiSendLen
        != CwxSocket::write_n(stream.getHandle(), szSendBuf, uiSendLen)) {
      printf("failure to send  a message, errno=%d\n", errno);
      break;
    }
    //recv msg
    if (0 >= CwxSocket::read(stream.getHandle(), head, msg)) {
      printf("failure to read the message reply, errno=%d\n", errno);
    }
    if (CWX_MQ_MSG_TYPE_MQ_REPLY == head.getMsgType()) {
      if (CWX_MQ_ERR_SUCCESS
          != cwx_mq_parse_mq_reply(reader, msg->rd_ptr(), msg->length(), &ret,
              &ullSid, &szErrMsg, szErr2K)) {
        printf("failure to parse mq reply, err=%s\n", szErr2K);
        break;
      }
      if (CWX_MQ_ERR_SUCCESS != ret) {
        printf("failure to send a message, err=%s\n", szErrMsg);
        break;
      }
    printf("send a message, ret=%d, sid=%"PRIu64"\n", ret, ullSid);
  } else if (head.isKeepAlive(false)) {
    head.setMsgType(head.getMsgType() + 1);
    char const* szHead = head.toNet();
    if (CwxMsgHead::MSG_HEAD_LEN
        != CwxSocket::write_n(stream.getHandle(), szHead,
            CwxMsgHead::MSG_HEAD_LEN)) {
      printf("Failure to send keep-alive reply, errno=%d\n", errno);
      break;
    }
    printf("success to parse report reply, ret=%d, err=%s\n", ret, szErrMsg);
  } else {
    printf("recv a unknow msg type, msg_attr=%u, msg_type=%u, data_len=%u\n",
        head.getAttr(), head.getMsgType(), head.getDataLen());
    break;
  }
  CwxMsgBlockAlloc::free(msg);
}
stream.close();
return 0;
}
