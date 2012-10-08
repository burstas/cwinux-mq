#include "CwxSocket.h"
#include "CwxINetAddr.h"
#include "CwxSockStream.h"
#include "CwxSockConnector.h"
#include "CwxGetOpt.h"
#include "CwxMqPoco.h"
using namespace cwinux;
string g_strHost;
CWX_UINT16 g_unPort = 0;
string g_queue;
string g_user;
string g_passwd;
string g_auth_user;
string g_auth_passwd;
///-1：失败；0：help；1：成功
int parseArg(int argc, char**argv) {
  CwxGetOpt cmd_option(argc, argv, "H:P:u:p:q:U:W:h");
  int option;
  while ((option = cmd_option.next()) != -1) {
    switch (option) {
      case 'h':
        printf("Delete a queue.\n");
        printf("%s  -H host -P port\n", argv[0]);
        printf("-H: mq server queue's host name\n");
        printf("-P: mq server queue's port\n");
        printf("-u: queue's user.\n");
        printf("-p: queue's user passwd.\n");
        printf("-q: queue's name, it can't be empty.\n");
        printf("-U: authentication user for queue.\n");
        printf("-W: authentication user password for queue.\n");
        printf("-h: help\n");
        return 0;
      case 'H':
        if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-')) {
          printf("-H requires an argument.\n");
          return -1;
        }
        g_strHost = cmd_option.opt_arg();
        break;
      case 'P':
        if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-')) {
          printf("-P requires an argument.\n");
          return -1;
        }
        g_unPort = strtoul(cmd_option.opt_arg(), NULL, 10);
        break;
      case 'u':
        if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-')) {
          printf("-u requires an argument.\n");
          return -1;
        }
        g_user = cmd_option.opt_arg();
        break;
      case 'p':
        if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-')) {
          printf("-p requires an argument.\n");
          return -1;
        }
        g_passwd = cmd_option.opt_arg();
        break;
      case 'q':
        if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-')) {
          printf("-q requires an argument.\n");
          return -1;
        }
        g_queue = cmd_option.opt_arg();
        break;
      case 'U':
        if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-')) {
          printf("-U requires an argument.\n");
          return -1;
        }
        g_auth_user = cmd_option.opt_arg();
        break;
      case 'W':
        if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-')) {
          printf("-W requires an argument.\n");
          return -1;
        }
        g_auth_passwd = cmd_option.opt_arg();
        break;
      case ':':
        printf("%c requires an argument.\n", cmd_option.opt_opt());
        return -1;
      case '?':
        break;
      default:
        printf("Invalid arg %s.\n", argv[cmd_option.opt_ind() - 1]);
        return -1;
    }
  }
  if (-1 == option) {
    if (cmd_option.opt_ind() < argc) {
      printf("Invalid arg %s.\n", argv[cmd_option.opt_ind()]);
      return -1;
    }
  }
  if (!g_strHost.length()) {
    printf("No host, set by -H\n");
    return -1;
  }
  if (!g_unPort) {
    printf("No port, set by -P\n");
    return -1;
  }
  if (!g_queue.length()) {
    printf("No queue, set by -q\n");
    return -1;
  }
  return 1;
}

int main(int argc, char** argv) {
  int iRet = parseArg(argc, argv);

  if (0 == iRet)
    return 0;
  if (-1 == iRet)
    return 1;

  CwxSockStream stream;
  CwxINetAddr addr(g_unPort, g_strHost.c_str());
  CwxSockConnector conn;
  if (0 != conn.connect(stream, addr)) {
    printf("failure to connect ip:port: %s:%u, errno=%d\n", g_strHost.c_str(),
      g_unPort, errno);
    return 1;
  }
  CwxPackageWriter writer;
  CwxPackageReader reader;
  CwxMsgHead head;
  CwxMsgBlock* block = NULL;
  char szErr2K[2048];
  char const* pErrMsg = NULL;

  do {
    if (CWX_MQ_ERR_SUCCESS
      != CwxMqPoco::packDelQueue(&writer, block, g_queue.c_str(),
      g_user.c_str(), g_passwd.c_str(), g_auth_user.c_str(),
      g_auth_passwd.c_str(), szErr2K)) {
        printf("failure to pack delete-queue package, err=%s\n", szErr2K);
        iRet = 1;
        break;
    }
    if (block->length()
      != (CWX_UINT32) CwxSocket::write_n(stream.getHandle(), block->rd_ptr(),
      block->length())) {
        printf("failure to send message, errno=%d\n", errno);
        iRet = 1;
        break;
    }
    CwxMsgBlockAlloc::free(block);
    block = NULL;
    //recv msg
    if (0 >= CwxSocket::read(stream.getHandle(), head, block)) {
      printf("failure to read the reply, errno=%d\n", errno);
      iRet = 1;
      break;
    }
    if (CwxMqPoco::MSG_TYPE_DEL_QUEUE_REPLY != head.getMsgType()) {
      printf("recv a unknow msg type, msg_type=%u\n", head.getMsgType());
      iRet = 1;
      break;

    }
    if (CWX_MQ_ERR_SUCCESS
      != CwxMqPoco::parseDelQueueReply(&reader, block, iRet, pErrMsg,
      szErr2K)) {
        printf("failure to unpack reply msg, err=%s\n", szErr2K);
        iRet = 1;
        break;
    }
    if (CWX_MQ_ERR_SUCCESS != iRet) {
      printf("failure to delete queue[%s], err_code=%d, err=%s\n",
        g_queue.c_str(), iRet, pErrMsg);
      iRet = 1;
      break;
    }
    iRet = 0;
    printf("success to delete queue[%s],user=%s,passwd=%s\n", g_queue.c_str(),
      g_user.c_str(), g_passwd.c_str());
  } while (0);
  if (block)
    CwxMsgBlockAlloc::free(block);
  stream.close();
  return iRet;
}
