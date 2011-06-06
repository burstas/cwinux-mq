#include "CwxSocket.h"
#include "CwxINetAddr.h"
#include "CwxSockStream.h"
#include "CwxSockConnector.h"
#include "CwxGetOpt.h"
#include "CwxMqPoco.h"
using namespace cwinux;
string g_strHost;
CWX_UINT16 g_unPort = 0;
string g_user;
string g_passwd;
string g_queue;
CWX_UINT32 g_num = 1;
bool   g_block = true;
CWX_UINT32 g_timeout = 0;
bool   g_commit = false;
///-1��ʧ�ܣ�0��help��1���ɹ�
int parseArg(int argc, char**argv)
{
    CwxGetOpt cmd_option(argc, argv, "H:P:u:p:q:b:n:t:h");
    int option;
    cmd_option.long_option("timeout", 'o', CwxGetOpt::ARG_REQUIRED);
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("fetch mq message from queue.\n", argv[0]);
            printf("%s  -H host -P port\n", argv[0]);
            printf("-H: mq server's queue host\n");
            printf("-P: mq server's queue port\n");
            printf("-u: queue's user name.\n");
            printf("-p: queue's user password.\n");
            printf("-q: queue's name.\n");
            printf("-b: block sign, 1:block when no message;0:return when no message.\n");
            printf("-n: message number to fetch. default is 1. 0 for fetching all.\n");
            printf("-t: queue type, 0:no commit queue;1:commit queue.\n");
            printf("--timeout: timeout second for commit queue. 0 for default value.\n");
            printf("-h: help\n");
            return 0;
        case 'H':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-H requires an argument.\n");
                return -1;
            }
            g_strHost = cmd_option.opt_arg();
            break;
        case 'P':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-P requires an argument.\n");
                return -1;
            }
            g_unPort = strtoul(cmd_option.opt_arg(), NULL, 0);
            break;
        case 'u':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-u requires an argument.\n");
                return -1;
            }
            g_user = cmd_option.opt_arg();
            break;
        case 'p':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-p requires an argument.\n");
                return -1;
            }
            g_passwd = cmd_option.opt_arg();
            break;
        case 'q':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-q requires an argument.\n");
                return -1;
            }
            g_queue = cmd_option.opt_arg();
            break;
        case 'b':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-b requires an argument.\n");
                return -1;
            }
            g_block = strtoul(cmd_option.opt_arg(),NULL,0)==0?false:true;
            break;
        case 'n':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-n requires an argument.\n");
                return -1;
            }
            g_num = strtoul(cmd_option.opt_arg(),NULL,0);
            break;
        case 't':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-t requires an argument.\n");
                return -1;
            }
            g_commit = strtoul(cmd_option.opt_arg(),NULL,0)==0?false:true;
            break;
        case '--timeout':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("--timeout requires an argument.\n");
                return -1;
            }
            g_timeout = strtoul(cmd_option.opt_arg(),NULL,0);
            break;
        case ':':
            printf("%c requires an argument.\n", cmd_option.opt_opt ());
            return -1;
        case '?':
            break;
        default:
            printf("Invalid arg %s.\n", argv[cmd_option.opt_ind()-1]);
            return -1;
        }
    }
    if (-1 == option)
    {
        if (cmd_option.opt_ind()  < argc)
        {
            printf("Invalid arg %s.\n", argv[cmd_option.opt_ind()]);
            return -1;
        }
    }
    if (!g_strHost.length())
    {
        printf("No host, set by -H\n");
        return -1;
    }
    if (!g_unPort)
    {
        printf("No port, set by -P\n");
        return -1;
    }
    if (!g_queue.length())
    {
        printf("No queue, set by -q\n");
        return -1;
    }
    return 1;
}

int main(int argc ,char** argv)
{
    int iRet = parseArg(argc, argv);

    if (0 == iRet) return 0;
    if (-1 == iRet) return 1;

    CwxSockStream  stream;
    CwxINetAddr  addr(g_unPort, g_strHost.c_str());
    CwxSockConnector conn;
    if (0 != conn.connect(stream, addr))
    {
        printf("failure to connect ip:port: %s:%u, errno=%d\n", g_strHost.c_str(), g_unPort, errno);
        return 1;
    }
    CwxPackageWriter writer;
    CwxPackageReader reader;
    CwxMsgHead head;
    CwxMsgBlock* block=NULL;
    char szErr2K[2048];
    char const* pErrMsg=NULL;
    CWX_UINT64 ullSid = 0;
    CWX_UINT32 num = 0;
    CWX_UINT32 group = 0;
    CWX_UINT32 type = 0;
    CWX_UINT32 attr = 0;
    CWX_UINT32 timestamp = 0;
    CwxKeyValueItem item;

    CwxMqPoco::init();
    do 
    {
        if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packReportData(
            &writer,
            block,
            0,
            g_sid>0?g_sid-1:g_sid,
            g_sid==0?true:false,
            0,
            g_window,
            g_subscribe.c_str(),
            g_user.c_str(),
            g_passwd.c_str(),
            NULL,
            false,
            szErr2K
            ))
        {
            printf("failure to pack report-queue package, err=%s\n", szErr2K);
            iRet = 1;
            break;
        }
        if (block->length() != CwxSocket::write_n(stream.getHandle(),
            block->rd_ptr(),
            block->length()))
        {
            printf("failure to send message, errno=%d\n", errno);
            iRet = 1;
            break;
        }
        CwxMsgBlockAlloc::free(block);
        block = NULL;
        while(1)
        {
            //recv msg
            if (0 >= CwxSocket::read(stream.getHandle(), head, block))
            {
                printf("failure to read the reply, errno=%d\n", errno);
                iRet = 1;
                break;
            }
            if (CwxMqPoco::MSG_TYPE_SYNC_REPORT_REPLY == head.getMsgType())
            {
                if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseReportDataReply(
                    reader,
                    block,
                    iRet,
                    ullSid,
                    pErrMsg,
                    szErr2K
                    ))
                {
                    printf("failure to unpack report-reply msg, err=%s\n", szErr2K);
                    iRet = 1;
                    break;
                }
                printf("failure to report dispatch, err-code=%d, err=%s\n", iRet, pErrMsg);
                iRet = 1;
                break;
            }
            else if (CwxMqPoco::MSG_TYPE_SYNC_DATA == head.getMsgType())
            {
                num++;
                if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseSyncData(
                    reader,
                    block,
                    ullSid,
                    timestamp,
                    item,
                    group,
                    type,
                    attr,
                    szErr2K))
                {
                    printf("failure to unpack recieve msg, err=%s\n", szErr2K);
                    iRet = 1;
                    break;
                }
                printf("%s|%u|%u|%u|%u|%s\n",
                    CwxCommon::toString(ullSid, szErr2K, 10),
                    timestamp,
                    group,
                    type,
                    attr,
                    item->m_szData);
                if (g_num)
                {
                    if (num >= g_num)
                    {
                        iRet = 0;
                        break;
                    }
                }
                CwxMsgBlockAlloc::free(block);
                block = NULL;
                if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncDataReply(writer,
                    block,
                    0,
                    ullSid,
                    szErr2K))
                {
                    printf("failure to pack recieve data reply package, err=%s\n", szErr2K);
                    iRet = 1;
                    break;
                }
                if (block->length() != CwxSocket::write_n(stream.getHandle(),
                    block->rd_ptr(),
                    block->length()))
                {
                    printf("failure to send message, errno=%d\n", errno);
                    iRet = 1;
                    break;
                }
                CwxMsgBlockAlloc::free(block);
                block = NULL;
                continue;
            }
            else
            {
                printf("recv a unknow msg type, msg_type=%u\n", head.getMsgType());
                iRet = 1;
                break;
            }
        }
    } while(0);
    if (block) CwxMsgBlockAlloc::free(block);
    CwxMqPoco::destory();
    stream.close();
    return iRet;
}
