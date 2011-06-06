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
string g_subscribe;
CWX_UINT64 g_sid = 0;
CWX_UINT32 g_window = 1;
CWX_UINT32 g_num = 1;
///-1£ºÊ§°Ü£»0£ºhelp£»1£º³É¹¦
int parseArg(int argc, char**argv)
{
    CwxGetOpt cmd_option(argc, argv, "H:P:u:p:s:w:n:h");
    int option;
    cmd_option.long_option("sid", 'i', CwxGetOpt::ARG_REQUIRED);
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("Recieve mq message from the dispatch port.\n", argv[0]);
            printf("%s  -H host -P port\n", argv[0]);
            printf("-H: mq server dispatch host\n");
            printf("-P: mq server dispatch port\n");
            printf("-u: dispatch's user name.\n");
            printf("-p: dispatch's user password.\n");
            printf("-s: dispatch's subscribe. default is *.\n");
            printf("-w: dispatch's window size, default is 1.\n");
            printf("-n: recieve message's number, default is 1.zero is all from the sid.\n");
            printf("--sid: start sid, zero for the current max sid.\n");
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
        case 's':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-s requires an argument.\n");
                return -1;
            }
            g_subscribe = cmd_option.opt_arg();
            break;
        case 'w':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-w requires an argument.\n");
                return -1;
            }
            g_window = strtoul(cmd_option.opt_arg(),NULL,0);
            break;
        case 'n':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-n requires an argument.\n");
                return -1;
            }
            g_num = strtoul(cmd_option.opt_arg(),NULL,0);
            break;
        case 'i':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("--sid requires an argument.\n");
                return -1;
            }
            g_sid = strtoull(cmd_option.opt_arg(),NULL,0);
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
    if (!g_subscribe.length()) g_subscribe = "*";
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
    CwxKeyValueItem const item;

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
                    &reader,
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
                    &reader,
                    block,
                    ullSid,
                    timestamp,
                    &item,
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
                    item.m_szData);
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
                if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncDataReply(&writer,
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
