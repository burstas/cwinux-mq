#include "CwxSocket.h"
#include "CwxINetAddr.h"
#include "CwxSockStream.h"
#include "CwxSockConnector.h"
#include "CwxGetOpt.h"
using namespace cwinux;

string g_strHost;
CWX_UINT16 g_unPort = 0;
///-1：失败；0：help；1：成功
bool parseArg(int argc, char**argv)
{
    CwxGetOpt cmd_option(argc, argv, "H:P:h");
    int option;
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("%s  -H host -P port\n", argv[0]);
            printf("-H: mq server host\n");
            printf("-P: mq server monitor port\n");
            return 0;
        case 'H':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-h requires an argument.\n");
                return -1;
            }
            g_strHost = cmd_option.opt_arg();
            break;
        case 'p':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-p requires an argument.\n");
                return -1;
            }
            g_unPort = strtoul(cmd_option.opt_arg(), NULL, 0);
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
        printf("No host, set by -h\n");
        return -1;
    }
    if (!g_unPort)
    {
        printf("No port, set by -p\n");
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
    //写stats数据
    char const* szStats = "stats\r\n";
    if (strlen(szStats) !=CwxSocket::write_n(stream.getHandle(), szStats, strlen(szStats)))
    {
        printf("Failure to send stats command, errno=%s\n", errno);
        return 1;
    }
    string strReply;
    char buf[1024];
    CwxTimeValue timeout(10);
    CwxTimeouter timer(&timeout);
    while(1)
    {
       //recv msg
       if (0 >= (iRet = CwxSocket::read(stream.getHandle(), buf, 1023, &timer)))
       {
           printf("failure to read stats reply, errno=%d\n", errno);
           iRet = 1;
           break;
       }
       buf[iRet] = 0;
       strReply += buf;
       if (strReply.find("END\r\n") != string::npos)
       {
           printf("%s", strReply.c_str());
           iRet =0;
           break;
       }
    }
    stream.close();
    return iRet;
}
