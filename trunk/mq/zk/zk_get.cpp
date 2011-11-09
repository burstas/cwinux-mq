#include "ZkJPoolAdaptor.h"
#include "CwxGetOpt.h"
#include "CwxTimeValue.h"
using namespace cwinux;

string g_strHost;
string g_strNode;
string g_strAuth;
///-1£ºÊ§°Ü£»0£ºhelp£»1£º³É¹¦
int parseArg(int argc, char**argv)
{
	CwxGetOpt cmd_option(argc, argv, "H:n:h:a:");
    int option;
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("get zookeeper node data.\n");
			printf("%s  -H host:port -n node -a usr:passwd \n", argv[0]);
			printf("-H: zookeeper's host:port\n");
            printf("-n: node name to create, it's full path.\n");
			printf("-a: user:passwd.\n");
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
        case 'n':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-n requires an argument.\n");
                return -1;
            }
            g_strNode = cmd_option.opt_arg();
            break;
		case 'a':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-a requires an argument.\n");
				return -1;
			}
			g_strAuth = cmd_option.opt_arg();
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
	if (!g_strNode.length())
	{
		printf("No node, set by -n\n");
		return -1;
	}
    return 1;
}

int main(int argc ,char** argv)
{
    int iRet = parseArg(argc, argv);

    if (0 == iRet) return 0;
    if (-1 == iRet) return 1;

	ZkJPoolAdaptor zk(g_strHost);
	if (0 != zk.init()){
		printf("Failure to init zk, err=%s\n", zk.getErrMsg());
		return -1;
	}
	if (0 != zk.connect())
	{
		printf("Failure to connect zk, err=%s\n", zk.getErrMsg());
		return -1;
	}
	
	int timeout = 5000;
	CWX_UINT32 uiBufLen = 1024 * 1024;
	char szBuf[uiBufLen + 1];
	struct Stat stat;
	while(timeout > 0){
		if (!zk.isConnected()){
			timeout --;
			ZkAdaptor::sleep(1);
			continue;
		}
		if (g_strAuth.length()){
			if (!zk.addAuth("digest", g_strAuth.c_str(), g_strAuth.length())){
				printf("Failure to auth, err=%s\n", zk.getErrMsg());
				return 1;
			}
		}
		sleep(3);
		int ret = zk.getNodeData(g_strNode, szBuf, uiBufLen, stat);
		if (0 == ret){
			printf("Not exist\n");
			return 0;
		}
		if (-1 == ret){
			printf("Failure to get node, err=%s\n", zk.getErrMsg());
			return 1;
		}
		char szTmp[64];
		time_t timestamp;
		printf("Success to get node for %s\n", g_strNode.c_str());
		printf("data:%s\n", szBuf);
		printf("czxid:%s\n", CwxCommon::toString(stat.czxid, szTmp, 16));
		printf("mzxid:%s\n", CwxCommon::toString(stat.mzxid, szTmp, 16));
		timestamp = stat.ctime/1000;
		printf("ctime:%d %s", (int)stat.ctime%1000, ctime_r(&timestamp, szTmp));
		timestamp = stat.mtime/1000;
		printf("mtime:%d %s", (int)stat.mtime%1000, ctime_r(&timestamp, szTmp));
		printf("version:%d\n", stat.version);
		printf("cversion:%d\n", stat.cversion);
		printf("aversion:%d\n", stat.aversion);
		printf("dataLength:%d\n", stat.dataLength);
		printf("numChildren:%d\n", stat.numChildren);
		printf("pzxid:%s\n", CwxCommon::toString(stat.pzxid, szTmp, 16));
		return 0;
	}
	printf("Timeout for connect zk:%s\n", g_strHost.c_str());
    return 1;
}
