#include "ZkAdaptor.h"
#include "CwxGetOpt.h"
#include "CwxTimeValue.h"
using namespace cwinux;

string g_strHost;
string g_strPath;
string g_strNode;
string g_strValue;
///-1£ºÊ§°Ü£»0£ºhelp£»1£º³É¹¦
int parseArg(int argc, char**argv)
{
	CwxGetOpt cmd_option(argc, argv, "H:p:n:v:h");
    int option;
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("create zookeeper node.\n");
			printf("%s  -H host:port -p path -n node \n", argv[0]);
			printf("-H: zookeeper's host:port\n");
            printf("-p: path for node\n");
            printf("-n: node name to create.\n");
			printf("-v: value for node.\n");
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
        case 'p':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-p requires an argument.\n");
                return -1;
            }
            g_strPath = cmd_option.opt_arg();
            break;
        case 'n':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-n requires an argument.\n");
                return -1;
            }
            g_strNode = cmd_option.opt_arg();
            break;
		case 'v':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-v requires an argument.\n");
				return -1;
			}
			g_strValue = cmd_option.opt_arg();
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
    if (!g_strPath.length())
    {
        printf("No path, set by -p\n");
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

	ZooKeeperAdapter zk(g_strHost);
	if (0 != zk.init()){
		printf("Failure to init zk, err=%s\n", zk.getErrMsg());
		return -1;
	}
	if (0 != zk.connect())
	{
		printf("Failure to connect zk, err=%s\n", zk.getErrMsg());
		return -1;
	}
	
	int timeout = 5;
	while(timeout > 0){
		if (!zk.isConnected()){
			timeout --;
			sleep(1);
			continue;
		}
		if (zk.createNode(g_strPath, g_strValue.c_str(), g_strValue.length()))
		{
			printf("Failure to create node, err=%s\n", zk.getErrMsg());
			return 1;
		}
		printf("Success to create node for %s in path:%s\n", g_strNode.c_str(), g_strPath.c_str());
		return 0;
	}
	printf("Timeout for connect zk:%s\n", g_strHost.c_str());
    return 1;
}
