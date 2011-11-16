#include "zk_common.h"


string g_strHost;
list<string> g_node;
string g_strOut;
string g_strFile;
list<string> g_auth;
///-1：失败；0：help；1：成功
int parseArg(int argc, char**argv)
{
	ZkGetOpt cmd_option(argc, argv, "H:n:f:a:o:h");
    int option;
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("update zookeeper node.\n");
			printf("%s  -H host:port -n node [-f node file] [-o output file] [-a usr:passwd]\n", argv[0]);
			printf("-H: zookeeper's host:port\n");
            printf("-n: node name to check, it can be multi.\n");
			printf("-f: node file. one per line by \\n \n");
			printf("-a: auth user's user:passwd. it can be multi.\n");
			printf("-o: output file, default is stdout\n");
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
            g_node.push_back(cmd_option.opt_arg());
            break;
		case 'f':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-f requires an argument.\n");
				return -1;
			}
			g_strFile = cmd_option.opt_arg();
			break;
		case 'a':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-a requires an argument.\n");
				return -1;
			}
			g_auth.push_back(cmd_option.opt_arg());
			break;
		case 'o':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-o requires an argument.\n");
				return -1;
			}
			g_strOut = cmd_option.opt_arg();
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
	if (g_node.begin() == g_node.end())
	{
		if (g_strFile.length())
		{
			string data;
			if (!readFile(g_strFile, data)){
				printf("Failure to read file:%s, errno=%d\n", g_strFile.c_str(), errno);
				return -1;
			}
			ZkAdaptor::split(data, g_node, '\n');
		}
		else{
			printf("Must specify the node to check by -n or -f\n");
			return -1;
		}
	}
    return 1;
}

//0:success
//1:参数错误
//2:执行结果错误

int main(int argc ,char** argv)
{
	FILE * outFd = NULL;
    int iRet = parseArg(argc, argv);

    if (0 == iRet) return 0;
    if (-1 == iRet) return 1;

	if (g_strOut.length())
	{
		outFd = fopen(g_strOut.c_str(), "w+b");
		if (!outFd){
			printf("Failure to open output file:%s, errno=%d\n", g_strOut.c_str(), errno);
			return 1;
		}
	}

	ZkToolAdaptor zk(g_strHost);
	if (0 != zk.init()){
		output(outFd, 2, zk.getErrCode(), "msg:  Failure to init zk, err=%s\n", zk.getErrMsg());
		if (outFd) fclose(outFd);
		return 2;
	}
	if (0 != zk.connect())
	{
		output(outFd, 2, zk.getErrCode(), "msg:  Failure to connect zk, err=%s\n", zk.getErrMsg());
		if (outFd) fclose(outFd);
		return 2;
	}
	
	int timeout = 5000;
	while(timeout > 0){
		if (!zk.isConnected()){
			timeout --;
			ZkAdaptor::sleep(1);
			continue;
		}
		//add auth
		if (g_auth.size())
		{
			list<string>::iterator iter = g_auth.begin();
			while(iter != g_auth.end())
			{
				if (!zk.addAuth("digest", iter->c_str(), iter->length(), 3000))
				{
					output(outFd, 2, 0,"msg:  Failure to auth, err=%s\n", zk.getErrMsg());
					if (outFd) fclose(outFd);
					return 2;
				}
				iter++;
			}
		}

		output(outFd, 0, 0, NULL, "node:  \nmsg:  success\nnode:\n");
		struct Stat stat;
		list<string>::iterator iter = g_node.begin();
		while(iter != g_node.end()){
			int ret = zk.nodeExists(*iter, stat);
			if (outFd){
				fprintf(outFd, "%s:%d\n", iter->c_str(), ret);
			}else{
				printf("%s:%d\n", iter->c_str(), ret);
			}
			iter++;
		}
		if (outFd) fclose(outFd);
		return 0;
	}
	output(outFd, 2, 0, NULL, "msg:  Timeout to connect zk\n");
	if (outFd) fclose(outFd);
	return 2;
}
