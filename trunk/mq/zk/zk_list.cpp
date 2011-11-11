#include "ZkJPoolAdaptor.h"
#include "CwxGetOpt.h"
#include "CwxTimeValue.h"
#include "CwxFile.h"
using namespace cwinux;

string g_strHost;
string g_strNode;
list<string> g_auth;
string g_strOut;
///-1：失败；0：help；1：成功
int parseArg(int argc, char**argv)
{
	CwxGetOpt cmd_option(argc, argv, "H:n:a:o:h");
    int option;
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("get zookeeper node's child.\n");
			printf("%s  -H host:port -n node [-a usr:passwd] [-o output file]\n", argv[0]);
			printf("-H: zookeeper's host:port\n");
            printf("-n: node name to create, it's full path.\n");
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
            g_strNode = cmd_option.opt_arg();
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
	if (!g_strNode.length())
	{
		printf("No node, set by -n\n");
		return -1;
	}
    return 1;
}

void output(FILE* fd, int result, int zkstate, char* format, char* msg)
{
	if (fd)
	{
		fprintf(fd, "ret:  %d\n", result);
		fprintf(fd, "zkstate:  %s\n", zkstate);
		if (format)
			fprintf(fd, format, msg);
		else
			fprintf(fd, msg);
	}
	else
	{
		printf("ret:  %d\n", result);
		printf("zkstate:  %s\n", zkstate);
		if (format)
			printf(format, msg);
		else
			printf(msg);
	}
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


	ZkJPoolAdaptor zk(g_strHost);
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
	list<string> childs;
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
				if (!zk.addAuth("digest", *iter, iter->length(), 3000))
				{
					output(outFd, 2, 0, "msg:  Failure to auth, err=%s\n", zk.getErrMsg());
					if (outFd) fclose(outFd);
					return 2;
				}
				iter++;
			}
		}

		int ret = !zk.getNodeChildren(g_strNode, childs);
		if (0 == ret){
			output(outFd, 2, zk.getErrCode(), NULL, "msg:  node doesnt' exist\n");
			if (outFd) fclose(outFd);
			return 2;
		}
		if (-1 == ret){
			output(outFd, 2, zk.getErrCode(), NULL, "msg:  Failure to get child, err=%s\n", zk.getErrMsg());
			if (outFd) fclose(outFd);
			return 2;
		}
		output(outFd, 0, 0, "node:  %s\nmsg:  success\n", g_strNode.c_str());
		list<string>::iterator iter=childs.begin();
		while(iter != childs.end())
		{
			if (outFd){
				if(iter == childs.begin())
					fprintf(outFd, "list:\n");
				fprintf(outFd, "%s\n", iter->c_str());
			}else{
				if(iter == childs.begin())
					printf("list:\n");
				printf("%s\n", iter->c_str());
			}
			iter++;
		}
		return 0;
	}
	output(outFd, 2, 0, NULL, "msg:  Timeout to connect zk\n");
	if (outFd) fclose(outFd);
	return 2;
}
