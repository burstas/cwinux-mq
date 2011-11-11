#include "ZkJPoolAdaptor.h"
#include "CwxGetOpt.h"
#include "CwxTimeValue.h"
using namespace cwinux;

string g_strHost;
string g_strNode;
string g_strValue;
string g_strOut;
string g_strFile;
bool   g_ephemeral = false;
bool   g_sequence = false;
list<string> g_auth;
list<string>  g_priv;
int    g_num;
///-1：失败；0：help；1：成功
int parseArg(int argc, char**argv)
{
	CwxGetOpt cmd_option(argc, argv, "H:n:d:f:a:o:l:N:esh");
    int option;
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("create zookeeper node for loop.\n");
			printf("%s  -H host:port -n node [-d data] [-f data file] [-o output file] [-a usr:passwd] [-l privilege] [-N num]\n", argv[0]);
			printf("-H: zookeeper's host:port\n");
			printf("-n: node name to create, it's full path.\n");
			printf("-d: value for node.\n");
			printf("-f: data's file. -d is used if it exists\n");
			printf("-a: auth user's user:passwd. it can be multi.\n");
			printf("-l: node's acl. it can be multi. it's value can be:\n");
			printf("    all               :  any privilege for any user;\n");
			printf("    self              : any privilege for creator; \n");
			printf("    read              : read for any user;\n");
			printf("    user:passwd:acrwd : digest auth for [user] with [passwd], \n");
			printf("          admin(a), create(c), read(r), write(w), delete(d)\n");
			printf("-o: output file, default is stdout\n");
			printf("-e: node is EPHEMERAL node\n");
			printf("-s: node is SEQUENCE node\n");
			printf("-N: number to create\n");
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
		case 'd':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-d requires an argument.\n");
				return -1;
			}
			g_strValue = cmd_option.opt_arg();
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
		case 'l':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-l requires an argument.\n");
				return -1;
			}
			g_priv.push_back(cmd_option.opt_arg());
			break;
		case 'o':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-o requires an argument.\n");
				return -1;
			}
			g_strOut = cmd_option.opt_arg();
			break;
		case 'N':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-N requires an argument.\n");
				return -1;
			}
			g_num = atoi(cmd_option.opt_arg());
			break;
		case 'e':
			g_ephemeral = true;
			break;
		case 's':
			g_sequence = true;
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
	if (!g_strValue.length())
	{
		if (g_strFile.length())
		{
			if (!CwxFile::readTxtFile(g_strFile, g_strValue)){
				printf("Failure to read file:%s, errno=%d\n", g_strFile.c_str(), errno);
				return -1;
			}
		}
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
					output(outFd, 2, 0,"msg:  Failure to auth, err=%s\n", zk.getErrMsg());
					if (outFd) fclose(outFd);
					return 2;
				}
				iter++;
			}
		}
		struct ACL_vector acl;
		struct ACL_vector *pacl=&ZOO_OPEN_ACL_UNSAFE;
		if (g_priv.size())
		{
			acl.count = g_priv.size();
			acl.data = new struct ACL[acl.count];
			int index=0;
			list<string>::iterator iter = g_priv.begin();
			while(iter != g_priv.end())
			{
				if (!ZkAdaptor::fillAcl(iter->c_str(), acl.data[index++]))
				{
					output(outFd, 2, 0,"msg:  invalid auth %s\n", iter->c_str());
					if (outFd) fclose(outFd);
					return 2;
				}
				iter++;
			}
			pacl = &acl;
		}
		int flags = 0;
		if (g_sequence) flags |= ZOO_SEQUENCE;
		if (g_ephemeral) flags |= ZOO_EPHEMERAL;
		char path[2048];
		char szNum[32];
		string strValue;
		for (int i=0; i<g_num; i++){
			if (!g_sequence){
				sprintf(szNum, "%d", i);
				strValue = g_strNode + szNum;
			}else{
				strValue = g_strNode;
			}
			int ret = zk.createNode(strValue, g_strValue.c_str(), g_strValue.length(), pacl, flags, path, 2048);

			if (0 == i)
			{
				if (-1 == ret){
					output(outFd, 2, zk.getErrCode(), "msg:  Failure to create node, err=%s\n", zk.getErrMsg());
					if (outFd) fclose(outFd);
					return 2;
				}
				if (0 == ret){
					output(outFd, 2, zk.getErrCode(), NULL, "msg:  node exists\n");
					if (outFd) fclose(outFd);
					return 2;
				}
				output(outFd, 0, 0, "msg:  success\n");
			}
			if (outFd){
				fprintf(outFd, "%s:%s\n", !g_sequence?g_strNode.c_str():path, (0==ret)?"exist":(1==ret)?"success":"failure");
			}else{
				printf("%s:%s\n", !g_sequence?g_strNode.c_str():path, (0==ret)?"exist":(1==ret)?"success":"failure");
			}
			if (1 != ret){
				if (outFd) fclose(outFd);
				return 2;
			}
		}
		if (outFd) fclose(outFd);
		return 0;
	}
	output(outFd, 2, 0, NULL, "msg:  Timeout to connect zk\n");
	if (outFd) fclose(outFd);
    return 2;
}
