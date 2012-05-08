#include "zk_common.h"
#include "ZkLocker.h"

string g_strHost;
string g_strNode;
string g_strPrev="lock";
bool   g_bWatchMaster = false;
list<string> g_auth;
list<string>  g_priv;
///-1：失败；0：help；1：成功
int parseArg(int argc, char**argv)
{
    ZkGetOpt cmd_option(argc, argv, "H:n:a:l:p:hm");
    int option;
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("Test for lock zookeeper node.\n");
			printf("%s  -H host:port -n node [-d data] [-f data file] [-o output file] [-a usr:passwd] [-l privilege]\n", argv[0]);
			printf("-H: zookeeper's host:port\n");
            printf("-n: node name for lock.\n");
            printf("-p: prex for lock. default is [lock].\n");
			printf("-a: auth user's user:passwd. it can be multi.\n");
			printf("-l: node's acl. it can be multi. it's value can be:\n");
			printf("    all               :  any privilege for any user;\n");
            printf("    self              : any privilege for creator; \n");
			printf("    read              : read for any user;\n");
			printf("    user:passwd:acrwd : digest auth for [user] with [passwd], \n");
			printf("          admin(a), create(c), read(r), write(w), delete(d)\n");
            printf("-m: watch master node\n");
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
		case 'l':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-l requires an argument.\n");
				return -1;
			}
			g_priv.push_back(cmd_option.opt_arg());
			break;
        case 'p':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-p requires an argument.\n");
                return -1;
            }
            g_strPrev = cmd_option.opt_arg();
            break;
        case 'm':
            g_bWatchMaster = true;
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

void get_lock(bool bLock, void* cbdata){
    ZkLocker* zk = (ZkLocker*)cbdata;
    string strSelf;
    string strOwner;
    string strPrev;
    zk->_getSelfNode(strSelf);
    zk->_getOwnerNode(strOwner);
    zk->_getPrevNode(strPrev);
    if (bLock){
        printf("get the lock:self=%s, owner=%s, prev=%s\n", strSelf.c_str(), strOwner.c_str(), strPrev.c_str());
    }else{
        printf("lost the lock:self=%s, owner=%s, prev=%s\n", strSelf.c_str(), strOwner.c_str(), strPrev.c_str());
    }
}

//0:success
//1:参数错误
//2:执行结果错误
int main(int argc ,char** argv)
{
    int iRet = parseArg(argc, argv);

    if (0 == iRet) return 0;
    if (-1 == iRet) return 1;

	ZkToolAdaptor zk(g_strHost);

	if (0 != zk.init()){
		printf("msg:  Failure to init zk, err=%s\n", zk.getErrMsg());
		return 2;
	}

	if (0 != zk.connect())
	{
		printf("msg:  Failure to connect zk, err=%s\n", zk.getErrMsg());
		return 2;
	}

    ZkLocker locker;
	
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
					printf("msg:  Failure to auth, err=%s\n", zk.getErrMsg());
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
					printf("msg:  invalid auth %s\n", iter->c_str());
					return 2;
				}
				iter++;
			}
			pacl = &acl;
		}
        if (0 != locker.init(&zk, (char*)g_strNode.c_str(), g_strPrev, get_lock, &zk, pacl))
        {
            printf("failure to init zk lock\n");
            return 2;
        }
        
        bool bNeedLock = true;
        string lockPath;
        string ownPath;
        printf("Starting to lock......");
        iRet = locker.lock(g_bWatchMaster);
        string strSelf;
        string strOwner;
        string strPrev;
        while(1){
            if (!zk.isConnected()){
                printf("Lost connnect..........\n");
                ::sleep(1);
                continue;
                bNeedLock = true;
            }
            if (0 != iRet){
                printf("Failure to lock, code=%d\n", iRet);
                iRet = locker.lock(g_bWatchMaster);
            }
            locker.getSelfNode(strSelf);
            locker.getOwnerNode(strOwner);
            locker.getPrevNode(strPrev);

            printf("Sleep two second.........., Locked:%s,  Mine:%s,  Owner:%s,   Prev:%s\n", locker.isLocked()?"yes":"no", strSelf.c_str(), strOwner.c_str(), strPrev.c_str());
            ::sleep(4);
        }
	}
	return 2;
}
