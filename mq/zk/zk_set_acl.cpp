#include "zk_common.h"


string g_strHost;
string g_strNode;
string g_strOut;
list<string> g_auth;
list<string>  g_priv;
int g_verion = -1;
bool   g_append = true;
bool   g_remove = false;
bool   g_set = false;
///-1：失败；0：help；1：成功
int parseArg(int argc, char**argv)
{
	ZkGetOpt cmd_option(argc, argv, "H:n:a:o:l:m:h");
    int option;
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("set zookeeper node's acl.\n");
			printf("%s  -H host:port -n node [-o output file] [-a usr:passwd] [-l privilege] [-v version]\n", argv[0]);
			printf("-H: zookeeper's host:port\n");
            printf("-n: node name to create, it's full path.\n");
			printf("-a: auth user's user:passwd. it can be multi.\n");
			printf("-l: node's acl. it can be multi. it's value can be:\n");
			printf("    all               :  any privilege for any user;\n");
            printf("    self              : any privilege for creator; \n");
			printf("    read              : read for any user;\n");
			printf("    digest:user:passwd:acrwd : digest auth for [user] with [passwd], \n");
			printf("          admin(a), create(c), read(r), write(w), delete(d)\n");
			printf("    ip:ip_addr[/bits]:acrwd : ip auth for ip or ip subnet\n"); 
			printf("-m: set Acl mode. [append]、[set]、[remove] can be set。default is append.\n");
			printf("-o: output file, default is stdout\n");
			printf("-v: node's version\n");
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
		case 'o':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-o requires an argument.\n");
				return -1;
			}
			g_strOut = cmd_option.opt_arg();
			break;
		case 'm':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-o requires an argument.\n");
				return -1;
			}
			g_append = false;
			g_remove = false;
			g_set = false;
			if (strcmp("append", cmd_option.opt_arg())==0){
				g_append = true;
			}else if (strcmp("remove", cmd_option.opt_arg()) == 0){
				g_remove = true;
			}else if (strcmp("set", cmd_option.opt_arg()) == 0){
				g_set = true;
			}else{
				printf("unknown mode[%s], just can be [append], [remove] or [set].", cmd_option.opt_arg());
				return -1;
			}
			break;
		case 'v':
			if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
			{
				printf("-v requires an argument.\n");
				return -1;
			}
			g_verion = atoi(cmd_option.opt_arg());
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

class AclItem{
public:
	AclItem(char * scheme, char *  id){
		m_scheme = scheme;
		m_id = id;
	}
	AclItem(AclItem const& item){
		m_scheme = item.m_scheme;
		m_id = item.m_id;
	}
public:
	AclItem& operator=(AclItem const& item)
	{
		if (this != &item)
		{
			m_scheme = item.m_scheme;
			m_id = item.m_id;
		}
		return *this;
	}
	bool operator < (AclItem const& item) const{
		int ret = strcmp(m_scheme ,item.m_scheme);
		if (ret < 0) return true;
		if (ret > 0) return false;
		return strcmp(m_id, item.m_id)<0?true:false;
	}
public:
	char *  m_scheme;
	char *  m_id;
};


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
	int ret = 0;
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

		struct Stat stat;
		struct ACL_vector aclOlds;
		struct ACL_vector acls;
		struct ACL_vector *pacls=&ZOO_OPEN_ACL_UNSAFE;
		struct ACL acl;
		map<AclItem, int32_t>  acl_map;
		if (g_priv.size())
		{
			if (!g_set){
				ret = zk.getAcl(g_strNode.c_str(), aclOlds, stat);
				if (0 == ret){
					output(outFd, 2, zk.getErrCode(), NULL, "msg: node doesn't exist\n");
					if (outFd) fclose(outFd);
					return 2;
				}else if (-1 == ret){
					output(outFd, 2, zk.getErrCode(), "msg:  Failure to get node acl, err=%s\n", zk.getErrMsg());
					if (outFd) fclose(outFd);
					return 2;
				}
				for (int i=0; i<aclOlds.count; i++){
					acl_map[AclItem(aclOlds.data[i].id.scheme, aclOlds.data[i].id.id)] =  aclOlds.data[i].perms;
				}
			}
			int index=0;
			list<string>::iterator iter = g_priv.begin();
			while(iter != g_priv.end())
			{
				if (!ZkAdaptor::fillAcl(iter->c_str(), acl))
				{
					output(outFd, 2, 0,"msg:  invalid auth %s\n", iter->c_str());
					if (outFd) fclose(outFd);
					return 2;
				}
				AclItem item(acl.id.scheme, acl.id.id);
				if (g_set){
					if (acl_map.find(item) != acl_map.end()){
						acl_map.find(item)->second |= acl.perms;
					}else{
						acl_map[item]=acl.perms;
					}
				}else if (g_append){
					if (acl_map.find(item) != acl_map.end()){
						acl_map.find(item)->second |= acl.perms;
					}else{
						acl_map[item]=acl.perms;
					}
					if (0 == acl_map.find(item)->second) acl_map.erase(item);
				}else{//g_remove=true
					if (acl_map.find(item) == acl_map.end()){
						output(outFd, 2, 0,"msg:  not exist for auth %s\n", iter->c_str());
						if (outFd) fclose(outFd);
						return 2;
					}
					acl_map.find(item)->second &= (~acl.perms);
					if (0 == acl_map.find(item)->second) acl_map.erase(item);
				}
				iter++;
			}
			if (0 == acl_map.size()){
				acls.count = 1;
				acls.data = new struct ACL[acls.count];
				ZkAdaptor::fillAcl("all", acls.data[0]);
			}else{
				acls.count = acl_map.size();
				acls.data = new struct ACL[acls.count];
				map<AclItem, int32_t>::iterator map_iter = acl_map.begin();
				index = 0;
				while(map_iter != acl_map.end())
				{
					acls.data[index].perms = map_iter->second;
					acls.data[index].id.scheme = map_iter->first.m_scheme;
					acls.data[index].id.id = map_iter->first.m_id;
					index++;
					map_iter++;
				}
			}
			pacls = &acls;
		}
		ret = zk.setAcl(g_strNode.c_str(), pacls, g_verion);
		delete [] acls.data;
		if (-1 == ret){
			output(outFd, 2, zk.getErrCode(), "msg:  Failure to set node acl, err=%s\n", zk.getErrMsg());
			if (outFd) fclose(outFd);
			return 2;
		}
		if (0 == ret){
			output(outFd, 2, zk.getErrCode(), NULL, "msg: node doesn't exist\n");
			if (outFd) fclose(outFd);
			return 2;
		}
		output(outFd, 0, 0, "node:  %s\nmsg:  success\n", g_strNode.c_str());
		if (outFd) fclose(outFd);
		return 0;
	}

	output(outFd, 2, 0, NULL, "msg:  Timeout to connect zk\n");
	if (outFd) fclose(outFd);
	return 2;
}
