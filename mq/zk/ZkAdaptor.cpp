#include "ZkAdaptor.h"
#include <string.h>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <sys/select.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>

void ZkAdaptor::authCompletion(int rc, const void *data)
{
	ZkAdaptor* zk=(ZkAdaptor*)data;
	if (ZOK == rc){
		zk->m_iAuthState = AUTH_STATE_SUCCESS;
	}else{
		zk->m_iAuthState = AUTH_STATE_FAIL;
	}
}


ZkAdaptor::ZkAdaptor(string const& strHost, CWX_UINT32 uiRecvTimeout)
{
	m_strHost = strHost;
	m_uiRecvTimeout = uiRecvTimeout;
	m_iAuthState = AUTH_STATE_SUCCESS;
	m_zkHandle = NULL;
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
}


ZkAdaptor::~ZkAdaptor()
{
	disconnect();
}


int ZkAdaptor::init(ZooLogLevel level)
{
	zoo_set_debug_level(level);
	return 0;
}


void ZkAdaptor::watcher(zhandle_t *, int type, int state, const char *path,
			 void* context)
{
	ZkAdaptor* adapter=(ZkAdaptor*)context;

	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE){
			return adapter->onConnect();
		} else if (state == ZOO_AUTH_FAILED_STATE) {
			return adapter->onFailAuth();
		} else if (state == ZOO_EXPIRED_SESSION_STATE) {
			return adapter->onExpired();
		} else if (state == ZOO_CONNECTING_STATE){
			return adapter->onConnecting();
		} else if (state == ZOO_ASSOCIATING_STATE){
			return adapter->onAssociating();
		}
	}else if (type == ZOO_CREATED_EVENT){
		return adapter->onNodeCreated(state, path);
	}else if (type == ZOO_DELETED_EVENT){
		return adapter->onNodeDeleted(state, path);
	}else if (type == ZOO_CHANGED_EVENT){
		return adapter->onNodeChanged(state, path);
	}else if (type == ZOO_CHILD_EVENT){
		return adapter->onNodeChildChanged(state, path);
	}else if (type == ZOO_NOTWATCHING_EVENT){
		return adapter->onNoWatching(state, path);
	}
	adapter->onOtherEvent(type, state, path);
}


int ZkAdaptor::connect(const clientid_t *clientid, int flags)
{
	// Clear the connection state
	disconnect();

	m_iAuthState = AUTH_STATE_SUCCESS;
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

	// Establish a new connection to ZooKeeper
	m_zkHandle = zookeeper_init(m_strHost.c_str(), 
		ZkAdaptor::watcher, 
		m_uiRecvTimeout,
		clientid,
		this,
		flags);

	if (m_zkHandle == NULL)
	{
		CwxCommon::snprintf(m_szErr2K, 2047, "Unable to connect to ZK running at '%s'", m_strHost.c_str());
		return -1;
	}
	return 0;
}


void ZkAdaptor::disconnect()
{
	if (m_zkHandle != NULL)
	{
		zookeeper_close (m_zkHandle);
		m_zkHandle = NULL;
	}
}

///node创建事件
void ZkAdaptor::onNodeCreated(int , char const* )
{
}

///node删除事件
void ZkAdaptor::onNodeDeleted(int , char const* )
{
}

///node修改事件
void ZkAdaptor::onNodeChanged(int , char const* )
{
}

///node child修改事件
void ZkAdaptor::onNodeChildChanged(int , char const* )
{
}

///node 不再watch事件
void ZkAdaptor::onNoWatching(int , char const* )
{
}	

void ZkAdaptor::onOtherEvent(int , int , const char *)
{
}


bool ZkAdaptor::addAuth(const char* scheme, const char* cert, int certLen, CWX_UINT32 timeout)
{
	int rc;
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return false;
	}

	m_iAuthState = AUTH_STATE_WAITING;
	rc = zoo_add_auth(m_zkHandle, scheme, cert, certLen, ZkAdaptor::authCompletion, this);
	if (rc != ZOK) // check return status
	{
		m_iErrCode = rc;
		m_iAuthState = AUTH_STATE_FAIL;
		CwxCommon::snprintf(m_szErr2K, 2047, "Error in auth , err:%s, err-code:%d.", zerror(rc), rc);
		return false;
	}
	int delay=5;
	while (timeout)
	{
		if (timeout < 5)
		{
			delay = timeout;
		}
		ZkAdaptor::sleep(delay);
		timeout -= delay;
		if (AUTH_STATE_WAITING == getAuthState())
		{
			continue;
		}
		else if (AUTH_STATE_FAIL == getAuthState())
		{
			strcpy(m_szErr2K, "failure to auth.");
			return false;
		}
		return true;
	}
	strcpy(m_szErr2K, "add auth timeout.");
	return false;
}


int ZkAdaptor::createNode(const string &path, 
						   char const* data,
						   CWX_UINT32 dataLen,
						   const struct ACL_vector *acl,
						   int flags,
						   char* pathBuf,
						   CWX_UINT32 pathBufLen)
{
	const int MAX_PATH_LENGTH = 2048;
	char realPath[MAX_PATH_LENGTH];
	realPath[0] = 0;
	int rc;
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	if (!pathBuf)
	{
		pathBuf = realPath;
		pathBufLen = MAX_PATH_LENGTH;
	}
	rc = zoo_create( m_zkHandle, 
		path.c_str(), 
		data,
		dataLen,
		acl?acl:&ZOO_OPEN_ACL_UNSAFE,
		flags,
		realPath,
		MAX_PATH_LENGTH);

	if (rc != ZOK) // check return status
	{
		m_iErrCode = rc;
		if (ZNODEEXISTS == rc) return 0;
		CwxCommon::snprintf(m_szErr2K, 2047, "Error in creating ZK node [%s], err:%s err-code:%d.", path.c_str(), zerror(rc), rc);
		return -1;
	}
	return 1;
}


int ZkAdaptor::deleteNode(const string &path,
						   bool recursive,
						   int version)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
	
	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	int rc;
	rc = zoo_delete( m_zkHandle, path.c_str(), version);

	if (rc != ZOK) //check return status
	{
		m_iErrCode = rc;
		if (ZNONODE == rc) return 0;
		if ((rc == ZNOTEMPTY) && recursive)
		{
			list<string> childs;
			if (!getNodeChildren(path, childs)) return false;
			string strPath;
			list<string>::iterator iter=childs.begin();
			while(iter != childs.end())
			{
				strPath = path + "/" + *iter;
				if (!deleteNode(strPath, true)) return false;
				iter++;
			}
			return deleteNode(path);
		}
		CwxCommon::snprintf(m_szErr2K, 2047, "Unable to delete zk node [%s], err:%s  err-code=%d", path.c_str(), zerror(rc), rc);
		return -1;
	}
	return 1;
}


int ZkAdaptor::getNodeChildren( const string &path, list<string>& childs, , int watch)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	String_vector children;
	memset( &children, 0, sizeof(children) );
	int rc;
	rc = zoo_get_children( m_zkHandle,
		path.c_str(), 
		watch,
		&children );

	if (rc != ZOK) // check return code
	{
		m_iErrCode = rc;
		if (rc == ZNONODE) return 0;
		CwxCommon::snprintf(m_szErr2K, 2047, "Failure to get node [%s] child, err:%s err-code:%d", path.c_str(), zerror(rc), rc);
		return false;
	}
	childs.clear();
	for (int i = 0; i < children.count; ++i)
	{
		childs.push_back(string(children.data[i]));
	}
	return true;
}

int ZkAdaptor::nodeExists(const string &path, struct Stat& stat, int watch)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	memset(&stat, 0, sizeof(stat) );
	int rc;
	rc = zoo_exists( m_zkHandle,
		path.c_str(),
		watch,
		&stat);
	if (rc != ZOK)
	{
		if (rc == ZNONODE) return 0;
		m_iErrCode = rc;
		CwxCommon::snprintf(m_szErr2K, 2047, "Error in checking existance of [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
		return -1;
	}
	return 1;
}

int ZkAdaptor::getNodeData(const string &path, char* data, CWX_UINT32& dataLen, struct Stat& stat, int watch)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	memset(&stat, 0, sizeof(stat) );

	int rc = 0;
	int len = dataLen;
	rc = zoo_get( m_zkHandle,
		path.c_str(),
		watch,
		data,
		&len,
		&stat);
	if (rc != ZOK) // checl return code
	{
		m_iErrCode = rc;
		if (rc == ZNONODE) return 0;
		CwxCommon::snprintf(m_szErr2K, 2047, "Error in fetching value of [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
		return -1;
	}
	dataLen = len;
	data[len] = 0x00;
	return 1;
}


int ZkAdaptor::setNodeData(const string &path, char const* data, CWX_UINT32 dataLen, int version)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	int rc;
	rc = zoo_set( m_zkHandle,
		path.c_str(),
		data,
		dataLen,
		version);

	if (rc != ZOK) // check return code
	{
		m_iErrCode = rc;
		if (rc == ZNONODE) return 0;
		
		CwxCommon::snprintf(m_szErr2K, 2047, "Error in set value of [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
		return -1;
	}
	// success
	return 1;
}

int ZkAdaptor::getAcl(const char *path, struct ACL_vector& acl, struct Stat& stat)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	int rc;
	memset(&stat, 0x00, sizeof(stat));
	rc = zoo_get_acl( m_zkHandle,
		path.c_str(),
		&acl,
		&stat);

	if (rc != ZOK) // check return code
	{
		m_iErrCode = rc;
		if (rc == ZNONODE) return 0;

		CwxCommon::snprintf(m_szErr2K, 2047, "Error in get acl for [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
		return -1;
	}
	// success
	return 1;
}

int ZkAdaptor::setAcl(const char *path, const struct ACL_vector *acl, int version)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	int rc;
	rc = zoo_set_acl( m_zkHandle,
		path.c_str(),
		version,
		acl?acl:&ZOO_OPEN_ACL_UNSAFE);

	if (rc != ZOK) // check return code
	{
		m_iErrCode = rc;
		if (rc == ZNONODE) return 0;

		CwxCommon::snprintf(m_szErr2K, 2047, "Error in set acl for [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
		return -1;
	}
	// success
	return 1;
}


void ZkAdaptor::sleep(CWX_UINT32 miliSecond)
{
	struct timeval tv;
	tv.tv_sec = miliSecond/1000;
	tv.tv_usec = (miliSecond%1000)*1000;
	select(1, NULL, NULL, NULL, &tv);
}

char* ZkAdaptor::base64(const unsigned char *input, int length)
{
	BIO *bmem, *b64;
	BUF_MEM *bptr;

	b64 = BIO_new(BIO_f_base64());
	bmem = BIO_new(BIO_s_mem());
	b64 = BIO_push(b64, bmem);
	BIO_write(b64, input, length);
	BIO_flush(b64);
	BIO_get_mem_ptr(b64, &bptr);

	char *buff = (char *)malloc(bptr->length);
	memcpy(buff, bptr->data, bptr->length-1);
	buff[bptr->length-1] = 0;
	BIO_free_all(b64);
	return buff;
}

void ZkAdaptor::sha1(char* input, int length, unsigned char *output)
{
	SHA_CTX   c;
	SHA1_Init(&c);
	SHA1_Update(&c, input, length);
	SHA1_Final(output, &c);
}

char* ZkAdaptor::digest(char* input, int length)
{
	unsigned char output[20];
	sha1(input, length, output);
	return base64(output, 20);
}

