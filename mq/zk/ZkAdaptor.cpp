#include "ZkAdaptor.h"
#include <string.h>
#include <sstream>
#include <iostream>
#include <algorithm>


ZooKeeperAdapter::ZooKeeperAdapter(string const& strHost, CWX_UINT32 uiRecvTimeout):
{
	m_strHost = strHost;
	m_uiRecvTimeout = uiRecvTimeout;
	m_zkHandle = NULL;
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
}

ZooKeeperAdapter::~ZooKeeperAdapter()
{
	disconnect();
}

int ZooKeeperAdapter::init(ZooLogLevel level)
{
	zoo_set_debug_level(level);
	return 0;
}

const char* ZooKeeperAdapter::state2String(int state)
{
	if (state == 0)
		return "CLOSED_STATE";
	if (state == ZOO_CONNECTING_STATE)
		return "CONNECTING_STATE";
	if (state == ZOO_ASSOCIATING_STATE)
		return "ASSOCIATING_STATE";
	if (state == ZOO_CONNECTED_STATE)
		return "CONNECTED_STATE";
	if (state == ZOO_EXPIRED_SESSION_STATE)
		return "EXPIRED_SESSION_STATE";
	if (state == ZOO_AUTH_FAILED_STATE)
		return "AUTH_FAILED_STATE";

	return "INVALID_STATE";
}


void ZooKeeperAdapter::watcher(zhandle_t *zzh, int type, int state, const char *path,
			 void* context)
{
	ZooKeeperAdapter* adapter=(ZooKeeperAdapter*)context;

	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE){
			adapter->onConnect();
		} else if (state == ZOO_AUTH_FAILED_STATE) {
			adapter->onFailAuth();
		} else if (state == ZOO_EXPIRED_SESSION_STATE) {
			adapter->onExpired();
		}
	}
	onOtherEvent(type, state, path);
}

int ZooKeeperAdapter::connect(CWX_UINT32 uiConnTimeout, const clientid_t *clientid, int flags)
{
	// Clear the connection state
	disconnect();

	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

	// Establish a new connection to ZooKeeper
	m_zkHandle = zookeeper_init(m_strHost.c_str(), 
		ZooKeeperAdapter::watcher, 
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

void ZooKeeperAdapter::disconnect()
{
	if (m_zkHandle != NULL)
	{
		zookeeper_close (m_zkHandle);
		m_zkHandle = NULL;
	}
}

bool ZooKeeperAdapter::createNode(const string &path, 
								  const string &value, 
								  int flags)
{
	const int MAX_PATH_LENGTH = 2048;
	char realPath[MAX_PATH_LENGTH];
	realPath[0] = 0;
	int rc;
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
	if (!validatePath(path)) return false;

	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return false;
	}
	rc = zoo_create( m_zkHandle, 
		path.c_str(), 
		value.c_str(),
		value.length(),
		&ZOO_OPEN_ACL_UNSAFE,
		flags,
		realPath,
		MAX_PATH_LENGTH);

	if (rc != ZOK) // check return status
	{
		m_iErrCode = rc;
		if (rc == ZNODEEXISTS)
		{
			//the node already exists
			CwxCommon::snprintf(m_szErr2K, 2047, "ZK node [%s] already exists.", path.c_str());
		}
		else if (rc == ZNONODE)
		{
			//the node not exists
			CwxCommon::snprintf(m_szErr2K, 2047, "ZK node [%s] doesn't exist.", path.c_str());
		}
		else
		{
			CwxCommon::snprintf(m_szErr2K, 2047, "Error in creating ZK node [%s], err-code:%d.", path.c_str(), rc);
		}
		return false;
	}
	return true;
}

bool ZooKeeperAdapter::deleteNode(const string &path,
								  int version)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
	// Validate the zk path
	if (!validatePath(path)) return false;
	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return false;
	}

	int rc;
	rc = zoo_delete( m_zkHandle, path.c_str(), version);

	if (rc != ZOK) //check return status
	{
		m_iErrCode = rc;
		if (rc == ZNONODE)
		{
			CwxCommon::snprintf(m_szErr2K, 2047, "ZK Node [%s] doesn't exist.", path.c_str());
		}
		else if (rc == ZNOTEMPTY)
		{
			CwxCommon::snprintf(m_szErr2K, 2047, "ZK Node [%s] not empty", path.c_str());
		}
		else
		{
			CwxCommon::snprintf(m_szErr2K, 2047, "Unable to delete zk node [%s], err-code=%d", path.c_str(), rc);
		}
		return false;
	}
	return true;
}

bool ZooKeeperAdapter::getNodeChildren( const string &path, list<string>& childs)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
	// Validate the zk path
	if (!validatePath(path)) return false;
	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return false;
	}
	String_vector children;
	memset( &children, 0, sizeof(children) );
	int rc;
	rc = zoo_get_children( m_zkHandle,
		path.c_str(), 
		0,
		&children );
	if (rc != ZOK) // check return code
	{
		m_iErrCode = rc;
		CwxCommon::snprintf(m_szErr2K, 2047, "Failure to get node [%s] child, err-code=%d", path.c_str(), rc);
		return false;
	}
	childs.clear();
	for (int i = 0; i < children.count; ++i)
	{
		childs.push_back(string(children.data[i]));
	}
	return true;
}

int ZooKeeperAdapter::nodeExists(const string &path)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
	// Validate the zk path
	if (!validatePath(path)) return -1;
	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	struct Stat tmpStat;
	struct Stat* stat = &tmpStat;
	memset( stat, 0, sizeof(Stat) );
	int rc;
	rc = zoo_exists( m_zkHandle,
		path.c_str(),
		0,
		stat);
	if (rc != ZOK)
	{
		if (rc == ZNONODE) return 0;
		m_iErrCode = rc;
		CwxCommon::snprintf(m_szErr2K, 2047, "Error in checking existance of [%s], err-code=%d", path.c_str(), rc);
		return -1;
	}
	return 1;
}

int ZooKeeperAdapter::getNodeData(const string &path, char* buf, CWX_UINT32& uiBufLen)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
	// Validate the zk path
	if (!validatePath(path)) return -1;
	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}

	struct Stat tmpStat;
	struct Stat* stat = &tmpStat;
	memset( stat, 0, sizeof(Stat) );

	int rc = 0;
	int len = uiBufLen;
	rc = zoo_get( m_zkHandle,
		path.c_str(),
		0,
		buf, &len, stat );
	if (rc != ZOK) // checl return code
	{
		m_iErrCode = rc;
		if (rc == ZNONODE) return 0;
		CwxCommon::snprintf(m_szErr2K, 2047, "Error in fetching value of [%s], err-code=%d", path.c_str(), rc);
		return -1;
	}
	uiBufLen = len;
	buf[len] = 0x00;
	return 1;
}


int ZooKeeperAdapter::setNodeData(const string &path, char const* buf, CWX_UINT32 uiBufLen, int version)
{
	m_iErrCode = 0;
	memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
	// Validate the zk path
	if (!validatePath(path)) return -1;
	if (!isConnected())
	{
		strcpy(m_szErr2K, "No connect");
		return -1;
	}
	int rc;
	rc = zoo_set( m_zkHandle,
		path.c_str(),
		buf,
		uiBufLen,
		version);
	if (rc != ZOK) // check return code
	{
		m_iErrCode = rc;
		if (rc == ZNONODE) return 0;
		CwxCommon::snprintf(m_szErr2K, 2047, "Error in set value of [%s], err-code=%d", path.c_str(), rc);
		return -1;
	}
	// success
	return 1;
}

bool ZooKeeperAdapter::validatePath(const string &path)
{
	m_iErrCode = 0;
	if (path.find ("/") != 0)
	{
		CwxCommon::snprintf(m_szErr2K, 2047, "Node path must start with '/' but it was '%s'", path.c_str());
		return false;
	}
	if (path.length() > 1)
	{
		if (path.rfind ("/") == path.length() - 1)
		{
			CwxCommon::snprintf(m_szErr2K, 2047, "Node path must not end with '/' but it was '%s'", path.c_str());
			return false;
		}
		if (path.find( "//" ) != string::npos)
		{
			CwxCommon::snprintf(m_szErr2K, 2047, "Node path must not contain '//',  but it was '%s'", path.c_str());
			return false;
		}
	}
	return true;
}


