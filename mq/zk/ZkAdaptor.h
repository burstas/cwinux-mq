
#ifndef __ZK_ADAPTER_H__
#define __ZK_ADAPTER_H__

#include "CwxGlobalMacro.h"
#include "CwxType.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"
#include "CwxCommon.h"
#include "CwxTimeValue.h"

CWINUX_USING_NAMESPACE

extern "C" {
#include "zookeeper.h"
}

class ZooKeeperAdapter
{
	enum{
		ZK_DEF_RECV_TIMEOUT_MILISECOND = 5000  ///<5秒
	};
public:
	///构造函数
	ZooKeeperAdapter(string const& strHost,
		CWX_UINT32 uiRecvTimeout=ZK_DEF_RECV_TIMEOUT_MILISECOND);
	///析构函数
	virtual ~ZooKeeperAdapter(); 
	///init, 0:成功；-1：失败
	int init(ZooLogLevel level=ZOO_LOG_LEVEL_WARN);

	///连接，-1：失败；0：成功
	virtual int connect(const clientid_t *clientid=NULL, int flags=0);
	///关闭连接
	void disconnect();
	///连接建立
	virtual void onConnect(){
	}
	///鉴权失败
	virtual void onFailAuth(){
	}
	///Session失效
	virtual void onExpired(){
	}
	///其他消息
	virtual void onOtherEvent(int type, int state, const char *path){
	}
	///连接是否建立
	bool isConnected(){
		if (m_zkHandle){
			int state = zoo_state (m_zkHandle);
			if (state == ZOO_CONNECTED_STATE) return true;
		} 
		return false;
	}

	/**
	* \brief Creates a new node identified by the given path. 
	* This method will optionally attempt to create all missing ancestors.
	* 
	* @param path the absolute path name of the node to be created
	* @param value the initial value to be associated with the node
	* @param flags the ZK flags of the node to be created
	* @return true if the node has been successfully created; false otherwise
	*/ 
	bool createNode(const string &path, 
		char const* buf,
		CWX_UINT32 uiBufLen, 
		int flags = 0);

	/**
	* \brief Deletes a node identified by the given path.
	* 
	* @param path the absolute path name of the node to be deleted
	* @param version the expected version of the node. The function will 
	*                fail if the actual version of the node does not match 
	*                the expected version
	* 
	* @return true if the node has been deleted; false otherwise
	*/
	bool deleteNode(const string &path,
		int version = -1);

	/**
	* \brief Retrieves list of all children of the given node.
	* 
	* @param path the absolute path name of the node for which to get children
	* @return the list of absolute paths of child nodes, possibly empty
	*/
	bool getNodeChildren( const string &path, list<string>& childs);

	/**
	* \brief Check the existance of path to a znode.
	* 
	* @param path the absolute path name of the znode
	* @return 1; 0:not exist; -1:failure
	*/
	int nodeExists(const string &path);

	/**
	* \brief Gets the given node's data.
	* 
	* @param path the absolute path name of the node to get data from
	* 
	* @return 1:exist; 0:not exist; -1:failure
	*/
	int getNodeData(const string &path, char* buf, CWX_UINT32& uiBufLen);

	/**
	* \brief Sets the given node's data.
	* 
	* @param path the absolute path name of the node to get data from
	* @param value the node's data to be set
	* @param version the expected version of the node. The function will 
	*                fail if the actual version of the node does not match 
	*                the expected version
	* 
	* @return 1:success; 0:not exist; -1: failure
	*/
	int setNodeData(const string &path, char const* buf, CWX_UINT32 uiBufLen, int version = -1);

	/**
	* \brief Validates the given path to a node in ZK.
	* 
	* @param the path to be validated
	* 
	* @return true:valid; false:not valid  if the given path is not valid
	*        (for instance it doesn't start with "/")
	*/
	bool validatePath(const string &path);

	///get handle
	zhandle_t* getZkHandle() { return m_zkHandle;}
	///get client id
	const clientid_t * getClientId() { return  isConnected()?zoo_client_id(m_zkHandle):NULL;}
	///get context
	const void * getContext() { return isConnected()?zoo_get_context(m_zkHandle):NULL;}
	/// get error code
	int  getErrCode() const { return m_iErrCode;}
	/// get error msg
	char const* getErrMsg() const { return m_szErr2K;}


private:
	static void watcher(zhandle_t *zzh, int type, int state, const char *path,
		void* context);
	static const char* state2String(int state);
private:

private:
	///The host addresses of ZK nodes.
	string       m_strHost;
	CWX_UINT32   m_uiRecvTimeout;
	///The current ZK session.
	zhandle_t*   m_zkHandle;
	///Err code
	int           m_iErrCode;
	///Err msg
	char          m_szErr2K[2048];
};

#endif /* __ZK_ADAPTER_H__ */
