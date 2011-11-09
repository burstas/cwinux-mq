#ifndef __ZK_ADAPTOR_H__
#define __ZK_ADAPTOR_H__

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

class ZkAdaptor
{
public:
	enum{
		ZK_DEF_RECV_TIMEOUT_MILISECOND = 5000  ///<5��
	};
	enum{
		AUTH_STATE_WAITING = 0,
		AUTH_STATE_FAIL = 1,
		AUTH_STATE_SUCCESS = 2
	};
public:
	///���캯��
	ZkAdaptor(string const& strHost,
		CWX_UINT32 uiRecvTimeout=ZK_DEF_RECV_TIMEOUT_MILISECOND);
	///��������
	virtual ~ZkAdaptor(); 
	///init, 0:�ɹ���-1��ʧ��
	int init(ZooLogLevel level=ZOO_LOG_LEVEL_WARN);

	///���ӣ�-1��ʧ�ܣ�0���ɹ�
	virtual int connect(const clientid_t *clientid=NULL, int flags=0);
	///�ر�����
	void disconnect();
	///���ӽ���
	virtual void onConnect(){
	}
	///���ڽ�����ϵ
	virtual void onAssociating(){
	}
	///���ڽ�������
	virtual void onConnecting(){

	}
	///��Ȩʧ��
	virtual void onFailAuth(){
	}
	///SessionʧЧ
	virtual void onExpired(){
	}
	///node�����¼�
	virtual void onNodeCreated(int state, char const* path);
	///nodeɾ���¼�
	virtual void onNodeDeleted(int state, char const* path);
	///node�޸��¼�
	virtual void onNodeChanged(int state, char const* path);
	///node child�޸��¼�
	virtual void onNodeChildChanged(int state, char const* path);
	///node ����watch�¼�
	virtual void onNoWatching(int state, char const* path);
	///������Ϣ
	virtual void onOtherEvent(int type, int state, const char *path);
	///�����Ƿ���
	bool isConnected(){
		if (m_zkHandle){
			int state = zoo_state (m_zkHandle);
			if (state == ZOO_CONNECTED_STATE) return true;
		} 
		return false;
	}
	///���Ӹ�Ȩ
	bool addAuth(const char* scheme, const char* cert, int certLen);
	///��ȡ��Ȩ״̬
	int getAuthState() const { return m_iAuthState;}

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
		bool recursive = false,
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
	int getNodeData(const string &path, char* buf, CWX_UINT32& uiBufLen, struct Stat& stat);

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

	///get host
	string const& getHost() const { return m_strHost;}
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
public:
	static void sleep(CWX_UINT32 uiMiliSecond);

private:
	static void watcher(zhandle_t *zzh, int type, int state, const char *path,
		void* context);
	static void authCompletion(int rc, const void *data);
private:

private:
	///The host addresses of ZK nodes.
	string       m_strHost;
	CWX_UINT32   m_uiRecvTimeout;
	///���auth
	int  		 m_iAuthState;
	///The current ZK session.
	zhandle_t*   m_zkHandle;
	///Err code
	int           m_iErrCode;
	///Err msg
	char          m_szErr2K[2048];
};

#endif /* __ZK_ADAPTER_H__ */