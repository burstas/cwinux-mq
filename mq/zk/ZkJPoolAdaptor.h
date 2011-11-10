#ifndef __ZK_JPOOL_ADAPTOR_H__
#define __ZK_JPOOL_ADAPTOR_H__

#include "ZkAdaptor.h"

class ZkJPoolAdaptor:public ZkAdaptor
{
public:
	///构造函数
	ZkJPoolAdaptor(string const& strHost,
		CWX_UINT32 uiRecvTimeout=ZK_DEF_RECV_TIMEOUT_MILISECOND);
	///析构函数
	virtual ~ZkJPoolAdaptor(); 
	///连接建立的回调函数，有底层的zk线程调用
	virtual void onConnect()
	{
		printf("Success to connect %s\n", getHost().c_str());
	}

	///正在建立联系的回调函数，有底层的zk线程调用
	virtual void onAssociating()
	{
		printf("Associating to zk %s\n", getHost().c_str());
	}
	///正在建立连接的回调函数，有底层的zk线程调用
	virtual void onConnecting()
	{
		printf("Connecting to zk %s\n", getHost().c_str());
	}

	///鉴权失败的回调函数，有底层的zk线程调用
	virtual void onFailAuth()
	{
		printf("Failure auth to zk %s\n", getHost().c_str());
	}

	///Session失效的回调函数，有底层的zk线程调用
	virtual void onExpired()
	{
		printf("Expired to zk %s\n", getHost().c_str());
	}
	/**
	*@brief  watch的node创建事件的回调函数，有底层的zk线程调用。应用于zoo_exists的watch。
	*@param [in] zk的watcher的state
	*@param [in] path watch的path.
	*@return void.
	*/
	virtual void onNodeCreated(int state, char const* path)
	{
		printf("Node[%s] is created for zk %s, state=%d\n", path, getHost().c_str(), state);
	}

	/**
	*@brief  watch的node删除事件的回调函数，有底层的zk线程调用。应用于 zoo_exists与zoo_get的watch。
	*@param [in] zk的watcher的state
	*@param [in] path watch的path.
	*@return void.
	*/
	virtual void onNodeDeleted(int state, char const* path)
	{
		printf("Node[%s] is deleted for zk %s, state=%d\n", path, getHost().c_str(), state);
	}

	/**
	*@brief  watch的node修改事件的回调函数，有底层的zk线程调用。应用于 zoo_exists与zoo_get的watch。
	*@param [in] zk的watcher的state
	*@param [in] path watch的path.
	*@return void.
	*/
	virtual void onNodeChanged(int state, char const* path)
	{
		printf("Node[%s] is changed for zk %s, state=%d\n", path, getHost().c_str(), state);
	}

	/**
	*@brief  watch的node孩子变更事件的回调函数，有底层的zk线程调用。应用于zoo_get_children的watch。
	*@param [in] zk的watcher的state
	*@param [in] path watch的path.
	*@return void.
	*/
	virtual void onNodeChildChanged(int state, char const* path)
	{
		printf("Node[%s]'s child is changed for zk %s, state=%d\n", path, getHost().c_str(), state);
	}

	/**
	*@brief  zk取消某个wathc的通知事件的回调函数，有底层的zk线程调用。
	*@param [in] zk的watcher的state
	*@param [in] path watch的path.
	*@return void.
	*/
	virtual void onNoWatching(int state, char const* path)
	{
		printf("Node[%s]'s watch is removed for zk %s, state=%d\n", path, getHost().c_str(), state);
	}

	///其他消息
	virtual void onOtherEvent(int type, int state, const char *path){
		printf("Recv event for %s, type=%d  state=%d  path=%s\n", getHost().c_str(), type, state, path);
	}
};

#endif /* __ZK_ADAPTER_H__ */
