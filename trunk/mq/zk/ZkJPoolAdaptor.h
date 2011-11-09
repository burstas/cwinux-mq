
#ifndef __ZK_JPOOL_ADAPTER_H__
#define __ZK_JPOOL_ADAPTER_H__

#include "ZkAdaptor.h"

class ZkJPoolAdapter:public ZkAdapter
{
public:
	///构造函数
	ZkJPoolAdapter(string const& strHost,
		CWX_UINT32 uiRecvTimeout=ZK_DEF_RECV_TIMEOUT_MILISECOND);
	///析构函数
	virtual ~ZkJPoolAdapter(); 
	///连接建立
	virtual void onConnect(){
		printf("Success to connect %s\n", m_strHost.c_str());
	}
	///鉴权失败
	virtual void onFailAuth(){
		printf("Failure auth to %s\n", m_strHost.c_str());
	}
	///Session失效
	virtual void onExpired(){
		printf("Session expired for %s\n", m_strHost.c_str());
	}
	///其他消息
	virtual void onOtherEvent(int type, int state, const char *path){
		printf("Recv event for %s, type=%d  state=%d  path=%s\n", m_strHost.c_str(), type, state, path);
	}
};

#endif /* __ZK_ADAPTER_H__ */
