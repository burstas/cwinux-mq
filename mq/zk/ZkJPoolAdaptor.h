#ifndef __ZK_JPOOL_ADAPTOR_H__
#define __ZK_JPOOL_ADAPTOR_H__

#include "ZkAdaptor.h"

class ZkJPoolAdaptor:public ZkAdaptor
{
public:
	///���캯��
	ZkJPoolAdaptor(string const& strHost,
		CWX_UINT32 uiRecvTimeout=ZK_DEF_RECV_TIMEOUT_MILISECOND);
	///��������
	virtual ~ZkJPoolAdaptor(); 
	///���ӽ���
	virtual void onConnect(){
		printf("Success to connect %s\n", getHost().c_str());
	}
	///��Ȩʧ��
	virtual void onFailAuth(){
		printf("Failure auth to %s\n", getHost().c_str());
	}
	///SessionʧЧ
	virtual void onExpired(){
		printf("Session expired for %s\n", getHost().c_str());
	}
	///������Ϣ
	virtual void onOtherEvent(int type, int state, const char *path){
		printf("Recv event for %s, type=%d  state=%d  path=%s\n", getHost().c_str(), type, state, path);
	}
};

#endif /* __ZK_ADAPTER_H__ */