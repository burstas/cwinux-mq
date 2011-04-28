#ifndef __CWX_DISPATCH_MACRO_H__
#define __CWX_DISPATCH_MACRO_H__
/*
版权声明：
    本软件遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
/**
@file CwxMqMacro.h
@brief MQ系列服务的宏定义文件。
@author cwinux@gmail.com
@version 1.0
@date 2010-09-23
@warning
@bug
*/
#include "CwxGlobalMacro.h"
#include "CwxType.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"

CWINUX_USING_NAMESPACE


///通信的key定义
#define CWX_MQ_DATA "data"
#define CWX_MQ_TYPE "type"
#define CWX_MQ_ATTR "attr"
#define CWX_MQ_RET  "ret"
#define CWX_MQ_SID  "sid"
#define CWX_MQ_ERR  "err"
#define CWX_MQ_BLOCK "block"
#define CWX_MQ_TIMESTAMP  "timestamp"
#define CWX_MQ_USER  "user"
#define CWX_MQ_PASSWD "passwd"
#define CWX_MQ_SUBSCRIBE "subscribe"
#define CWX_MQ_QUEUE "queue"
#define CWX_MQ_GROUP "group"
#define CWX_MQ_CHUNK "chunk"
#define CWX_MQ_M     "m"
#define CWX_MQ_WINDOW "window"

///错误代码定义
#define CWX_MQ_SUCCESS          0  ///<成功
#define CWX_MQ_NO_MSG           1   ///<没有数据
#define CWX_MQ_INVALID_MSG      2 ///<接收到的数据包无效，也就是不是kv结构
#define CWX_MQ_BINLOG_INVALID   3///<接收到的binlog数据无效
#define CWX_MQ_NO_KEY_DATA       4 ///<接收到的binlog，没有【data】的key
#define CWX_MQ_INVALID_DATA_KV   5 ///<data的可以为key/value，但格式非法
#define CWX_MQ_NO_SID            6 ///<接收到的report数据包中，没有【sid】的key
#define CWX_MQ_NO_RET            7 ///<接收到的数据包中，没有【ret】
#define CWX_MQ_NO_ERR            8 ///<接收到的数据包中，没有【err】
#define CWX_MQ_NO_TIMESTAMP      9 ///<接收到的数据中，没有【timestamp】
#define CWX_MQ_FAIL_AUTH         10 ///<鉴权失败
#define CWX_MQ_INVALID_BINLOG_TYPE 11 ///<binlog的type错误
#define CWX_MQ_INVALID_MSG_TYPE   12 ///<接收到的消息类型无效
#define CWX_MQ_INVALID_SID        13  ///<回复的sid无效
#define CWX_MQ_FAIL_ADD_BINLOG    14 ///<往binglog mgr中添加binlog失败
#define CWX_MQ_NO_QUEUE        15 ///<队列不存在
#define CWX_MQ_INVALID_SUBSCRIBE 16 ///<无效的消息订阅类型
#define CWX_MQ_INNER_ERR        17 ///<其他内部错误，一般为内存

#define CWX_MQ_PROXY_NO_AUTH_GROUP    100 ///<消息的group没有被允许
#define CWX_MQ_PROXY_FORBID_GROUP     101 ///<消息的group被禁止
#define CWX_MQ_PROXY_NO_AUTH          102 ///<消息的group被禁止
#define CWX_MQ_PROXY_TIMEOUT          103 ///<发送超时
#define CWX_MQ_PROXY_MQ_INVALID       104 ///<mq服务不可用
#endif
