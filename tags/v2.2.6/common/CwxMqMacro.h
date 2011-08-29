#ifndef __CWX_DISPATCH_MACRO_H__
#define __CWX_DISPATCH_MACRO_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
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
#define CWX_MQ_SIGN   "sign"
#define CWX_MQ_CRC32  "crc32"
#define CWX_MQ_MD5    "md5"
#define CWX_MQ_NAME   "name"
#define CWX_MQ_AUTH_USER "auth_user"
#define CWX_MQ_AUTH_PASSWD "auth_passwd"
#define CWX_MQ_COMMIT  "commit"
#define CWX_MQ_TIMEOUT "timeout"
#define CWX_MQ_DEF_TIMEOUT "def_timeout"
#define CWX_MQ_MAX_TIMEOUT "max_timeout"
#define CWX_MQ_UNCOMMIT "uncommit"
#define CWX_MQ_ZIP     "zip"
#define CWX_MQ_DELAY   "delay"
///错误代码定义
#define CWX_MQ_ERR_SUCCESS          0  ///<成功
#define CWX_MQ_ERR_NO_MSG           1   ///<没有数据
#define CWX_MQ_ERR_INVALID_MSG      2 ///<接收到的数据包无效，也就是不是kv结构
#define CWX_MQ_ERR_BINLOG_INVALID   3///<接收到的binlog数据无效
#define CWX_MQ_ERR_NO_KEY_DATA       4 ///<接收到的binlog，没有【data】的key
#define CWX_MQ_ERR_INVALID_DATA_KV   5 ///<data的可以为key/value，但格式非法
#define CWX_MQ_ERR_NO_SID            6 ///<接收到的report数据包中，没有【sid】的key
#define CWX_MQ_ERR_NO_RET            7 ///<接收到的数据包中，没有【ret】
#define CWX_MQ_ERR_NO_ERR            8 ///<接收到的数据包中，没有【err】
#define CWX_MQ_ERR_NO_TIMESTAMP      9 ///<接收到的数据中，没有【timestamp】
#define CWX_MQ_ERR_FAIL_AUTH         10 ///<鉴权失败
#define CWX_MQ_ERR_INVALID_BINLOG_TYPE 11 ///<binlog的group错误，不能为0xFFFFFFFF
#define CWX_MQ_ERR_INVALID_MSG_TYPE   12 ///<接收到的消息类型无效
#define CWX_MQ_ERR_INVALID_SID        13  ///<回复的sid无效
#define CWX_MQ_ERR_FAIL_ADD_BINLOG    14 ///<往binglog mgr中添加binlog失败
#define CWX_MQ_ERR_NO_QUEUE        15 ///<队列不存在
#define CWX_MQ_ERR_INVALID_SUBSCRIBE 16 ///<无效的消息订阅类型
#define CWX_MQ_ERR_INNER_ERR        17 ///<其他内部错误，一般为内存
#define CWX_MQ_ERR_INVALID_MD5      18 ///<MD5校验失败
#define CWX_MQ_ERR_INVALID_CRC32    19 ///<CRC32校验失败
#define CWX_MQ_ERR_NO_NAME          20 ///<没有name字段
#define CWX_MQ_ERR_TIMEOUT          21 ///<commit队列类型的消息commit超时
#define CWX_MQ_ERR_INVALID_COMMIT   22 ///<commit命令无效
#define CWX_MQ_ERR_USER_TO0_LONG     23 ///<队列的用户名太长
#define CWX_MQ_ERR_PASSWD_TOO_LONG   24 ///<队列的口令太长
#define CWX_MQ_ERR_NAME_TOO_LONG   25 ///<队列名字太长
#define CWX_MQ_ERR_SCRIBE_TOO_LONG   26 ///<队列订阅表达式太长
#define CWX_MQ_ERR_NAME_EMPTY        27 ///<队列的名字为空
#define CWX_MQ_ERR_QUEUE_EXIST       28 ///<队列存在
#define CWX_MQ_ERR_LOST_SYNC         29 ///<失去了同步状态
#define CWX_MQ_ERR_INVALID_QUEUE_NAME 30 ///<无效的队列名字，必须为[a-z,A-Z,0-9,-,_]


#define CWX_MQ_PROXY_NO_AUTH_GROUP    100 ///<消息的group没有被允许
#define CWX_MQ_PROXY_FORBID_GROUP     101 ///<消息的group被禁止
#define CWX_MQ_PROXY_NO_AUTH          102 ///<消息的group被禁止
#define CWX_MQ_PROXY_TIMEOUT          103 ///<发送超时
#define CWX_MQ_PROXY_MQ_INVALID       104 ///<mq服务不可用



#define CWX_MQ_MIN_TIMEOUT_SECOND     1 ///<最小的超时秒数
#define CWX_MQ_MAX_TIMEOUT_SECOND     1800 ///<最大的超时秒数
#define CWX_MQ_DEF_TIMEOUT_SECOND     5  ///<缺省的超时秒数

#define CWX_MQ_MAX_QUEUE_NAME_LEN        64 ///<最大队列名长度
#define CWX_MQ_MAX_QUEUE_USER_LEN        64 ///<最大的队列用户长度
#define CWX_MQ_MAX_QUEUE_PASSWD_LEN      64 ///<最大的用户口令长度
#define CWX_MQ_MAX_QUEUE_SCRIBE_LEN      800 ///<最大订阅表达式的长度

#define CWX_MQ_MAX_MSG_SIZE           10 * 1024 * 1024 ///<最大的消息大小
#define CWX_MQ_MAX_CHUNK_KSIZE         20 * 1024 ///<最大的chunk size

#define CWX_MQ_ZIP_EXTRA_BUF           128


#define CWX_MQ_MAX_BINLOG_FLUSH_COUNT  10000 ///<服务启动时，最大的skip sid数量

#endif
