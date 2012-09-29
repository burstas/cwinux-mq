#ifndef __CWX_MQ_MACRO_H__
#define __CWX_MQ_MACRO_H__
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
#define CWX_MQ_D    "d"  ///<data的key
#define CWX_MQ_RET  "ret"
#define CWX_MQ_SID  "sid"
#define CWX_MQ_ERR  "err"
#define CWX_MQ_B    "b" ///<是否block
#define CWX_MQ_T    "t" ///<时间戳
#define CWX_MQ_U    "u" ///<user的key
#define CWX_MQ_P    "p"  ///<passwd的key
#define CWX_MQ_SOURCE "source"
#define CWX_MQ_Q   "q"   ///<队列的名字
#define CWX_MQ_G    "g" ///<group的key
#define CWX_MQ_CHUNK "chunk"
#define CWX_MQ_M     "m"
#define CWX_MQ_SIGN   "sign"
#define CWX_MQ_SESSION "session"
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
#define CWX_MQ_ERR_ERROR            1  ///<错误
#define CWX_MQ_ERR_FAIL_AUTH        2 ///<鉴权失败
#define CWX_MQ_ERR_LOST_SYNC        3 ///<失去了同步状态
#define CWX_MQ_PROXY_NO_AUTH_GROUP    100 ///<消息的group没有被允许
#define CWX_MQ_PROXY_FORBID_GROUP     101 ///<消息的group被禁止
#define CWX_MQ_PROXY_NO_AUTH          102 ///<消息的group被禁止
#define CWX_MQ_PROXY_TIMEOUT          103 ///<发送超时
#define CWX_MQ_PROXY_MQ_INVALID       104 ///<mq服务不可用
#define CWX_MQ_CONN_TIMEOUT_SECOND    5  ///<mq的同步连接的连接超时时间
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
