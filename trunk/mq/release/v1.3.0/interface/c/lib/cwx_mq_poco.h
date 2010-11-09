#ifndef __CWX_MQ_POCO_H__
#define __CWX_MQ_POCO_H__
/*
版权声明：
    本软件为个人所有，遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
但有以下例外：
    腾讯公司及与腾讯公司有直接业务与合作关系的公司不得使用此软件。原因可参考：
http://it.sohu.com/20100903/n274684530.shtml
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
/**
@file cwx_mq_poco.h
@brief MQ系列服务的接口协议定义。
@author cwinux@gmail.com
@version 1.0
@date 2010-10-06
@warning
@bug
*/

#ifdef __cplusplus
extern "C" {
#endif

#include "cwx_package_reader.h"
#include "cwx_package_writer.h"
#include "cwx_msg_header.h"


///协议的消息类型定义
///RECV服务类型的消息类型定义
#define CWX_MQ_MSG_TYPE_MQ  1 ///<数据提交消息
#define CWX_MQ_MSG_TYPE_MQ_REPLY 2 ///<数据提交消息的回复
#define CWX_MQ_MSG_TYPE_MQ_COMMIT 3 ///<数据commit消息
#define CWX_MQ_MSG_TYPE_MQ_COMMIT_REPLY 4 ///<commit消息的回复
///分发的消息类型定义
#define CWX_MQ_MSG_TYPE_SYNC_REPORT 5 ///<同步SID点报告消息类型
#define CWX_MQ_MSG_TYPE_SYNC_REPORT_REPLY 6 ///<失败返回
#define CWX_MQ_MSG_TYPE_SYNC_DATA 7
#define CWX_MQ_MSG_TYPE_SYNC_DATA_REPLY 8
///MQ Fetch服务类型的消息类型定义
#define CWX_MQ_MSG_TYPE_FETCH_DATA 9 ///<数据获取消息类型
#define CWX_MQ_MSG_TYPE_FETCH_DATA_REPLY 10 ///<回复数据获取消息类型

///binlog内部的sync binlogleixing
#define CWX_MQ_GROUP_SYNC 0XFFFFFFFF 

///协议的key定义
#define CWX_MQ_KEY_DATA "data"
#define CWX_MQ_KEY_TYPE "type"
#define CWX_MQ_KEY_ATTR "attr"
#define CWX_MQ_KEY_RET  "ret"
#define CWX_MQ_KEY_SID  "sid"
#define CWX_MQ_KEY_ERR  "err"
#define CWX_MQ_KEY_BLOCK "block"
#define CWX_MQ_KEY_TIMESTAMP  "timestamp"
#define CWX_MQ_KEY_USER  "user"
#define CWX_MQ_KEY_PASSWD "passwd"
#define CWX_MQ_KEY_SUBSCRIBE "subscribe"
#define CWX_MQ_KEY_QUEUE "queue"
#define CWX_MQ_KEY_GROUP "group"

///协议错误代码定义
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
#define CWX_MQ_ERR_INVALID_BINLOG_TYPE 11 ///<binlog的type错误
#define CWX_MQ_ERR_INVALID_MSG_TYPE   12 ///<接收到的消息类型无效
#define CWX_MQ_ERR_FAIL_ADD_BINLOG    13 ///<往binglog mgr中添加binlog失败
#define CWX_MQ_ERR_NO_QUEUE        14 ///<队列不存在
#define CWX_MQ_ERR_INVALID_SUBSCRIBE 15 ///<无效的消息订阅类型
#define CWX_MQ_ERR_INNER_ERR        16 ///<其他内部错误，一般为内存


/**
*@brief 形成mbus或mq的一个消息包
*@param [in] writer package的writer。
*@param [in] uiTaskId task-id,回复的时候会返回。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] data msg的data。
*@param [in] group msg的group。
*@param [in] type msg的type。
*@param [in] attr msg的attr。
*@param [in] user 接收的mbus或mq的user，若为空，则表示没有用户。
*@param [in] passwd 接收的mbus或mq的passwd，若为空，则表示没有口令。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_mq(struct CWX_PG_WRITER * writer,
        CWX_UINT32 uiTaskId,
        char* buf,
        CWX_UINT32* buf_len,
        struct CWX_KEY_VALUE_ITEM_S const* data,
        CWX_UINT32 group,
        CWX_UINT32 type,
        CWX_UINT32 attr,
        char const* user,
        char const* passwd,
        char* szErr2K
        );
/**
*@brief 解析mbus或mq的一个消息包
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in] msg_len msg的长度。
*@param [out] data 返回msg的data。
*@param [out] group msg的group。
*@param [out] type 返回msg的type。
*@param [out] attr 返回msg的attr。
*@param [out] user 返回msg中的用户，0表示不存在。
*@param [out] passwd 返回msg中的用户口令，0表示不存在。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_mq(struct CWX_PG_READER* reader,
        char const* msg,
        CWX_UINT32  msg_len,
        struct CWX_KEY_VALUE_ITEM_S const** data,
        CWX_UINT32* group,
        CWX_UINT32* type,
        CWX_UINT32* attr,
        char const** user,
        char const** passwd,
        char* szErr2K);

/**
*@brief pack mbus或mq的一个reply消息包
*@param [in] writer package的writer。
*@param [in] uiTaskId 收到消息的task-id，原样返回。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] ret 错误代码。
*@param [in] ullSid 消息成功接收时的sid。
*@param [in] szErrMsg 消息失败时的错误消息。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_mq_reply(struct CWX_PG_WRITER * writer,
    CWX_UINT32 uiTaskId,
    char* buf,
    CWX_UINT32* buf_len,
    int ret,
    CWX_UINT64 ullSid,
    char const* szErrMsg,
    char* szErr2K);

/**
*@brief 解析mbus或mq的一个reply消息包
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in] msg_len msg的长度。
*@param [out] ret 返回msg的ret。
*@param [out] ullSid 返回msg的sid。
*@param [out] szErrMsg 返回msg的err-msg。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_mq_reply(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    int* ret,
    CWX_UINT64* ullSid,
    char const** szErrMsg,
    char* szErr2K);

/**
*@brief pack mbus或mq的commit消息包
*@param [in] writer package的writer。
*@param [in] uiTaskId 消息的task-id。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] user 接收的mbus或mq的user，若为空，则表示没有用户。
*@param [in] passwd 接收的mbus或mq的passwd，若为空，则表示没有口令。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_commit(struct CWX_PG_WRITER * writer,
    CWX_UINT32 uiTaskId,
    char* buf,
    CWX_UINT32* buf_len,
    char const* user,
    char const* passwd,
    char* szErr2K);
/**
*@brief 解析mbus或mq的一个commit消息包
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in] msg_len msg的长度。
*@param [out] user 返回msg中的用户，0表示不存在。
*@param [out] passwd 返回msg中的用户口令，0表示不存在。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_commit(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    char const** user,
    char const** passwd,
    char* szErr2K);


/**
*@brief pack mbus或mq的commit reply的消息包
*@param [in] writer package的writer。
*@param [in] uiTaskId 收到消息的task-id，原样返回。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] ret 执行状态码。
*@param [in] szErrMsg 执行失败时的错误消息。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_commit_reply(struct CWX_PG_WRITER * writer,
    CWX_UINT32 uiTaskId,
    char* buf,
    CWX_UINT32* buf_len,
    int ret,
    char const* szErrMsg,
    char* szErr2K);

/**
*@brief 解析mbus或mq的一个commit reply消息包
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in] msg_len msg的长度。
*@param [out] ret 执行状态码。
*@param [out] szErrMsg 执行失败时的错误消息。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_commit_reply(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    int* ret,
    char const** szErrMsg,
    char* szErr2K);

/**
*@brief pack mbus或mq的report消息包
*@param [in] writer package的writer。
*@param [in] uiTaskId task-id。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] ullSid 同步的sid。
*@param [in] bNewly 是否从当前binlog开始接收。
*@param [in] subscribe 订阅的消息类型。
*@param [in] user 接收的mbus或mq的user，若为空，则表示没有用户。
*@param [in] passwd 接收的mbus或mq的passwd，若为空，则表示没有口令。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_sync_report(struct CWX_PG_WRITER * writer,
    CWX_UINT32 uiTaskId,
    char* buf,
    CWX_UINT32* buf_len,
    CWX_UINT64 ullSid,
    int      bNewly,
    char const* subscribe,
    char const* user,
    char const* passwd,
    char* szErr2K);
/**
*@brief parse mbus或mq的report消息包
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in] msg_len msg的长度。
*@param [in] ullSid 同步的sid。
*@param [in] bNewly 是否从当前binlog开始接收。
*@param [in] subscribe 订阅的消息类型。
*@param [in] user 接收的mbus或mq的user，若为空，则表示没有用户。
*@param [in] passwd 接收的mbus或mq的passwd，若为空，则表示没有口令。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_sync_report(struct CWX_PG_READER* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    CWX_UINT64* ullSid,
    int*       bNewly,
    char const** subscribe,
    char const** user,
    char const** passwd,
    char* szErr2K);

/**
*@brief pack mbus或mq的report失败时的reply消息包
*@param [in] writer package的writer。
*@param [in] uiTaskId 收到report的task-id。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] ret report失败的错误代码。
*@param [in] ullSid report的sid。
*@param [in] szErrMsg report失败的原因。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_sync_report_reply(struct CWX_PG_WRITER * writer,
        CWX_UINT32 uiTaskId,
        char* buf,
        CWX_UINT32* buf_len,
        int ret,
        CWX_UINT64 ullSid,
        char const* szErrMsg,
        char* szErr2K);
/**
*@brief parse mbus或mq的report失败时的reply消息包
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in] msg_len msg的长度。
*@param [in] ret report失败的错误代码。
*@param [in] ullSid report的sid。
*@param [in] szErrMsg report失败的原因。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_sync_report_reply(struct CWX_PG_READER* reader,
        char const* msg,
        CWX_UINT32 msg_len,
        int* ret,
        CWX_UINT64* ullSid,
        char const** szErrMsg,
        char* szErr2K);

/**
*@brief pack mbus或mq的sync msg的消息包
*@param [in] writer package的writer。
*@param [in] uiTaskId task-id。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] ullSid 消息的sid。
*@param [in] uiTimeStamp 消息接收时的时间。
*@param [in] data 消息的data。
*@param [in] group 消息的group。
*@param [in] type 消息的type。
*@param [in] attr 消息的attr。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_sync_data(struct CWX_PG_WRITER * writer,
        CWX_UINT32 uiTaskId,
        char* buf,
        CWX_UINT32* buf_len,
        CWX_UINT64 ullSid,
        CWX_UINT32 uiTimeStamp,
        struct CWX_KEY_VALUE_ITEM_S const* data,
        CWX_UINT32 group,
        CWX_UINT32 type,
        CWX_UINT32 attr,
        char* szErr2K);
/**
*@brief parse mbus或mq的sync msg的消息包
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in out] msg_len msg的长度。
*@param [out] ullSid 消息的sid。
*@param [out] uiTimeStamp 消息接收时的时间。
*@param [out] data 消息的data。
*@param [out] group 消息的group。
*@param [out] type 消息的type。
*@param [out] attr 消息的attr。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_sync_data(struct CWX_PG_READER* reader,
        char const* msg,
        CWX_UINT32 msg_len,
        CWX_UINT64* ullSid,
        CWX_UINT32* uiTimeStamp,
        struct CWX_KEY_VALUE_ITEM_S const** data,
        CWX_UINT32* group,
        CWX_UINT32* type,
        CWX_UINT32* attr,
        char* szErr2K);

/**
*@brief pack mbus或mq的sync msg的消息包的回复
*@param [in] writer package的writer。
*@param [in] uiTaskId task-id。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] ullSid 消息的sid。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_sync_data_reply(struct CWX_PG_WRITER * writer,
        CWX_UINT32 uiTaskId,
        char* buf,
        CWX_UINT32* buf_len,
        CWX_UINT64 ullSid,
        char* szErr2K);
/**
*@brief parse mbus或mq的sync msg的消息包的回复
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in] msg_len msg的长度。
*@param [in] ullSid 消息的sid。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_sync_data_reply(struct CWX_PG_READER* reader,
        char const* msg,
        CWX_UINT32 msg_len,
        CWX_UINT64* ullSid,
        char* szErr2K);

/**
*@brief pack mq的fetch msg的消息包
*@param [in] writer package的writer。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] bBlock 在没有消息的时候是否block，1：是；0：不是。
*@param [in] queue_name 队列的名字。
*@param [in] user 接收的mbus或mq的user，若为空，则表示没有用户。
*@param [in] passwd 接收的mbus或mq的passwd，若为空，则表示没有口令。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_fetch_mq(struct CWX_PG_WRITER * writer,
        char* buf,
        CWX_UINT32* buf_len,
        int bBlock,
        char const* queue_name,
        char const* user,
        char const* passwd,
        char* szErr2K);
/**
*@brief parse  mq的fetch msg的消息包
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in] msg_len msg的长度。
*@param [in] bBlock 在没有消息的时候是否block，1：是；0：不是。
*@param [in] queue_name 队列的名字。
*@param [in] user 接收的mbus或mq的user，若为空，则表示没有用户。
*@param [in] passwd 接收的mbus或mq的passwd，若为空，则表示没有口令。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_fetch_mq(struct CWX_PG_READER* reader,
        char const* msg,
        CWX_UINT32 msg_len,
        int* bBlock,
        char const** queue_name,
        char const** user,
        char const** passwd,
        char* szErr2K);

/**
*@brief pack mq的fetch msg的reply消息包
*@param [in] writer package的writer。
*@param [out] buf 输出形成的数据包。
*@param [in out] buf_len 传入buf的空间大小，返回形成的数据包的大小。
*@param [in] ret 获取mq消息的状态码。
*@param [in] szErrMsg 状态不是CWX_MQ_ERR_SUCCESS的错误消息。
*@param [in] ullSid 成功时，返回消息的sid。
*@param [in] uiTimeStamp 成功时，返回消息的时间戳。
*@param [in] data 成功时，返回消息的data。
*@param [in] group 成功时，返回消息的group。
*@param [in] type 成功时，返回消息的type。
*@param [in] attr 成功时，返回消息的attr。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_pack_fetch_mq_reply(struct CWX_PG_WRITER * writer,
        char* buf,
        CWX_UINT32* buf_len,
        int  ret,
        char const* szErrMsg,
        CWX_UINT64 ullSid,
        CWX_UINT32 uiTimeStamp,
        struct CWX_KEY_VALUE_ITEM_S const* data,
        CWX_UINT32 group,
        CWX_UINT32 type,
        CWX_UINT32 attr,
        char* szErr2K);
/**
*@brief parse  mq的fetch msg的reply消息包
*@param [in] reader package的reader。
*@param [in] msg 接收到的mq消息，不包括msg header。
*@param [in] msg_len msg的长度。
*@param [in] ret 获取mq消息的状态码。
*@param [in] szErrMsg 状态不是CWX_MQ_ERR_SUCCESS的错误消息。
*@param [in] ullSid 成功时，返回消息的sid。
*@param [in] uiTimeStamp 成功时，返回消息的时间戳。
*@param [in] data 成功时，返回消息的data。
*@param [in] group 成功时，返回消息的group。
*@param [in] type 成功时，返回消息的type。
*@param [in] attr 成功时，返回消息的attr。
*@param [out] szErr2K 出错时的错误消息，若为空则表示不获取错误消息。
*@return CWX_MQ_ERR_SUCCESS：成功；其他都是失败
*/
int cwx_mq_parse_fetch_mq_reply(struct CWX_PG_READER* reader,
        char const* msg,
        CWX_UINT32 msg_len,
        int*  ret,
        char const** szErrMsg,
        CWX_UINT64* ullSid,
        CWX_UINT32* uiTimeStamp,
        struct CWX_KEY_VALUE_ITEM_S const** data,
        CWX_UINT32* group,
        CWX_UINT32* type,
        CWX_UINT32* attr,
        char* szErr2K);

#ifdef __cplusplus
}
#endif


#endif
