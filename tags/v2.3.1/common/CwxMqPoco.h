#ifndef __CWX_MQ_POCO_H__
#define __CWX_MQ_POCO_H__
/*
版权声明：
    本软件遵循GNU GPL V3（http://www.gnu.org/licenses/gpl.html），
    联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
/**
@file CwxMqPoco.h
@brief MQ系列服务的接口协议定义对象。
@author cwinux@gmail.com
@version 1.0
@date 2010-09-23
@warning
@bug
*/

#include "CwxMqMacro.h"
#include "CwxMsgBlock.h"
#include "CwxPackageReader.h"
#include "CwxPackageWriter.h"
#include "CwxCrc32.h"
#include "CwxMd5.h"

///订阅规则的规则信息对象
class CwxMqSubscribeItem{
public:
    CwxMqSubscribeItem(){
        m_bAll = false;
        m_bMod = false;
        m_uiModBase = 0;
        m_uiModIndex = 0;
    }
    CwxMqSubscribeItem(CwxMqSubscribeItem const& item){
        m_bAll = item.m_bAll;
        m_bMod = item.m_bMod;
        m_uiModBase = item.m_uiModBase;
        m_uiModIndex = item.m_uiModIndex;
        m_set = item.m_set;
    }
    CwxMqSubscribeItem& operator=(CwxMqSubscribeItem const& item){
        if (this != &item){
            m_bAll = item.m_bAll;
            m_bMod = item.m_bMod;
            m_uiModBase = item.m_uiModBase;
            m_uiModIndex = item.m_uiModIndex;
            m_set = item.m_set;
        }
        return *this;
    }
public:
    ///是否订阅指定的id
    inline bool isSubscribe(CWX_UINT32 id) const{
        if (m_bAll) return true;
        if (m_bMod) return (id%m_uiModBase)==m_uiModIndex;
        list<pair<CWX_UINT32, CWX_UINT32> >::const_iterator iter = m_set.begin();
        while(iter != m_set.end()){
            if ((id>=iter->first) && (id<=iter->second)) return true;
            iter++;
        }
        return false;
    }
public:
    bool    m_bAll; ///<是否全部订阅
    bool    m_bMod; ///<是否为求余模式
    CWX_UINT32  m_uiModBase; ///<求余的基数
    CWX_UINT32  m_uiModIndex; ///<余数值
    list<pair<CWX_UINT32, CWX_UINT32> > m_set; ///<订阅的group或type的范围列表
};

///订阅规则表达式对象
class CwxMqSubscribe{
public:
    CwxMqSubscribe(){
        m_bAll = false;
    }

    CwxMqSubscribe(CwxMqSubscribe const& item){
        m_bAll = item.m_bAll;
        m_subscribe = item.m_subscribe;
    }

    CwxMqSubscribe& operator=(CwxMqSubscribe const& item){
        if (this != &item){
            m_bAll = item.m_bAll;
            m_subscribe = item.m_subscribe;
        }
        return *this;
    }

public:
    ///是否订阅指定的group、type对
    inline bool isSubscribe(CWX_UINT32 uiGroup) const{
        if (!m_bAll){
            list<CwxMqSubscribeItem/*group*/>::const_iterator iter = m_subscribe.begin();
            while(iter != m_subscribe.end()){
                if (iter->isSubscribe(uiGroup)) return true;
                iter++;
            }
            return false;
        }
        return true;
    }
public:
    bool    m_bAll; ///<是否订阅全部消息
    list<CwxMqSubscribeItem/*group*/ > m_subscribe; ///<订阅规则列表
};

//mq的协议定义对象
class CwxMqPoco{
public:
    enum ///<消息类型定义
    {
        ///RECV服务类型的消息类型定义
        MSG_TYPE_RECV_DATA = 1, ///<数据提交消息
        MSG_TYPE_RECV_DATA_REPLY = 2, ///<数据提交消息的回复
        MSG_TYPE_RECV_COMMIT = 3, ///<数据commit消息
        MSG_TYPE_RECV_COMMIT_REPLY = 4, ///<commit消息的回复
        ///分发的消息类型定义
        MSG_TYPE_SYNC_REPORT = 5, ///<同步SID点报告消息类型
        MSG_TYPE_SYNC_REPORT_REPLY = 6, ///<失败返回
        MSG_TYPE_SYNC_SESSION_REPORT = 7, ///<session的报告
        MSG_TYPE_SYNC_SESSION_REPORT_REPLY = 8, ///<session报告的回复
        MSG_TYPE_SYNC_DATA = 9,  ///<发送数据
        MSG_TYPE_SYNC_DATA_REPLY = 10, ///<数据的回复
        MSG_TYPE_SYNC_DATA_CHUNK = 11,  ///<发送数据
        MSG_TYPE_SYNC_DATA_CHUNK_REPLY = 12, ///<数据的回复
        ///MQ Fetch服务类型的消息类型定义
        MSG_TYPE_FETCH_DATA = 13, ///<数据获取消息类型
        MSG_TYPE_FETCH_DATA_REPLY = 14, ///<回复数据获取消息类型
        ///创建mq queue消息
        MSG_TYPE_CREATE_QUEUE = 101, ///<创建MQ QUEUE的消息类型
        MSG_TYPE_CREATE_QUEUE_REPLY = 102, ///<回复创建MQ QUEUE的消息类型
        ///删除mq queue消息
        MSG_TYPE_DEL_QUEUE = 103, ///<删除MQ QUEUE的消息类型
        MSG_TYPE_DEL_QUEUE_REPLY = 104, ///<回复删除MQ QUEUE的消息类型
        ///错误消息
        MSG_TYPE_SYNC_ERR = 105  ///<数据同步错误消息
    };
    enum{
        MAX_CONTINUE_SEEK_NUM = 8192
    };
public:
    ///初始化协议。返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int init(char* szErr2K=NULL);
    ///释放协议。
    static void destory();
    ///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packRecvData(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CwxKeyValueItem const& data,
        CWX_UINT32 group,
        char const* user=NULL,
        char const* passwd=NULL,
        char const* sign=NULL,
        bool        zip=false,
        char* szErr2K=NULL
        );

    ///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseRecvData(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CwxKeyValueItem const*& data,
        CWX_UINT32& group,
        char const*& user,
        char const*& passwd,
        char* szErr2K=NULL);

    ///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseRecvData(CwxPackageReader* reader,
        char const* msg,
        CWX_UINT32  msg_len,
        CwxKeyValueItem const*& data,
        CWX_UINT32& group,
        char const*& user,
        char const*& passwd,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packRecvDataReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        int ret,
        CWX_UINT64 ullSid,
        char const* szErrMsg,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseRecvDataReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        int& ret,
        CWX_UINT64& ullSid,
        char const*& szErrMsg,
        char* szErr2K=NULL);

    ///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packCommit(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        char const* user=NULL,
        char const* passwd=NULL,
        char* szErr2K=NULL
        );
    ///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseCommit(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        char const*& user,
        char const*& passwd,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packCommitReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        int ret,
        char const* szErrMsg,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseCommitReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        int& ret,
        char const*& szErrMsg,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packReportData(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CWX_UINT64 ullSid,
        bool      bNewly,
        CWX_UINT32  uiChunkSize,
        char const* subscribe = NULL,
        char const* user=NULL,
        char const* passwd=NULL,
        char const* sign=NULL,
        bool        zip = false,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseReportData(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CWX_UINT64& ullSid,
        bool&       bNewly,
        CWX_UINT32&  uiChunkSize,
        char const*& subscribe,
        char const*& user,
        char const*& passwd,
        char const*& sign,
        bool&        zip,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packReportDataReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CWX_UINT64 ullSession,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseReportDataReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CWX_UINT64& ullSession,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packReportNewConn(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CWX_UINT64 ullSession,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseReportNewConn(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CWX_UINT64& ullSession,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packSyncData(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CWX_UINT64 ullSid,
        CWX_UINT32 uiTimeStamp,
        CwxKeyValueItem const& data,
        CWX_UINT32 group,
        char const* sign,
        bool       zip,
        CWX_UINT64 ullSeq,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packSyncDataItem(CwxPackageWriter* writer,
        CWX_UINT64 ullSid,
        CWX_UINT32 uiTimeStamp,
        CwxKeyValueItem const& data,
        CWX_UINT32 group,
        char const* sign=NULL,
        char* szErr2K=NULL);
    static int packMultiSyncData(
        CWX_UINT32 uiTaskId,
        char const* szData,
        CWX_UINT32 uiDataLen,
        CwxMsgBlock*& msg,
        CWX_UINT64 ullSeq,
        bool  zip = false,
        char* szErr2K=NULL
        );
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseSyncData(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CWX_UINT64& ullSid,
        CWX_UINT32& uiTimeStamp,
        CwxKeyValueItem const*& data,
        CWX_UINT32& group,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseSyncData(CwxPackageReader* reader,
        char const* szData,
        CWX_UINT32 uiDataLen,
        CWX_UINT64& ullSid,
        CWX_UINT32& uiTimeStamp,
        CwxKeyValueItem const*& data,
        CWX_UINT32& group,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packSyncDataReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CWX_UINT16 unMsgType,
        CWX_UINT64 ullSeq,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseSyncDataReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CWX_UINT64& ullSeq,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packFetchMq(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        bool bBlock,
        char const* queue_name,
        char const* user=NULL,
        char const* passwd=NULL,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseFetchMq(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        bool& bBlock,
        char const*& queue_name,
        char const*& user,
        char const*& passwd,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packFetchMqReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        int  ret,
        char const* szErrMsg,
        CWX_UINT64 ullSid,
        CWX_UINT32 uiTimeStamp,
        CwxKeyValueItem const& data,
        CWX_UINT32 group,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseFetchMqReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        int&  ret,
        char const*& szErrMsg,
        CWX_UINT64& ullSid,
        CWX_UINT32& uiTimeStamp,
        CwxKeyValueItem const*& data,
        CWX_UINT32& group,
        char* szErr2K=NULL);


    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseCreateQueue(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        char const*& name,
        char const*& user,
        char const*& passwd,
        char const*& scribe,
        char const*& auth_user,
        char const*& auth_passwd,
        CWX_UINT64&  ullSid,///< 0：当前最大值，若小于当前最小值，则采用当前最小sid值
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packCreateQueue(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        char const* name,
        char const* user,
        char const* passwd,
        char const* scribe,
        char const* auth_user,
        char const* auth_passwd,
        CWX_UINT64  ullSid=0,///< 0：当前最大值，若小于当前最小值，则采用当前最小sid值
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseCreateQueueReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        int&  ret,
        char const*& szErrMsg,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packCreateQueueReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        int  ret,
        char const* szErrMsg,
        char* szErr2K=NULL);


    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseDelQueue(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        char const*& name,
        char const*& user,
        char const*& passwd,
        char const*& auth_user,
        char const*& auth_passwd,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packDelQueue(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        char const* name,
        char const* user,
        char const* passwd,
        char const* auth_user,
        char const* auth_passwd,
        char* szErr2K=NULL);


    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseDelQueueReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        int&  ret,
        char const*& szErrMsg,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packDelQueueReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        int  ret,
        char const* szErrMsg,
        char* szErr2K=NULL);
    ///pack report或sync的出错消息包。返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int packSyncErr(CwxPackageWriter* writer, ///<用于pack的writer
        CwxMsgBlock*& msg, ///<返回的消息包，对象由内部分配
        CWX_UINT32 uiTaskId, ///<消息包的task id
        int ret, ///<错误代码
        char const* szErrMsg, ///<错误消息
        char* szErr2K=NULL///<pack出错时的错误信息
        );

    ///parse report或sync的出错数据包。返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
    static int parseSyncErr(CwxPackageReader* reader, ///<reader
        CwxMsgBlock const* msg, ///<数据包
        int& ret,  ///<错误代码
        char const*& szErrMsg,  ///<错误消息
        char* szErr2K=NULL ///<解包时的错误信息
        );
    ///设置数据同步包的seq号
    inline static void setSeq(char* szBuf, CWX_UINT64 ullSeq){
        CWX_UINT32 byte4 = (CWX_UINT32)(ullSeq>>32);
        byte4 = CWX_HTONL(byte4);
        memcpy(szBuf, &byte4, 4);
        byte4 = (CWX_UINT32)(ullSeq&0xFFFFFFFF);
        byte4 = CWX_HTONL(byte4);
        memcpy(szBuf + 4, &byte4, 4);

    }    
    ///获取数据同步包的seq号
    inline static CWX_UINT64 getSeq(char const* szBuf) {
        CWX_UINT64 ullSeq = 0;
        CWX_UINT32 byte4;
        memcpy(&byte4, szBuf, 4);
        ullSeq = CWX_NTOHL(byte4);
        memcpy(&byte4, szBuf+4, 4);
        ullSeq <<=32;
        ullSeq += CWX_NTOHL(byte4);
        return ullSeq;
    }

    ///返回sync记录。
    inline static char const* getSyncRecordData(){
        return m_pWriter->getMsg();
    }
    ///获取sync记录的长度
    inline static CWX_UINT32 getSyncRecordDataLen(){
        return m_pWriter->getMsgSize();
    }
    ///是否继续查找订阅的消息类型
    inline static bool isContinueSeek(CWX_UINT32 uiSeekedNum){
        return MAX_CONTINUE_SEEK_NUM>uiSeekedNum;
    }
    ///是否为有效地消息订阅语法
    static bool isValidSubscribe(string const& strSubscribe, string& strErrMsg);
    ///解析订阅的语法
    /*
    表达式为
    group_express;group_express...
    其中：
    group_express: [*]|[group_index%group_num]|[begin-end,begin-end,...]
    *：全部
    group_index%group_num：对group以group_num求余，余数为group_index的。
    begin-end：group范围，多个范围可以以【,】分割，若begin==end，则只写begin就可以了
    */
    static bool parseSubsribe(string const& strSubscribe, CwxMqSubscribe& subscribe, string& strErrMsg);
    ///消息是否订阅
    inline static bool isSubscribe(CwxMqSubscribe const& subscribe, CWX_UINT32 uiGroup){
        return subscribe.isSubscribe(uiGroup);
    }
private:
    ///禁止创建对象实例
    CwxMqPoco(){
    }
    ///析构函数
    ~CwxMqPoco();
    ///解析一个订阅表达式
    static bool parseSubsribeExpress(string const& strSubsribeExpress,
        CwxMqSubscribeItem& express,
        string& strErrMsg);
private:
    static CwxPackageWriter*   m_pWriter;
};





#endif
