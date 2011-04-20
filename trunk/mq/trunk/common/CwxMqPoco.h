#ifndef __CWX_MQ_POCO_H__
#define __CWX_MQ_POCO_H__
/*
版权声明：
    本软件遵循GNU LGPL（http://www.gnu.org/copyleft/lesser.html），
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
#include "CwxMqTss.h"
#include "CwxMsgBlock.h"

class CwxMqSubscribeItem
{
public:
    CwxMqSubscribeItem()
    {
        m_bAll = false;
        m_bMod = false;
        m_uiModBase = 0;
        m_uiModIndex = 0;
    }
    CwxMqSubscribeItem(CwxMqSubscribeItem const& item)
    {
        m_bAll = item.m_bAll;
        m_bMod = item.m_bMod;
        m_uiModBase = item.m_uiModBase;
        m_uiModIndex = item.m_uiModIndex;
        m_set = item.m_set;
    }
    CwxMqSubscribeItem& operator=(CwxMqSubscribeItem const& item)
    {
        if (this != &item)
        {
            m_bAll = item.m_bAll;
            m_bMod = item.m_bMod;
            m_uiModBase = item.m_uiModBase;
            m_uiModIndex = item.m_uiModIndex;
            m_set = item.m_set;
        }
        return *this;
    }
public:
    inline bool isSubscribe(CWX_UINT32 id) const
    {
        if (m_bAll) return true;
        if (m_bMod) return (id%m_uiModBase)==m_uiModIndex;
        list<pair<CWX_UINT32, CWX_UINT32> >::const_iterator iter = m_set.begin();
        while(iter != m_set.end())
        {
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

class CwxMqSubscribe
{
public:
    CwxMqSubscribe()
    {
        m_bAll = false;
    }

    CwxMqSubscribe(CwxMqSubscribe const& item)
    {
        m_bAll = item.m_bAll;
        m_subscribe = item.m_subscribe;
    }

    CwxMqSubscribe& operator=(CwxMqSubscribe const& item)
    {
        if (this != &item)
        {
            m_bAll = item.m_bAll;
            m_subscribe = item.m_subscribe;
        }
        return *this;
    }

public:
    inline bool isSubscribe(CWX_UINT32 uiGroup, CWX_UINT32 uiType) const
    {
        if (!m_bAll)
        {
            list<pair<CwxMqSubscribeItem/*group*/, CwxMqSubscribeItem/*type*/> >::const_iterator iter = m_subscribe.begin();
            while(iter != m_subscribe.end())
            {
                if (iter->first.isSubscribe(uiGroup) && iter->second.isSubscribe(uiType)) return true;
                iter++;
            }
            return false;
        }
        return true;
    }
public:
    bool    m_bAll; ///<是否订阅全部消息
    list<pair<CwxMqSubscribeItem/*group*/, CwxMqSubscribeItem/*type*/> > m_subscribe; ///<订阅规则列表
};

//mq的协议定义对象
class CwxMqPoco
{
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
        MSG_TYPE_SYNC_DATA = 7,  ///<发送数据
        MSG_TYPE_SYNC_DATA_REPLY = 8, ///<数据的回复
        ///MQ Fetch服务类型的消息类型定义
        MSG_TYPE_FETCH_DATA = 9, ///<数据获取消息类型
        MSG_TYPE_FETCH_DATA_REPLY = 10, ///<回复数据获取消息类型
    };
    enum
    {
        SYNC_GROUP_TYPE=0XFFFFFFFF,
        SYNC_SECOND_INTERNAL=60,
        SYNC_RECORD_INTERNAL=10000,
        MAX_CONTINUE_SEEK_NUM = 2048
    };
public:
    ///初始化协议。返回值，CWX_MQ_SUCCESS：成功；其他都是失败
    static int init(char* szErr2K=NULL);
    ///释放协议。
    static void destory();
    ///返回值，CWX_MQ_SUCCESS：成功；其他都是失败
    static int packRecvData(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CwxKeyValueItem const& data,
        CWX_UINT32 group,
        CWX_UINT32 type,
        CWX_UINT32 attr,
        char const* user=NULL,
        char const* passwd=NULL,
        char* szErr2K=NULL
        );
    ///返回值，CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseRecvData(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CwxKeyValueItem const*& data,
        CWX_UINT32& group,
        CWX_UINT32& type,
        CWX_UINT32& attr,
        char const*& user,
        char const*& passwd,
        char* szErr2K=NULL);


    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int packRecvDataReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        int ret,
        CWX_UINT64 ullSid,
        char const* szErrMsg,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseRecvDataReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        int& ret,
        CWX_UINT64& ullSid,
        char const*& szErrMsg,
        char* szErr2K=NULL);

    ///返回值，CWX_MQ_SUCCESS：成功；其他都是失败
    static int packCommit(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        char const* user=NULL,
        char const* passwd=NULL,
        char* szErr2K=NULL
        );
    ///返回值，CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseCommit(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        char const*& user,
        char const*& passwd,
        char* szErr2K=NULL);



    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int packCommitReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        int ret,
        char const* szErrMsg,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseCommitReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        int& ret,
        char const*& szErrMsg,
        char* szErr2K=NULL);


    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int packReportData(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CWX_UINT64 ullSid,
        bool      bNewly,
        char const* subscribe = NULL,
        char const* user=NULL,
        char const* passwd=NULL,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseReportData(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CWX_UINT64& ullSid,
        bool&       bNewly,
        char const*& subscribe,
        char const*& user,
        char const*& passwd,
        char* szErr2K=NULL);


    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int packReportDataReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        int ret,
        CWX_UINT64 ullSid,
        char const* szErrMsg,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseReportDataReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        int& ret,
        CWX_UINT64& ullSid,
        char const*& szErrMsg,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int packSyncData(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CWX_UINT64 ullSid,
        CWX_UINT32 uiTimeStamp,
        CwxKeyValueItem const& data,
        CWX_UINT32 group,
        CWX_UINT32 type,
        CWX_UINT32 attr,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseSyncData(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CWX_UINT64& ullSid,
        CWX_UINT32& uiTimeStamp,
        CwxKeyValueItem const*& data,
        CWX_UINT32& group,
        CWX_UINT32& type,
        CWX_UINT32& attr,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int packSyncDataReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        CWX_UINT32 uiTaskId,
        CWX_UINT64 ullSid,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseSyncDataReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        CWX_UINT64& ullSid,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int packFetchMq(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        bool bBlock,
        char const* queue_name,
        char const* user=NULL,
        char const* passwd=NULL,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseFetchMq(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        bool& bBlock,
        char const*& queue_name,
        char const*& user,
        char const*& passwd,
        char* szErr2K=NULL);

    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int packFetchMqReply(CwxPackageWriter* writer,
        CwxMsgBlock*& msg,
        int  ret,
        char const* szErrMsg,
        CWX_UINT64 ullSid,
        CWX_UINT32 uiTimeStamp,
        CwxKeyValueItem const& data,
        CWX_UINT32 group,
        CWX_UINT32 type,
        CWX_UINT32 attr,
        char* szErr2K=NULL);
    ///返回值：CWX_MQ_SUCCESS：成功；其他都是失败
    static int parseFetchMqReply(CwxPackageReader* reader,
        CwxMsgBlock const* msg,
        int&  ret,
        char const*& szErrMsg,
        CWX_UINT64& ullSid,
        CWX_UINT32& uiTimeStamp,
        CwxKeyValueItem const* data,
        CWX_UINT32& group,
        CWX_UINT32& type,
        CWX_UINT32& attr,
        char* szErr2K=NULL);
    ///true：需要产品sync记录；false：不需要产生sync记录
    inline static bool isNeedSyncRecord(CWX_UINT32 uiRecordNum, time_t ttLastSyncTime)
    {
        return (uiRecordNum>SYNC_RECORD_INTERNAL) || ((CWX_UINT32)(time(NULL) - ttLastSyncTime) > SYNC_SECOND_INTERNAL);
    }
    ///true：是sync记录；false：不是sync记录
    inline static bool isSyncRecord(CWX_UINT32 uiGroup)
    {
        return uiGroup == SYNC_GROUP_TYPE;
    }
    ///返回sync记录。
    inline static char const* getSyncRecordData()
    {
        return m_pWriter->getMsg();
    }
    ///获取sync记录的长度
    inline static CWX_UINT32 getSyncRecordDataLen()
    {
        return m_pWriter->getMsgSize();
    }
    ///是否继续查找订阅的消息类型
    inline static bool isContinueSeek(CWX_UINT32 uiSeekedNum)
    {
        return MAX_CONTINUE_SEEK_NUM>uiSeekedNum;
    }
    ///是否为有效地消息订阅语法
    static bool isValidSubscribe(string const& strSubscribe, string& strErrMsg);
    ///解析订阅的语法
    /*
    表达式为
    group_express:type_express;group_express:type_express...
    其中：
    group_express: [*]|[group_index%group_num]|[begin-end,begin-end,...]
    *：全部
    group_index%group_num：对group以group_num求余，余数为group_index的。
    begin-end：group范围，多个范围可以以【,】分割，若begin==end，则只写begin就可以了
    type_express:  [*]|[type_index%typte_num]|[begin-end,begin-end,...]
    各部分含义同group
    */
    static bool parseSubsribe(string const& strSubscribe, CwxMqSubscribe& subscribe, string& strErrMsg);
    ///消息是否订阅
    inline static bool isSubscribe(CwxMqSubscribe const& subscribe, bool bSync, CWX_UINT32 uiGroup, CWX_UINT32 uiType)
    {
        if (uiGroup == SYNC_GROUP_TYPE)
        {
            return bSync;
        }
        return subscribe.isSubscribe(uiGroup, uiType);
    }
private:
    ///禁止创建对象实例
    CwxMqPoco()
    {
    }
    ///析构函数
    ~CwxMqPoco();
    ///解析一个订阅规则
    static bool parseSubsribeRule(string const& strSubsribeRule,
        pair<CwxMqSubscribeItem/*group*/, CwxMqSubscribeItem/*type*/>& rule,
        bool& bAll,
        string& strErrMsg);
    ///解析一个订阅表达式
    static bool parseSubsribeExpress(string const& strSubsribeExpress,
        CwxMqSubscribeItem& express,
        string& strErrMsg);
private:
    static CwxPackageWriter*   m_pWriter;
};





#endif
