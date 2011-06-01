#include "CwxMqBinAsyncHandler.h"
#include "CwxMqApp.h"
///构造函数
CwxMqBinAsyncHandler::CwxMqBinAsyncHandler(CwxMqApp* pApp, CwxAppChannel* channel):CwxAppHandler4Channel(channel)
{
    m_pApp = pApp;
    m_uiRecvHeadLen = 0;
    m_uiRecvDataLen = 0;
    m_recvMsgData = 0;
}
///析构函数
CwxMqBinAsyncHandler::~CwxMqBinAsyncHandler()
{
    if (m_recvMsgData) CwxMsgBlockAlloc::free(m_recvMsgData);
    if (m_dispatch.m_pCursor) m_pApp->getBinLogMgr()->destoryCurser(m_dispatch.m_pCursor);
}

/**
@brief 连接可读事件，返回-1，close()会被调用
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqBinAsyncHandler::onInput()
{
    ///接受消息
    int ret = CwxAppHandler4Channel::recvPackage(getHandle(),
        m_uiRecvHeadLen,
        m_uiRecvDataLen,
        m_szHeadBuf,
        m_header,
        m_recvMsgData);
    ///如果没有接受完毕（0）或失败（-1），则返回
    if (1 != ret) return ret;
    ///接收到一个完整的数据包
    ///获取线程的tss实例
    CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
    ///消息处理
    ret = recvMessage(tss);
    ///如果没有释放接收的数据包，释放
    if (m_recvMsgData) CwxMsgBlockAlloc::free(m_recvMsgData);
    this->m_recvMsgData = NULL;
    this->m_uiRecvHeadLen = 0;
    this->m_uiRecvDataLen = 0;
    return ret;
}

//1：不从engine中移除注册；0：从engine中移除注册但不删除handler；-1：从engine中将handle移除并删除。
int CwxMqBinAsyncHandler::onConnClosed()
{
    ///从epoll engine中移除handler
    return -1;
}



///连接关闭后，需要清理环境
int CwxMqBinAsyncHandler::recvMessage(CwxMqTss* pTss)
{
    CWX_UINT64 ullSid = 0;
    CWX_UINT32 uiChunk = 0;
    CWX_UINT32 uiWindow = 1;
    bool  bNewly = false;
    char const* subscribe=NULL;
    char const* user=NULL;
    char const* passwd=NULL;
    char const* sign = NULL;
    bool bzip = false;
    int iRet = CWX_MQ_SUCCESS;
    int iState = 0;
    do 
    {

        if (CwxMqPoco::MSG_TYPE_SYNC_DATA_REPLY == m_header.getMsgType())
        {
            if (!m_dispatch.m_bSync)
            { ///如果连接不是同步状态，则是错误
                strcpy(pTss->m_szBuf2K, "Client no in sync state");
                CWX_DEBUG((pTss->m_szBuf2K));
                iRet = CWX_MQ_INVALID_MSG_TYPE;
                break;
            }
            if (!m_dispatch.m_sendingSid.size())
            {
                strcpy(pTss->m_szBuf2K, "Not sent binlog data");
                CWX_DEBUG((pTss->m_szBuf2K));
                iRet = CWX_MQ_INVALID_MSG_TYPE;
                break;
            }
            ///若是同步sid的报告消息,则获取报告的sid
            iRet = CwxMqPoco::parseSyncDataReply(pTss->m_pReader,
                m_recvMsgData,
                ullSid,
                pTss->m_szBuf2K);
            if (CWX_MQ_SUCCESS != iRet)
            {
                CWX_DEBUG(("Failure to parse sync_data reply package, err:%s", pTss->m_szBuf2K));
                break;
            }
            ///检查返回的sid
            if (ullSid != *m_dispatch.m_sendingSid.begin())
            {
                char szBuf1[64];
                char szBuf2[64];
                CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Reply sid[%s] is not the right sid[%s], close conn.",
                    CwxCommon::toString(ullSid, szBuf1, 10),
                    CwxCommon::toString(*m_dispatch.m_sendingSid.begin(), szBuf2, 10));
                CWX_ERROR((pTss->m_szBuf2K));
               iRet = CWX_MQ_INVALID_SID;
                break;
            }
            ///将sid从分发sid的set中去掉
            m_dispatch.m_sendingSid.erase(m_dispatch.m_sendingSid.begin());
            ///发送下一条binlog
            iState = sendBinLog(pTss);
            if (-1 == iState)
            {
                CWX_ERROR((pTss->m_szBuf2K));
                return -1; ///关闭连接
            }
            else if (0 == iState)
            {///产生continue的消息
                m_pApp->getAsyncDispChannel()->regRedoHander(this);
            }
            ///返回
            return 0;
        }
        else if (CwxMqPoco::MSG_TYPE_SYNC_REPORT == m_header.getMsgType())
        {
            ///禁止重复report sid。若cursor存在，表示已经报告过一次
            if (m_dispatch.m_pCursor)
            {
                iRet = CWX_MQ_INVALID_MSG;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Can't report sync sid duplicatly.");
                CWX_ERROR((pTss->m_szBuf2K));
                break;
            }
            ///若是同步sid的报告消息,则获取报告的sid
            iRet = CwxMqPoco::parseReportData(pTss->m_pReader,
                m_recvMsgData,
                ullSid,
                bNewly,
                uiChunk,
                uiWindow,
                subscribe,
                user,
                passwd,
                sign,
                bzip,
                pTss->m_szBuf2K);
            if (CWX_MQ_SUCCESS != iRet)
            {///若不存在，则错误返回
                CWX_ERROR(("Failure to parse report msg, err=%s%s", pTss->m_szBuf2K));
                break;
            }
            if (m_pApp->getConfig().getCommon().m_bMaster)
            {
                if (m_pApp->getConfig().getMaster().m_async.getUser().length())
                {
                    if ( (m_pApp->getConfig().getMaster().m_async.getUser() != user) ||
                        (m_pApp->getConfig().getMaster().m_async.getPasswd() != passwd))
                    {
                        iRet = CWX_MQ_FAIL_AUTH;
                        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                        CWX_DEBUG((pTss->m_szBuf2K));
                        break;
                    }
                }
            }
            else
            {
                if (m_pApp->getConfig().getSlave().m_async.getUser().length())
                {
                    if ( (m_pApp->getConfig().getSlave().m_async.getUser() != user) ||
                        (m_pApp->getConfig().getSlave().m_async.getPasswd() != passwd))
                    {
                        iRet = CWX_MQ_FAIL_AUTH;
                        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Failure to auth user[%s] passwd[%s]", user, passwd);
                        CWX_DEBUG((pTss->m_szBuf2K));
                        break;
                    }
                }
            }
            string strSubcribe=subscribe?subscribe:"";
            string strErrMsg;
            if (!CwxMqPoco::parseSubsribe(subscribe, m_dispatch.m_subscribe, strErrMsg))
            {
                iRet = CWX_MQ_INVALID_SUBSCRIBE;
                CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Invalid subscribe[%s], err=%s", strSubcribe.c_str(), strErrMsg.c_str());
                CWX_DEBUG((pTss->m_szBuf2K));
                break;
            }
            m_dispatch.m_bSync = true;
            m_dispatch.m_uiChunk = uiChunk;
            m_dispatch.m_bZip = bzip;
            m_dispatch.m_strSign = sign;
            if ((m_dispatch.m_strSign != CWX_MQ_CRC32) &&
                (m_dispatch.m_strSign != CWX_MQ_MD5))
            {//如果签名不是CRC32或MD5，则忽略
                m_dispatch.m_strSign.erase();
            }
            if (m_dispatch.m_uiChunk)
            {
                if (m_dispatch.m_uiChunk > CwxMqConfigCmn::MAX_CHUNK_SIZE_KB) m_dispatch.m_uiChunk = CwxMqConfigCmn::MAX_CHUNK_SIZE_KB;
                if (m_dispatch.m_uiChunk < CwxMqConfigCmn::MIN_CHUNK_SIZE_KB) m_dispatch.m_uiChunk = CwxMqConfigCmn::MIN_CHUNK_SIZE_KB;
                m_dispatch.m_uiChunk *= 1024;
            }
            if (!uiWindow)
                m_dispatch.m_uiWindow = CwxMqConfigCmn::DEF_WINDOW_NUM;
            else
                m_dispatch.m_uiWindow = uiWindow;
            if (m_dispatch.m_uiWindow > CwxMqConfigCmn::MAX_WINDOW_NUM) m_dispatch.m_uiWindow = CwxMqConfigCmn::MAX_WINDOW_NUM;
            if (m_dispatch.m_uiWindow < CwxMqConfigCmn::MIN_WINDOW_NUM) m_dispatch.m_uiWindow = CwxMqConfigCmn::MIN_WINDOW_NUM;
            m_dispatch.m_sendingSid.clear();

            if (bNewly)
            {///不sid为空，则取当前最大sid-1
                ullSid = m_pApp->getBinLogMgr()->getMaxSid();
                if (ullSid) ullSid--;
            }
            ///回复iRet的值
            iRet = CWX_MQ_SUCCESS;
            ///创建binlog读取的cursor
            CwxBinLogCursor* pCursor = m_pApp->getBinLogMgr()->createCurser();
            if (!pCursor)
            {
                iRet = CWX_MQ_INNER_ERR;
                strcpy(pTss->m_szBuf2K, "Failure to create cursor");
                CWX_ERROR((pTss->m_szBuf2K));
                break;
            }
            ///设置cursor
            m_dispatch.m_pCursor = pCursor;
            m_dispatch.m_ullStartSid = ullSid;
            m_dispatch.m_bSync = true;
            ///发送下一条binlog
            iState = sendBinLog(pTss);
            if (-1 == iState)
            {
                CWX_ERROR((pTss->m_szBuf2K));
                return -1; ///关闭连接
            }
            else if (0 == iState)
            {///产生continue的消息
                m_pApp->getAsyncDispChannel()->regRedoHander(this);
            }
            ///返回
            return 0;
        }
        else
        {
            ///若其他消息，则返回错误
            CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u", m_header.getMsgType());
            iRet = CWX_MQ_INVALID_MSG_TYPE;
            CWX_ERROR((pTss->m_szBuf2K));
        }

    } while(0);
    

    ///形成失败时候的回复数据包
    CwxMsgBlock* pBlock = NULL;
    if (CWX_MQ_SUCCESS != CwxMqPoco::packReportDataReply(pTss->m_pWriter,
        pBlock,
        m_header.getTaskId(),
        iRet,
        ullSid,
        pTss->m_szBuf2K,
        pTss->m_szBuf2K))
    {
        CWX_ERROR(("Failure to create binlog reply package, err:%s", pTss->m_szBuf2K));
        return -1;
    }
    ///发送回复的数据包
    pBlock->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
    pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_ASYNC);
    pBlock->send_ctrl().setHostId(0);
    pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
    if (0 != this->putMsg(pBlock))
    {
        CWX_ERROR(("Failure to send msg to reciever, conn[%u]", getHandle()));
        CwxMsgBlockAlloc::free(pBlock);
        return -1;
        ///关闭连接
    }
    return 0;
}

/**
@brief Handler的redo事件，在每次dispatch时执行。
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqBinAsyncHandler::onRedo()
{
    ///判断是否有可发送的消息
    if (!m_dispatch.m_sendingSid.size() ||
        (*(m_dispatch.m_sendingSid.end()--) < m_pApp->getBinLogMgr()->getMaxSid()))
    {
        CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
        ///发送下一条binlog
        int iState = sendBinLog(tss);
        if (-1 == iState)
        {
            CWX_ERROR((tss->m_szBuf2K));
            return -1; ///关闭连接
        }
        else if (0 == iState)
        {///产生continue的消息
            m_pApp->getAsyncDispChannel()->regRedoHander(this);
        }
    }
    else
    {
        ///重新redo handler
        m_pApp->getAsyncDispChannel()->regRedoHander(this);
    }
    ///返回
    return 0;
}

CWX_UINT32 CwxMqBinAsyncHandler::onEndSendMsg(CwxMsgBlock*& )
{
    ///如果还有发送窗口
    if (m_dispatch.m_sendingSid.size() < m_pApp->getConfig().getCommon().m_uiWindowSize)
    {
        CwxMqTss* tss = (CwxMqTss*)CwxTss::instance();
        ///发送下一条binlog
        int iState = sendBinLog(tss);
        if (-1 == iState)
        {
            CWX_ERROR((tss->m_szBuf2K));
        }
    }
    return CwxMsgSendCtrl::UNDO_CONN;
}


///-1：失败，0：无效的消息；1：成功
int CwxMqBinAsyncHandler::packOneBinLog(CwxPackageReader* reader,
                         CwxPackageWriter* writer,
                         CwxMsgBlock*& block,
                         char const* szData,
                         CWX_UINT32  uiDataLen,
                         char* szErr2K)
{
    CwxKeyValueItem const* pItem = NULL;
    ///unpack data的数据包
    if (reader->unpack(szData, uiDataLen, false,true))
    {
        ///获取CWX_MQ_DATA的key，此为真正data数据
        pItem = reader->getKey(CWX_MQ_DATA);
        if (pItem)
        {
            ///形成binlog发送的数据包
            if (CWX_MQ_SUCCESS != CwxMqPoco::packSyncData(writer,
                block,
                0,
                m_dispatch.m_pCursor->getHeader().getSid(),
                m_dispatch.m_pCursor->getHeader().getDatetime(),
                *pItem,
                m_dispatch.m_pCursor->getHeader().getGroup(),
                m_dispatch.m_pCursor->getHeader().getType(),
                m_dispatch.m_pCursor->getHeader().getAttr(),
                m_dispatch.m_strSign.c_str(),
                m_dispatch.m_bZip,
                szErr2K))
            {
                ///形成数据包失败
                CWX_ERROR(("Failure to pack binlog package, err:%s", szErr2K));
                return -1;
            }
        }
        else
        {///读取的数据无效                
            CWX_ERROR(("Can't find key[%s] in binlog, sid=%s", CWX_MQ_DATA,
                CwxCommon::toString(m_dispatch.m_pCursor->getHeader().getSid(), szErr2K)));
            return 0;
        }            
    }
    else
    {///binlog的数据格式错误，不是kv
        CWX_ERROR(("Can't unpack binlog, sid=%s", CwxCommon::toString(m_dispatch.m_pCursor->getHeader().getSid(), szErr2K)));
        return 0;
    }
    return 1;
}

///-1：失败，否则返回添加数据的尺寸
int CwxMqBinAsyncHandler::packMultiBinLog(CwxPackageReader* reader,
                           CwxPackageWriter* writer,
                           CwxPackageWriter* writer_item,
                           char const* szData,
                           CWX_UINT32  uiDataLen,
                           CWX_UINT32&  uiLen,
                           char* szErr2K)
{
    CwxKeyValueItem const* pItem = NULL;
    ///unpack data的数据包
    if (reader->unpack(szData, uiDataLen, false,true))
    {
        ///获取CWX_MQ_DATA的key，此为真正data数据
        pItem = reader->getKey(CWX_MQ_DATA);
        if (pItem)
        {
            ///形成binlog发送的数据包
            if (CWX_MQ_SUCCESS != CwxMqPoco::packSyncDataItem(writer_item,
                m_dispatch.m_pCursor->getHeader().getSid(),
                m_dispatch.m_pCursor->getHeader().getDatetime(),
                *pItem,
                m_dispatch.m_pCursor->getHeader().getGroup(),
                m_dispatch.m_pCursor->getHeader().getType(),
                m_dispatch.m_pCursor->getHeader().getAttr(),
                szErr2K))
            {
                ///形成数据包失败
                CWX_ERROR(("Failure to pack binlog package, err:%s", szErr2K));
                return -1;
            }
        }
        else
        {///读取的数据无效                
            CWX_ERROR(("Can't find key[%s] in binlog, sid=%s", CWX_MQ_DATA,
                CwxCommon::toString(m_dispatch.m_pCursor->getHeader().getSid(), szErr2K)));
            return 0;
        }            
    }
    else
    {///binlog的数据格式错误，不是kv
        CWX_ERROR(("Can't unpack binlog, sid=%s", CwxCommon::toString(m_dispatch.m_pCursor->getHeader().getSid(), szErr2K)));
        return 0;
    }
    if (!writer->addKeyValue(CWX_MQ_M, writer_item->getMsg(), writer_item->getMsgSize(),true))
    {
        ///形成数据包失败
        CWX_ERROR(("Failure to pack binlog package, err:%s", writer->getErrMsg()));
        return -1;
    }
    uiLen = CwxPackage::getKvLen(strlen(CWX_MQ_M),  writer_item->getMsgSize());
    return 1;
}

//1：发现记录；0：没有发现；-1：错误
int CwxMqBinAsyncHandler::seekToLog(CWX_UINT32& uiSkipNum, bool bSync)
{
    int iRet = 0;
    if (m_dispatch->m_bNext)
    {
        iRet = m_pApp->getBinLogMgr()->next(m_dispatch.m_pCursor);
        if (0 == iRet) return 0; ///完成状态
        if (-1 == iRet)
        {///<失败
            CWX_ERROR(("Failure to seek cursor, err:%s", m_dispatch.m_pCursor->getErrMsg()));
            return -1;
        }
    }
    uiSkipNum++;
    m_dispatch->m_bNext = false;
    while (!CwxMqPoco::isSubscribe(m_dispatch->m_subscribe,
        bSync,
        m_dispatch.m_pCursor->getHeader().getGroup(),
        m_dispatch.m_pCursor->getHeader().getType()))
    {
        iRet = m_pApp->getBinLogMgr()->next(m_dispatch.m_pCursor);
        if (0 == iRet)
        {
            m_dispatch->m_bNext = true;
            return 0; ///完成状态
        }
        if (-1 == iRet)
        {///<失败
            CWX_ERROR(("Failure to seek cursor, err:%s", m_dispatch.m_pCursor->getErrMsg()));
            return -1;
        }
        uiSkipNum ++;
        if (!CwxMqPoco::isContinueSeek(uiSkipNum))
        {
            return 0;///未完成状态
        }
    }
    return 1;
}

//1：成功；0：太大；-1：错误
int CwxMqBinAsyncHandler::seekToReportSid()
{
    int iRet = 0;
    if (m_pApp->getBinLogMgr()->isUnseek(m_dispatch.m_pCursor))
    {//若binlog的读取cursor悬空，则定位
        if (m_dispatch->m_ullStartSid < m_pApp->getBinLogMgr()->getMaxSid())
        {
            iRet = m_pApp->getBinLogMgr()->seek(m_dispatch.m_pCursor, m_dispatch->m_ullStartSid);
            if (-1 == iRet)
            {
                CWX_ERROR(("Failure to seek,  err:%s", m_dispatch.m_pCursor->getErrMsg()));
                return -1;
            }
            else if (0 == iRet)
            {
                char szBuf1[64];
                char szBuf2[64];
                CWX_DEBUG(("Should seek to sid[%s] with max_sid[[%s], but not.",
                    CwxCommon::toString(m_dispatch->m_ullStartSid, szBuf1),
                    CwxCommon::toString(m_pApp->getBinLogMgr()->getMaxSid(), szBuf2)));
                return 0;
            }
            ///若成功定位，则读取当前记录
            m_dispatch->m_bNext = m_dispatch->m_ullStartSid == m_dispatch.m_pCursor->getHeader().getSid()?true:false;
        }
        else
        {///若需要同步发送的sid不小于当前最小的sid，则依旧为悬空状态
            return 0;///完成状态
        }
    }
    return 1;
}

///0：未发送一条binlog；
///1：发送了一条binlog；
///-1：失败；
///2：窗口满了
int CwxMqBinAsyncHandler::sendBinLog(CwxMqTss* pTss)
{
    if (m_dispatch->m_uiWindow <= m_dispatch->m_sendingSid.size()) return 2;
    int iRet = 0;
    CWX_UINT32 uiDataLen;
    char* pBuf = NULL;
    CwxMsgBlock* pBlock = NULL;
    CWX_UINT32 uiSkipNum = 0;
    CWX_UINT32 uiKeyLen = 0;
    CWX_UINT32 uiTotalLen = 0;
    CWX_UINT64 ullSid = 0;
    if (m_pApp->getBinLogMgr()->isUnseek(m_dispatch.m_pCursor))
    {//若binlog的读取cursor悬空，则定位
        if (1 != (iRet = seekToReportSid())) return iRet;
    }

    if (m_dispatch->m_uiChunk)
    {
        pTss->m_pWriter->beginPack();
    }
    while(1)
    {
        if ( 1 != (iRet = seekToLog(uiSkipNum, true))) break;
        //设置移到下一个记录位置
        m_dispatch->m_bNext = true;
        uiDataLen = m_dispatch.m_pCursor->getHeader().getLogLen();
        ///准备data读取的buf
        pBuf = pTss->getBuf(uiDataLen);        
        ///读取data
        iRet = m_pApp->getBinLogMgr()->fetch(m_dispatch.m_pCursor, pBuf, uiDataLen);
        if (-1 == iRet)
        {//读取失败
            CWX_ERROR(("Failure to fetch data, err:%s", m_dispatch.m_pCursor->getErrMsg()));
            iRet = -1;
            break;
        }
        if (!m_dispatch->m_uiChunk)
        {
            iRet = packOneBinLog(pTss->m_pReader,
                pTss->m_pWriter,
                pBlock,
                pBuf,
                uiDataLen,
                pTss->m_szBuf2K);
            if (0 == iRet) continue;
            if (1 == iRet) ullSid = m_dispatch.m_pCursor->getHeader().getSid();
            break;
        }
        else
        {
            iRet = packMultiBinLog(pTss->m_pReader, 
                pTss->m_pWriter,
                pTss->m_pItemWriter,
                pBuf,
                uiDataLen,
                uiKeyLen,
                pTss->m_szBuf2K);
            if (1 == iRet)
            {
                ullSid = m_dispatch.m_pCursor->getHeader().getSid();
                uiTotalLen += uiKeyLen;
                if (uiTotalLen >= m_dispatch->m_uiChunk) break;
            }
            if (-1 == iRet) break;
            continue;
        }
    }
    if (-1 == iRet) return -1;

    if (!m_dispatch->m_uiChunk)
    {
        if (0 == iRet) return 0;
    }
    else
    {
        if (0 == uiTotalLen) return 0;
        //add sign
        if (m_dispatch.m_strSign.length())
        {
            if (m_dispatch.m_strSign == CWX_MQ_CRC32)//CRC32签名
            {
                CWX_UINT32 uiCrc32 = CwxCrc32::value(pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
                if (!pTss->m_pWriter->addKeyValue(CWX_MQ_CRC32, &uiCrc32, sizeof(uiCrc32)))
                {
                    CWX_ERROR(("Failure to add key value, err:%s", pTss->m_pWriter->getErrMsg()));
                    return -1;
                }
            }
            else if (m_dispatch.m_strSign == CWX_MQ_MD5)//md5签名
            {
                CwxMd5 md5;
                char szMd5[16];
                md5.update(pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
                md5.final(szMd5);
                if (!pTss->m_pWriter->addKeyValue(CWX_MQ_MD5, szMd5, 16))
                {
                    CWX_ERROR(("Failure to add key value, err:%s", pTss->m_pWriter->getErrMsg()));
                    return -1;
                }
            }
        }

        pTss->m_pWriter->pack();
        if (CWX_MQ_SUCCESS != CwxMqPoco::packMultiSyncData(0,
            pTss->m_pWriter->getMsg(), 
            pTss->m_pWriter->getMsgSize(),
            pBlock,
            m_dispatch.m_bZip,
            pTss->m_szBuf2K))
        {
            return -1;
        }
    }
    ///根据svr类型，发送数据包
    pBlock->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
    pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_ASYNC);
    pBlock->send_ctrl().setHostId(0);
    pBlock->event().setTaskId(m_dispatch.m_pCursor->getHeader().getSid()&0xFFFFFFFF);
    pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::FINISH_NOTICE);
    if (!putMsg(pBlock))
    {
        CWX_ERROR(("Failure to send binlog"));
        CwxMsgBlockAlloc::free(pBlock);
        return -1;
    }
    m_dispatch->m_sendingSid.insert(ullSid);
    return 1; ///发送了一条消息
}

