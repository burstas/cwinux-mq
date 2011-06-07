#include "CwxSocket.h"
#include "CwxINetAddr.h"
#include "CwxSockStream.h"
#include "CwxSockConnector.h"
#include "CwxGetOpt.h"
#include "CwxMqPoco.h"
#include "CwxZlib.h"
using namespace cwinux;
string g_strHost;
CWX_UINT16 g_unPort = 0;
string g_user;
string g_passwd;
string g_subscribe;
CWX_UINT64 g_sid = 0;
CWX_UINT32 g_window = 1;
CWX_UINT32 g_num = 1;
string    g_sign;
bool      g_zip = false;
CWX_UINT32 g_chunk = 0;
CWX_UINT32 const CWX_TOOL_MAX_CHUNK_SIZE =10 * 1024 * 1024;
unsigned char g_unzip[CWX_TOOL_MAX_CHUNK_SIZE * 4];
CWX_UINT32 const g_unzip_buf_len = CWX_TOOL_MAX_CHUNK_SIZE * 4;
unsigned long g_unzip_len = 0;
///-1：失败；0：help；1：成功
int parseArg(int argc, char**argv)
{
    CwxGetOpt cmd_option(argc, argv, "H:P:u:p:s:w:n:S:zh");
    int option;
    cmd_option.long_option("sid", 'i', CwxGetOpt::ARG_REQUIRED);
    cmd_option.long_option("chunk", 'C', CwxGetOpt::ARG_REQUIRED);
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("Recieve mq message from the dispatch port.\n", argv[0]);
            printf("%s  -H host -P port\n", argv[0]);
            printf("-H: mq server dispatch host\n");
            printf("-P: mq server dispatch port\n");
            printf("-u: dispatch's user name.\n");
            printf("-p: dispatch's user password.\n");
            printf("-s: dispatch's subscribe. default is *.\n");
            printf("-w: dispatch's window size, default is 1.\n");
            printf("-n: recieve message's number, default is 1.zero is all from the sid.\n");
            printf("-z: zip compress sign. no compress by default\n");
            printf("-S: signature type, %s or %s. no signature by default\n", CWX_MQ_MD5, CWX_MQ_CRC32);
            printf("--sid: start sid, zero for the current max sid.\n");
            printf("--chunk: chunk size in kbyte. default is zero for no chunk.\n");
            printf("-h: help\n");
            return 0;
        case 'H':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-H requires an argument.\n");
                return -1;
            }
            g_strHost = cmd_option.opt_arg();
            break;
        case 'P':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-P requires an argument.\n");
                return -1;
            }
            g_unPort = strtoul(cmd_option.opt_arg(), NULL, 0);
            break;
        case 'u':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-u requires an argument.\n");
                return -1;
            }
            g_user = cmd_option.opt_arg();
            break;
        case 'p':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-p requires an argument.\n");
                return -1;
            }
            g_passwd = cmd_option.opt_arg();
            break;
        case 's':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-s requires an argument.\n");
                return -1;
            }
            g_subscribe = cmd_option.opt_arg();
            break;
        case 'w':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-w requires an argument.\n");
                return -1;
            }
            g_window = strtoul(cmd_option.opt_arg(),NULL,0);
            break;
        case 'n':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-n requires an argument.\n");
                return -1;
            }
            g_num = strtoul(cmd_option.opt_arg(),NULL,0);
            break;
        case 'C':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-c requires an argument.\n");
                return -1;
            }
            g_chunk = strtoul(cmd_option.opt_arg(),NULL,0);
            if (g_chunk > CWX_TOOL_MAX_CHUNK_SIZE/1024) g_chunk = CWX_TOOL_MAX_CHUNK_SIZE * 1024;
            break;
        case 'z':
            g_zip = true;
            break;
        case 'S':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-s requires an argument.\n");
                return -1;
            }
            g_sign = cmd_option.opt_arg();
            if ((g_sign != CWX_MQ_CRC32) && (g_sign != CWX_MQ_MD5))
            {
                printf("signature must be %s or %s\n", CWX_MQ_MD5, CWX_MQ_CRC32);
            }
            break;
        case 'i':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("--sid requires an argument.\n");
                return -1;
            }
            g_sid = strtoull(cmd_option.opt_arg(),NULL,0);
            break;
        case ':':
            printf("%c requires an argument.\n", cmd_option.opt_opt ());
            return -1;
        case '?':
            break;
        default:
            printf("Invalid arg %s.\n", argv[cmd_option.opt_ind()-1]);
            return -1;
        }
    }
    if (-1 == option)
    {
        if (cmd_option.opt_ind()  < argc)
        {
            printf("Invalid arg %s.\n", argv[cmd_option.opt_ind()]);
            return -1;
        }
    }
    if (!g_strHost.length())
    {
        printf("No host, set by -H\n");
        return -1;
    }
    if (!g_unPort)
    {
        printf("No port, set by -P\n");
        return -1;
    }
    if (!g_subscribe.length()) g_subscribe = "*";
    return 1;
}


static int output(CwxPackageReader& reader, char const* szMsg, CWX_UINT32 uiMsgLen)
{
    CWX_UINT64 ullSid = 0;
    CWX_UINT32 num = 0;
    CWX_UINT32 group = 0;
    CWX_UINT32 type = 0;
    CWX_UINT32 attr = 0;
    CWX_UINT32 timestamp = 0;
    CwxKeyValueItem const* item=NULL;
    char szErr2K[2048];
    if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseSyncData(
        &reader,
        szMsg,
        uiMsgLen,
        ullSid,
        timestamp,
        item,
        group,
        type,
        attr,
        szErr2K))
    {
        printf("failure to unpack recieve msg, err=%s\n", szErr2K);
        return -1;
    }
    printf("%s|%u|%u|%u|%u|%s\n",
        CwxCommon::toString(ullSid, szErr2K, 10),
        timestamp,
        group,
        type,
        attr,
        item->m_szData);
    return 0;
}

static bool checkSign(char const* data,
                                   CWX_UINT32 uiDateLen,
                                   char const* szSign,
                                   char const* sign)
{
    if (!sign) return true;
    if (strcmp(sign, CWX_MQ_CRC32) == 0)//CRC32签名
    {
        CWX_UINT32 uiCrc32 = CwxCrc32::value(data, uiDateLen);
        if (memcmp(&uiCrc32, szSign, sizeof(uiCrc32)) == 0) return true;
        return false;
    }
    else if (strcmp(sign, CWX_MQ_MD5)==0)//md5签名
    {
        CwxMd5 md5;
        unsigned char szMd5[16];
        md5.update((const unsigned char*)data, uiDateLen);
        md5.final(szMd5);
        if (memcmp(szMd5, szSign, 16) == 0) return true;
        return false;
    }
    return true;
}

static bool isFinish(CWX_UINT32 num)
{
    if (g_num)
    {
        return (num >= g_num);
    }
    return false;
}

int main(int argc ,char** argv)
{
    int iRet = parseArg(argc, argv);

    if (0 == iRet) return 0;
    if (-1 == iRet) return 1;

    CwxSockStream  stream;
    CwxINetAddr  addr(g_unPort, g_strHost.c_str());
    CwxSockConnector conn;
    if (0 != conn.connect(stream, addr))
    {
        printf("failure to connect ip:port: %s:%u, errno=%d\n", g_strHost.c_str(), g_unPort, errno);
        return 1;
    }
    CwxPackageWriter writer;
    CwxPackageReader reader;
    CwxPackageReader reader_chunk;
    CwxMsgHead head;
    CwxMsgBlock* block=NULL;
    char szErr2K[2048];
    char const* pErrMsg=NULL;
    CWX_UINT64 ullSid = 0;
    CWX_UINT32 num = 0;
    CwxMqPoco::init();
    do 
    {
        if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packReportData(
            &writer,
            block,
            0,
            g_sid>0?g_sid-1:g_sid,
            g_sid==0?true:false,
            g_chunk * 1024,
            g_window,
            g_subscribe.c_str(),
            g_user.c_str(),
            g_passwd.c_str(),
            g_sign.c_str(),
            g_zip,
            szErr2K
            ))
        {
            printf("failure to pack report-queue package, err=%s\n", szErr2K);
            iRet = 1;
            break;
        }
        if (block->length() != CwxSocket::write_n(stream.getHandle(),
            block->rd_ptr(),
            block->length()))
        {
            printf("failure to send message, errno=%d\n", errno);
            iRet = 1;
            break;
        }
        CwxMsgBlockAlloc::free(block);
        block = NULL;
        while(1)
        {
            //recv msg
            if (0 >= CwxSocket::read(stream.getHandle(), head, block))
            {
                printf("failure to read the reply, errno=%d\n", errno);
                iRet = 1;
                break;
            }
            if (CwxMqPoco::MSG_TYPE_SYNC_REPORT_REPLY == head.getMsgType())
            {
                if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseReportDataReply(
                    &reader,
                    block,
                    iRet,
                    ullSid,
                    pErrMsg,
                    szErr2K
                    ))
                {
                    printf("failure to unpack report-reply msg, err=%s\n", szErr2K);
                    iRet = 1;
                    break;
                }
                printf("failure to report dispatch, err-code=%d, err=%s\n", iRet, pErrMsg);
                iRet = 1;
                break;
            }
            else if (CwxMqPoco::MSG_TYPE_SYNC_DATA == head.getMsgType())
            {
                if (head.isAttr(CwxMsgHead::ATTR_COMPRESS))
                {
                    g_unzip_len = g_unzip_buf_len;
                    if (!CwxZlib::unzip(g_unzip, g_unzip_len, (unsigned char const*)block->rd_ptr(), block->length()))
                    {
                        printf("failure to unzip recieved msg.\n");
                        iRet = 1;
                        break;
                    }
                }
                if (!g_chunk)
                {
                    num++;
                    if (0 != output(reader,
                        head.isAttr(CwxMsgHead::ATTR_COMPRESS)?(char const*)g_unzip:block->rd_ptr(),
                        head.isAttr(CwxMsgHead::ATTR_COMPRESS)?g_unzip_len:block->length()))
                    {
                        iRet = 1;
                        break;
                    }
                    iRet = 0;
                    if (isFinish(num))
                        break;
                }
                else
                {
                    if (0 != reader_chunk.unpack(head.isAttr(CwxMsgHead::ATTR_COMPRESS)?(char const*)g_unzip:block->rd_ptr(),
                        head.isAttr(CwxMsgHead::ATTR_COMPRESS)?g_unzip_len:block->length(),
                        false,
                        false))
                    {
                        printf("failure to unpack msg, err=%s\n", reader_chunk.getErrMsg());
                        iRet = 1;
                        break;
                    }
                    if (g_sign.length())
                    {
                        CwxKeyValueItem const* pItem = reader_chunk.getKey(g_sign.c_str());
                        if (pItem)
                        {//存在签名key
                            if (!checkSign(reader_chunk.getMsg(),
                                pItem->m_szKey - CwxPackage::getKeyOffset() - reader_chunk.getMsg(),
                                pItem->m_szData,
                                g_sign.c_str()))
                            {
                                printf("failure to check %s signature\n", g_sign.c_str());
                                iRet = 1;
                                break;
                            }
                        }
                    }
                    iRet = 0;
                    for (CWX_UINT16 i=0; i<reader_chunk.getKeyNum()-1; i++)
                    {
                        if(0 != strcmp(reader_chunk.getKey(i)->m_szKey, CWX_MQ_M))
                        {
                            printf("Master multi-binlog's key must be:%s, but:%s", CWX_MQ_M, reader_chunk.getKey(i)->m_szKey);
                            iRet = 1;
                            break;
                        }
                        num++;
                        if (0 != output(reader,
                            reader_chunk.getKey(i)->m_szData,
                            reader_chunk.getKey(i)->m_uiDataLen))
                        {
                            iRet = 1;
                            break;
                        }
                        if (isFinish(num))
                            break;
                    }
                }
                if (1 == iRet) break;

                CwxMsgBlockAlloc::free(block);
                block = NULL;
                if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncDataReply(&writer,
                    block,
                    0,
                    ullSid,
                    szErr2K))
                {
                    printf("failure to pack recieve data reply package, err=%s\n", szErr2K);
                    iRet = 1;
                    break;
                }
                if (block->length() != CwxSocket::write_n(stream.getHandle(),
                    block->rd_ptr(),
                    block->length()))
                {
                    printf("failure to send message, errno=%d\n", errno);
                    iRet = 1;
                    break;
                }
                CwxMsgBlockAlloc::free(block);
                block = NULL;
                continue;
            }
            else
            {
                printf("recv a unknow msg type, msg_type=%u\n", head.getMsgType());
                iRet = 1;
                break;
            }
        }
    } while(0);
    if (block) CwxMsgBlockAlloc::free(block);
    CwxMqPoco::destory();
    stream.close();
    return iRet;
}
