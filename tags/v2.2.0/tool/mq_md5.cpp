#include "CwxMd5.h"
#include "CwxGetOpt.h"
#include "CwxStl.h"
#include "CwxFile.h"
using namespace cwinux;
string g_strMd5;
string g_strMd5File;
///-1£ºÊ§°Ü£»0£ºhelp£»1£º³É¹¦
int parseArg(int argc, char**argv)
{
    CwxGetOpt cmd_option(argc, argv, "c:f:h");
    int option;
    while( (option = cmd_option.next()) != -1)
    {
        switch (option)
        {
        case 'h':
            printf("cwx_md5 -c md5 string -f file to md5.\n");
            return 0;
        case 'f':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-f requires an argument.\n");
                return -1;
            }
            g_strMd5File = cmd_option.opt_arg();
            break;
        case 'c':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-c requires an argument.\n");
                return -1;
            }
            g_strMd5 = cmd_option.opt_arg();
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
    if (!g_strMd5.length() && !g_strMd5File.length())
    {
        printf("must set md5 contest by -c or -f\n");
        return -1;
    }
    return 1;
}

int main(int argc ,char** argv)
{
    int iRet = parseArg(argc, argv);
    unsigned char szMd5[16];

    if (0 == iRet) return 0;
    if (-1 == iRet) return 1;
    CwxMd5 md5;
    if (g_strMd5.length())
    {
        md5.update((unsigned char*)g_strMd5.c_str(), g_strMd5.length());
        md5.final(szMd5);
    }
    else
    {
        ssize_t file_size = CwxFile::getFileSize(g_strMd5File.c_str());
        if (-1 == file_size)
        {
            printf("failure to get file size,  file:%s, errno=%d\n", g_strMd5File.c_str(), errno);
            return -1;
        }
        unsigned char* buf=(unsigned char*)malloc(file_size);
        if (!buf)
        {
            printf("failure to malloc memory, size=%d\n", file_size);
            return -1;
        }
        FILE* fd = fopen(g_strMd5File.c_str(), "rb");
        if (!fd)
        {
            free(buf);
            printf("failure to open file :%s, errno=%d\n", g_strMd5File.c_str(), errno);
            return -1;
        }
        fread(buf, 1, file_size, fd);
        fclose(fd);
        md5.update(buf, file_size);
        md5.final(szMd5);
        free(buf);
    }
    char szTmp[33];
    CWX_UINT32 i=0;
    for (i=0; i<16; i++)
    {
        sprintf(szTmp + i*2, "%2.2x", szMd5[i]);
    }
    printf("md5:%s\n", szTmp);
    return 0;
}
