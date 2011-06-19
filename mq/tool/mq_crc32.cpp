#include "CwxCrc32.h"
#include "CwxGetOpt.h"
#include "CwxStl.h"
using namespace cwinux;
string g_strCrc32;
string g_strCrc32File;
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
            printf("cwx_crc32 -c crc32 string -f file to crc32.\n");
            return 0;
        case 'f':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-f requires an argument.\n");
                return -1;
            }
            g_strCrc32File = cmd_option.opt_arg();
            break;
        case 'c':
            if (!cmd_option.opt_arg() || (*cmd_option.opt_arg() == '-'))
            {
                printf("-c requires an argument.\n");
                return -1;
            }
            g_strCrc32 = cmd_option.opt_arg();
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
    if (!g_strCrc32.length() || !g_strCrc32File.length())
    {
        printf("must set crc32 contest by -c or -f\n");
        return -1;
    }
    return 1;
}

int main(int argc ,char** argv)
{
    int iRet = parseArg(argc, argv);
    CWX_UINT32 uiCrc32;

    if (0 == iRet) return 0;
    if (-1 == iRet) return 1;
    if (g_strCrc32.length())
    {
        uiCrc32 = CwxCrc32::value(g_strCrc32.c_str(), g_strCrc32.length());
    }
    else
    {
        ssize_t file_size = CwxFile::getFileSize(g_strCrc32File.c_str());
        if (-1 == file_size)
        {
            printf("failure to get file size,  file:%s, errno=%d\n", g_strCrc32File.c_str(), errno);
            return -1;
        }
        unsigned char* buf=(unsigned char*)malloc(file_size);
        if (!buf)
        {
            printf("failure to malloc memory, size=%u\n", file_size);
            return -1;
        }
        FILE* fd = fopen(g_strCrc32File.c_str(), "rb");
        if (!fd)
        {
            free(buf);
            printf("failure to open file :%s, errno=%d\n", g_strCrc32File.c_str(), errno);
            return -1;
        }
        fread(buf, 1, file_size, fd);
        fclose(fd);
        uiCrc32 = CwxCrc32::value(buf, file_size);
        free(buf);
    }
    printf("crc32:%x\n", uiCrc32);
    return 0;
}
