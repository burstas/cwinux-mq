#include "CwxAppProcessMgr.h"
#include "CwxMproxyApp.h"

int main(int argc, char** argv)
{
    CwxMproxyApp* pApp = new CwxMproxyApp();
    if (0 != CwxAppProcessMgr::init(pApp)) return 1;
    CwxAppProcessMgr::start(argc, argv, 200, 300);
    return 0;
}
