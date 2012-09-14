#include "CwxAppProcessMgr.h"
#include "CwxMqImportApp.h"

int main(int argc, char** argv){
    CwxMqImportApp* pApp = new CwxMqImportApp();
    if (0 != CwxAppProcessMgr::init(pApp)) return 1;
    CwxAppProcessMgr::start(argc, argv, 200, 300);
    return 0;
}
