#include "CwxAppProcessMgr.h"
#include "CwxMqFetchApp.h"

int main(int argc, char** argv) {
  CwxMqFetchApp* pApp = new CwxMqFetchApp();
  if (0 != CwxAppProcessMgr::init(pApp))
    return 1;
  CwxAppProcessMgr::start(argc, argv, 200, 300);
  return 0;
}
