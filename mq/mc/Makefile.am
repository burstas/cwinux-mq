##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)


bin_PROGRAMS = cwx_mc
cwx_mc_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)" -W -Wall  -Wno-deprecated
cwx_mc_SOURCES = CwxMcApp.cpp\
		CwxMcConfig.cpp\
		cwx_mc.cpp\
		CwxMcQueue.cpp\
		CwxMcQueueHandler.cpp\
		CwxMcStore.cpp\
		CwxMcSyncHandler.cpp\
		../common/CwxMqDef.cpp\
                ../common/CwxMqPoco.cpp\
		../common/CwxMqTss.cpp\
                ../common/CwxMqConnector.cpp.cpp\
                ../common/CwxBinLogMgr.cpp
cwx_mc_LDADD=
cwx_mc_LDFLAGS =-L@cwinux_dir@/lib
LIBS=-lcwxapp -lcwxnet -lcwxcmn -lpthread -ldl -lrt -lz
INCLUDES=-I@cwinux_dir@/include/app -I@cwinux_dir@/include/cmn -I@cwinux_dir@/include/net  -I../common

## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
