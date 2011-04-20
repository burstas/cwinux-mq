##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)


bin_PROGRAMS = cwx_mq
cwx_mq_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)" -W -Wall
cwx_mq_SOURCES = \
        ../common/CwxMqTss.cpp\
        ../common/CwxMqPoco.cpp\
        ../common/CwxMqSysFile.cpp\
	../common/CwxMqDef.cpp  ../common/CwxMqQueueMgr.cpp\
	CwxMqApp.cpp CwxMqConfig.cpp  CwxMqFetchHandler.cpp\
	CwxMqAsyncHandler.cpp  cwx_mq.cpp       CwxMqMasterHandler.cpp  CwxMqRecvHandler.cpp
cwx_mq_LDADD=
cwx_mq_LDFLAGS =-L@xml_expat_dir@/lib -L@cwinux_dir@/lib -L@libevent_dir@/lib
LIBS=-lcwxapp -lcwxnet -lcwxparse -lcwxstore -lcwxcmn -lexpat -levent -lpthread -ldl -lrt
INCLUDES=-I@cwinux_dir@/include/app -I@cwinux_dir@/include/cmn -I@cwinux_dir@/include/net -I@cwinux_dir@/include/parse -I@cwinux_dir@/include/store -I@xml_expat_dir@/include -I@libevent_dir@/include  -I../common

## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
