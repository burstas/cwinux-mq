##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)


bin_PROGRAMS = cwx_mproxy
cwx_mproxy_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)" -W -Wall
cwx_mproxy_SOURCES = \
        ../common/CwxMqTss.cpp\
        ../common/CwxMqPoco.cpp\
	../common/CwxMqDef.cpp  ../common/CwxMqQueueMgr.cpp\
	CwxMproxyApp.cpp  CwxMproxyConfig.cpp  cwx_mproxy.cpp\
	CwxMproxyMqHandler.cpp  CwxMproxyRecvHandler.cpp  CwxMproxyTask.cpp
cwx_mproxy_LDADD=
cwx_mproxy_LDFLAGS =-L@xml_expat_dir@/lib -L@cwinux_dir@/lib 
LIBS=-lcwxapp -lcwxutil -lcwxnet -lcwxparse -lcwxstore -lcwxcmn -lexpat -lpthread -ldl -lrt -lz
INCLUDES=-I@cwinux_dir@/include/app -I@cwinux_dir@/include/cmn -I@cwinux_dir@/include/net -I@cwinux_dir@/include/parse -I@cwinux_dir@/include/store -I@cwinux_dir@/include/util -I@xml_expat_dir@/include  -I../common

## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
