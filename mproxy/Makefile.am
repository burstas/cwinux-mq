##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)


bin_PROGRAMS = cwx_mproxy
cwx_mproxy_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)" -W -Wall  -Wno-deprecated
cwx_mproxy_SOURCES = \
        ../common/CwxMqTss.cpp\
        ../common/CwxMqPoco.cpp\
	../common/CwxMqDef.cpp\
	CwxMproxyApp.cpp  CwxMproxyConfig.cpp  cwx_mproxy.cpp\
	CwxMproxyMqHandler.cpp  CwxMproxyRecvHandler.cpp  CwxMproxyTask.cpp
cwx_mproxy_LDADD=
cwx_mproxy_LDFLAGS =-L@cwinux_dir@/lib 
LIBS=-lcwxapp -lcwxnet -lcwxcmn -lpthread -ldl -lrt -lz
INCLUDES=-I@cwinux_dir@/include/app -I@cwinux_dir@/include/cmn -I@cwinux_dir@/include/net  -I../common

## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
