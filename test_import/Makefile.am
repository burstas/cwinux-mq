##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)


bin_PROGRAMS = cwx_mq_import
cwx_mq_import_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)" -W -Wall  -Wno-deprecated
cwx_mq_import_SOURCES = \
	../common/CwxMqPoco.cpp  ../common/CwxMqTss.cpp\
	CwxMqImportApp.cpp  CwxMqImportConfig.cpp  cwx_mq_import.cpp ../common/CwxMqDef.cpp
cwx_mq_import_LDADD=
cwx_mq_import_LDFLAGS =-L@cwinux_dir@/lib 
LIBS=-lcwxapp -lcwxnet -lcwxcmn -lpthread -ldl -lrt -lz
INCLUDES=-I@cwinux_dir@/include/app -I@cwinux_dir@/include/cmn -I@cwinux_dir@/include/net  -I../common

## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
