##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)


bin_PROGRAMS =zk_create
zk_create_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
zk_create_SOURCES = ZkAdaptor.cpp  zk_create.cpp
zk_create_LDADD= 
zk_create_LDFLAGS = -L@cwinux_dir@/lib  -L@zk_dir@/lib

LIBS=-lcwxcmn -lpthread -lzookeeper_st
INCLUDES=-I@cwinux_dir@/include/cmn  -I@zk_dir@/include


## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
