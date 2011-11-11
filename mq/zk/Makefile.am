##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)


bin_PROGRAMS =zk_create zk_get zk_update zk_list zk_delete zk_loopcreate zk_get_acl zk_set_acl
zk_create_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
zk_create_SOURCES = ZkAdaptor.cpp ZkJPoolAdaptor.cpp  zk_create.cpp zk_common.cpp
zk_create_LDADD= 
zk_create_LDFLAGS = -L@cwinux_dir@/lib  -L@zk_dir@/lib

zk_loopcreate_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
zk_loopcreate_SOURCES = ZkAdaptor.cpp ZkJPoolAdaptor.cpp  zk_loopcreate.cpp  zk_common.cpp
zk_loopcreate_LDADD=
zk_loopcreate_LDFLAGS = -L@cwinux_dir@/lib  -L@zk_dir@/lib

zk_get_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated 
zk_get_SOURCES = ZkAdaptor.cpp ZkJPoolAdaptor.cpp  zk_get.cpp  zk_common.cpp
zk_get_LDADD=
zk_get_LDFLAGS = -L@cwinux_dir@/lib  -L@zk_dir@/lib

zk_delete_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
zk_delete_SOURCES = ZkAdaptor.cpp ZkJPoolAdaptor.cpp  zk_delete.cpp  zk_common.cpp
zk_delete_LDADD=
zk_delete_LDFLAGS = -L@cwinux_dir@/lib  -L@zk_dir@/lib

zk_update_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
zk_update_SOURCES = ZkAdaptor.cpp ZkJPoolAdaptor.cpp  zk_update.cpp  zk_common.cpp
zk_update_LDADD=
zk_update_LDFLAGS = -L@cwinux_dir@/lib  -L@zk_dir@/lib

zk_list_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
zk_list_SOURCES = ZkAdaptor.cpp ZkJPoolAdaptor.cpp  zk_list.cpp  zk_common.cpp
zk_list_LDADD=
zk_list_LDFLAGS = -L@cwinux_dir@/lib  -L@zk_dir@/lib

zk_get_acl_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
zk_get_acl_SOURCES = ZkAdaptor.cpp ZkJPoolAdaptor.cpp  zk_get_acl.cpp  zk_common.cpp
zk_get_acl_LDADD=
zk_get_acl_LDFLAGS = -L@cwinux_dir@/lib  -L@zk_dir@/lib

zk_set_acl_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
zk_set_acl_SOURCES = ZkAdaptor.cpp ZkJPoolAdaptor.cpp  zk_set_acl.cpp  zk_common.cpp
zk_set_acl_LDADD=
zk_set_acl_LDFLAGS = -L@cwinux_dir@/lib  -L@zk_dir@/lib -L@openssl_dir@/lib


LIBS=-lcwxcmn -lpthread -lzookeeper_mt -lssl
INCLUDES=-I@cwinux_dir@/include/cmn  -I@zk_dir@/include -I@openssl_dir@/include


## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
