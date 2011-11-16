##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)

COMMON_SRC=ZkAdaptor.cpp ZkToolAdaptor.cpp zk_common.cpp

bin_PROGRAMS =zk_create zk_get zk_update zk_list zk_delete zk_loopcreate zk_get_acl zk_set_acl zk_exist
zk_create_CPPFLAGS = -W -Wall -Wno-deprecated
zk_create_SOURCES = $(COMMON_SRC) zk_create.cpp
zk_create_LDADD= 
zk_create_LDFLAGS = -L@zk_dir@/lib

zk_loopcreate_CPPFLAGS =  -W -Wall -Wno-deprecated
zk_loopcreate_SOURCES = $(COMMON_SRC) zk_loopcreate.cpp 
zk_loopcreate_LDADD=
zk_loopcreate_LDFLAGS = -L@zk_dir@/lib

zk_get_CPPFLAGS =  -W -Wall -Wno-deprecated 
zk_get_SOURCES = $(COMMON_SRC) zk_get.cpp 
zk_get_LDADD=
zk_get_LDFLAGS = -L@zk_dir@/lib

zk_delete_CPPFLAGS =  -W -Wall -Wno-deprecated
zk_delete_SOURCES =  $(COMMON_SRC) zk_delete.cpp  
zk_delete_LDADD=
zk_delete_LDFLAGS = -L@zk_dir@/lib

zk_update_CPPFLAGS =  -W -Wall -Wno-deprecated
zk_update_SOURCES = $(COMMON_SRC) zk_update.cpp  
zk_update_LDADD=
zk_update_LDFLAGS = -L@zk_dir@/lib

zk_list_CPPFLAGS =  -W -Wall -Wno-deprecated
zk_list_SOURCES =  $(COMMON_SRC) zk_list.cpp
zk_list_LDADD=
zk_list_LDFLAGS = -L@zk_dir@/lib

zk_get_acl_CPPFLAGS =  -W -Wall -Wno-deprecated
zk_get_acl_SOURCES = $(COMMON_SRC)  zk_get_acl.cpp
zk_get_acl_LDADD=
zk_get_acl_LDFLAGS = -L@zk_dir@/lib

zk_set_acl_CPPFLAGS =  -W -Wall -Wno-deprecated
zk_set_acl_SOURCES = $(COMMON_SRC)  zk_set_acl.cpp 
zk_set_acl_LDADD=
zk_set_acl_LDFLAGS =  -L@zk_dir@/lib -L@openssl_dir@/lib

zk_exist_CPPFLAGS =  -W -Wall -Wno-deprecated
zk_exist_SOURCES = $(COMMON_SRC)  zk_exist.cpp
zk_exist_LDADD=
zk_exist_LDFLAGS =  -L@zk_dir@/lib -L@openssl_dir@/lib

LIBS=-lpthread -lzookeeper_mt -lssl
INCLUDES=-I@zk_dir@/include -I@openssl_dir@/include


## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
