##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)


bin_PROGRAMS =mq_statinfo mq_add mq_del mq_fetch mq_recv mq_send mq_md5 mq_crc32
mq_statinfo_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
mq_statinfo_SOURCES = stat_info.cpp 
mq_statinfo_LDADD= 
mq_statinfo_LDFLAGS = -L@cwinux_dir@/lib 

mq_add_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
mq_add_SOURCES = add_mq.cpp ../common/CwxMqPoco.cpp
mq_add_LDADD=
mq_add_LDFLAGS = -L@cwinux_dir@/lib 

mq_del_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
mq_del_SOURCES = remove_mq.cpp ../common/CwxMqPoco.cpp
mq_del_LDADD=
mq_del_LDFLAGS = -L@cwinux_dir@/lib 

mq_send_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
mq_send_SOURCES = mq_send.cpp ../common/CwxMqPoco.cpp
mq_send_LDADD=
mq_send_LDFLAGS = -L@cwinux_dir@/lib 

mq_recv_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
mq_recv_SOURCES = mq_recv.cpp ../common/CwxMqPoco.cpp
mq_recv_LDADD=
mq_recv_LDFLAGS = -L@cwinux_dir@/lib 

mq_fetch_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
mq_fetch_SOURCES = mq_fetch.cpp ../common/CwxMqPoco.cpp
mq_fetch_LDADD=
mq_fetch_LDFLAGS = -L@cwinux_dir@/lib 

mq_md5_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
mq_md5_SOURCES = mq_md5.cpp
mq_md5_LDADD=
mq_md5_LDFLAGS = -L@cwinux_dir@/lib -W -Wall -Wno-deprecated

mq_crc32_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"  -W -Wall -Wno-deprecated
mq_crc32_SOURCES = mq_crc32.cpp
mq_crc32_LDADD=
mq_crc32_LDFLAGS = -L@cwinux_dir@/lib

LIBS=-lcwxnet -lcwxcmn -lcwxutil -lz
INCLUDES=-I@cwinux_dir@/include/net -I@cwinux_dir@/include/cmn  -I@cwinux_dir@/include/util -I../common


## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
