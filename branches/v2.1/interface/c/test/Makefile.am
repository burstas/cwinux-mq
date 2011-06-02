##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE=$(shell date +%F)


bin_PROGRAMS = mq_test_send mq_test_recv mq_test_fetch
mq_test_send_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"
mq_test_send_SOURCES = send.cpp
mq_test_send_LDADD= 
mq_test_send_LDFLAGS = -L@libcwinux_dir@/lib -L$(srcdir)/../lib/.libs  -L@libcwinux_c_dir@/lib -W -Wall

mq_test_recv_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"
mq_test_recv_SOURCES = recv.cpp
mq_test_recv_LDADD= 
mq_test_recv_LDFLAGS = -L@libcwinux_dir@/lib -L$(srcdir)/../lib/.libs  -L@libcwinux_c_dir@/lib -W -Wall

mq_test_fetch_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)"
mq_test_fetch_SOURCES = fetch.cpp
mq_test_fetch_LDADD= 
mq_test_fetch_LDFLAGS = -L@libcwinux_dir@/lib -L$(srcdir)/../lib/.libs  -L@libcwinux_c_dir@/lib -W -Wall



LIBS=-lcwxnet -lcwxcmn -lcwxmq -lcwinux  -lz
INCLUDES=-I$(srcdir)/../lib/ -I@libcwinux_dir@/include/net -I@libcwinux_dir@/include/cmn -I@libcwinux_c_dir@/include


## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
