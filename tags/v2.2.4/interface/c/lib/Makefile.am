##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir = 

lib_LTLIBRARIES = libcwxmq.la
libcwxmq_la_CPPFLAGS = -W -Wall

libcwxmq_la_SOURCES = cwx_mq_poco.c
libcwxmq_la_LDFLAGS = 

cwxmqincludedir=$(prefix)/include/
cwxmqinclude_HEADERS=cwx_mq_poco.h

	
INCLUDES=-I@libcwinux_c_dir@/include -I$(srcdir)

## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
