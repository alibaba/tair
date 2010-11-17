AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
	         -I${top_srcdir}/src/common 

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a

AM_LDFLAGS=-lz -lrt -lpthread -ldl  ${GCOV_LIB}

noinst_LIBRARIES=libtair_plugins.a
libtair_plugins_a_SOURCES=plugin_manager.cpp

lib_LTLIBRARIES=libsample_plugin.la

include_HEADERS= plugin_manager.hpp base_plugin.hpp 

libsample_plugin_la_SOURCES=sample_plugin.cpp base_plugin.hpp plugin_manager.hpp sample_plugin.hpp
libsample_plugin_la_LDFLAGS= -fpic -shared 

