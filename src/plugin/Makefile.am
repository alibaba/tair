AM_CPPFLAGS= -I${top_srcdir}/deps/tbsys-install/include \
			 -I${top_srcdir}/deps/libeasy-install/include/easy \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/client\
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/dataserver

AM_CXXFLAGS=-Werror

LDADD=${top_srcdir}/deps/tbsys-install/lib/libtbsys.a \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
	  ${top_srcdir}/deps/libeasy-install/lib64/libeasy.a \
	  $(top_builddir)/src/common/libtair_common.a

AM_LDFLAGS=-lz -lrt -lpthread -ldl ${GCOV_LIB} -lcrypto

noinst_LIBRARIES=libtair_plugins.a
libtair_plugins_a_SOURCES=plugin_manager.cpp

lib_LTLIBRARIES=libsample_plugin.la

include_HEADERS= plugin_manager.hpp base_plugin.hpp

libsample_plugin_la_SOURCES=sample_plugin.cpp base_plugin.hpp plugin_manager.hpp sample_plugin.hpp
libsample_plugin_la_LDFLAGS= -fpic -shared

