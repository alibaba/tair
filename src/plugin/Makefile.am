AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
	     -I${top_srcdir}/src/ \
	     -I${top_srcdir}/src/common \
	     -I${top_srcdir}/src/packets \
	     -I${top_srcdir}/src/client \
	     -I${top_srcdir}/src/storage \
	     -I${top_srcdir}/src/statserver/include \
	     -I${top_srcdir}/src/dataserver

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(top_builddir)/src/common/libtair_common.a

AM_LDFLAGS=-lz -lrt -lpthread -ldl  ${GCOV_LIB}

noinst_LIBRARIES=libtair_plugins.a
libtair_plugins_a_SOURCES=plugin_manager.cpp

lib_LTLIBRARIES=libsample_plugin.la  libsync_plugin.la

include_HEADERS= plugin_manager.hpp base_plugin.hpp

libsample_plugin_la_SOURCES=sample_plugin.cpp base_plugin.hpp plugin_manager.hpp sample_plugin.hpp
libsample_plugin_la_LDFLAGS= -fpic -shared


libsync_plugin_la_SOURCES=base_plugin.hpp plugin_manager.hpp \
												remote_sync_manager.hpp remote_sync_manager.cpp sync_packet_list.cpp  \
												sync_packet_list.hpp \
												$(top_builddir)/src/common/file_list_factory.hpp  \
												$(top_builddir)/src/common/file_op.cpp \
												$(top_builddir)/src/client/tair_client_api_impl.cpp

libsync_plugin_la_LDFLAGS= -fpic -shared

