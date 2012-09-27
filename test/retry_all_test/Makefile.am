AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
	     -I$(TBLIB_ROOT)/include/tbnet \
	     -I${top_srcdir}/src \
	     -I${top_srcdir}/src/common \
	     -I${top_srcdir}/src/packets \
	     -I${top_srcdir}/src/plugin \
	     -I${top_srcdir}/src/client \
	     -I${top_srcdir}/src/storage \
	     -I${top_srcdir}/src/statserver \
	     -I${top_srcdir}/src/statserver/include \
	     -I${top_srcdir}/src/dataserver \
	     @BOOST_CPPFLAGS@

AM_LDFLAGS= -rdynamic $(BOOST_LDFLAGS)  ${GCOV_LIB}

if WITH_COMPRESS
COMPRESS_LDFLAGS= -lsnappy
endif

LDADD=$(top_builddir)/src/packets/libtair_packets.a \
	  $(top_builddir)/src/client/.libs/libtairclientapi_impl.a \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
		$(TBLIB_ROOT)/lib/libtbnet.a \
		$(TBLIB_ROOT)/lib/libtbsys.a \
		$(top_builddir)/src/common/libtair_common.a \
		-lz -lrt -lpthread \
		$(COMPRESS_LDFLAGS) \
		-ldl \
		@BOOST_THREAD_LIB@



noinst_PROGRAMS=retry_all_test
retry_all_test_SOURCES=tair_client_proxy.hpp tair_client_proxy.cpp \
											 retry_all_test.cpp
