AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
	     -I$(TBLIB_ROOT)/include/tbnet \
	     -I${top_srcdir}/src \
	     -I${top_srcdir}/src/common \
	     -I${top_srcdir}/src/packets \
	     -I${top_srcdir}/src/plugin \
	     -I${top_srcdir}/src/client \
	     @BOOST_CPPFLAGS@

AM_LDFLAGS= -rdynamic $(BOOST_LDFLAGS)  ${GCOV_LIB}

if WITH_COMPRESS
COMPRESS_LDFLAGS= -lsnappy
endif

LDADD=$(top_builddir)/src/packets/libtair_packets.a \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
		$(TBLIB_ROOT)/lib/libtbnet.a \
		$(TBLIB_ROOT)/lib/libtbsys.a \
		$(top_builddir)/src/common/libtair_common.a \
		-lz -lrt -lpthread \
		$(COMPRESS_LDFLAGS) \
		-ldl \
		@BOOST_THREAD_LIB@



noinst_PROGRAMS=statistics_test
statistics_test_SOURCES=statistics_test.cpp
