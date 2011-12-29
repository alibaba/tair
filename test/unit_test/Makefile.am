AM_CPPFLAGS= @BOOST_CPPFLAGS@ -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage/mdb \
			 -I${top_srcdir}/src/storage/kdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/client \
			 -I${top_srcdir}/src/plugin \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/dataserver \
       -I${top_srcdir}/src  \
       -I${top_srcdir}/test 

LDADD= \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a 


AM_LDFLAGS=-lpthread -L${top_srcdir}/test/lib/ -lgtest_main -lgtest  -lz -lrt ${BOOST_LDFLAGS} ${GCOV_LIB}  @BOOST_THREAD_LIB@

sbin_PROGRAMS=string_local_cache_test data_entry_local_cache_test local_cache_bvt_test


string_local_cache_test_SOURCES=string_local_cache_test.cpp
data_entry_local_cache_test_SOURCES=data_entry_local_cache_test.cpp
local_cache_bvt_test_SOURCES=local_cache_bvt_test.cpp

