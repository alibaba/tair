AM_CPPFLAGS= @BOOST_CPPFLAGS@ -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage/mdb \
			 -I${top_srcdir}/src/storage/kdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/client \
       -I${top_srcdir}/src  \
       -I${top_srcdir}/src/storage/kdb/kyotocabinet

LDADD= \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
	  $(top_builddir)/src/storage/mdb/.libs/libmdb.a \
	  $(top_builddir)/src/storage/kdb/libkdb.a \
    ${top_builddir}/src/storage/kdb/kyotocabinet/libkyotocabinet.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a 

base_source=interface_test_main.cpp interface_test_base.cpp interface_test_base.h

source_list=${base_source} \
			CTAIRCLIENTAPI_03_x_put_Test.cpp \
			CTAIRCLIENTAPI_03_x_put_oterfunction_Test.cpp \
			CTAIRCLIENTAPI_04_x_get_Test.cpp \
			CTAIRCLIENTAPI_05_x_remove_Test.cpp \
			CTAIRCLIENTAPI_13_x_incr_Test.cpp \
			CTAIRCLIENTAPI_13_x_incr_otherfunction_Test.cpp \
			CTAIRCLIENTAPI_14_x_decr_Test.cpp \
			CTAIRCLIENTAPI_14_x_decr_otherfunction_Test.cpp \
			CTAIRCLIENTAPI_08_x_getItems_Test.cpp \
			CTAIRCLIENTAPI_07_x_addItems_Test.cpp \
			CTAIRCLIENTAPI_07_x_addItems_otherfunction_Test.cpp \
			CTAIRCLIENTAPI_09_x_getAndRemove_Test.cpp \
			CTAIRCLIENTAPI_15_x_set_count_Test.cpp \
			CTAIRCLIENTAPI_16_x_lock_Test.cpp \
			CTAIRCLIENTAPI_17_x_mget_Test.cpp \
			CTAIRCLIENTAPI_18_x_range_Test.cpp

AM_LDFLAGS=-lpthread -lgtest  -lz -lrt ${BOOST_LDFLAGS} ${GCOV_LIB}  @BOOST_THREAD_LIB@

sbin_PROGRAMS=interface_test kdb_test_stat kdb_test_manager kdb_test_bucket kdb_test_kyotocabinet

interface_test_SOURCES=${source_list}

kdb_test_stat_SOURCES=test_stat_manager.cpp
kdb_test_manager_SOURCES=test_kdb_manager.cpp
kdb_test_bucket_SOURCES=test_kdb_bucket.cpp
kdb_test_kyotocabinet_SOURCES=test_kdb_kyotocabinet.cpp
