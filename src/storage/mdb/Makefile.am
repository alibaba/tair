AM_CPPFLAGS=-I${top_srcdir}/src \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet \
	    -I$(top_srcdir)/src/storage/fdb \
	    -I$(top_srcdir)/src/storage \
            -I$(top_srcdir)/src/common \
	    -I${top_srcdir}/src/storage/mdb\
	    ${BOOST_CPPFLAGS}

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a


shm_source_list=cache_hashmap.cpp \
	        mdb_manager.cpp \
	        mem_cache.cpp \
		mem_pool.cpp \
		../../common/data_dumpper.cpp

AM_LDFLAGS=-lpthread -lz -static-libgcc $(BOOST_LDFLAGS) @BOOST_THREAD_LIB@  ${GCOV_LIB}
lib_LTLIBRARIES=libmdb.la libmdb_c.la

libmdb_la_SOURCES=${shm_source_list} mdb_factory.cpp mdb_define.cpp \
					cache_hashmap.hpp	\
					mdb_define.hpp	\
					mdb_factory.hpp	\
					mdb_manager.hpp	\
					mdb_stat.hpp	\
					mem_cache.hpp   \
					../../common/stat_helper.cpp \
					mem_pool.hpp
#libmdb_a_LDFLAGS=${AM_LDFLAGS}
libmdb_c_la_SOURCES=${shm_source_list} mdb_factory.cpp mdb_define.cpp \
					cache_hashmap.hpp	\
					mdb_define.hpp	\
					mdb_factory.hpp	\
					mdb_manager.hpp	\
					mdb_stat.hpp	\
					mem_cache.hpp   \
					mem_pool.hpp \
					libmdb_c.hpp \
					../../common/stat_helper.cpp \
					libmdb_c.cpp
include_HEADERS=mdb_factory.hpp mdb_manager.hpp libmdb_c.hpp

noinst_PROGRAMS=mdbtest mdbSlabAndAreaTest mdbAreaTest lazyClearTest libmdb_test_c
mdbtest_SOURCES=mdb_test.cpp
mdbtest_LDADD=libmdb.la $(LDADD) -lpthread -lrt  @BOOST_THREAD_LIB@
mdbtest_LDFLAGS=-static

#noinst_PROGRAMS=mdbSlabAndAreaTest
mdbSlabAndAreaTest_SOURCES=mdb_slab_test.cpp

mdbSlabAndAreaTest_LDADD=libmdb.la $(LDADD) -lpthread -lrt  @BOOST_THREAD_LIB@
mdbSlabAndAreaTest_LDFLAGS=-static

#noinst_PROGRAMS=lazyClearTest
lazyClearTest_SOURCES=lazy_clear_test.cpp
lazyClearTest_LDADD=libmdb_c.la $(LDADD) -lpthread -lrt @BOOST_THREAD_LIB@
lazyClearTest_LDFLAGS=-static
#noinst_PROGRAMS=mdbAreaTest
mdbAreaTest_SOURCES=mdb_area_test.cpp
mdbAreaTest_LDADD=libmdb.la $(LDADD) -lpthread -lrt  @BOOST_THREAD_LIB@
mdbAreaTest_LDFLAGS=-static

libmdb_test_c_SOURCES=libmdb_test_c.cpp
libmdb_test_c_LDADD=libmdb_c.la $(LDADD) -lpthread -lrt @BOOST_THREAD_LIB@
libmdb_test_c_LDFLAGS=-static
