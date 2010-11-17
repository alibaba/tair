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
noinst_LIBRARIES=libmdb.a

libmdb_a_SOURCES=${shm_source_list} mdb_factory.cpp mdb_define.cpp \
					cache_hashmap.hpp	\
					mdb_define.hpp	\
					mdb_factory.hpp	\
					mdb_manager.hpp	\
					mdb_stat.hpp	\
					mem_cache.hpp   \
					mem_pool.hpp
#libmdb_a_LDFLAGS=${AM_LDFLAGS} 

noinst_PROGRAMS=mdbtest mdbSlabAndAreaTest mdbAreaTest 
mdbtest_SOURCES=mdb_test.cpp ../../common/data_dumpper.cpp ../../common/stat_helper.cpp 
mdbtest_LDADD=libmdb.a $(LDADD) -lpthread -lrt  @BOOST_THREAD_LIB@

#noinst_PROGRAMS=mdbSlabAndAreaTest
mdbSlabAndAreaTest_SOURCES=mdb_slab_test.cpp ../../common/data_dumpper.cpp ../../common/stat_helper.cpp

mdbSlabAndAreaTest_LDADD=libmdb.a $(LDADD) -lpthread -lrt  @BOOST_THREAD_LIB@

#noinst_PROGRAMS=mdbAreaTest
mdbAreaTest_SOURCES=mdb_area_test.cpp ../../common/data_dumpper.cpp ../../common/stat_helper.cpp 
mdbAreaTest_LDADD=libmdb.a $(LDADD) -lpthread -lrt  @BOOST_THREAD_LIB@

