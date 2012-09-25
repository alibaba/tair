SUBDIRS=leveldb

AM_CPPFLAGS=-I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I${top_builddir}/src/storage/ldb/leveldb/include -I${top_builddir}/src/storage/ldb/leveldb/ -I${top_srcdir}/src -I${top_srcdir}/src/storage -I${top_srcdir}/src/common -I.

if WITH_TCMALLOC
AM_CPPFLAGS += -DWITH_TCMALLOC
endif

AM_LDFLAGS=-lpthread -lrt ${GCOV_LIB}
noinst_LIBRARIES=libldb.a

libldb_a_SOURCES=ldb_manager.cpp ldb_manager.hpp ldb_instance.hpp ldb_instance.cpp stat_manager.cpp stat_manager.hpp ldb_define.hpp ldb_define.cpp bg_task.hpp bg_task.cpp ldb_comparator.hpp ldb_comparator.cpp ldb_gc_factory.hpp ldb_gc_factory.cpp ldb_cache_stat.hpp ldb_cache_stat.cpp ldb_bloom.cpp ldb_remote_sync_logger.hpp ldb_remote_sync_logger.cpp


view_cache_stat_LDADD=libldb.a ${top_builddir}/src/common/libtair_common.a $(TBLIB_ROOT)/lib/libtbsys.a

sbin_PROGRAMS=view_cache_stat ldb_rsync
view_cache_stat_SOURCES=view_cache_stat.cpp

ldb_rsync_SOURCES=ldb_rsync.cpp
ldb_rsync_CPPFLAGS=${AM_CPPFLAGS} -I${top_srcdir}/src/packets -I${top_srcdir}/src/dataserver \
		  -I${top_srcdir}/src/plugin -I${top_srcdir}/src/statserver/include
ldb_rsync_LDADD=libldb.a ./leveldb/libleveldb.a ./leveldb/libsnappy.a ${top_builddir}/src/client/.libs/libtairclientapi.a \
		${top_builddir}/src/common/libtair_common.a $(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a\
		${BOOST_LDFLAGS} @BOOST_THREAD_LIB@ -lpthread -lz -lrt
