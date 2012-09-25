if WITH_KDB
KDB_CPPFLAGS= -DWITH_KDB -I${top_srcdir}/src/storage/kdb -I${top_srcdir}/src/storage/kdb/kyotocabinet
KDB_LDFLAGS= $(top_builddir)/src/storage/kdb/libkdb.a $(top_builddir)/src/storage/kdb/kyotocabinet/libkyotocabinet.a
endif
if WITH_LDB
LDB_CPPFLAGS= -DWITH_LDB -I${top_srcdir}/src/storage/ldb
LDB_LDFLAGS= $(top_builddir)/src/storage/ldb/libldb.a \
             $(top_builddir)/src/storage/ldb/leveldb/libleveldb.a \
             $(top_builddir)/src/storage/ldb/leveldb/libsnappy.a
endif
if WITH_COMPRESS
COMPRESS_LDFLAGS= -lsnappy
endif

AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
	     -I$(TBLIB_ROOT)/include/tbnet \
	     -I${top_srcdir}/src \
	     -I$(TBLIB_ROOT)/src/configserver \
	     -I${top_srcdir}/src/storage \
	     -I${top_srcdir}/src/dataserver \
	     -I${top_srcdir}/src/common \
	     -I${top_srcdir}/src/statserver/include \
	     -I${top_srcdir}/src/packets \
	     -I${top_srcdir}/src/storage/fdb \
	     -I${top_srcdir}/src/storage/mdb \
	     $(KDB_CPPFLAGS) \
	     $(LDB_CPPFLAGS) \
	     -I${top_srcdir}/src/plugin \
	     -I${top_srcdir}/src/client\
	     @BOOST_CPPFLAGS@

AM_LDFLAGS= -rdynamic $(BOOST_LDFLAGS)  ${GCOV_LIB} ${TCMALLOC_LDFLAGS}

LDADD=$(top_builddir)/src/client/.libs/libtairclientapi.a \
      $(top_builddir)/src/storage/mdb/.libs/libmdb.a \
      $(top_builddir)/src/storage/fdb/libfdb.a \
      $(KDB_LDFLAGS) \
      $(LDB_LDFLAGS) \
      $(COMPRESS_LDFLAGS) \
      $(top_builddir)/src/plugin/libtair_plugins.a \
      $(top_builddir)/src/packets/libtair_packets.a \
      $(top_builddir)/src/statserver/storage/libtair_stat.a \
      $(top_builddir)/src/storage/libdump.a \
      $(top_builddir)/src/common/libtair_common.a \
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a \
      -lz -lrt -lpthread \
      -ldl \
      @BOOST_THREAD_LIB@


sbin_PROGRAMS=tair_server repair_rsync

include_HEADERS=item_manager.hpp tair_manager.hpp table_manager.hpp duplicate_manager.hpp duplicate_base.hpp \
		update_log.hpp tinyjson.hpp

tair_server_SOURCES=duplicate_manager.cpp item_manager.cpp stat_processor.cpp request_processor.cpp tair_manager.cpp update_log.cpp \
		    heartbeat_thread.cpp migrate_manager.cpp table_manager.cpp tair_server.cpp \
		    $(top_builddir)/src/common/stat_helper.cpp \
		    $(top_builddir)/src/common/scoped_wrlock.hpp	\
		    $(top_builddir)/src/common/BlockQueueEx.hpp \
		    $(top_builddir)/src/common/blockqueue.hpp   \
		    $(top_builddir)/src/common/syncproc.hpp   \
		    base_migrate.hpp	\
		    base_tair_manager.hpp	\
		    duplicate_base.hpp	\
		    dup_sync_manager.cpp \
		    dup_sync_manager.hpp \
		    duplicate_manager.hpp	\
		    heartbeat_thread.hpp	\
		    item_manager.hpp	\
		    migrate_manager.hpp	\
		    request_processor.hpp	\
		    table_manager.hpp	\
		    tair_manager.hpp	\
		    tair_server.hpp	\
		    tinyjson.hpp	\
		    update_log.hpp \
		    prefix_locker.hpp \
		    stat_processor.h remote_sync_manager.hpp remote_sync_manager.cpp

repair_rsync_SOURCES=repair_rsync.cpp
