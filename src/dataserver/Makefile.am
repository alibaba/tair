LDB_CPPFLAGS= -DWITH_LDB -I${top_srcdir}/src/storage/ldb
LDB_LDFLAGS= $(top_builddir)/src/storage/ldb/libldb.a \
			 $(top_builddir)/src/storage/ldb/libleveldb.a \
			 $(top_builddir)/src/common/libsnappy.a

if WITH_COMPRESS
COMPRESS_LDFLAGS= -lsnappy
endif

AM_CPPFLAGS= -I${top_srcdir}/deps/libeasy-install/include/easy \
			 -I${top_srcdir}/deps/tbsys-install/include \
			 -I${top_srcdir}/src \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/dataserver \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/storage/mdb \
			 -I${top_srcdir}/src/plugin \
			 -I${top_srcdir}/src/client \
			 -Imodules $(LDB_CPPFLAGS)

AM_CXXFLAGS=-Werror

if WITH_TCMALLOC
AM_CPPFLAGS += -DWITH_TCMALLOC
endif

AM_LDFLAGS= -rdynamic ${GCOV_LIB} ${TCMALLOC_LDFLAGS} -lcrypto

LDADD=$(top_builddir)/src/client/.libs/libtairclientapi.a \
	  $(top_builddir)/src/storage/mdb/.libs/libmdb.a \
	  $(LDB_LDFLAGS) \
	  $(COMPRESS_LDFLAGS) \
	  $(top_builddir)/src/plugin/libtair_plugins.a \
	  $(top_builddir)/src/packets/libtair_packets.a \
	  $(top_builddir)/src/storage/libdump.a \
	  $(top_builddir)/src/common/libtair_common.a \
	  ${top_srcdir}/deps/tbsys-install/lib/libtbsys.a \
	  ${top_srcdir}/deps/libeasy-install/lib64/libeasy.a \
	  -lssl -lz -lrt -lpthread \
	  -ldl -lcurl

sbin_PROGRAMS=tair_server repair_rsync

include_HEADERS=tair_manager.hpp table_manager.hpp duplicate_manager.hpp duplicate_base.hpp update_log.hpp

tair_server_SOURCES=duplicate_manager.cpp \
					stat_processor.cpp \
					request_processor.cpp \
					tair_manager.cpp \
					update_log.cpp \
					heartbeat_thread.cpp \
					migrate_manager.cpp \
					recovery_manager.cpp \
					table_manager.cpp \
					tair_server.cpp \
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
					migrate_manager.hpp	\
					recovery_manager.hpp	\
					request_processor.hpp	\
					table_manager.hpp	\
					tair_manager.hpp	\
					tair_server.hpp	\
					update_log.hpp \
					prefix_locker.hpp \
					modules/rt_collector.cpp \
					modules/rt_collector.hpp \
					modules/rt_object.cpp \
					modules/rt_object.hpp \
					modules/hotkey.cpp \
					modules/hotkey.hpp \
					modules/op_killer.hpp \
					cluster_handler.hpp \
					cluster_handler.cpp \
					i_remote_sync_manager.hpp \
					new_remote_sync_manager.hpp \
					new_remote_sync_manager.cpp \
					configuration_manager.h \
					configuration_manager.cpp \
					configuration_namespace.hpp \
					configuration_flowcontrol.hpp \
					stat_processor.h \
					remote_sync_manager.hpp \
					remote_sync_manager.cpp \
					dup_depot.hpp dup_depot.cpp \
					flow_controller_impl.cpp \
					flow_controller_impl.h \
					stat_manager_mock.cpp

repair_rsync_SOURCES=repair_rsync.cpp cluster_handler.cpp

