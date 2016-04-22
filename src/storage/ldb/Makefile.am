SUBDIRS=. tools

AM_CPPFLAGS= -DWITH_TBUTIL \
			 -I${top_srcdir}/deps/tbsys-install/include \
			 -I${top_srcdir}/deps/libeasy-install/include/easy \
			 -I${top_builddir}/src/storage/ldb/leveldb/include \
			 -I${top_builddir}/src/storage/ldb/leveldb/ \
			 -I${top_srcdir}/src \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/common \
			 -I.

AM_CXXFLAGS= -Werror -fPIC

if WITH_TCMALLOC
AM_CPPFLAGS += -DWITH_TCMALLOC
endif

AM_LDFLAGS=-lpthread -lrt ${GCOV_LIB}
noinst_LIBRARIES=libldb.a

libldb_a_SOURCES=ldb_manager.cpp \
				 ldb_manager.hpp \
				 ldb_instance.hpp \
				 ldb_instance.cpp \
				 stat_manager.cpp \
				 stat_manager.hpp \
				 ldb_define.hpp \
				 ldb_define.cpp \
				 bg_task.hpp \
				 bg_task.cpp \
				 ldb_comparator.hpp \
				 ldb_numeric_comparator.cpp \
				 ldb_comparator.cpp \
				 ldb_gc_factory.hpp \
				 ldb_gc_factory.cpp \
				 ldb_cache_stat.hpp \
				 ldb_cache_stat.cpp \
				 ldb_bloom.cpp \
				 ldb_remote_sync_logger.hpp \
				 ldb_remote_sync_logger.cpp \
				 ldb_balancer.hpp \
				 ldb_balancer.cpp \
				 ldb_stat_manager.hpp \
				 ldb_stat_manager.cpp \
				 local_binlog_queue.hpp \
				 local_binlog_queue.cpp \
				 rt_profiler.cpp \
				 rt_profiler.hpp \
				 file_op.hpp \
				 file_op.cpp \
				 data_file_reader.hpp \
				 data_file_reader.cpp

##############################
# leveldb
##############################
PORTCFLAGS= -fno-builtin-memcmp \
			-pthread -DOS_LINUX \
			-DSNAPPY -I${top_srcdir}/src/common/snappy \
			${LEVELDB_PORT_CFLAGS}

noinst_LIBRARIES += libleveldb.a

leveldb_srcdir=./leveldb

libleveldb_a_CPPFLAGS=${AM_CPPFLAGS} ${PORTCFLAGS}
libleveldb_a_SOURCES=${leveldb_srcdir}/db/builder.cc \
					 ${leveldb_srcdir}/db/c.cc \
					 ${leveldb_srcdir}/db/db_impl.cc \
					 ${leveldb_srcdir}/db/db_iter.cc \
					 ${leveldb_srcdir}/db/filename.cc \
					 ${leveldb_srcdir}/db/dbformat.cc \
					 ${leveldb_srcdir}/db/log_reader.cc \
					 ${leveldb_srcdir}/db/log_writer.cc \
					 ${leveldb_srcdir}/db/memtable.cc \
					 ${leveldb_srcdir}/db/repair.cc \
					 ${leveldb_srcdir}/db/table_cache.cc \
					 ${leveldb_srcdir}/db/version_edit.cc \
					 ${leveldb_srcdir}/db/version_set.cc \
					 ${leveldb_srcdir}/db/write_batch.cc \
					 ${leveldb_srcdir}/port/port_posix.cc \
					 ${leveldb_srcdir}/table/block.cc \
					 ${leveldb_srcdir}/table/block_builder.cc \
					 ${leveldb_srcdir}/table/format.cc \
					 ${leveldb_srcdir}/table/iterator.cc \
					 ${leveldb_srcdir}/table/merger.cc \
					 ${leveldb_srcdir}/table/table.cc \
					 ${leveldb_srcdir}/table/table_builder.cc \
					 ${leveldb_srcdir}/table/two_level_iterator.cc \
					 ${leveldb_srcdir}/table/filter_block.cc \
					 ${leveldb_srcdir}/table/filter_block.h \
					 ${leveldb_srcdir}/util/arena.cc \
					 ${leveldb_srcdir}/util/cache.cc \
					 ${leveldb_srcdir}/util/coding.cc \
					 ${leveldb_srcdir}/util/comparator.cc \
					 ${leveldb_srcdir}/util/crc32c.cc \
					 ${leveldb_srcdir}/util/env.cc \
					 ${leveldb_srcdir}/util/env_posix.cc \
					 ${leveldb_srcdir}/util/hash.cc \
					 ${leveldb_srcdir}/util/histogram.cc \
					 ${leveldb_srcdir}/util/logging.cc \
					 ${leveldb_srcdir}/util/options.cc \
					 ${leveldb_srcdir}/util/status.cc \
					 ${leveldb_srcdir}/util/config.h \
					 ${leveldb_srcdir}/util/config.cc \
					 ${leveldb_srcdir}/util/filter_policy.cc \
					 ${leveldb_srcdir}/util/bloom.cc \
					 ${leveldb_srcdir}/db/builder.h \
					 ${leveldb_srcdir}/db/db_iter.h \
					 ${leveldb_srcdir}/db/filename.h \
					 ${leveldb_srcdir}/db/log_reader.h \
					 ${leveldb_srcdir}/db/memtable.h \
					 ${leveldb_srcdir}/db/snapshot.h \
					 ${leveldb_srcdir}/db/version_edit.h \
					 ${leveldb_srcdir}/db/version_set.h \
					 ${leveldb_srcdir}/db/write_batch_internal.h \
					 ${leveldb_srcdir}/db/db_impl.h \
					 ${leveldb_srcdir}/db/dbformat.h \
					 ${leveldb_srcdir}/db/log_format.h \
					 ${leveldb_srcdir}/db/log_writer.h \
					 ${leveldb_srcdir}/db/skiplist.h \
					 ${leveldb_srcdir}/db/table_cache.h \
					 ${leveldb_srcdir}/port/atomic_pointer.h \
					 ${leveldb_srcdir}/port/atomic_count.h \
					 ${leveldb_srcdir}/port/port.h \
					 ${leveldb_srcdir}/port/port_android.h \
					 ${leveldb_srcdir}/port/port_example.h \
					 ${leveldb_srcdir}/port/port_posix.h \
					 ${leveldb_srcdir}/port/sha1_portable.h \
					 ${leveldb_srcdir}/table/block.h \
					 ${leveldb_srcdir}/table/block_builder.h \
					 ${leveldb_srcdir}/table/format.h \
					 ${leveldb_srcdir}/table/iterator_wrapper.h \
					 ${leveldb_srcdir}/table/merger.h \
					 ${leveldb_srcdir}/table/two_level_iterator.h \
					 ${leveldb_srcdir}/util/arena.h \
					 ${leveldb_srcdir}/util/coding.h \
					 ${leveldb_srcdir}/util/config.h \
					 ${leveldb_srcdir}/util/crc32c.h \
					 ${leveldb_srcdir}/util/hash.h \
					 ${leveldb_srcdir}/util/histogram.h \
					 ${leveldb_srcdir}/util/logging.h \
					 ${leveldb_srcdir}/util/mutexlock.h \
					 ${leveldb_srcdir}/util/posix_logger.h \
					 ${leveldb_srcdir}/util/random.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/c.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/cache.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/db.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/env.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/iterator.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/options.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/slice.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/table.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/table_builder.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/write_batch.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/status.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/comparator.h \
					 ${leveldb_srcdir}/include/${leveldb_srcdir}/filter_policy.h

##############################
if WITH_INTERNALTEST
noinst_LIBRARIES+=libtestldb.a
libtestldb_a_CPPFLAGS=${AM_CPPFLAGS} ${PORTCFLAGS} ${GCOV_LIB}
libtestldb_a_SOURCES=${libldb_a_SOURCES} ${libleveldb_a_SOURCES}
endif

