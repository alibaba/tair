PORTCFLAGS= -fno-builtin-memcmp \
	    -pthread -DOS_LINUX \
	    -DSNAPPY -I$./snappy \
	    ${LEVELDB_PORT_CFLAGS}

AM_CPPFLAGS=-I. -I./include ${PORTCFLAGS}
AM_LDFLAGS=-lpthread ${GCOV_LIB}

noinst_LIBRARIES=libleveldb.a libsnappy.a

libleveldb_a_SOURCES= \
	./db/builder.cc ./db/c.cc ./db/db_impl.cc ./db/db_iter.cc ./db/filename.cc ./db/dbformat.cc \
	./db/log_reader.cc ./db/log_writer.cc ./db/memtable.cc ./db/repair.cc ./db/table_cache.cc \
	./db/version_edit.cc ./db/version_set.cc ./db/write_batch.cc ./port/port_posix.cc \
	./table/block.cc ./table/block_builder.cc ./table/format.cc ./table/iterator.cc \
	./table/merger.cc ./table/table.cc ./table/table_builder.cc ./table/two_level_iterator.cc \
	./util/arena.cc ./util/cache.cc ./util/coding.cc ./util/comparator.cc ./util/crc32c.cc \
	./util/env.cc ./util/env_posix.cc ./util/hash.cc ./util/histogram.cc ./util/logging.cc \
	./util/options.cc ./util/status.cc ./util/config.h ./util/config.cc \
	./db/builder.h ./db/db_iter.h ./db/filename.h ./db/log_reader.h ./db/memtable.h ./db/snapshot.h \
	./db/version_edit.h ./db/version_set.h ./db/write_batch_internal.h ./db/db_impl.h ./db/dbformat.h \
	./db/log_format.h ./db/log_writer.h ./db/skiplist.h ./db/table_cache.h \
	./port/atomic_pointer.h ./port/port.h ./port/port_android.h ./port/port_example.h\
	./port/port_posix.h ./port/sha1_portable.h ./table/block.h ./table/block_builder.h ./table/format.h \
	./table/iterator_wrapper.h  ./table/merger.h  ./table/two_level_iterator.h\
	./util/arena.h ./util/coding.h ./util/config.h ./util/crc32c.h ./util/hash.h ./util/histogram.h \
	./util/logging.h ./util/mutexlock.h ./util/posix_logger.h ./util/random.h \
	./include/leveldb/c.h ./include/leveldb/cache.h ./include/leveldb/db.h ./include/leveldb/env.h \
	./include/leveldb/iterator.h ./include/leveldb/options.h ./include/leveldb/slice.h \
	./include/leveldb/table.h ./include/leveldb/table_builder.h ./include/leveldb/write_batch.h \
	./include/leveldb/status.h ./include/leveldb/comparator.h

libsnappy_a_SOURCES= ./snappy/snappy-internal.h ./snappy/snappy-c.h ./snappy/snappy-c.cc ./snappy/snappy-sinksource.cc\
	             ./snappy/snappy-sinksource.h ./snappy/snappy-stubs-internal.h ./snappy/snappy-stubs-internal.cc\
		     ./snappy/snappy-stubs-public.h.in ./snappy/snappy.cc ./snappy/snappy.h ./snappy/config.h.in
