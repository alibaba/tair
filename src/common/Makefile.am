AM_CPPFLAGS= -I${top_srcdir}/deps/libeasy-install/include/easy \
			 -I${top_srcdir}/deps/tbsys-install/include \
			 -I$(top_srcdir)/src/packets \
			 -I$(top_srcdir)/src/client \
			 -I${top_srcdir}/src \
			 -I${top_srcdir}/src/storage

if WITH_COMPRESS
COMPRESS_CPPFLAGS= compressor.hpp snappy_compressor.hpp
endif

if WITH_TCMALLOC
AM_CPPFLAGS += -DWITH_TCMALLOC
endif

AM_LDFLAGS= -rdynamic ${GCOV_LIB} \
			-lz -lrt -lpthread \
			-ldl

AM_CXXFLAGS= -Werror

LDADD= ${top_srcdir}/deps/tbsys-install/lib/libtbsys.a \
	   ${top_srcdir}/deps/libeasy-install/lib64/libeasy.a \
	   $(top_builddir)/src/storage/ldb/libsnappy.a

base_src=data_dumpper.cpp	\
		 easy_helper.cpp \
		 stat_helper.cpp	\
		 util.cpp	\
		 data_dumpper.hpp	\
		 data_entry.hpp	\
		 data_entry.cpp	\
		 define.hpp	\
		 dump_data_info.hpp	\
		 hash.hpp	\
		 item_data_info.hpp	\
		 item_data_info.cpp \
		 log.hpp	\
		 mmap_file.hpp	\
		 stat_helper.hpp	\
		 stat_info.hpp	\
		 util.hpp	\
		 wait_object.hpp   \
		 file_op.hpp file_op.cpp \
		 BlockQueueEx.hpp \
		 blockqueue.hpp \
		 syncproc.hpp \
		 scoped_wrlock.hpp \
		 key_value_pack.hpp \
		 $(COMPRESS_CPPFLAGS) \
		 file_list_factory.hpp \
		 counter_wrapper.hpp \
		 tair_atomic.hpp \
		 record_logger.hpp \
		 record_logger.cpp \
		 record_logger_util.hpp \
		 record_logger_util.cpp \
		 tair_stat.hpp \
		 tair_stat.cpp \
		 stat_define.hpp \
		 stat_define.cpp \
		 record.hpp \
		 record.cpp \
		 bucket_queue.hpp \
		 bucket_queue.cpp \
		 log_reader.hpp \
		 timed_collections.hpp \
		 timed_collections.cpp \
		 mixin_test_helper.hpp \
		 mixin_test_helper.cpp \
		 cJSON.h \
		 cJSON.c \
		 httpclient.hpp \
		 httpclient.cpp \
		 result.hpp \
		 thread_guard.hpp

noinst_LIBRARIES=libtair_common.a libsnappy.a

include_HEADERS=item_data_info.hpp \
				data_entry.hpp \
				key_value_pack.hpp \
				define.hpp \
				util.hpp \
				hash.hpp \
				log.hpp \
				tair_atomic.hpp \
				rwlock.hpp \
				seqlock.hpp \
				databuffer.hpp \
				easy_helper.hpp \
				wait_object.hpp \
				async_callback_def.hpp \
				flow_controller.h  \
				flowrate.h \
				inval_stat.hpp $(COMPRESS_CPPFLAGS)

libtair_common_a_SOURCES = ${base_src}

##############################
# snappy
##############################
noinst_LIBRARIES += libsnappy.a

snappy_srcdir=./snappy
libsnappy_a_SOURCES=${snappy_srcdir}/snappy-internal.h \
					${snappy_srcdir}/snappy-c.h \
					${snappy_srcdir}/snappy-c.cc \
					${snappy_srcdir}/snappy-sinksource.cc \
					${snappy_srcdir}/snappy-sinksource.h \
					${snappy_srcdir}/snappy-stubs-internal.h \
					${snappy_srcdir}/snappy-stubs-internal.cc\
					${snappy_srcdir}/snappy-stubs-public.h.in \
					${snappy_srcdir}/snappy.cc \
					${snappy_srcdir}/snappy.h\
					${snappy_srcdir}/config.h.in

