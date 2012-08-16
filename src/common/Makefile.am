AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
	     -I$(TBLIB_ROOT)/include/tbnet \
	     -I${top_srcdir}/src/storage \
	     @BOOST_CPPFLAGS@

if WITH_COMPRESS
COMPRESS_CPPFLAGS= compressor.hpp snappy_compressor.hpp
endif

AM_LDFLAGS= -rdynamic $(BOOST_LDFLAGS)  ${GCOV_LIB} \
      -lz -lrt -lpthread \
      -ldl\
      @BOOST_THREAD_LIB@

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

base_src=data_dumpper.cpp	\
	 stat_helper.cpp	\
	 util.cpp	\
	 data_dumpper.hpp	\
	 data_entry.hpp	\
	 data_entry.cpp	\
	 define.hpp	\
	 dump_data_info.hpp	\
	 hash.hpp	\
	 item_data_info.hpp	\
	 log.hpp	\
	 mmap_file.hpp	\
	 stat_helper.hpp	\
	 stat_info.hpp	\
	 util.hpp	\
	 wait_object.hpp	 \
         file_op.hpp \
         file_op.cpp \
	 BlockQueueEx.hpp \
	 blockqueue.hpp \
	 syncproc.hpp \
	 scoped_wrlock.hpp \
   file_list_factory.hpp \
   $(COMPRESS_CPPFLAGS) \
	 counter_wrapper.hpp \
   tair_atomic.hpp

noinst_LIBRARIES=libtair_common.a

include_HEADERS=item_data_info.hpp data_entry.hpp define.hpp util.hpp hash.hpp log.hpp $(COMPRESS_CPPFLAGS)

libtair_common_a_SOURCES = ${base_src}

