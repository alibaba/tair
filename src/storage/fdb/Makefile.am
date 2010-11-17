AM_CPPFLAGS=-I${top_srcdir}/src -I${top_srcdir}/src/common -I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I$(top_srcdir)/src/storage/mdb -I$(top_srcdir)/src/storage
LDADD=$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a $(top_builddir)/src/storage/mdb/libmdb.a
source_list =fdb_manager.cpp fdb_bucket.cpp fdb_item_manager.cpp file_op.cpp free_blocks_manager.cpp locker.cpp data_file_reader.cpp 

AM_LDFLAGS=-lpthread   ${GCOV_LIB}
noinst_LIBRARIES=libfdb.a
#noinst_PROGRAMS=fbtest

libfdb_a_SOURCES=${source_list}	\
		 data_file_reader.hpp	\
		 fdb_bucket.hpp	\
		 fdb_item.hpp	\
		 fdb_item_manager.hpp	\
		 fdb_manager.hpp	\
		 file_op.hpp	\
		 free_blocks_manager.hpp	\
		 locker.hpp	\
		 mmap_file.hpp	
#libfdb_a_LDFLAGS=${AM_LDFLAGS} -static-libgcc
