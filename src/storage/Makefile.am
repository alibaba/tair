if WITH_KDB 
KDB=kdb 
endif
if WITH_LDB
LDB=ldb
endif
SUBDIRS=mdb fdb $(KDB) $(LDB)
DIST_SUBDIRS=mdb fdb kdb ldb
AM_CPPFLAGS=-I${top_srcdir}/src -I${top_srcdir}/src/common -I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I$(top_srcdir)/src/storage/mdb  -I$(top_srcdir)/src/statserver/include -I$(top_srcdir)/src/storage
LDADD=$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a $(top_builddir)/src/storage/mdb/libmdb.a
source_list = dump_filter.cpp dump_manager.cpp fdb/file_op.cpp
AM_LDFLAGS=-lpthread   ${GCOV_LIB} 
noinst_LIBRARIES=libdump.a
libdump_a_SOURCES=${source_list} dump_filter.hpp dump_manager.hpp storage_manager.hpp

include_HEADERS=storage_manager.hpp dump_manager.hpp dump_filter.hpp
