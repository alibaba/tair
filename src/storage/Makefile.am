SUBDIRS=mdb ldb
DIST_SUBDIRS=mdb ldb
AM_CPPFLAGS=-I${top_srcdir}/src \
			-I${top_srcdir}/src/common \
			-I${top_srcdir}/deps/libeasy-install/include/easy \
			-I${top_srcdir}/deps/tbsys-install/include \
			-I$(top_srcdir)/src/storage/mdb \
			-I$(top_srcdir)/src/storage

LDADD=${top_srcdir}/deps/tbsys-install/lib/libtbsys.a \
	  $(top_builddir)/src/storage/mdb/libmdb.a

source_list = dump_filter.cpp dump_manager.cpp
AM_CXXFLAGS=-Werror
AM_LDFLAGS=-lpthread  ${GCOV_LIB}
noinst_LIBRARIES=libdump.a
libdump_a_SOURCES=${source_list} dump_filter.hpp dump_manager.hpp storage_manager.hpp

include_HEADERS=storage_manager.hpp dump_manager.hpp dump_filter.hpp

