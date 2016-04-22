AM_CPPFLAGS=-I${top_srcdir}/src \
			-I${top_srcdir}/deps/libeasy-install/include/easy \
			-I${top_srcdir}/deps/tbsys-install/include \
			-I$(top_srcdir)/src/storage \
			-I$(top_srcdir)/src/common \
			-I${top_srcdir}/src/storage/mdb

LDADD=${top_srcdir}/deps/tbsys-install/lib/libtbsys.a

shm_source_list=cache_hashmap.cpp \
				mdb_manager.cpp \
				mem_cache.cpp \
				mdb_instance.hpp \
				mdb_instance.cpp \
				mem_pool.cpp \
				mdb_stat_manager.hpp \
				mdb_stat_manager.cpp \
				${top_srcdir}/src/common/data_dumpper.cpp \
				${top_srcdir}/src/common/stat_define.hpp \
				${top_srcdir}/src/common/stat_define.cpp \
				${top_srcdir}/src/common/util.hpp \
				${top_srcdir}/src/common/util.cpp

AM_LDFLAGS=-lpthread -lz -static-libgcc ${GCOV_LIB}
AM_CXXFLAGS=-Werror -fPIC
lib_LTLIBRARIES=libmdb.la libmdb_c.la

libmdb_la_SOURCES=${shm_source_list} \
				  mdb_factory.cpp mdb_define.cpp \
				  cache_hashmap.hpp	\
				  mdb_define.hpp	\
				  mdb_factory.hpp	\
				  mdb_manager.hpp	\
				  mdb_stat.hpp	\
				  mem_cache.hpp   \
				  mem_pool.hpp \
				  lock_guard.hpp \
				  ../../common/stat_helper.cpp

libmdb_c_la_SOURCES=${shm_source_list} \
					mdb_factory.cpp mdb_define.cpp \
					cache_hashmap.hpp	\
					mdb_define.hpp	\
					mdb_factory.hpp	\
					mdb_manager.hpp	\
					mdb_stat.hpp	\
					mem_cache.hpp   \
					mem_pool.hpp \
					libmdb_c.hpp \
					libmdb_c.cpp \
					../../common/stat_helper.cpp

include_HEADERS=mdb_factory.hpp mdb_manager.hpp libmdb_c.hpp

sbin_PROGRAMS = mdb_reader
mdb_reader_SOURCES=mdb_reader.cpp
mdb_reader_LDADD=libmdb.la $(LDADD) -lpthread -lrt
mdb_reader_LDFLAGS=-static

