SUBDIRS=kyotocabinet

AM_CPPFLAGS=-I${top_builddir}/src/storage/kdb/kyotocabinet -I${top_srcdir}/src -I${top_srcdir}/src/common -I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I. -I..
LDADD=$(TBLIB_ROOT)/lib/libtbsys.a ${top_builddir}/src/storage/kdb/kyotocabinet/libkyoyocabinet.a

AM_LDFLAGS=-lpthread   ${GCOV_LIB}
noinst_LIBRARIES=libkdb.a
 
libkdb_a_SOURCES=kdb_bucket.cpp kdb_manager.cpp locker.cpp stat_manager.cpp locker.hpp kdb_bucket.h \
		 kdb_manager.h stat_manager.h
