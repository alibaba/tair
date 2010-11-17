AM_CPPFLAGS= @BOOST_CPPFLAGS@ -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage/mdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/client
LDADD= \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
	  $(top_builddir)/src/storage/mdb/libmdb.a \
	  $(top_builddir)/src/storage/fdb/libfdb.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a 

base_source=interface_test_main.cpp interface_test_base.cpp interface_test_base.h

source_list=${base_source} \
			CTAIRCLIENTAPI_03_x_put_Test.cpp \
			CTAIRCLIENTAPI_03_x_put_oterfunction_Test.cpp \
			CTAIRCLIENTAPI_04_x_get_Test.cpp \
			CTAIRCLIENTAPI_05_x_remove_Test.cpp \
			CTAIRCLIENTAPI_13_x_incr_Test.cpp \
			CTAIRCLIENTAPI_13_x_incr_otherfunction_Test.cpp \
			CTAIRCLIENTAPI_14_x_decr_Test.cpp \
			CTAIRCLIENTAPI_14_x_decr_otherfunction_Test.cpp \
			CTAIRCLIENTAPI_08_x_getItems_Test.cpp \
			CTAIRCLIENTAPI_07_x_addItems_Test.cpp \
			CTAIRCLIENTAPI_07_x_addItems_otherfunction_Test.cpp \
			CTAIRCLIENTAPI_09_x_getAndRemove_Test.cpp

AM_LDFLAGS=-lpthread -lgtest  -lz -lrt ${BOOST_LDFLAGS} @BOOST_THREAD_LIB@

sbin_PROGRAMS=interface_test

interface_test_SOURCES=${source_list}
