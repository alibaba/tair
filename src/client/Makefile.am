AM_CPPFLAGS= -fPIC -I${top_srcdir}/src \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet \
	    -I${top_srcdir}/src/storage/fdb \
	    -I${top_srcdir}/src/storage/mdb \
            -I${top_srcdir}/src/common \
	    -I${top_srcdir}/src/storage/ \
            -I${top_srcdir}/src/packets \
            -I${top_srcdir}/src/dataserver \
	    -I${top_srcdir}/src/plugin \
	    ${BOOST_CPPFLAGS}

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a 

api_source_list=tair_client_api.cpp tair_client_api_impl.cpp \
		${top_srcdir}/src/packets/packet_factory.cpp \
		tair_client_capi.hpp tair_client_api.hpp \
	       	tair_client_api_impl.hpp tair_client_capi.hpp \
		tair_client.hpp

AM_LDFLAGS=-lpthread -lz ${BOOST_LDFLAGS} @BOOST_THREAD_LIB@  ${GCOV_LIB}

lib_LTLIBRARIES=libtairclientapi.la libtairclientapi_c.la
sbin_PROGRAMS=tairclient

tairclient_SOURCES=tair_client.cpp  
tairclient_LDADD=libtairclientapi.la ${LDADD} 

libtairclientapi_la_SOURCES=${api_source_list}
libtairclientapi_c_la_SOURCES=tair_client_capi.cpp
libtairclientapi_c_la_LIBADD=libtairclientapi.la 

include_HEADERS=tair_client_api.hpp tair_client_capi.hpp
