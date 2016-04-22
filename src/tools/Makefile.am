AM_CPPFLAGS= -I${top_srcdir}/src \
						 -I${top_srcdir}/deps/libeasy-install/include/easy \
						 -I${top_srcdir}/deps/tbsys-install/include \
						 -I${top_srcdir}/src/common \
						 -I${top_srcdir}/src/packets \
						 -I${top_srcdir}/src/client \
						 -I${top_srcdir}/src/dataserver \
						 -I${top_srcdir}/src/plugin

AM_CXXFLAGS= -Werror -fPIC

if WITH_COMPRESS
COMPRESS_LDFLAGS= -lsnappy
endif

LDADD=$(top_srcdir)/src/client/.libs/libtairclientapi.a \
			$(top_srcdir)/src/common/libtair_common.a \
			$(top_builddir)/src/common/libsnappy.a \
			${top_srcdir}/deps/tbsys-install/lib/libtbsys.a \
			${top_srcdir}/deps/libeasy-install/lib64/libeasy.a \
			$(COMPRESS_LDFLAGS) \
			-lz -lrt

AM_LDFLAGS=-lpthread -lcurl -lssl -lz -lrt ${GCOV_LIB} -lcrypto

sbin_PROGRAMS=dump_transfer

dump_transfer_SOURCES=dump_transfer.cpp
dump_transfer_LDADD=${LDADD}
