AM_CPPFLAGS=-DWITH_TBUTIL \
	    -I${top_srcdir}/deps/tbsys-install/include \
	    -I${top_srcdir}/deps/libeasy-install/include/easy \
	    -I${top_builddir}/src/storage/ldb/leveldb/include \
	    -I${top_builddir}/src/storage/ldb/leveldb/ \
	    -I${top_srcdir}/src \
	    -I${top_srcdir}/src/storage \
	    -I${top_srcdir}/src/storage/ldb \
	    -I${top_srcdir}/src/common  \
	    -I.

AM_LDFLAGS=-lpthread ${GCOV_LIB}
AM_CXXFLAGS= -Werror -fPIC

ldb_path=${top_srcdir}/src/storage/ldb
ldb_libs=${ldb_path}/libldb.a \
	 ${ldb_path}/libleveldb.a \
	 ${top_builddir}/src/common/libsnappy.a \
	 ${top_builddir}/src/common/libtair_common.a \
	 ${top_builddir}/src/storage/mdb/.libs/libmdb.a \
	 ${top_srcdir}/deps/libeasy-install/lib64/libeasy.a \
	 ${top_srcdir}/deps/tbsys-install/lib/libtbsys.a -lrt -lz -lssl -lcrypto

util_srcs=ldb_util.cpp ldb_util.hpp

sbin_PROGRAMS=view_cache_stat \
	      ldb_rsync \
	      ldb_sst_picker \
	      ldb_manifest_merger \
	      ldb_dump \
	      ldb_manifest_rebuild \
	      ldb_gclog_reader \
	      ldb_sst_check \
	      leveldbutil \
	      ldb_sst_filter \
	      ldb_bucket_check \
	      ldb_bucket_distribution \
	      ldb_verify \
	      statistics_verify

view_cache_stat_SOURCES=view_cache_stat.cpp
view_cache_stat_LDADD=${ldb_path}/libldb.a ${top_builddir}/src/common/libtair_common.a ${top_srcdir}/deps/tbsys-install/lib/libtbsys.a -lrt

ldb_rsync_SOURCES=ldb_rsync.cpp ${top_builddir}/src/dataserver/cluster_handler.cpp ${util_srcs}
ldb_rsync_CPPFLAGS=${AM_CPPFLAGS} -I${top_srcdir}/src/packets -I${top_srcdir}/src/dataserver \
			-I${top_srcdir}/src/plugin -I${top_srcdir}/src/common
ldb_rsync_LDADD=${top_builddir}/src/client/.libs/libtairclientapi.a ${ldb_libs}

ldb_sst_picker_SOURCES=ldb_sst_picker.cpp ${util_srcs}
ldb_sst_picker_LDADD=${ldb_libs}

ldb_manifest_merger_SOURCES=ldb_manifest_merger.cpp ${util_srcs}
ldb_manifest_merger_LDADD=${ldb_libs}

ldb_dump_SOURCES=ldb_dump.cpp ${util_srcs}
ldb_dump_LDADD=${ldb_libs}

ldb_manifest_rebuild_SOURCES=ldb_manifest_rebuild.cpp ${util_srcs}
ldb_manifest_rebuild_LDADD=${ldb_libs}

ldb_gclog_reader_SOURCES=ldb_gclog_reader.cpp ${util_srcs}
ldb_gclog_reader_LDADD=${ldb_libs}

ldb_sst_check_SOURCES=ldb_sst_check.cpp ${util_srcs}
ldb_sst_check_LDADD=${ldb_libs}

leveldbutil_SOURCES=leveldb_main.cc  ${util_srcs}
leveldbutil_LDADD=${ldb_libs}

ldb_verify_SOURCES=ldb_verify.cpp ${top_builddir}/src/dataserver/cluster_handler.cpp ${util_srcs}
ldb_verify_CPPFLAGS=${AM_CPPFLAGS} -I${top_srcdir}/src/packets -I${top_srcdir}/src/dataserver \
			-I${top_srcdir}/src/plugin -I${top_srcdir}/src/common/
ldb_verify_LDADD=${top_builddir}/src/client/.libs/libtairclientapi.a ${ldb_libs}

statistics_verify_SOURCES=statistics_verify.cpp ${top_builddir}/src/dataserver/cluster_handler.cpp ${util_srcs}
statistics_verify_CPPFLAGS=${AM_CPPFLAGS} -I${top_srcdir}/src/packets -I${top_srcdir}/src/dataserver \
			-I${top_srcdir}/src/plugin -I${top_srcdir}/src/common
statistics_verify_LDADD=${top_builddir}/src/client/.libs/libtairclientapi.a ${ldb_libs}


ldb_bucket_check_SOURCES=ldb_bucket_check.cpp ${util_srcs}
ldb_bucket_check_CPPFLAGS=${AM_CPPFLAGS} -I${top_srcdir}/src/packets -I${top_srcdir}/src/dataserver \
                          -I${top_srcdir}/src/plugin -I${top_srcdir}/src/common
ldb_bucket_check_LDADD=${top_builddir}/src/client/.libs/libtairclientapi.a ${ldb_libs}

ldb_sst_filter_SOURCES=ldb_sst_filter.cpp ${util_srcs}
ldb_sst_filter_LDADD=${ldb_libs}

ldb_bucket_distribution_SOURCES=ldb_bucket_distribution.cpp ${util_srcs}
ldb_bucket_distribution_LDADD=${ldb_libs}
