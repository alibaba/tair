AC_INIT(tair, 2.3.2.1,ruohai@taobao.com)

AM_INIT_AUTOMAKE(tair, 2.3.2.1)
AC_PROG_CXX
AC_LANG([C++])
AC_PROG_LIBTOOL

m4_include([ax_boost.m4])
m4_include([tcmalloc.m4])

dnl attain svn info url, depending on `svn(1.4.2) info`'s output format.
dnl God bless SVN doesn't change its printf.'
svn_info="NONE"
if svn info &>/dev/null; then
  info=`svn info`
  svn_info=`echo "$info" | awk '/URL/{print $NF}'`"@r"`echo "$info" | awk '/Last Changed Rev/{print $NF}'`
fi

AC_ARG_WITH([release],
	    AS_HELP_STRING([--with-release],
			   [use optimize (default is NO)]),
			   [
			    if test "$withval" = "yes"; then
				    CXXFLAGS="-g -O2 -finline-functions -fno-strict-aliasing -Wall -DBOOST_SPIRIT_THREADSAFE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DNDEBUG -DTAIR_SVN_INFO=\\\"${svn_info}\\\""
			    fi
			    ],
			    [CXXFLAGS="-g -Wall -DBOOST_SPIRIT_THREADSAFE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DTAIR_DEBUG -DTAIR_SVN_INFO=\\\"${svn_info}\\\""]
			    )
GCOV_LIB=
AC_SUBST(GCOV_LIB)
AC_ARG_WITH([profile],
	    AS_HELP_STRING([--with-profile],
			   [enable code coverage(default is NO)]),
			   [
			    if test "$withval" = "yes"; then
				    CXXFLAGS="-g -fprofile-arcs -ftest-coverage ${CXXFLAGS}"
				    GCOV_LIB=-lgcov
			    fi
			    ])


ac_default_prefix=${HOME}/tair_bin # for convenience

AC_MSG_CHECKING([variable TBLIB_ROOT])
if test "${TBLIB_ROOT+set}" = set; then
	AC_MSG_RESULT([yes])
else
	AC_MSG_ERROR([TBLIB_ROOT have not been set])
	exit 1;
fi

AM_CONDITIONAL([TEST_OPT],[test "$build_test" = yes])

AC_ARG_WITH([compress],
		AS_HELP_STRING([--with-compress],
			[with compress (default is NO)]),
		[
		if test "$withval" = "yes"; then
		with_compress=yes
		AC_CHECK_HEADER(snappy.h, [],
		[
		AC_MSG_ERROR([snappy have not been installed])
		exit 1;
		])
		AC_CHECK_LIB(snappy, _ZN6snappy11RawCompressEPKcmPcPm, [],
		[
		AC_MSG_ERROR([snappy have not been installed])
		exit 1;
		])
		fi
		],
		[ with_compress=no ]
	   )
AS_IF([test x$with_compress = xyes],
  [ CXXFLAGS="${CXXFLAGS} -DWITH_COMPRESS" ],)
AM_CONDITIONAL([WITH_COMPRESS],[test x$with_compress = xyes])

AC_ARG_WITH([kdb],
		AS_HELP_STRING([--with-kdb],
			[with kdb (default is yes)]),
		[
		if test "$withval" = "yes"; then
		with_kdb=yes
		fi
		],
		[ with_kdb=yes ]
	   )
AM_CONDITIONAL([WITH_KDB],[test x$with_kdb = xyes])

AC_ARG_WITH([ldb],
		AS_HELP_STRING([--with-ldb],
			[with ldb (default is YES)]), [
		if test "$withval" = "yes"; then
		with_ldb=yes
		fi
		],
		[
		  with_ldb=yes
		]
	   )

AS_IF([test x$with_ldb = xyes],
    [
    AC_MSG_NOTICE([ with ldb ])
    # configure leveldb && snappy
    m4_include([src/storage/ldb/leveldb/ax_port_leveldb.m4])
    AC_CONFIG_HEADERS([src/storage/ldb/leveldb/snappy/config.h])
    AC_CONFIG_FILES([src/storage/ldb/leveldb/snappy/snappy-stubs-public.h])
    ],
  [AC_MSG_NOTICE([ without ldb ])])

AM_CONDITIONAL([WITH_LDB],[test x$with_ldb = xyes])
AX_BOOST([1.30.0],,exit 1)
AC_CHECK_LIB(readline, readline,
             [
              CXXFLAGS="${CXXFLAGS} -DHAVE_LIBREADLINE"
              with_readline="yes"
              ],, -ltermcap)
AM_CONDITIONAL([WITH_READLINE], [test x$with_readline = xyes])

TS_CHECK_TCMALLOC

AC_CONFIG_FILES([\
		 Makefile \
		 src/Makefile \
		 src/common/Makefile \
		 src/plugin/Makefile \
		 src/storage/Makefile \
		 src/storage/fdb/Makefile \
		 src/storage/mdb/Makefile \
		 src/storage/kdb/Makefile \
		 src/storage/kdb/kyotocabinet/Makefile \
		 src/storage/ldb/Makefile \
		 src/storage/ldb/leveldb/Makefile \
		 src/packets/Makefile \
		 src/statserver/storage/Makefile \
		 src/statserver/Makefile \
		 src/statserver/include/Makefile \
		 src/configserver/Makefile \
		 src/dataserver/Makefile \
		 src/client/Makefile \
		 src/invalserver/Makefile \
		 src/tools/Makefile \
		 test/Makefile \
		 test/interface_test/Makefile \
		 test/unit_test/Makefile \
		 scripts/Makefile \
		 share/Makefile
		 ])
AC_OUTPUT
