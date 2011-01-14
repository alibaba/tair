AC_INIT(tair, 2.3,ruohai@taobao.com)

AM_INIT_AUTOMAKE(tair, 2.3) 
m4_include([ax_boost.m4])
AC_ARG_WITH([release],
	    AS_HELP_STRING([--with-release],
			   [use optimize (default is NO)]),
			   [
			    if test "$withval" = "yes"; then
				    CXXFLAGS="-O2 -finline-functions -Wall -DBOOST_SPIRIT_THREADSAFE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE"
			    fi
			    ],
			    [CXXFLAGS="-g -Wall -DBOOST_SPIRIT_THREADSAFE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DTAIR_DEBUG"]
			    )
GCOV_LIB=
AC_SUBST(GCOV_LIB)
AC_ARG_WITH([profile],
	    AS_HELP_STRING([--with-profile],
			   [enable code coverage(default is NO)]),
			   [
			    if test "$withval" = "yes"; then
				    CXXFLAGS="-fprofile-arcs -ftest-coverage ${CXXFLAGS}"
				    GCOV_LIB=-lgcov
			    fi
			    ])


ac_default_prefix=${HOME}/tair_bin # for convenience
AC_PROG_CXX
AC_LANG([C++])
AC_PROG_LIBTOOL
AC_MSG_CHECKING([variable TBLIB_ROOT])
if test "${TBLIB_ROOT+set}" = set; then
	AC_MSG_RESULT([yes])
else
	AC_MSG_ERROR([TBLIB_ROOT have not been set])
	exit 1;
fi

AM_CONDITIONAL([TEST_OPT],[test "$build_test" = yes])
AC_ARG_WITH([kdb],
		AS_HELP_STRING([--with-kdb],
			[with kdb (default is NO)]),
		[
		if test "$withval" = "yes"; then
		with_kdb=yes
		fi
		],
		[ with_kdb=no ]
	   )
AM_CONDITIONAL([WITH_KDB],[test x$with_kdb = xyes])

AX_BOOST([1.30.0],,exit 1)
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
		 src/packets/Makefile \
		 src/configserver/Makefile \
		 src/dataserver/Makefile \
		 src/client/Makefile \
		 test/Makefile \
		 test/interface_test/Makefile \
		 scripts/Makefile \
		 share/Makefile
		 ])
AC_OUTPUT
