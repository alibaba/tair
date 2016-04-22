AC_INIT([tair], [3.2.4], [opensource@taobao.com])

AM_INIT_AUTOMAKE([foreign])
AC_PROG_CXX
AC_LANG([C++])
AC_PROG_LIBTOOL

m4_include([tcmalloc.m4])

BITS=`if [[ 64 -eq $(getconf LONG_BIT) ]]; then echo -n $(getconf LONG_BIT); fi`
AC_SUBST(BITS)

dnl attain git info url, depending on `git info`'s output format.
dnl God bless GIT doesn't change its printf.'
git_info="NONE"
if git remote &>/dev/null; then
  git_info=`git remote -v | grep push | awk '{print $2}'`"@"`git log -1 | grep commit | awk '{print $2}'`
fi

AC_ARG_WITH([release],
      AS_HELP_STRING([--with-release],
         [use optimize (default is NO)]),
         [
          if test "$withval" = "yes"; then
            CXXFLAGS="-g -O2 -finline-functions -fno-strict-aliasing -Wall -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DNDEBUG"
          fi
          ],
          [CXXFLAGS="-g -Wall -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DTAIR_DEBUG -D__MIXIN_DEBUG__"]
          )

AC_ARG_WITH([internaltest],
      AS_HELP_STRING([--with-internaltest],
         [use for internal test(default is NO)]),
         [
          if test "$withval" = "yes"; then
            CXXFLAGS="-g -DINTERNALTEST ${CXXFLAGS}"
            with_internaltest="yes"
          fi
          ])
AM_CONDITIONAL([WITH_INTERNALTEST],[test x$with_internaltest = xyes])

AC_ARG_WITH([svn],
      AS_HELP_STRING([--with-svn],
         [with svn info(default is YES)]),
         [
            CXXFLAGS="${CXXFLAGS} -DTAIR_GIT_INFO=\\\"$withval\\\""
         ],
         [
            CXXFLAGS="${CXXFLAGS} -DTAIR_GIT_INFO=\\\"${git_info}\\\""
         ]
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

dnl -------------------------------------------------------------------------
dnl Checking hashmap
dnl -------------------------------------------------------------------------
AC_CHECK_HEADERS([ext/hash_map], [], AC_MSG_FAILURE([hash_map not found]))
AC_RUN_IFELSE([AC_LANG_PROGRAM([[#include <ext/hash_map>]],
                 [[#ifdef _BACKWARD_BACKWARD_WARNING_H
                 return 1;
                 #endif]])
        ],
        [],
        [CXXFLAGS="${CXXFLAGS} -Wno-deprecated"])

ac_default_prefix=${HOME}/tair_bin # for convenience

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

AC_ARG_WITH([diamond],
         AS_HELP_STRING([--with-diamond],
               [with diamond surport, only used in alibaba-inc (default is NO)]),
         [
           if test "$withval" = "yes"; then
              with_diamond=no
              #only DiamondClient.h is needed while compile.
              AC_CHECK_HEADER(DiamondClient.h, [],
                          [
                              AC_MSG_ERROR([t-diamond lib have not been installed])
                              exit 1;
                          ]
                          )
           fi
         ],
         [ with_diamond=no ]
         )

AS_IF([test x$with_diamond = xyes],
  [ CXXFLAGS="${CXXFLAGS} -DWITH_DIAMOND" ],)
AM_CONDITIONAL([WITH_DIAMOND],[test x$with_diamond = xyes])

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
    m4_include([src/storage/ldb/ax_port_leveldb.m4])
    AC_CONFIG_HEADERS([src/common/snappy/config.h])
    AC_CONFIG_FILES([src/common/snappy/snappy-stubs-public.h])
    ],
  [AC_MSG_NOTICE([ without ldb ])])

AM_CONDITIONAL([WITH_LDB],[test x$with_ldb = xyes])
AC_CHECK_LIB(readline, readline,
             [
              CXXFLAGS="${CXXFLAGS} -DHAVE_LIBREADLINE"
              with_readline="yes"
              ],, -ltermcap)
AM_CONDITIONAL([WITH_READLINE], [test x$with_readline = xyes])

CXXFLAGS="-DEASY_MULTIPLICITY ${CXXFLAGS}"
TS_CHECK_TCMALLOC

AC_CONFIG_FILES([\
     Makefile \
     src/Makefile \
     src/common/Makefile \
     src/plugin/Makefile \
     src/storage/Makefile \
     src/storage/mdb/Makefile \
     src/storage/ldb/Makefile \
     src/storage/ldb/tools/Makefile \
     src/packets/Makefile \
     src/configserver/Makefile \
     src/dataserver/Makefile \
     src/client/Makefile \
     src/tools/Makefile \
     scripts/Makefile \
     share/Makefile
     ])
AC_OUTPUT
