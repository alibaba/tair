#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(t_libeasy, 1.0.17-21.el6, )
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([src/io/easy_io.h])
AC_PREFIX_DEFAULT(/usr)

# Checks for programs.
AC_PROG_CXX
AC_PROG_LN_S
AC_PROG_LIBTOOL
AC_PROG_RANLIB

if test "x$prefix" = "xNONE" ; then
    prefix="/usr/local"
fi
includedir=${prefix}/include/easy
PRESET_CFLAGS="$CFLAGS"
PRESET_CPPFLAGS="$CPPFLAGS"
PRESET_LDFLAGS="$LDFLAGS"
CFLAGS="-g $PRESET_CFLAGS -Wall -Werror -fPIC -fno-strict-aliasing"
CPPFLAGS="$PRESET_CPPFLAGS -D_GNU_SOURCE -D__STDC_LIMIT_MACROS"

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([sys/inotify.h])
# Checks for typedefs, structures, and compiler characteristics.
# Checks for library functions.
AC_CHECK_LIB([pthread], [main], [], exit 1)
AC_CHECK_LIB([ssl], [SSL_CTX_new], [], exit 1)

MOSTLYCLEANFILES=
AC_SUBST(MOSTLYCLEANFILES)
AC_ARG_WITH([gcov], AS_HELP_STRING([--with-gcov], [enable code coverage(default is NO)]), [
    if test "$withval" = "yes"; then
        CFLAGS="${PRESET_CFLAGS} -Wall -Werror -fPIC -ggdb3 -fprofile-arcs -ftest-coverage"
	LDFLAGS="-lgcov ${PRESET_LDFLAGS} -fprofile-arcs -ftest-coverage"
    fi
])
MOSTLYCLEANFILES="*.gcno *.gcda" 
DEFAULT_INCLUDES="-I`pwd`/src/include/ -I`pwd`/src/util/"
DEFAULT_INCLUDES="$DEFAULT_INCLUDES -I`pwd`/src/thread/ -I`pwd`/src/io/ -I`pwd`/src/packet/ -I`pwd`/src/packet/http -I."
VERSION_MAP="`pwd`/src/libeasy.map"
AC_SUBST(DEFAULT_INCLUDES)
AC_SUBST(VERSION_MAP)
PRESET_LDADD="`pwd`/src/.libs/libeasy.a"
AC_SUBST(PRESET_LDADD)

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 src/include/Makefile
                 src/util/Makefile
                 src/thread/Makefile
                 src/io/Makefile
                 src/packet/Makefile
                 test/Makefile
                 test/include/Makefile
                 test/util/Makefile
                 test/thread/Makefile
                 test/io/Makefile
                 test/packet/Makefile
                 sample/Makefile
		])
AC_OUTPUT

if test -f libtool; then
  sed -i -e '/^soname_spec=/ s/major/{major}.\\${age}/g' libtool
  sed -i -e '/^library_names_spec=/ s/major/{major}.\\${age}/g' libtool
fi
