# port Google leveldb.
# see http://code.google.com/p/leveldb/ for detail.
# based on git-svn-id: https://leveldb.googlecode.com/svn/trunk@50 62dab493-f737-651d-591e-8d6aee1b9529.

# absolute path
m4_include([src/storage/ldb/ax_port_snappy.m4])

save_cppflags="${CPPFLAGS}"
check_cflags="${CPPFLAGS} -std=c++0x -x c++"

# Detect C++0x -- this determines whether we'll use port_noatomic.h
# or port_posix.h by:
# 1. Rrying to compile with -std=c++0x and including <cstdatomic>.
# 2. If g++ returns error code, we know to use port_posix.h
AC_CACHE_CHECK(
	[if C++0x (cstdatomic) is supported],
	[ac_cv_cxx0x_supported],
	[
	 AC_LANG_PUSH([C++])
	 AC_COMPILE_IFELSE(AC_LANG_PROGRAM([[@%:@include <cstdatomic>]],
			      [[return 0;]]),
			      [ac_cv_cxx0x_supported="yes"],
			      [ac_cv_cxx0x_supported="no"])
         AC_LANG_POP([C++])
  ])

CPPFLAGS="$save_cppflags"

if test "x$ac_cv_cxx0x_supported" = "xyes"; then
     LEVELDB_PORT_CFLAGS="-DLEVELDB_PLATFORM_POSIX -DLEVELDB_CSTDATOMIC_PRESENT -std=c++0x"
else
     LEVELDB_PORT_CFLAGS="-DLEVELDB_PLATFORM_POSIX"
fi

AC_SUBST([LEVELDB_PORT_CFLAGS])
