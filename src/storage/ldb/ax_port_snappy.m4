# port Google snappy.
# see http://code.google.com/p/snappy/ for detail.
# based on release snappy-1.0.4.

m4_define([snappy_major], [1])
m4_define([snappy_minor], [0])
m4_define([snappy_patchlevel], [4])

# Libtool shared library interface versions (current:revision:age)
# Update this value for every release!  (A:B:C will map to foo.so.(A-C).C.B)
# http://www.gnu.org/software/libtool/manual/html_node/Updating-version-info.html
m4_define([snappy_ltversion], [2:2:1])

AC_C_BIGENDIAN
AC_CHECK_HEADERS([stdint.h stddef.h sys/mman.h sys/resource.h windows.h byteswap.h sys/byteswap.h sys/endian.h])

# Don't use AC_FUNC_MMAP, as it checks for mappings of already-mapped memory,
# which we don't need (and does not exist on Windows).
AC_CHECK_FUNC([mmap])

# See if we have __builtin_expect.
# TODO: Use AC_CACHE.
AC_MSG_CHECKING([if the compiler supports __builtin_expect])
 
AC_TRY_COMPILE(, [
    return __builtin_expect(1, 1) ? 1 : 0
], [
    snappy_have_builtin_expect=yes
    AC_MSG_RESULT([yes])
], [
    snappy_have_builtin_expect=no
    AC_MSG_RESULT([no])
])
if test x$snappy_have_builtin_expect = xyes ; then
    AC_DEFINE([HAVE_BUILTIN_EXPECT], [1], [Define to 1 if the compiler supports __builtin_expect.])
fi

# See if we have working count-trailing-zeros intrinsics.
# TODO: Use AC_CACHE.
AC_MSG_CHECKING([if the compiler supports __builtin_ctzll])

AC_TRY_COMPILE(, [
    return (__builtin_ctzll(0x100000000LL) == 32) ? 1 : 0
], [
    snappy_have_builtin_ctz=yes
    AC_MSG_RESULT([yes])
], [
    snappy_have_builtin_ctz=no
    AC_MSG_RESULT([no])
])
if test x$snappy_have_builtin_ctz = xyes ; then
    AC_DEFINE([HAVE_BUILTIN_CTZ], [1], [Define to 1 if the compiler supports __builtin_ctz and friends.])
fi

# These are used by snappy-stubs-public.h.in.
if test "$ac_cv_header_stdint_h" = "yes"; then
    AC_SUBST([ac_cv_have_stdint_h], [1])
else
    AC_SUBST([ac_cv_have_stdint_h], [0])
fi
if test "$ac_cv_header_stddef_h" = "yes"; then
    AC_SUBST([ac_cv_have_stddef_h], [1])
else
    AC_SUBST([ac_cv_have_stddef_h], [0])
fi

# Export the version to snappy-stubs-public.h.
SNAPPY_MAJOR="snappy_major"
SNAPPY_MINOR="snappy_minor"
SNAPPY_PATCHLEVEL="snappy_patchlevel"

AC_SUBST([SNAPPY_MAJOR])
AC_SUBST([SNAPPY_MINOR])
AC_SUBST([SNAPPY_PATCHLEVEL])
AC_SUBST([SNAPPY_LTVERSION], snappy_ltversion)
