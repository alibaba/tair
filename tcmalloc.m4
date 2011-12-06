dnl
dnl tcmalloc.m4: tair's tcmalloc autoconf macros
dnl TCMALLOC_LDFLAGS

AC_DEFUN([TS_CHECK_TCMALLOC], 
[
    AC_ARG_WITH([tcmalloc],
        AS_HELP_STRING([--with-tcmalloc=DIR],
                      [use tcmalloc (default is no) specify the root directory for tcmalloc library (optional)]),
        [
          if test "$withval" = "no"; then
              want_tcmalloc="no"
          elif test "$withval" = "yes"; then
              want_tcmalloc="yes"
              ac_tcmalloc_path=""
          else
              want_tcmalloc="yes"
              ac_tcmalloc_path="$withval"
          fi
        ],
        [want_tcmalloc="no"]
  )

	if test "x$want_tcmalloc" = "xyes"; then
		if test "$ac_tcmalloc_path" != ""; then
			ax_tclib="tcmalloc -L$ac_tcmalloc_path/lib"
		else
			ax_tclib="tcmalloc"
	  fi
    AC_CHECK_LIB($ax_tclib, tc_cfree,TCMALLOC_LDFLAGS="-l$ax_tclib" AC_SUBST(TCMALLOC_LDFLAGS),
       [AC_MSG_FAILURE([tcmalloc link failed (--without-tcmalloc to disable)])]
    )
	fi


    AC_ARG_WITH([tcmalloc_minimal],
        AS_HELP_STRING([--with-tcmalloc_minimal=DIR],
                      [use tcmalloc_minimal(default is no) specify the root directory for tcmalloc_minimal library (optional)]),
        [
          if test "$withval" = "no"; then
              want_tcmalloc_minimal="no"
          elif test "$withval" = "yes"; then
              want_tcmalloc_minimal="yes"
              ac_tcmalloc_minimal_path=""
          else
              want_tcmalloc_minimal="yes"
              ac_tcmalloc_minimal_path="$withval"
          fi
        ],
        [want_tcmalloc_minimal="no"]
  )

	if test "x$want_tcmalloc_minimal" = "xyes"; then
		if test "$ac_tcmalloc_minimal_path" != ""; then
			ax_tclib="tcmalloc_minimal -L$ac_tcmalloc_minimal_path/lib"
		else
			ax_tclib="tcmalloc_minimal"
	  fi
    AC_CHECK_LIB($ax_tclib, tc_cfree,TCMALLOC_LDFLAGS="-l$ax_tclib" AC_SUBST(TCMALLOC_LDFLAGS),
       [AC_MSG_FAILURE([tcmalloc_minimal link failed (--without-tcmalloc_minimal to disable)])]
    )
	fi

])
