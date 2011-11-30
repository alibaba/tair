dnl -------------------------------------------------------- -*- autoconf -*-
dnl Licensed to the Apache Software Foundation (ASF) under one or more
dnl contributor license agreements.  See the NOTICE file distributed with
dnl this work for additional information regarding copyright ownership.
dnl The ASF licenses this file to You under the Apache License, Version 2.0
dnl (the "License"); you may not use this file except in compliance with
dnl the License.  You may obtain a copy of the License at
dnl
dnl     http://www.apache.org/licenses/LICENSE-2.0
dnl
dnl Unless required by applicable law or agreed to in writing, software
dnl distributed under the License is distributed on an "AS IS" BASIS,
dnl WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
dnl See the License for the specific language governing permissions and
dnl limitations under the License.

dnl
dnl tcmalloc.m4: tair's tcmalloc autoconf macros
dnl TCMALLOC_LDFLAGS

dnl This is kinda fugly, but need a way to both specify a directory and which
dnl of the many tcmalloc libraries to use ...
AC_DEFUN([TS_CHECK_TCMALLOC], [
AC_ARG_WITH([tcmalloc],
  [AS_HELP_STRING([--with-tcmalloc],[specify the tcmalloc library to use [default=tcmalloc]])],
  [
  with_tcmalloc_lib="$withval"
	TCMALLOC_LDFLAGS="-ltcmalloc"
  has_tcmalloc=1
  ],[
  with_tcmalloc_lib="tcmalloc"
	TCMALLOC_LDFLAGS="-ltcmalloc"
  has_tcmalloc=1
  ]
)

AC_ARG_WITH([tcmalloc-dir], [AC_HELP_STRING([--with-tcmalloc-dir=DIR], [use the tcmalloc library])],
[
  if test "$withval" != "no"; then
    if test "x${enable_jemalloc}" = "xyes"; then
      AC_MSG_ERROR([Cannot compile with both tcmalloc and jemalloc])
    fi

	  AC_SUBST(TCMALLOC_LDFLAGS)
    tcmalloc_have_libs=0
    if test "x$withval" != "xyes" && test "x$withval" != "x"; then
      tcmalloc_ldflags="$withval/lib"
      #TS_ADDTO(LDFLAGS, [-L${tcmalloc_ldflags}])
      #TS_ADDTO(LIBTOOL_LINK_FLAGS, [-rpath ${tcmalloc_ldflags}])
	    TCMALLOC_LDFLAGS="-ltcmalloc -L$tcmalloc_ldflags"
    fi
    AC_CHECK_LIB(${with_tcmalloc_lib}, tc_cfree , [tcmalloc_have_lib=1])
    if test "$tcmalloc_have_lib" != "0"; then
      #TS_ADDTO(LIBS, [-l${with_tcmalloc_lib}])
      has_tcmalloc=1      
    fi
  fi
])
AC_SUBST(has_tcmalloc)
])
