#!/bin/bash

if [ "$1" == "clean" ]; then
   make distclean
   rm -rf aclocal.m4 autom4te.cache config.guess config.sub configure depcomp INSTALL install-sh ltmain.sh missing config.status config.log libtool COPYING
   find . -name 'Makefile' -exec rm -f {} \;
   find . -name 'Makefile.in' -exec rm -f {} \;
   exit;
fi

libtoolize --force
aclocal
autoconf
automake --add-missing --force
