#!/bin/bash

if [ "$1" == "clean" ]; then
    make distclean &>/dev/null
    rm -rf aclocal.m4 autom4te.cache config.guess config.sub configure depcomp INSTALL install-sh ltmain.sh missing config.log  config.status libtool
    rm -f *.rpm *.gz
    find . -name 'Makefile' -exec rm -f {} \;
    find . -name '.deps' -exec rm -rf {} \;
    find . -name 'Makefile.in' -exec rm -f {} \;
    exit;
fi

libtoolize --force
aclocal
autoconf
automake --add-missing --force --warnings=no-portability
