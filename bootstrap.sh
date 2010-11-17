#!/bin/bash

if [ "$1" == "clean" ]; then
    rm -rf aclocal.m4 autom4te.cache config.guess config.sub configure depcomp INSTALL install-sh ltmain.sh missing config.log  config.status libtool
    find . -name 'Makefile' -exec rm -f {} \;
    find . -name 'Makefile.in' -exec rm -f {} \;
    exit;
fi

libtoolize --force
aclocal
autoconf
automake --add-missing --force

