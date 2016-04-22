#!/bin/bash

prefix=`pwd`

clean_deps_tmp()
{
  rm -rf $prefix/deps/tbsys-install
  rm -rf $prefix/deps/libeasy-install
}

if [ "$1" == "clean" ]; then
    # clean deps
    clean_deps_tmp
    cd $prefix/deps/tbsys/
    ./build.sh clean > /dev/null 2>&1

    cd $prefix/deps/libeasy
    ./bootstrap.sh clean > /dev/null 2>&1

    cd $prefix

    # clean tair
    make distclean &>/dev/null
    rm -rf aclocal.m4 autom4te.cache config.guess config.sub configure depcomp INSTALL install-sh ltmain.sh missing config.log  config.status libtool compile
    rm -f *.rpm *.gz
    find . -name 'Makefile' -exec rm -f {} \;
    find . -name '.deps' -exec rm -rf {} \;
    find . -name '.libs' -exec rm -rf {} \;
    find . -name 'Makefile.in' -exec rm -f {} \;
    exit;
fi

# install deps
if [ "$1" != "skip-deps" ]; then
    clean_deps_tmp

    cd $prefix/deps/tbsys/
    ./build.sh

    cd $prefix/deps/libeasy
    ./bootstrap.sh
    ./configure --prefix=$prefix/deps/libeasy-install --libdir=$prefix/deps/libeasy-install/lib64 --enable-static=yes --enable-shared=no
    make -j && make install

    cd $prefix
fi

libtoolize --force
aclocal
autoconf
automake --add-missing --force --warnings=no-portability

