#!/bin/bash

if [ "$1" = 'clean' ]; then
	make clean distclean
	sh autogen.sh clean
  rm -f compile
  exit
fi

prefix=`pwd`

sh autogen.sh
CXXFLAGS='-O2 -Wall -D_NO_EXCEPTION' ./configure --prefix=$prefix/../tbsys-install
make -j
make install

