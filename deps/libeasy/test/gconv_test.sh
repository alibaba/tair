#!/bin/bash

ROOT_DIR=/home/duolong/run/cai/htdocs/gconv
if [ ! -z "$1" ]; then
    ROOT_DIR="$1"
fi


CUR_PATH=`pwd`
cd `dirname $0`/..
BASE_HOME=`pwd`
echo $BASE_HOME

cd $CUR_PATH 
make -s check
lcov -d $BASE_HOME/src -t 'gcov' -o "$ROOT_DIR/src.info" -c
lcov -d $BASE_HOME/test -t 'gcov' -o "$ROOT_DIR/test.info" -c
lcov -a $ROOT_DIR/test.info -a $ROOT_DIR/src.info -o $ROOT_DIR/total.inf
genhtml -o $ROOT_DIR/result $ROOT_DIR/total.inf 

