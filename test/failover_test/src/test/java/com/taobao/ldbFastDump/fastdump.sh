#!/bin/bash
PORT=5368

export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH:/usr/local/lib:/opt/csr/common/lib:/usr/local/lib64

group=$1
par2=$2
shift
while [ ! x$1 = x ]
do
    all="$all $1"
    shift
done
./sbin/tairclient -c 10.232.4.14:${PORT} -g $group -l "$all" 2>out

getCount()
{
    line=`cat out |wc -l`
    if [ $line -eq 8 ]
    then
        echo 0
        return 0
    fi
    cat out | grep getCount| tail -1|awk -F " " '{print $3}'
}

cmd()
{
    cat out
}

case $par2 in
stat)
    getCount
    ;;
*)
    cmd
    ;;
esac
exit 0
