#!/bin/bash
size=22G
if [ x$1 != x ]
then
	echo $1
	size=$1
fi
sed -i "s;^\(none */dev/shm * tmpfs * defaults\).* \( * 0 0\);\1,size=${size}\2;g" /etc/fstab
mount -o remount /dev/shm
