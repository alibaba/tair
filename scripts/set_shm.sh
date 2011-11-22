#!/bin/bash
size=23G
if [ x$1 != x ]
then
	echo $1
	size=$1
fi
sed -i "/tmpfs/ s/defaults/rw,size=${size}/" /etc/fstab
mount -o remount /dev/shm
