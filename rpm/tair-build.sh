#!/bin/bash
#for taobao abs
temppath=$1
cd $temppath/packages
if [ `cat /etc/redhat-release|cut -d " " -f 7|cut -d "." -f 1` = 4 ]
then
sed -i  "s/^Release:.*$/Release: "$4".el4/" $2.spec
else
sed -i  "s/^Release:.*$/Release: "$4".el5/" $2.spec
fi
sed -i  "s/^Version:.*$/Version: "$3"/" $2.spec
cd $temppath
chmod +x bootstrap.sh
./bootstrap.sh
export TBLIB_ROOT=/opt/csr/common
./configure
make PREFIX=/opt/csr/tair-2.3 rpms
#make rpms
mv *.rpm rpm/
