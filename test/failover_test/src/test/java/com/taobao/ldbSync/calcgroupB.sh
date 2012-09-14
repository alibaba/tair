#!/bin/bash
TAIR_BIN=/home/admin/tair_bin_sync/
ACS=10.232.4.14
BCS=10.232.4.17
A1=10.232.4.14
A2=10.232.4.15
A3=10.232.4.16
A4=10.232.4.23
B1=10.232.4.17
B2=10.232.4.18
B3=10.232.4.19
B4=10.232.4.24
CSPORT=5168
DSPORT=5161
GROUP=group_1
CONF="etc/group.conf"

if [ "$1" = "invoke" ]
then
	./sbin/cst_monitor data/data/group_1_server_table 2>/dev/null > B.group
	scp -q B.group admin@$ACS:$TAIR_BIN
	echo "successful"
elif [ "$1" = "flowlimit" ]
then
	./sbin/tairclient -c $BCS:$CSPORT -g group_1 -l "flowlimit 0 1 99999999999 in" 2>/dev/null
	./sbin/tairclient -c $BCS:$CSPORT -g group_1 -l "flowlimit 0 1 99999999999 out" 2>/dev/null
	./sbin/tairclient -c $BCS:$CSPORT -g group_1 -l "flowlimit 0 1 99999999999 ops" 2>/dev/null	
	echo "successful"
elif [ "$1" = "restore" ]
then
        #echo "restore"
        sed -i '/_server_list/d' $CONF
        sed -i "/data center A/a\_server_list=$B1:$DSPORT\n\_server_list=$B2:$DSPORT\n\_server_list=$B3:$DSPORT" $CONF
        echo "successful"
fi
exit 0
