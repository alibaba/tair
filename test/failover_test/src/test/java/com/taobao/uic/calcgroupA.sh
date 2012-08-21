#!/bin/bash
TAIR_BIN=/home/admin/tair_bin_uic/
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

EOFDelete()
{
ex $1 <<EOF
1,9d
wq
EOF
}

CalcGroup()
{
	./sbin/cst_monitor data/data/${GROUP}_server_table 2>/dev/null  | sed '/DEBUG/d' > $1
}

if [ "$1" = "checkAB" ]
then
	bResult=`ssh admin@$B1 "cd $TAIR_BIN; ./calcgroupB.sh invoke"`
	#echo $bResult
	CalcGroup A.group
	EOFDelete A.group
	EOFDelete B.group
	sed -i "s/$A1/$B1/" A.group
	sed -i "s/$A2/$B2/" A.group
	sed -i "s/$A3/$B3/" A.group
	comm -3 A.group B.group |wc -l
elif [ "$1" = "backds" ]
then
	#echo "backds"
	sed -i "s/$A1/$A4/" $2
	comm -3 $2 $3 |wc -l
elif [ "$1" = "compare" ]
then
	#echo "compare"
	comm -3 $2 $3 |wc -l
elif [ "$1" = "calc" ]
then
	#echo "calc"
	CalcGroup $2
	EOFDelete $2
	echo "successful"
elif [ "$1" = "delall" ]
then
	#echo "delall 0"
	./sbin/tairclient -c $ACS:$CSPORT -g $GROUP -l "delall 0" 2
elif [ "$1" = "flowlimit" ]
then
	#echo "flowlimit"
	./sbin/tairclient -c $ACS:$CSPORT -g $GROUP -l "flowlimit 0 1 99999999999 in" 2>/dev/null
	./sbin/tairclient -c $ACS:$CSPORT -g $GROUP -l "flowlimit 0 1 99999999999 out" 2>/dev/null
	./sbin/tairclient -c $ACS:$CSPORT -g $GROUP -l "flowlimit 0 1 99999999999 ops" 2>/dev/null
	echo "successful"
elif [ "$1" = "restore" ]
then
	#echo "restore"
	sed -i '/_server_list/d' $CONF
	sed -i "/data center A/a\_server_list=$A1:$DSPORT\n\_server_list=$A2:$DSPORT\n\_server_list=$A3:$DSPORT" $CONF
	echo "successful"
elif [ "$1" = "resetserver" ]
then
	#echo "resetserver"
	./sbin/tairclient -c $ACS:$CSPORT -g $GROUP -l "resetserver group_1 $A1:$DSPORT" 2>/dev/null
	echo "successful"
elif [ "$1" = "resetgroup" ]
then
	#echo "resetgroup"
	./sbin/tairclient -c $ACS:$CSPORT -g $GROUP -l "resetserver group_1" 2>/dev/null
	echo "successful"
fi

#rm -f *.group
exit 0
