if [ $# != 1 ]; then
  echo "USAGE: $0 arealist like 0,1,3";
  exit 1;
fi

df -h | grep "/data/" | awk '{print $6}' | while read line; do
	DUMPPATH=`echo $line | awk -F '[/]' '{print $3}'`;
	DUMPPATH='dumpdata/'$DUMPPATH;
	DUMPFILE=$DUMPPATH'/helloworld';
	mkdir -p $DUMPPATH;
	DBPATH=$line'/ldb';
	MANIFESTPATH=$DBPATH'/backupversions/'
	MANIFEST=`ls -tla $MANIFESTPATH | grep MANIFEST | awk '{print $9}'`;
	echo "./ldb_dump -m yes -p $DBPATH -f $MANIFESTPATH$MANIFEST -c bitcmp -b -1 -d $DUMPFILE -a $1 &"
	./ldb_dump -m yes -p $DBPATH -f $MANIFESTPATH$MANIFEST -c bitcmp -b -1 -d $DUMPFILE -a $1 &
done
