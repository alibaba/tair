if [ $# != 2 ]; then
	echo "USAGE: $0 config_server groupname";
	exit 1;
fi

ls -l dumpdata/ | grep u | awk '{print $9}' | while read line; do
	UPLOADPATH='dumpdata/'$line;
	UPLOADFILE=$UPLOADPATH'/helloworld';
	UPLOADLOG=$UPLOADPATH'/log.txt';
	echo "./dump_transfer -c $1 -g $2 -f $UPLOADFILE -l $UPLOADLOG -t 4 -i 10 -m ldb &";
	./dump_transfer -c $1 -g $2 -f $UPLOADFILE -l $UPLOADLOG -t 4 -i 10 -m ldb &
done
