#! /bin/bash

if [ $# != 3 ]; then
	echo "Usage: $0 role base_path server_table"; # 0 is the first config, other means defalut slave config. etc: bin/check_version.sh 0 /home/admin/tair_2.3  group_session_server_table
	exit 1;
fi

role=$1
BIN_PATH=$2
SERVER_TABLE_NAME=$3
TABLE_FILE=$BIN_PATH/data/data/$SERVER_TABLE_NAME
TABLE_BAK_FILE=$BIN_PATH/logs/$SERVER_TABLE_NAME
TABLE_TXT_FILE=$BIN_PATH/logs/$SERVER_TABLE_NAME.txt
GROUP_FILE=$BIN_PATH/etc/group.conf
CST_BIN=$BIN_PATH/sbin/cst_monitor

while true
do
  if [ ! -f $TABLE_FILE ]; then
    echo "$TABLE_FILE is not exist.";
    sleep 10;
    continue;
  fi
  cp $TABLE_FILE $TABLE_BAK_FILE
  retcode=$?
  if [ $retcode -ne 0 ]; then
    echo "cp $TABLE_FILE $TABLE_BAK_FILE, error code: $retcode";
    sleep 10;
    continue;
  fi
  $CST_BIN $TABLE_BAK_FILE > $TABLE_TXT_FILE

  cur_client_version=`grep clientVersion $TABLE_TXT_FILE |awk -F ":" '{print $2}'`
  migrate_block_count=`grep migrateBlockCount $TABLE_TXT_FILE |awk -F ":" '{print $2}'`

  if [ $migrate_block_count -eq -1 ]; then
    div_result=`echo $cur_client_version/2|bc`
    mod_version=`echo $div_result%2|bc`;

    if [ $role -eq 0 ]; then
      echo "i am the first configserver, now version: $cur_client_version.";
      if [ $mod_version -ne 0 ]; then
        echo "first configserver's version/2%2 should be 0, mod version: $mod_version";
        touch $GROUP_FILE;
      fi
    else
      echo "i am not the first configserver, now version: $cur_client_version.";
      if [ $mod_version -eq 0 ]; then
        echo "slave configserver's version/2%2 should not be 0, mod version: $mod_version";
        touch $GROUP_FILE;
      fi
    fi
  else
    echo "table is migrating. mig_count: $migrate_block_count";
  fi
  # NOTICE: _server_down_time must be littler then this sleep time
  sleep 10;
done
