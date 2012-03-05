TAIR_DIR=`dirname $0`
TAIR_BIN_DIR=./sbin
TAIR_ETC_DIR=./etc
SERVER_COUNT=1

cd ${TAIR_DIR}

ulimit -c unlimited

export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH

if [ x$2 != x ]
then
  SERVER_COUNT=$2
fi

VAL_LOG_PATH=./val_log
VAL_CMD="valgrind --tool=memcheck --leak-check=full --show-reachable=yes --log-file=${VAL_LOG_PATH}/valgrind.log."`date +%m%d%s`

DS_CMD=${TAIR_BIN_DIR}/tair_server
CS_CMD=${TAIR_BIN_DIR}/tair_cfg_svr
IV_CMD=${TAIR_BIN_DIR}/inval_server

VAL_DS_CMD="${VAL_CMD} ${TAIR_BIN_DIR}/tair_server"
VAL_CS_CMD="${VAL_CMD} ${TAIR_BIN_DIR}/tair_cfg_svr"
VAL_IV_CMD="${VAL_CMD} ${TAIR_BIN_DIR}/inval_server"

check_folder()
{
  if [ ! -d "$VAL_LOG_PATH" ]; then
    mkdir "$VAL_LOG_PATH"
  fi
}

#$1 dataserver.conf*
check_shm_size()
{
  #check slab_mem_size and tmpfs size
  engine=`grep "storage_engine *=" $1 | grep -v "#" | awk -F '[ =]' '{print $NF}'`
  if [ "$engine" == "" ]
  then
    echo "can not find storage_engine"
    return 1
  fi

  if [ $engine == "mdb" ]
  then
    safety_free_size=500;
    tmpfs_size=`df -m |grep tmpfs | awk '{print $2}'`
    let "safety_size = $tmpfs_size - $safety_free_size"
    slab_mem_size=`grep slab_mem_size $1 | grep -v "#" | awk -F '[ =]' '{print $NF}'`

    if [ "$slab_mem_size" == "" ]
    then
      echo "can not find slab_mem_size"
      return 1
    fi

    echo "mdb engine. tmpfs_size: "$tmpfs_size"  safety_size: "$safety_size"  slab_mem_size: "$slab_mem_size
    if [ $slab_mem_size -ge $safety_size ]
    then
      echo "slab_mem_size is not little then safety_size."
      return 1
    else
      return 0
    fi
  else
    return 0
  fi
}

#$1 CMD
start_cs()
{
  $1 -f ${TAIR_ETC_DIR}/configserver.conf
}

start_ds()
{
  if check_shm_size ${TAIR_ETC_DIR}/dataserver.conf
  then
    $1 -f ${TAIR_ETC_DIR}/dataserver.conf
  fi
  for((i=1;i<$SERVER_COUNT;++i))
  do
    if [ -f ${TAIR_ETC_DIR}/dataserver.conf.$i ]
    then
      if check_shm_size ${TAIR_ETC_DIR}/dataserver.conf.$i
      then
        $1 -f ${TAIR_ETC_DIR}/dataserver.conf.$i
      fi
    else
      break;
    fi
    sleep 2;
  done
  sleep 2;
}
start_iv()
{
  $1 -f ${TAIR_ETC_DIR}/invalserver.conf
}

stop_cs()
{
  kill `cat logs/config.pid`
}

stop_ds()
{
  kill `cat logs/server.pid`
  for((i=2;i<$SERVER_COUNT;++i))
  do
    if [ -f logs/server.$i.log ]
    then
      kill `cat logs/server.$i.pid`
    else
      break;
    fi
  done
}

stop_iv()
{
  kill `cat logs/inval.pid`
}


valgrind_stop_all()
{
  killall -15 memcheck
}


clean()
{
  cd ${TAIR_DIR}
  rm logs/* -f
  rm fdb/* -rf
  rm data/* -rf
  rm /dev/shm/mdb_shm* -f
  rm bdb/* -rf
}

log_debug2warn()
{
  cd ${TAIR_ETC_DIR} && sed -i "s/log_level=debug/log_level=warn/" *.conf
}

log_warn2debug()
{
  cd ${TAIR_ETC_DIR} && sed -i "s/log_level=warn/log_level=debug/" *.conf
}

log_debug2info()
{
  cd ${TAIR_ETC_DIR} && sed -i "s/log_level=debug/log_level=info/" *.conf
}

log_info2debug()
{
  cd ${TAIR_ETC_DIR} && sed -i "s/log_level=info/log_level=debug/" *.conf
}

tomdb()
{
  cd ${TAIR_ETC_DIR} && sed -i "s/^storage_engine.*$/storage_engine=mdb/" *.conf
  cd ${TAIR_ETC_DIR} && sed -i "s/mdb_type=mdb_shm.*$/mdb_type=mdb/" *.conf
}

tomdb_shm()
{
  cd ${TAIR_ETC_DIR} && sed -i "s/^storage_engine.*$/storage_engine=mdb/" *.conf
  cd ${TAIR_ETC_DIR} && sed -i "s/mdb_type=mdb.*$/mdb_type=mdb_shm/" *.conf
}

tofdb()
{
  cd ${TAIR_ETC_DIR} &&  sed -i "s/^storage_engine.*$/storage_engine=fdb/" *.conf
}

tobdb()
{
  cd ${TAIR_ETC_DIR} &&  sed -i "s/^storage_engine.*$/storage_engine=bdb/" *.conf
}

case "$1" in
  start_cs)
  start_cs  "${CS_CMD}"
  ;;
  stop_cs)
  stop_cs
  ;;
  start_ds)
  start_ds "${DS_CMD}"
  ;;
  stop_ds)
  stop_ds
  ;;
  start_iv)
  start_iv "${IV_CMD}"
  ;;
  stop_iv)
  stop_iv
  ;;
  valgrind_start_cs)
  check_folder
  start_cs "${VAL_CS_CMD}"
  ;;
  valgrind_start_ds)
  check_folder
  start_ds "${VAL_DS_CMD}"
  ;;
  valgrind_start_iv)
  check_folder
  start_iv "${VAL_IV_CMD}"
  ;;
  valgrind_stop)
  valgrind_stop_all
  ;;
  clean)
  clean
  ;;
  log_debug2warn)
  log_debug2warn
  ;;
  log_warn2debug)
  log_warn2debug
  ;;
  log_debug2info)
  log_debug2info
  ;;
  log_info2debug)
  log_info2debug
  ;;
  tomdb)
  tomdb
  ;;
  tofdb)
  tofdb
  ;;
  tobdb)
  tobdb
  ;;
  tomdb_shm)
  tomdb_shm
  ;;
  *)
  echo "usage: $0 {start_cs|stop_cs|start_ds|stop_ds|start_iv|stop_iv [SERVER_COUNT]|lean|log_debug2warn|log_warn2debug}"
esac

