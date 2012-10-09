/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * commonly used data structures and value define
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_DEFINE_H
#define TAIR_DEFINE_H

#include <stdint.h>
#include <tbnet.h>
#include "util.hpp"

// for large file support on 32 bit machines
#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif
#define _FILE_OFFSET_BITS 64
#if __WORDSIZE == 64
#define PRI64_PREFIX "l"
#else
#define PRI64_PREFIX "ll"
#endif

#ifndef UNUSED
#define UNUSED(a) /*@-noeffect*/if (0) (void)(a)/*@=noeffect*/;
#endif
#define LIKELY(x) (__builtin_expect(!!(x),1))
#define UNLIKELY(x) (__builtin_expect(!!(x),0))
#define IS_ADDCOUNT_TYPE(flag) (((flag) & TAIR_ITEM_FLAG_ADDCOUNT) == TAIR_ITEM_FLAG_ADDCOUNT)
#define IS_ITEM_TYPE(flag) ( ((flag) & TAIR_ITEM_FLAG_ITEM) )
#define IS_DELETED(flag) ( ((flag) & TAIR_ITEM_FLAG_DELETED) )

#define CAN_OVERRIDE(old_flag,new_flag)         \
   ({                                           \
      bool ret = false;                         \
      if((old_flag) & (new_flag))               \
         ret = true;                            \
      ret;                                      \
   })

#define INCR_DATA_SIZE 6
// add JavaClient header, little-endian encode INT
#define SET_INCR_DATA_COUNT(buf, count)         \
  do {                                          \
     (buf)[0] = '\0';                           \
     (buf)[1] = '\026';                         \
     (buf)[2] = (count) & 0xFF;                 \
     (buf)[3] = ((count) >> 8) & 0xFF;          \
     (buf)[4] = ((count) >> 16) & 0xFF;         \
     (buf)[5] = ((count) >> 24) & 0xFF;         \
  } while (0)

#define TAIR_SLEEP(stop, interval) ({int count=interval*5; while(count-->0 && !stop) usleep(200000);})

#define TAIR_MAX_PATH_LEN            256
#define TAIR_AREA_ENCODE_SIZE        2
#define TAIR_MAX_KEY_SIZE            1024
#define TAIR_MAX_KEY_SIZE_WITH_AREA  (TAIR_MAX_KEY_SIZE+TAIR_AREA_ENCODE_SIZE)
#define TAIR_MAX_DATA_SIZE           1000000
#define TAIR_MAX_AREA_COUNT          1024
#define TAIR_MAX_DUP_MAP_SIZE        102400
#define TAIR_PACKET_FLAG             0x6D426454
#define TAIR_DTM_VERSION             0x31766256
#define TAIR_HTM_VERSION             0x31766257

#define TAIR_DEFAULT_COMPRESS_THRESHOLD 8192
#define TAIR_COMPRESS_TYPE_NUM          1
#define TAIR_ITEM_FLAG_COMPRESS         4
#define TAIR_VALUE_HEADER_LENGTH        2 // the length of value header, in unit of bytes
#define COMPRESS_TYPE_OFFSET            12 // the offset of compress type in value header
#define COMPRESS_FLAG                   1 // the offset of compress flag in value header

#define TAIR_FLAG_SERVER            (0x0000ffffffffffffLL)
#define TAIR_FLAG_CFG_DOWN          (0x4000000000000000LL)
#define TAIR_FLAG_NOEXP             (0x0c000000)
// TAIR_POS_MASK & serverid  is  rack_id, so change this to fit your rack
#define TAIR_POS_MASK               (0x000000000000ffffLL)
#define TAIR_MAX_FILENAME_LEN       (256)
#define TAIR_DATA_NEED_MIGRATE          (1)
//for migrate
#define MAX_MUPDATE_PACKET_SIZE     (16 * 1024)
#define MAX_MPUT_PACKET_SIZE     (500 * 1024)
/////////////////////////////
//for data duplicate
#define MISECONDS_BEFOR_SEND_RETRY  (1000000)
#define SLEEP_MISECONDS             (10)
//for prefix
#define PREFIX_KEY_OFFSET 	        (22)
#define PREFIX_KEY_MASK 		        (0x3FFFFF)
#define MAGIC_ITEM_META_LDB_PREFIX  (0x0101)
#define RANGE_DEFAULT_LIMIT         (1000)
//hasnext flag for getrange
#define FLAG_HASNEXT                (0x01)
#define FLAG_HASNEXT_MASK           (0xFFFE)
//////////////////////////////////////////////
#define TAIRPUBLIC_SECTION           "public"
#define TAIRSERVER_SECTION           "tairserver"
#define CONFSERVER_SECTION           "configserver"
#define INVALSERVER_SECTION           "invalserver"
#define TAIRFDB_SECTION              "fdb"
#define TAIRKDB_SECTION              "kdb"
#define TAIRLDB_SECTION              "ldb"
#define REMOTE_TAIRSERVER_SECTION    "remote"
#define LOCAL_TAIRSERVER_SECTION      "local"

// TAIR_SERVER
#define TAIR_PORT                    "port"
#define TAIR_HEARTBEAT_PORT          "heartbeat_port"
#define TAIR_SENGINE                 "storage_engine"
#define TAIR_PROFILER_THRESHOLD      "profiler_threshold"
#define TAIR_PROCESS_THREAD_COUNT    "process_thread_num"
#define TAIR_LOG_FILE                "log_file"
#define TAIR_CONFIG_SERVER           "config_server"
#define TAIR_PID_FILE                "pid_file"
#define TAIR_LOG_LEVEL               "log_level"
#define TAIR_DEV_NAME                "dev_name"
#define TAIR_DUP_SYNC                "dup_sync"
#define TAIR_COUNT_NEGATIVE          "allow_count_negative"
#define TAIR_LOCAL_MODE              "local_mode"
#define TAIR_GROUP_FILE              "group_file"
#define TAIR_DATA_DIR                "data_dir"
#define TAIR_EVICT_DATA_PATH         "evict_data_path"
#define TAIR_STAT_FILE_PATH          "stat_file_path"
#define TAIR_STAT_DEFAULT_FILEPATH   "tair_stat.dat"
#define TAIR_DUMP_DIR                "data_dump_dir"
#define TAIR_DEFAULT_DUMP_DIR        "dump"
#define TAIR_TASK_QUEUE_SIZE         "task_queue_size"
#define TAIR_DO_RSYNC                "do_rsync"
#define TAIR_RSYNC_MTIME_CARE        "rsync_mtime_care"
#define TAIR_RSYNC_CONF              "rsync_conf"
#define TAIR_RSYNC_DATA_DIR          "rsync_data_dir"
#define TAIR_RSYNC_DO_RETRY          "rsync_do_retry"
#define TAIR_RSYNC_RETRY_LOG_MEM_SIZE "rsync_retry_log_mem_size"
#define TAIR_RSYNC_FAIL_LOG_SIZE     "rsync_fail_log_size"

//MDB
#define TAIR_SLAB_MEM_SIZE           "slab_mem_size"
#define TAIR_SLAB_BASE_SIZE          "slab_base_size"
#define TAIR_SLAB_FACTOR             "slab_factor"
#define TAIR_MDB_TYPE                "mdb_type" //mdb or mdb_shm
#define TAIR_MDB_SHM_PATH            "mdb_shm_path"
#define TAIR_SLAB_PAGE_SIZE          "slab_page_size"
#define TAIR_MDB_HASH_BUCKET_SHIFT   "mdb_hash_bucket_shift"
#define TAIR_CHECK_EXPIRED_HOUR_RANGE "check_expired_hour_range"
#define TAIR_CHECK_SLAB_HOUR_RANGE    "check_slab_hour_range"

//eg: mdb_default_capacity_size=0,1073741824;1,100
#define TAIR_MDB_DEFAULT_CAPACITY     "mdb_default_capacity_size"

#define TAIR_ULOG_DIR                   "ulog_dir"
#define TAIR_ULOG_MIGRATE_BASENAME      "ulog_migrate_base_name"
#define TAIR_ULOG_MIGRATE_DEFAULT_BASENAME   "tair_ulog_migrate"
#define TAIR_ULOG_FILENUM               "ulog_file_number"
#define TAIR_ULOG_DEFAULT_FILENUM       3
#define TAIR_ULOG_FILESIZE              "ulog_file_size"
#define TAIR_ULOG_DEFAULT_FILESIZE      64// 64MB
#define TAIR_DUP_SYNC_MODE              0
#define TAIR_COUNT_NEGATIVE_MODE        1 //default is allow count negative


#define TAIR_SERVER_DEFAULT_PORT        5191
#define TAIR_SERVER_DEFAULT_HB_PORT     6191

#define TAIR_MAX_ITEM_COUNT             10000000
//KDB
#define KDB_MAP_SIZE                    "map_size"
#define KDB_MAP_SIZE_DEFAULT            10 * 1024 * 1024 // 10MB
#define KDB_BUCKET_SIZE                 "bucket_size"
#define KDB_BUCKET_SIZE_DEFAULT         1048583ULL
#define KDB_RECORD_ALIGN                "record_align"
#define KDB_RECORD_ALIGN_DEFAULT        128
#define KDB_DATA_DIR                    "data_dir"
#define KDB_DEFAULT_DATA_DIR            "data/kdb"
#define KDB_CACHE_SIZE                  "cache_size"
#define LOCKER_SIZE                     128

// LDB
#define LDB_DATA_DIR                    "data_dir"
#define LDB_DEFAULT_DATA_DIR            "data/ldb"
#define LDB_DB_INSTANCE_COUNT           "ldb_db_instance_count"
#define LDB_BUCKET_INDEX_TO_INSTANCE_STRATEGY "ldb_bucket_index_to_instance_strategy"
#define LDB_BUCKET_INDEX_FILE_DIR       "ldb_bucket_index_file_dir"
#define LDB_BUCKET_INDEX_CAN_UPDATE     "ldb_bucket_index_can_update"
#define LDB_LOAD_BACKUP_VERSION         "ldb_load_backup_version"
#define LDB_DB_VERSION_CARE             "ldb_db_version_care"
#define LDB_CACHE_STAT_FILE_SIZE        "ldb_cache_stat_file_size"
#define LDB_COMPACT_RANGE               "ldb_compact_range"
#define LDB_CHECK_COMPACT_INTERVAL      "ldb_check_compact_interval"
#define LDB_USE_CACHE_COUNT             "ldb_use_cache_count"
#define LDB_MIGRATE_BATCH_COUNT         "ldb_migrate_batch_count"
#define LDB_MIGRATE_BATCH_SIZE          "ldb_migrate_batch_size"
#define LDB_COMPARATOR_TYPE             "ldb_comparator_type"
#define LDB_USERKEY_SKIP_META_SIZE      "ldb_userkey_skip_meta_size"
#define LDB_USERKEY_NUM_DELIMITER       "ldb_userkey_num_delimiter"
#define LDB_USE_BLOOMFILTER             "ldb_use_bloomfilter"
#define LDB_USE_MMAP_RANDOM_ACCESS      "ldb_use_mmap_random_access"

#define LDB_PARANOID_CHECK              "ldb_paranoid_check"
#define LDB_MAX_OPEN_FILES              "ldb_max_open_files"
#define LDB_WRITE_BUFFER_SIZE           "ldb_write_buffer_size"
#define LDB_MAX_MEM_USAGE_FOR_MEMTABLE  "ldb_max_mem_usage_for_memtable"
#define LDB_TARGET_FILE_SIZE            "ldb_target_file_size"
#define LDB_BLOCK_SIZE                  "ldb_block_size"
#define LDB_BLOCK_RESTART_INTERVAL      "ldb_block_restart_interval"
#define LDB_BLOCK_CACHE_SIZE            "ldb_block_cache_size"
#define LDB_ARENABLOCK_SIZE             "ldb_arenablock_size"
#define LDB_COMPRESSION                 "ldb_compression"
#define LDB_L0_COMPACTION_TRIGGER       "ldb_l0_compaction_trigger"
#define LDB_L0_SLOWDOWN_WRITE_TRIGGER   "ldb_l0_slowdown_write_trigger"
#define LDB_L0_STOP_WRITE_TRIGGER       "ldb_l0_stop_write_trigger"
#define LDB_MAX_MEMCOMPACT_LEVEL        "ldb_max_memcompact_level"
#define LDB_BASE_LEVEL_SIZE             "ldb_base_level_size"
#define LDB_READ_VERIFY_CHECKSUMS       "ldb_read_verify_checksums"
#define LDB_WRITE_SYNC                  "ldb_write_sync"
#define LDB_BLOOMFILTER_BITS_PER_KEY    "ldb_bloomfilter_bits_per_key"
#define LDB_FILTER_BASE_LOGARITHM       "ldb_filter_base_logarithm"
#define LDB_RANGE_MAX_SIZE              "ldb_range_max_size"
#define LDB_LIMIT_COMPACT_LEVEL_COUNT   "ldb_limit_compact_level_count"
#define LDB_LIMIT_COMPACT_COUNT_INTERVAL "ldb_limit_compact_count_interval"
#define LDB_LIMIT_COMPACT_TIME_INTERVAL "ldb_limit_compact_time_interval"
#define LDB_LIMIT_COMPACT_TIME_RANGE    "ldb_limit_compact_time_range"
#define LDB_LIMIT_DELETE_OBSOLETE_FILE_INTERVAL      "ldb_limit_delete_obsolete_file_interval"
#define LDB_DO_SEEK_COMPACTION          "ldb_do_seek_compaction"

// file storage engine config items
#define FDB_INDEX_MMAP_SIZE             "index_mmap_size"
#define FDB_CACHE_SIZE                  "cache_size"
#define FDB_BUCKET_SIZE                 "bucket_size"
#define FDB_FREE_BLOCK_POOL_SIZE        "free_block_pool_size"
#define FDB_FREE_BLOCK_MAXSIZE          1024
#define FDB_DATA_DIR                    "data_dir"
#define FDB_DEFAULT_DATA_DIR            "data/fdb"

#define FDB_NAME                        "fdb_name"
#define FDB_DEFAULT_NAME                "tair_fdb"
#define FDB_HAS_RAID                    "has_raid"

// CONFIG_SERVER
#define TAIR_CONFIG_SERVER_DEFAULT_PORT 5198
#define TAIR_DEFAULT_DATA_DIR           "data"
#define TAIR_STR_GROUP_DATA_NEED_MOVE   "_data_move"
#define TAIR_STR_MIN_CONFIG_VERSION     "_min_config_version"
#define TAIR_CONFIG_MIN_VERSION         (10)
#define TAIR_STR_MIN_DATA_SRVER_COUNT   "_min_data_server_count"
#define TAIR_STR_BUILD_STRATEGY         "_build_strategy"
#define TAIR_BUILD_STRATEGY             (1)
#define TAIR_STR_ACCEPT_STRATEGY        "_accept_strategy"  // default is 0, need touch. if 1, accept ds automatically
#define TAIR_STR_DATALOST_FLAG          "_allow_lost_flag"  // default is 0, not allow data lost. if 1: if one bucket lost all of copies, configserver will force rebuild table, and assign this bucket to alive dataservers
#define TAIR_STR_BUCKET_DISTRI_FLAG     "_bucket_placement_flag" // default is 0, if 1: force buckets are not one to one correspondence in ds at first run
#define TAIR_STR_BUILD_DIFF_RATIO       "_build_diff_ratio"
#define TAIR_BUILD_DIFF_RATIO           "0.6"
#define TAIR_STR_SERVER_LIST            "_server_list"
#define TAIR_STR_PLUGINS_LIST           "_plugIns_list"
#define TAIR_STR_AREA_CAPACITY_LIST     "_areaCapacity_list"
#define TAIR_STR_POS_MASK               "_pos_mask"
#define TAIR_STR_SERVER_DOWNTIME        "_server_down_time"
#define TAIR_SERVER_DOWNTIME            (4)
#define TAIR_STR_COPY_COUNT             "_copy_count"
#define TAIR_DEFAULT_COPY_COUNT         (1)
#define TAIR_STR_BUCKET_NUMBER          "_bucket_number"
#define TAIR_DEFAULT_BUCKET_NUMBER      (1023)
#define TAIR_STR_REPORT_INTERVAL        "_report_interval"
#define TAIR_DEFAULT_REPORT_INTERVAL    (5)       //means 5 seconds
#define TAIR_MULTI_GROUPS               "groups"
#define TAIR_GROUP_STATUS               "group_status"
#define TAIR_GROUP_STATUS_ON            "on"
#define TAIR_PRE_LOAD_FLAG              "_pre_load_flag" // 1: need preload; 0: no need; default 0
#define TAIR_TMP_DOWN_SERVER            "tmp_down_server"
#define TAIR_CONFIG_VALUE_DELIMITERS    " ;"

#define TAIR_SERVER_OP_TIME             (4)
#define TAIR_CLIENT_OP_TIME             (2)
#define TAIR_MAX_CLIENT_OP_TIME             (5)

//REMOTE_TAIRSERVER_SECTION
#define TAIR_MASTER_NAME                "master"
#define TAIR_SLAVE_NAME                 "slaver"
#define TAIR_GROUP_NAME                "group"
#define REMOTE_DATA_DIR                 "data_dir"
#define REMOTE_DEFAULT_DATA_DIR         "data/remote"

// invalserver
#define TAIR_INVAL_SERVER_DEFAULT_PORT  5196
#define TAIR_IGNORE_ZERO_AREA           "ignore_zero_area"
#define TAIR_MAX_INVALSVR_CNT           32
#define TAIR_INVAL_DATA_DIR             "data_dir"
#define TAIR_INVAL_DEFAULT_DATA_DIR     "data"
#define TAIR_INVAL_CACHED_PACKET_COUNT   "cached_packet_count"
#define TAIR_INVAL_DEFAULT_CACHED_PACKET_COUNT (100000)
enum {
  SYNC_INVALID,
  ASYNC_INVALID,
};
//////////////////////////////////////////////
enum {
   TAIR_ITEM_FLAG_ADDCOUNT = 0x1,
   TAIR_ITEM_FLAG_DELETED = 0x2,
   TAIR_ITEM_FLAG_ITEM = 0x4,
   TAIR_ITEM_FLAG_LOCKED = 0x8,
   TAIR_ITEM_FLAG_NEWMETA = 0x10,
};

// 'cause key's data_entry.data_meta.flag is meaningless when requsting,
// here is a trick to set flag in client to data_entry.data_meta.flag to deliver
// some information to server
enum {
  TAIR_CLIENT_PUT_PUT_CACHE_FLAG = 0,
  TAIR_CLIENT_PUT_SKIP_CACHE_FLAG = 1,
  TAIR_CLIENT_DATA_MTIME_CARE = 2,
};
#define SHOULD_PUT_FILL_CACHE(flag) \
  (!((flag) & TAIR_CLIENT_PUT_SKIP_CACHE_FLAG))

enum {
   TAIR_RETURN_SUCCESS = 0,
   TAIR_DUP_WAIT_RSP = 133,
   TAIR_HAS_MORE_DATA = 150,

   TAIR_RETURN_NOT_INIT = -4002,
   TAIR_RETURN_NOT_SUPPORTED = -4001,
   TAIR_RETURN_PROXYED = -4000,
   TAIR_RETURN_FAILED = -3999,
   TAIR_RETURN_DATA_NOT_EXIST = -3998,
   TAIR_RETURN_VERSION_ERROR = -3997,
   TAIR_RETURN_TYPE_NOT_MATCH = -3996,

   TAIR_RETURN_PLUGIN_ERROR = -3995,
   TAIR_RETURN_SERIALIZE_ERROR = -3994,
   TAIR_RETURN_ITEM_EMPTY = -3993,
   TAIR_RETURN_OUT_OF_RANGE = -3992,
   TAIR_RETURN_ITEMSIZE_ERROR = -3991,

   TAIR_RETURN_SEND_FAILED = -3990,
   TAIR_RETURN_TIMEOUT = -3989,
   TAIR_RETURN_DATA_EXPIRED = -3988,
   TAIR_RETURN_SERVER_CAN_NOT_WORK = -3987,
   TAIR_RETURN_WRITE_NOT_ON_MASTER = -3986,

   TAIR_RETURN_DUPLICATE_BUSY = -3985,
   TAIR_RETURN_MIGRATE_BUSY = -3984,

   TAIR_RETURN_PARTIAL_SUCCESS = -3983,
   TAIR_RETURN_INVALID_ARGUMENT = -3982,
   TAIR_RETURN_CANNOT_OVERRIDE = -3981,

   TAIR_RETURN_DEC_BOUNDS= -3980,
   TAIR_RETURN_DEC_ZERO= -3979,
   TAIR_RETURN_DEC_NOTFOUND= -3978,

   TAIR_RETURN_PROXYED_ERROR = -3977,

   TAIR_RETURN_INVAL_CONN_ERROR = -3970,
   TAIR_RETURN_HIDDEN = -3969,
   TAIR_RETURN_QUEUE_OVERFLOWED = -3968,
   TAIR_RETURN_SHOULD_PROXY = -3967,

   // for lock
   TAIR_RETURN_LOCK_EXIST = -3975,
   TAIR_RETURN_LOCK_NOT_EXIST = -3974,

   // remove/update and mtime_care but mtime is early
   TAIR_RETURN_MTIME_EARLY = -3976,

   TAIR_RETURN_REMOVE_NOT_ON_MASTER= -4101,
   TAIR_RETURN_REMOVE_ONE_FAILED= -4102,

   TAIR_RETURN_DUPLICATE_IDMIXED= -5001,
   TAIR_RETURN_DUPLICATE_DELAY= -5002,
   TAIR_RETURN_DUPLICATE_REACK= -5003,
   TAIR_RETURN_DUPLICATE_ACK_WAIT= -5004,
   TAIR_RETURN_DUPLICATE_ACK_TIMEOUT= -5005,
   TAIR_RETURN_DUPLICATE_SEND_ERROR= -5006,

   TAIR_RETURN_REMOTE_NOTINITED= -5106,
   TAIR_RETURN_REMOTE_SLOW= -5107,
   TAIR_RETURN_REMOTE_NOTINIT= -5108,
   TAIR_RETURN_REMOTE_DISKSAVE_FAILED=-5109,
   TAIR_RETURN_REMOTE_MISS=-5110,
   TAIR_RETURN_REMOTE_RSP_FAILED=-5111,
   TAIR_RETURN_REMOTE_NOLOCAL=-5112,
};

enum {
   TAIR_SERVERFLAG_CLIENT = 0,
   TAIR_SERVERFLAG_DUPLICATE,
   TAIR_SERVERFLAG_MIGRATE,
   TAIR_SERVERFLAG_PROXY,
   TAIR_SERVERFLAG_RSYNC,
   // rsynced proxy request should not be rsynced again,
   // this flag is useless when do proxy logic is eliminated.
   TAIR_SERVERFLAG_RSYNC_PROXY,
};

namespace {
   const int TAIR_OPERATION_VERSION   = 1;
   const int TAIR_OPERATION_DUPLICATE = 2;
   const int TAIR_OPERATION_RSYNC     = 4;
   const int TAIR_OPERATION_UNLOCK    = 8;
   const int TAIR_DUPLICATE_BUSY_RETRY_COUNT = 10;
}

enum TAIR_COMPRESS_TYPE {
  TAIR_SNAPPY_COMPRESS = 0,
};

typedef enum {
  // all cmd type should be larger than TAIR_SERVER_CMD_MIN_TYPE
  TAIR_SERVER_CMD_MIN_TYPE = 0,
  TAIR_SERVER_CMD_FLUSH_MMT,
  TAIR_SERVER_CMD_RESET_DB,
  TAIR_SERVER_CMD_RESET_DS,
  TAIR_SERVER_CMD_GET_GROUP_STATUS,
  TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER,
  TAIR_SERVER_CMD_SET_GROUP_STATUS,
  TAIR_SERVER_CMD_SET_MIGRATE_WAIT_MS,
  TAIR_SERVER_CMD_PAUSE_GC,
  TAIR_SERVER_CMD_RESUME_GC,
  TAIR_SERVER_CMD_RELEASE_MEM,
  TAIR_SERVER_CMD_STAT_DB,
  TAIR_SERVER_CMD_SET_CONFIG,
  TAIR_SERVER_CMD_GET_CONFIG,
  TAIR_SERVER_CMD_BACKUP_DB,
  TAIR_SERVER_CMD_PAUSE_RSYNC,
  TAIR_SERVER_CMD_RESUME_RSYNC,
  // all cmd type should be less TAIR_SERVER_CMD_MAX_TYPE
  TAIR_SERVER_CMD_MAX_TYPE,
} ServerCmdType;

typedef enum {
  CMD_RANGE_ALL = 1,  
  CMD_RANGE_VALUE_ONLY,  
  CMD_RANGE_KEY_ONLY,  
} RangeCmdType;

typedef enum {
  TAIR_CLUSTER_TYPE_NONE = 0,
  TAIR_CLUSTER_TYPE_SINGLE_CLUSTER,
  TAIR_CLUSTER_TYPE_MULTI_CLUSTER,
} TairClusterType;

typedef enum
{
  NONE = 0,
  LOCK_STATUS,
  LOCK_VALUE,
  UNLOCK_VALUE,
  PUT_AND_LOCK_VALUE
} LockType;

typedef enum {
  TAIR_REMOTE_SYNC_TYPE_NONE = 0,
  TAIR_REMOTE_SYNC_TYPE_DELETE,
  // all types that need value when doing remote synchronizaton must be larger
  // than TAIR_REMOTE_SYNC_TYPE_WITH_VALUE_START.
  TAIR_REMOTE_SYNC_TYPE_WITH_VALUE_START,
  TAIR_REMOTE_SYNC_TYPE_PUT,
  // all types MUST be less than TAIR_REMOTE_SYNC_TYPE_MAX
  TAIR_REMOTE_SYNC_TYPE_MAX,
} TairRemoteSyncType;

#endif
/////////////
