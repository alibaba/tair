/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#ifndef TAIR_DEFINE_H
#define TAIR_DEFINE_H

#include <stdint.h>
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

#define showstack() do {                   \
  char** frames;                           \
  void* array[16];                         \
  size_t size;                             \
  size = backtrace(array, 16);             \
  frames = backtrace_symbols(array, size); \
  for (size_t i = 0; i < size; i++) {      \
    fprintf(stderr, "%s\n", frames[i]);      \
  }                                        \
}while(0)

#define CAN_OVERRIDE(old_flag, new_flag)         \
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
#define TAIR_MAX_KEY_COUNT           1024
#define TAIR_MAX_KEY_SIZE_WITH_AREA  (TAIR_MAX_KEY_SIZE+TAIR_AREA_ENCODE_SIZE)
#define TAIR_MAX_DATA_SIZE           1000000
#define TAIR_MAX_AREA_COUNT          65536
#define TAIR_AUTO_ALLOC_AREA         65536
#define TAIR_MAX_DUP_MAP_SIZE        102400
#define TAIR_AREA_KEY_MAX_LEN        512
#define TAIR_PACKET_FLAG             0x6D426454
#define TAIR_DTM_VERSION             0x31766256
#define TAIR_HTM_VERSION             0x31766257

#define TAIR_DATA_MAX_VERSION        (uint16_t)(-1)
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
#define PREFIX_KEY_OFFSET           (22)
#define PREFIX_KEY_MASK             (0x3FFFFF)
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
#define TAIRLDB_SECTION              "ldb"
#define REMOTE_TAIRSERVER_SECTION    "remote"
#define LOCAL_TAIRSERVER_SECTION      "local"
#define FLOW_CONTROL_SECTION         "flow_control"

// TAIR_SERVER
#define TAIR_PORT                    "port"
#define TAIR_HEARTBEAT_PORT          "heartbeat_port"
#define TAIR_SENGINE                 "storage_engine"
#define TAIR_PROFILER_THRESHOLD      "profiler_threshold"
#define TAIR_PROCESS_THREAD_COUNT    "process_thread_num"
#define TAIR_IO_PROCESS_THREAD_COUNT "io_thread_num"
#define TAIR_DUP_IO_THREAD_COUNT     "dup_io_thread_num"
#define TAIR_PROCESS_BULK_WRITE_THREAD_COUNT "bulk_write_thread_num"
#define TAIR_RSYNC_IO_THREAD_NUM     "rsync_io_thread_num"
#define TAIR_RSYNC_TASK_THREAD_NUM   "rsync_task_thread_num"
#define TAIR_RSYNC_QUEUE_LIMIT       "rsync_queue_limit"
#define TAIR_MAX_QUEUED_MEM          "max_queued_mem"
#define TAIR_LOG_FILE                "log_file"
#define TAIR_CONFIG_SERVER           "config_server"
#define TAIR_PID_FILE                "pid_file"
#define TAIR_AREA_MAP_FILE           "area_map_file"
#define TAIR_LOG_LEVEL               "log_level"
#define TAIR_DEV_NAME                "dev_name"
#define TAIR_DUP_SYNC                "dup_sync"
#define TAIR_DUP_TIMEOUT             "dup_timeout"
#define TAIR_COUNT_NEGATIVE          "allow_count_negative"
#define TAIR_LOCAL_MODE              "local_mode"
#define TAIR_LOCAL_MODE_BUCKET       "local_mode_bucket"
#define TAIR_GROUP_FILE              "group_file"
#define TAIR_DATA_DIR                "data_dir"
#define TAIR_EVICT_DATA_PATH         "evict_data_path"
#define TAIR_STAT_FILE_PATH          "stat_file_path"
#define TAIR_STAT_DEFAULT_FILEPATH   "tair_stat.dat"
#define TAIR_DUMP_DIR                "data_dump_dir"
#define TAIR_DEFAULT_DUMP_DIR        "dump"
#define TAIR_TASK_QUEUE_SIZE         "task_queue_size"
#define TAIR_DO_RSYNC                "do_rsync"
#define TAIR_RSYNC_VERSION           "rsync_version"
#define TAIR_RSYNC_MTIME_CARE        "rsync_mtime_care"
#define TAIR_RSYNC_WAIT_US           "rsync_wait_us"
#define TAIR_RSYNC_CONF              "rsync_conf"
#define TAIR_RSYNC_DATA_DIR          "rsync_data_dir"
#define TAIR_RSYNC_DO_RETRY          "rsync_do_retry"
#define TAIR_RSYNC_RETRY_LOG_MEM_SIZE "rsync_retry_log_mem_size"
#define TAIR_RSYNC_FAIL_LOG_SIZE     "rsync_fail_log_size"
#define TAIR_DEFAULT_DATASERVER_CONF "etc/dataserver.conf"
#define TAIR_DO_DUP_DEPOT            "do_dup_depot"
#define TAIR_DUP_DEPOT_DIR           "dup_depot_dir"
#define TAIR_IS_NAMESPACE_LOAD       "is_namespace_load"
#define TAIR_IS_FLOWCONTROL_LOAD     "is_flowcontrol_load"
#define TAIR_ADMIN_FILE              "tair_admin_file"

//for ds flow control
#define TAIR_DEFAULT_NET_UPPER        "default_net_upper"
#define TAIR_DEFAULT_NET_LOWER        "default_net_lower"
#define TAIR_DEFAULT_OPS_UPPER        "default_ops_upper"
#define TAIR_DEFAULT_OPS_LOWER        "default_ops_lower"
#define TAIR_DEFAULT_TOTAL_NET_UPPER  "default_total_net_upper"
#define TAIR_DEFAULT_TOTAL_NET_LOWER  "default_total_net_lower"
#define TAIR_DEFAULT_TOTAL_OPS_UPPER  "default_total_ops_upper"
#define TAIR_DEFAULT_TOTAL_OPS_LOWER  "default_total_ops_lower"

//for admin server
#define ADMINSERVER_SECTION           "admin_server"
#define ADMINSERVER_STORAGE_SECTION   "persist_storage"
#define ADMINSERVER_STORAGE_MASTER    "storage_master"
#define ADMINSERVER_STORAGE_SLAVE     "storage_slave"
#define ADMINSERVER_STORAGE_GROUP     "storage_group"
#define ADMINSERVER_STORAGE_NAMESPACE "storage_namespace"
#define ADMINSERVER_DEFAULT_PORT      6198
#define ADMINSERVER_CONFIG_KEY        "admin_server_list"
#define ADMINSERVER_GROUP_NAME        "group_name"

//for admin config
#define ADMINCONF_NAMESPACE_SECTION   "namespace"
#define ADMINCONF_FLOWCONTROL_SECTION "flowcontrol"

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
#define TAIR_MDB_INST_SHIFT           "mdb_inst_shift"
#define TAIR_MDB_PUT_REMOVE_EXPIRE    "put_remove_expired"
#define TAIR_MEM_MERGE_HOUR_RANGE     "mem_merge_hour_range"
#define TAIR_MEM_MERGE_MOVE_COUNT     "mem_merge_move_count"

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

#define TAIR_ULOG_FAILOVER_BASENAME           "failover_base_name"
#define TAIR_ULOG_FAILOVER_DEFAULT_BASENAME   "failover"

#define TAIR_SERVER_DEFAULT_PORT        5191

#define TAIR_MAX_ITEM_COUNT             10000000

// LDB
#define LOCKER_SIZE                     1024
#define LDB_DATA_DIR                    "data_dir"
#define LDB_DEFAULT_DATA_DIR            "data/ldb"
#define LDB_DB_INSTANCE_COUNT           "ldb_db_instance_count"
#define LDB_DB_DISK_COUNT               "ldb_db_disk_count"
#define LDB_DB_MULTIAREA_INSTANCE       "ldb_db_multiarea_instance"
#define LDB_BUCKET_INDEX_TO_INSTANCE_STRATEGY "ldb_bucket_index_to_instance_strategy"
#define LDB_BUCKET_INDEX_FILE_DIR       "ldb_bucket_index_file_dir"
#define LDB_BUCKET_INDEX_CAN_UPDATE     "ldb_bucket_index_can_update"
#define LDB_BUCKET_INDEX_FINAL_BALANCE  "ldb_bucket_index_final_balance"
#define LDB_AREA_INDEX_FILE_DIR         "ldb_area_index_file_dir"
#define LDB_LOAD_BACKUP_VERSION         "ldb_load_backup_version"
#define LDB_DB_VERSION_CARE             "ldb_db_version_care"
#define LDB_CACHE_STAT_FILE_SIZE        "ldb_cache_stat_file_size"
#define LDB_COMPACT_GC_RANGE            "ldb_compact_gc_range"
#define LDB_CHECK_COMPACT_INTERVAL      "ldb_check_compact_interval"
#define LDB_CHECK_CLEAN_INTERVAL        "ldb_check_clean_interval"
#define LDB_BACKUP_MAX_SIZE             "ldb_backup_max_size"
#define LDB_USE_CACHE_COUNT             "ldb_use_cache_count"
#define LDB_MIGRATE_BATCH_COUNT         "ldb_migrate_batch_count"
#define LDB_MIGRATE_BATCH_SIZE          "ldb_migrate_batch_size"
#define LDB_DELRANGE_BATCH_COUNT        "ldb_delrange_batch_count"
#define LDB_DELRANGE_BATCH_SIZE         "ldb_delrange_batch_size"
#define LDB_COMPARATOR_TYPE             "ldb_comparator_type"
#define LDB_USERKEY_SKIP_META_SIZE      "ldb_userkey_skip_meta_size"
#define LDB_USERKEY_NUM_DELIMITER       "ldb_userkey_num_delimiter"
#define LDB_USE_BLOOMFILTER             "ldb_use_bloomfilter"
#define LDB_USE_MMAP_RANDOM_ACCESS      "ldb_use_mmap_random_access"
#define LDB_GET_RANGE_LIMIT             "ldb_get_range_limit"
#define LDB_RANGE_SCAN_TIMES_LIMIT      "ldb_range_scan_times_limit"

#define LDB_PARANOID_CHECK              "ldb_paranoid_check"
#define LDB_MAX_OPEN_FILES              "ldb_max_open_files"
#define LDB_WRITE_BUFFER_SIZE           "ldb_write_buffer_size"
#define LDB_MAX_MEM_USAGE_FOR_MEMTABLE  "ldb_max_mem_usage_for_memtable"
#define LDB_TARGET_FILE_SIZE            "ldb_target_file_size"
#define LDB_BLOCK_SIZE                  "ldb_block_size"
#define LDB_BLOCK_RESTART_INTERVAL      "ldb_block_restart_interval"
#define LDB_TABLE_CACHE_SIZE            "ldb_table_cache_size"
#define LDB_BLOCK_CACHE_SIZE            "ldb_block_cache_size"
#define LDB_ARENABLOCK_SIZE             "ldb_arenablock_size"
#define LDB_COMPRESSION                 "ldb_compression"
#define LDB_L0_COMPACTION_TRIGGER       "ldb_l0_compaction_trigger"
#define LDB_L0_LIMIT_WRITE_WITH_COUNT   "ldb_l0_limit_write_with_count"
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
#define LDB_DO_BACKUP_SST_FILE          "ldb_do_backup_sst_file"
#define LDB_DO_SPLIT_MMT_COMPACTION     "ldb_do_split_mmt_compaction"
#define LDB_SPECIFY_COMPACT_TIME_RANGE  "ldb_specify_compact_time_range"
#define LDB_SPECIFY_COMPACT_MAX_THRESHOLD   "ldb_specify_compact_max_threshold"
#define LDB_SPECIFY_COMPACT_THRESHOLD       "ldb_specify_compact_threshold"
#define LDB_SPECIFY_COMPACT_SCORE_THRESHOLD  "ldb_specify_compact_score_threshold"
#define TAIR_ADD_GC_BUCKET           "add_gc_bucket"
#define TAIR_ADD_GC_AREA             "add_gc_area"
#define TAIR_REMOVE_GC_BUCKET        "remove_gc_bucket"
#define TAIR_REMOVE_GC_AREA          "remove_gc_area"
#define LDB_REMOVE_EXTRA_SST            "ldb_remove_extra_sst_start"
#define LDB_REMOVE_EXTRA_SST_NO_CHECK   "ldb_remove_extra_sst_no_check_start"
#define LDB_REMOVE_EXTRA_SST_STOP       "ldb_remove_extra_sst_stop"
#define LDB_REMOVE_GC_SST               "ldb_remove_gc_sst_start"
#define LDB_REMOVE_GC_SST_NO_CHECK      "ldb_remove_gc_sst_no_check_start"
#define LDB_REMOVE_GC_SST_STOP          "ldb_remove_gc_sst_stop"
#define LDB_REMOVE_CORRUPTION_SST       "ldb_remove_corruption_sst_start"
#define LDB_REMOVE_CORRUPTION_SST_NO_CHECK   "ldb_remove_corruption_sst_no_check_start"
#define LDB_REMOVE_CORRUPTION_SST_STOP  "ldb_remove_corruption_sst_stop"
#define LDB_LOG_FILE_KEEP_INTERVAL "ldb_log_file_keep_interval"

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
#define TAIR_STR_AREA_KEY_LIST          "_areaKey_list"
#define TAIR_STR_POS_MASK               "_pos_mask"
#define TAIR_STR_SERVER_DOWNTIME        "_server_down_time"
#define TAIR_SERVER_DOWNTIME            (4)
#define TAIR_SERVER_STANDBYTIME         (4)
#define TAIR_STR_COPY_COUNT             "_copy_count"
#define TAIR_DEFAULT_COPY_COUNT         (1)
#define TAIR_STR_BUCKET_NUMBER          "_bucket_number"
#define TAIR_DEFAULT_BUCKET_NUMBER      (1023)
#define TAIR_MAX_BUCKET_NUMBER          ((1<<16)-1)
#define TAIR_STR_REPORT_INTERVAL        "_report_interval"
#define TAIR_DEFAULT_REPORT_INTERVAL    (5)       //means 5 seconds
#define TAIR_MULTI_GROUPS               "groups"
#define TAIR_GROUP_STATUS               "group_status"
#define TAIR_GROUP_STATUS_ON            "on"
#define TAIR_AREA_STATUS                "area_status"
#define TAIR_AREA_STATUS_ON             "on"
#define TAIR_AREA_STATUS_OFF            "off"
#define TAIR_PRE_LOAD_FLAG              "_pre_load_flag" // 1: need preload; 0: no need; default 0
#define TAIR_ALWAYS_UPDATE_CAPACITY_INFO "_always_update_capacity_info"
#define TAIR_ALLOW_FAILOVER_SERVER      "_allow_failover_server"
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
#define TAIR_INVAL_MAX_FAILED_COUNT "max_failed_count"
#define TAIR_INVAL_DEFAULT_MAX_FAILED_COUNT (100)
#define TAIR_INVAL_CLUSTER_LIST "cluster_list"
#define TAIR_INVAL_CLUSTER_MASTER_CFG "master"
#define TAIR_INVAL_CLUSTER_SLAVE_CFG "slave"
#define TAIR_INVAL_CLUSTER_MODE "mode"
#define TAIR_INVAL_CLUSTER_REMOTE_MODE "remote"
#define TAIR_INVAL_CLUSTER_LOCAL_MODE "local"
enum {
    SYNC_INVALID,
    ASYNC_INVALID,
};
enum {
    LOCAL_MODE = 0,
    REMOTE_MODE = 1,
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
// some information to server, newbi
enum {
    TAIR_CLIENT_PUT_PUT_CACHE_FLAG = 0x0,
    TAIR_CLIENT_PUT_SKIP_CACHE_FLAG = 0x1,
    TAIR_CLIENT_DATA_MTIME_CARE = 0x2,
    TAIR_CLIENT_HAS_COMPARE_EPOCH = 0x4,
    TAIR_SERVER_WITH_STAT = 0x8,
    TAIR_SERVER_FLAG_UNIT = 0x40,  // XX1XXXXXB => REMOTE && XX0XXXXXB => LOCAL, store at key
    TAIR_SERVER_FLAG_SENDING_UNIT = 0x80
};

#define set_meta_flag_unit(flag) ((flag) = (flag) | TAIR_SERVER_FLAG_UNIT)
#define is_meta_flag_unit(flag)  (((flag) & TAIR_SERVER_FLAG_UNIT) == TAIR_SERVER_FLAG_UNIT)

#define is_meta_flag_sending_unit(flag) (((flag) & TAIR_SERVER_FLAG_SENDING_UNIT) == TAIR_SERVER_FLAG_SENDING_UNIT)
#define set_meta_flag_sending_unit(flag) ((flag) = (flag) | TAIR_SERVER_FLAG_SENDING_UNIT)
#define unset_meta_flag_sending_unit(flag) ((flag) = (flag) & (~TAIR_SERVER_FLAG_SENDING_UNIT))

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
    TAIR_RETURN_SERVER_CAN_NOT_WORK = -3987,
    TAIR_RETURN_WRITE_NOT_ON_MASTER = -3986,

    TAIR_RETURN_DUPLICATE_BUSY = -3985,
    TAIR_RETURN_MIGRATE_BUSY = -3984,

    TAIR_RETURN_PARTIAL_SUCCESS = -3983,
    TAIR_RETURN_INVALID_ARGUMENT = -3982,
    TAIR_RETURN_CANNOT_OVERRIDE = -3981,

    TAIR_RETURN_DEC_BOUNDS = -3980,
    TAIR_RETURN_DEC_ZERO = -3979,
    TAIR_RETURN_DEC_NOTFOUND = -3978,

    TAIR_RETURN_PROXYED_ERROR = -3977,

    TAIR_RETURN_FLOW_CONTROL = -3971,
    TAIR_RETURN_INVAL_CONN_ERROR = -3970,
    TAIR_RETURN_HIDDEN = -3969,
    TAIR_RETURN_QUEUE_OVERFLOWED = -3968,
    TAIR_RETURN_SHOULD_PROXY = -3967,

    // for lock
            TAIR_RETURN_LOCK_EXIST = -3975,
    TAIR_RETURN_LOCK_NOT_EXIST = -3974,

    // remove/update and mtime_care but mtime is early
            TAIR_RETURN_MTIME_EARLY = -3976,

    TAIR_RETURN_REMOVE_NOT_ON_MASTER = -4101,
    TAIR_RETURN_REMOVE_ONE_FAILED = -4102,

    TAIR_RETURN_OP_INVALID_PARAM = -4201,
    TAIR_RETURN_OP_GROUP_NOT_FOUND = -4202,
    TAIR_RETURN_NO_MORE_AREA = -4203,
    TAIR_RETURN_AREA_EXISTED = -4204,
    TAIR_RETURN_AREA_NOT_EXISTED = -4205,
    TAIR_RETURN_NOT_PERMITTED = -4206,
    TAIR_RETURN_REFUSED = -4207,

    TAIR_RETURN_DUPLICATE_IDMIXED = -5001,
    TAIR_RETURN_DUPLICATE_DELAY = -5002,
    TAIR_RETURN_DUPLICATE_REACK = -5003,
    TAIR_RETURN_DUPLICATE_ACK_WAIT = -5004,
    TAIR_RETURN_DUPLICATE_ACK_TIMEOUT = -5005,
    TAIR_RETURN_DUPLICATE_SEND_ERROR = -5006,

    TAIR_RETURN_REMOTE_NOTINITED = -5106,
    TAIR_RETURN_REMOTE_SLOW = -5107,
    TAIR_RETURN_REMOTE_NOTINIT = -5108,
    TAIR_RETURN_REMOTE_DISKSAVE_FAILED = -5109,
    TAIR_RETURN_REMOTE_MISS = -5110,
    TAIR_RETURN_REMOTE_RSP_FAILED = -5111,
    TAIR_RETURN_REMOTE_NOLOCAL = -5112,
    //cann't find the dataserver.
            TAIR_RETURN_NONE_DATASERVER = -5113,
    //for bounded counter.
            TAIR_RETURN_COUNTER_OUT_OF_RANGE = -5114,
    TAIR_RETURN_PAUSED = -5116, // just use inside of dataserver
    TAIR_RETURN_READ_ONLY = -5117,

    TAIR_RETURN_KEY_EXISTS = -5200,
};

enum {
    TAIR_SERVERFLAG_CLIENT = 0,
    TAIR_SERVERFLAG_DUPLICATE,   // 1
    TAIR_SERVERFLAG_MIGRATE,     // 2
    TAIR_SERVERFLAG_PROXY,       // 3
    TAIR_SERVERFLAG_RSYNC,       // 4
    // rsynced proxy request should not be rsynced again,
    // this flag is useless when do proxy logic is eliminated.
            TAIR_SERVERFLAG_RSYNC_PROXY, // 5
};

enum {
    TAIR_ADMIN_CONFIG_FLAG_NAMESPACE = 1,
    TAIR_ADMIN_CONFIG_FLAG_FLOWCONTROL,
};

enum {
    ACTIVE_FAILOVER = 0,
    ACTIVE_RECOVER = 1,
};

namespace {
const int TAIR_OPERATION_VERSION = 1;
const int TAIR_OPERATION_DUPLICATE = 2;
const int TAIR_OPERATION_RSYNC = 4;
const int TAIR_OPERATION_UNLOCK = 8;
const int TAIR_DUPLICATE_BUSY_RETRY_COUNT = 10;
}

enum TAIR_COMPRESS_TYPE {
    TAIR_SNAPPY_COMPRESS = 0,
};

typedef enum {
    // all cmd type should be larger than TAIR_SERVER_CMD_MIN_TYPE
            TAIR_SERVER_CMD_MIN_TYPE = 0,
    TAIR_SERVER_CMD_FLUSH_MMT = 1,        //for mput, flush mmt to ldb && release tcmalloc mem
    TAIR_SERVER_CMD_RESET_DB = 2,         //for multigroup fastdump, reset a area unit
    TAIR_SERVER_CMD_RESET_DS = 3,         //to ds, clear tmp_down_server
    TAIR_SERVER_CMD_GET_GROUP_STATUS = 4, //for multigroup fastdump, to cs, get group status, obsolete
    //  use TAIR_SERVER_CMD_GET_AREA_STATUS
            TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER = 5,
    TAIR_SERVER_CMD_SET_GROUP_STATUS = 6, //for multigroup fastdump, to cs, get group status, obsolete
    //  use TAIR_SERVER_CMD_SET_AREA_STATUS
            TAIR_SERVER_CMD_SET_MIGRATE_WAIT_MS = 7,
    TAIR_SERVER_CMD_PAUSE_GC = 8,
    TAIR_SERVER_CMD_RESUME_GC = 9,
    TAIR_SERVER_CMD_RELEASE_MEM = 10,     // = 10
    TAIR_SERVER_CMD_STAT_DB = 11,
    TAIR_SERVER_CMD_SET_CONFIG = 12,
    TAIR_SERVER_CMD_GET_CONFIG = 13,
    TAIR_SERVER_CMD_BACKUP_DB = 14,
    TAIR_SERVER_CMD_PAUSE_RSYNC = 15,     //to ldb, stop multicluster rsync
    TAIR_SERVER_CMD_RESUME_RSYNC = 16,    //to ldb, resume multicluster rsync
    TAIR_SERVER_CMD_START_BALANCE = 17,
    TAIR_SERVER_CMD_STOP_BALANCE = 18,
    TAIR_SERVER_CMD_SET_BALANCE_WAIT_MS = 19,
    TAIR_SERVER_CMD_UNLOAD_BACKUPED_DB = 20,
    TAIR_SERVER_CMD_CLOSE_UNUESD_BUCKETS = 21,
    TAIR_SERVER_CMD_SYNC_BUCKET = 22,
    TAIR_SERVER_CMD_MIGRATE_BUCKET = 23,
    TAIR_SERVER_CMD_ALLOC_AREA = 24,      //to cs, alloc area && add quota
    TAIR_SERVER_CMD_SET_QUOTA = 25,       //to cs, modify/delete quota
    TAIR_SERVER_CMD_GET_AREA_STATUS = 32, //for multigroup fastdump, to cs, get group area status
    TAIR_SERVER_CMD_SET_AREA_STATUS = 33, //for multigroup fastdump, to cs, get group area status
    TAIR_SERVER_CMD_GET_REVISE_STATUS = 34,
    TAIR_SERVER_CMD_START_REVISE_STAT = 35,
    TAIR_SERVER_CMD_STOP_REVISE_STAT = 36,
    TAIR_SERVER_CMD_MAP_AREAMAP = 37,
    TAIR_SERVER_CMD_GET_AREAMAP = 38,
    TAIR_ADMIN_SERVER_CMD_MAP_AREAMAP = 39,          //to admin server, map area
    TAIR_ADMIN_SERVER_CMD_GET_AREAMAP = 40,          //to admin server, get area map
    TAIR_SERVER_CMD_CLEAR_MDB = 43,
    TAIR_SERVER_CMD_LDB_INSTANCE_CONFIG = 44, // set config for ldb instance
    TAIR_SERVER_CMD_SWITCH_BUCKET = 45, // switch bucket
    TAIR_SERVER_CMD_ACTIVE_FAILOVER_OR_RECOVER = 46, // active failover

    TAIR_SERVER_CMD_NEW_AREAUNIT = 47,    //obsolete
    TAIR_SERVER_CMD_ADD_AREA = 48,        //obsolete
    TAIR_SERVER_CMD_DELETE_AREAUNIT = 49, //obsolete
    TAIR_SERVER_CMD_DELETE_AREA = 50,     //obsolete
    TAIR_SERVER_CMD_LINK_AREA = 51,       //obsolete
    TAIR_SERVER_CMD_RESET_AREAUNIT = 52,  //obsolete
    TAIR_SERVER_CMD_SWITCH_AREAUNIT = 53, //obsolete

    TAIR_ADMIN_SERVER_CMD_ALLOC_AREA_RING = 54,      //to admin server, allow area ring
    TAIR_ADMIN_SERVER_CMD_GET_MASTER_AREA_LIST = 55, //to admin server, obtain area list of the ring
    TAIR_ADMIN_SERVER_CMD_GET_AREA_RING = 56,        //to admin server, obtain the area ring
    TAIR_ADMIN_SERVER_CMD_GET_NEXT_AREAMAP = 57,     //to admin server, obtain the next area in the area ring
    TAIR_ADMIN_SERVER_CMD_GET_PREV_AREAMAP = 58,     //to admin server, obtain the previous area in the area ring
    TAIR_SERVER_CMD_QUERY_GC_STATUS = 59,           //to dataserver, inquire the area gc status
    TAIR_SERVER_CMD_REPLACE_DS = 60,                // replace ds one to one

    TAIR_SERVER_CMD_GET_RSYNC_STAT = 61,
    TAIR_SERVER_CMD_URGENT_OFFLINE = 62,            // urgent offline
    TAIR_ADMIN_SERVER_CMD_DEL_AREA_RING = 63,        //to adminserver ,remove the area ring.
    TAIR_SERVER_CMD_GET_BUCKET_DISTRIBUTION = 64,    // to data server, get bucket distribution
    TAIR_SERVER_CMD_OPKILL = 65,                     // enable opkill
    TAIR_SERVER_CMD_OPKILL_DISABLE = 66,             // disable opkill
    TAIR_SERVER_CMD_PAUSE_DUPLICATE = 67,            // pause duplication
    TAIR_SERVER_CMD_RESUME_DUPLICATE = 68,            // resume duplication
    TAIR_SERVER_CMD_SET_FLOW_POLICY = 69,            // set flow policy

    TAIR_ADMIN_SERVER_CMD_SET_AREA_MULTIVERSION = 71, //to admin server, set max version count for multiversion
    TAIR_ADMIN_SERVER_CMD_GET_AREA_MULTIVERSION = 72, //to admin server, get max version count for multiversion

    TAIR_ADMIN_SERVER_CMD_MODIFY_STAT_HIGN_OPS = 80, //to configserver, set all dataserver's stat hign ops value

    TAIR_SERVER_CMD_MODIFY_CHKEXPIR_HOUR_RANGE = 81,  // to dataserver, set check expired key hour range
    TAIR_SERVER_CMD_MODIFY_MDB_CHECK_GRANULARITY = 82,  // to dataserver, set mdb_check_granularity

    TAIR_SERVER_CMD_MODIFY_SLAB_MERGE_HOUR_RANGE = 83,  // to dataserver, set slab merge hour range
    TAIR_SERVER_CMD_MODIFY_PUT_REMOVE_EXPIRED = 84,  // to dataserver, set put with remove expired keys

    // 100-130 will use for unitization, other commands don't use this range, thanks
            TAIR_SERVER_CMD_SET_RSYNC_CONFIG_SERVICE = 100,  // set rsync config address
    TAIR_SERVER_CMD_SET_RSYNC_CONFIG_INTERVAL = 101, // set rsync config update interval
    TAIR_SERVER_CMD_NS_READ_ONLY_ON = 102, // turn namespace readonly on
    TAIR_SERVER_CMD_NS_READ_ONLY_OFF = 103, // turn namespace readonly off
    TAIR_SERVER_CMD_GET_NS_READ_ONLY_STATUS = 104, // show namespace readyonly related status
    TAIR_SERVER_CMD_GET_CLUSTER_INFO = 105,
    TAIR_SERVER_CMD_WATCH_RSYNC = 106,
    TAIR_SERVER_CMD_STAT_RSYNC = 107,
    TAIR_SERVER_CMD_OPTIONS_RSYNC = 108,
    TAIR_SERVER_CMD_RSYNC_CONNECTION_STAT = 109,
    // all cmd type should be less TAIR_SERVER_CMD_MAX_TYPE
            TAIR_SERVER_CMD_MAX_TYPE,
} ServerCmdType;

typedef enum {
    CMD_RANGE_ALL = 1,
    CMD_RANGE_VALUE_ONLY,
    CMD_RANGE_KEY_ONLY,
    CMD_RANGE_ALL_REVERSE,
    CMD_RANGE_VALUE_ONLY_REVERSE,
    CMD_RANGE_KEY_ONLY_REVERSE,
    // all GET_RANGE_CMD should < DEL_RANGE_CMD
            CMD_DEL_RANGE,
    CMD_DEL_RANGE_REVERSE,
} RangeCmdType;

typedef enum {
    RANGE_FROM_CUR = 0,
    RANGE_FROM_NEXT = 1,
} RangeOffsetType;

typedef enum {
    TAIR_CLUSTER_TYPE_NONE = 0,
    TAIR_CLUSTER_TYPE_SINGLE_CLUSTER,
    TAIR_CLUSTER_TYPE_MULTI_CLUSTER,
} TairClusterType;

typedef enum {
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
    TAIR_REMOTE_SYNC_TYPE_BATCH,
    TAIR_REMOTE_SYNC_TYPE_DO_NOTHING,
    // all types MUST be less than TAIR_REMOTE_SYNC_TYPE_MAX
            TAIR_REMOTE_SYNC_TYPE_MAX,
} TairRemoteSyncType;

enum {
    RSYNC_SELF = 1,
    RSYNC_INNERUNIT = 2,
    RSYNC_INTERUNIT = 3,
};

#define TIME_OF_SECOND (1)
#define TIME_OF_MINUTE (60)
#define TIME_OF_HOUR   (60 * 60)
#define TIME_OF_DAY    (24 * 60 * 60)

enum DataserverCtrlOpType {
    DATASERVER_CTRL_OP_ADD,
    DATASERVER_CTRL_OP_DELETE,
    // max, size
            DATASERVER_CTRL_OP_NR
};

enum DataserverCtrlReturnType {
    DATASERVER_CTRL_RETURN_SUCCESS,
    DATASERVER_CTRL_RETURN_FILE_OP_FAILED,
    DATASERVER_CTRL_RETURN_SEND_FAILED,
    DATASERVER_CTRL_RETURN_TIMEOUT,
    DATASERVER_CTRL_RETURN_SERVER_NOT_FOUND,
    DATASERVER_CTRL_RETURN_SERVER_EXIST,
    DATASERVER_CTRL_RETURN_GROUP_NOT_FOUND,
    DATASERVER_CTRL_RETURN_INVAL_OP_TYPE,
    DATASERVER_CTRL_RETURN_NO_CONFIGSERVER,
    //max, unkown error
            DATASERVER_CTRL_RETURN_NR
};


#endif
/////////////
