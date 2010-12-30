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

#define TAIR_SLEEP(stop, interval) ({int count=interval*5; while(count-->0 && !stop) usleep(200000);})

#define TAIR_MAX_KEY_SIZE            1024
#define TAIR_MAX_DATA_SIZE           1000000
#define TAIR_MAX_AREA_COUNT          1024
#define TAIR_PACKET_FLAG             0x6D426454
#define TAIR_DTM_VERSION             0x31766256
#define TAIR_HTM_VERSION             0x31766257

#define TAIR_FLAG_SERVER            (0x0000ffffffffffffLL)
#define TAIR_FLAG_CFG_DOWN          (0x4000000000000000LL)
#define TAIR_FLAG_NOEXP             (0x0c000000)
// TAIR_POS_MASK & serverid  is  rack_id, so change this to fit your rack
#define TAIR_POS_MASK               (0x000000000000ffffLL)
#define TAIR_MAX_FILENAME_LEN       (256)
#define TAIR_DATA_NEED_MIGRATE          (1)
//for migrate
#define MAX_MUPDATE_PACKET_SIZE     (16 * 1024)
/////////////////////////////
//for data duplicate
#define MISECONDS_BEFOR_SEND_RETRY  (1000000)
#define SLEEP_MISECONDS             (10)
//////////////////////////////////////////////
#define TAIRPUBLIC_SECTION           "public"
#define TAIRSERVER_SECTION           "tairserver"
#define CONFSERVER_SECTION           "configserver"
#define TAIRFDB_SECTION                  "fdb"
// TAIR_SERVER
#define TAIR_PORT                    "port"
#define TAIR_HEARTBEAT_PORT                  "heartbeat_port"
#define TAIR_SENGINE                     "storage_engine"
#define TAIR_PROFILER_THRESHOLD      "profiler_threshold"
#define TAIR_PROCESS_THREAD_COUNT    "process_thread_num"
#define TAIR_LOG_FILE                "log_file"
#define TAIR_CONFIG_SERVER           "config_server"
#define TAIR_PID_FILE                "pid_file"
#define TAIR_LOG_LEVEL               "log_level"
#define TAIR_DEV_NAME                "dev_name"

#define TAIR_GROUP_FILE              "group_file"
#define TAIR_DATA_DIR                "data_dir"
#define TAIR_EVICT_DATA_PATH         "evict_data_path"
#define TAIR_STAT_FILE_PATH                  "stat_file_path"
#define TAIR_STAT_DEFAULT_FILEPATH   "tair_stat.dat"
#define TAIR_DUMP_DIR                        "data_dump_dir"
#define TAIR_DEFAULT_DUMP_DIR        "dump"
#define TAIR_TASK_QUEUE_SIZE         "task_queue_size"
//MDB
#define TAIR_SLAB_MEM_SIZE           "slab_mem_size"
#define TAIR_SLAB_BASE_SIZE          "slab_base_size"
#define TAIR_SLAB_FACTOR             "slab_factor"
#define TAIR_MDB_TYPE                    "mdb_type" //mdb or mdb_shm
#define TAIR_MDB_SHM_PATH                "mdb_shm_path"
#define TAIR_SLAB_PAGE_SIZE              "slab_page_size"
#define TAIR_MDB_HASH_BUCKET_SHIFT   "mdb_hash_bucket_shift"
#define TAIR_CHECK_EXPIRED_HOUR_RANGE   "check_expired_hour_range"
#define TAIR_CHECK_SLAB_HOUR_RANGE          "check_slab_hour_range"

#define TAIR_ULOG_DIR                   "ulog_dir"
#define TAIR_ULOG_MIGRATE_BASENAME      "ulog_migrate_base_name"
#define TAIR_ULOG_MIGRATE_DEFAULT_BASENAME   "tair_ulog_migrate"
#define TAIR_ULOG_FILENUM               "ulog_file_number"
#define TAIR_ULOG_DEFAULT_FILENUM       3
#define TAIR_ULOG_FILESIZE              "ulog_file_size"
#define TAIR_ULOG_DEFAULT_FILESIZE      64// 64MB

#define TAIR_SERVER_DEFAULT_PORT        5191
#define TAIR_SERVER_DEFAULT_HB_PORT     6191

// file storage engine config items
#define FDB_INDEX_MMAP_SIZE                 "index_mmap_size"
#define FDB_CACHE_SIZE                      "cache_size"
#define FDB_BUCKET_SIZE                     "bucket_size"
#define FDB_FREE_BLOCK_POOL_SIZE        "free_block_pool_size"
#define FDB_FREE_BLOCK_MAXSIZE          1024
#define FDB_DATA_DIR                    "data_dir"
#define FDB_DEFAULT_DATA_DIR            "fdb/data"

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
//////////////////////////////////////////////
enum {
   TAIR_ITEM_FLAG_ADDCOUNT = 1,
   TAIR_ITEM_FLAG_DELETED = 2,
   TAIR_ITEM_FLAG_ITEM = 4,
   TAIR_ITEM_FLAG_SET,
};

enum {
   TAIR_RETURN_SUCCESS = 0,

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
};

enum {
   TAIR_SERVERFLAG_CLIENT = 0,
   TAIR_SERVERFLAG_DUPLICATE,
   TAIR_SERVERFLAG_MIGRATE,
   TAIR_SERVERFLAG_PROXY,
};
#endif
/////////////
