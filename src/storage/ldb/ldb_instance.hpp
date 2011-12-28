/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb db engine implementation
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_LDB_INSTANCE_H
#define TAIR_STORAGE_LDB_INSTANCE_H

#include "leveldb/db.h"

#include "common/data_entry.hpp"
#include "ldb_define.hpp"
#include "ldb_gc_factory.hpp"
#include "bg_task.hpp"
#include "stat_manager.hpp"
#include "ldb_cache_stat.hpp"

namespace tair
{
  class mdb_manager;
  namespace storage
  {
    class storage_manager;
    namespace ldb
    {
      class LdbInstance
      {
      public:
        LdbInstance();
        explicit LdbInstance(int32_t index, bool db_version_care, storage_manager* cache, bool put_fill_cache);
        ~LdbInstance();

        typedef __gnu_cxx::hash_map<int32_t, stat_manager*> STAT_MANAGER_MAP;
        typedef STAT_MANAGER_MAP::iterator STAT_MANAGER_MAP_ITER;

        bool init_buckets(const std::vector<int32_t> buckets);
        void close_buckets(const std::vector<int32_t> buckets);

        void destroy();

        int put(int bucket_number, tair::common::data_entry& key,
                tair::common::data_entry& value,
                bool version_care, uint32_t expire_time);
        int get(int bucket_number, tair::common::data_entry& key, tair::common::data_entry& value);
        int remove(int bucket_number, tair::common::data_entry& key, bool version_care);

        bool begin_scan(int bucket_number);
        bool end_scan();
        bool get_next_items(std::vector<item_data_info*>& list);
        void get_stats(tair_stat* stat);

        int clear_area(int32_t area);
        bool exist(int32_t bucket_number);

        // stat util
        void stat_add(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count);
        void stat_sub(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count);

        const char* db_path() { return db_path_; }
        // use inner leveldb/gc_factory when compact
        leveldb::DB* db() { return db_; }
        LdbGcFactory* gc_factory() { return &gc_; }

      private:
        int do_get(LdbKey& ldb_key, std::string& value, bool fill_cache, bool update_stat = true);
        int do_put(LdbKey& ldb_key, LdbItem& ldb_item);
        int do_remove(LdbKey& ldb_key);

        bool init_db();
        void stop();
        void sanitize_option(leveldb::Options& options);
        tbsys::CThreadMutex* get_mutex(const tair::common::data_entry& key);

      private:
        // index of this instance
        int32_t index_;
        char db_path_[PATH_MAX];
        // because version care strategy, we must "get" when "put", it's expensive to get an unexist item,
        // so this flag control whether we really must do it.
        // (statistics data may not be exact, as it is always.)
        bool db_version_care_;
        // write and read option to leveldb
        leveldb::WriteOptions write_options_;
        leveldb::ReadOptions read_options_;
        // lock to protect cache. cause leveldb and mdb has its own lock,
        // but the combination of operation over db_ and cache_ must atomic to avoid dirty data,
        // no matter op is read or write, cause read db means writing to cache.
        // No cache, no lock
        tbsys::CThreadMutex* mutex_;
        leveldb::DB* db_;
        // mdb cache
        mdb_manager* cache_;
        // cache stat
        CacheStat cache_stat_;
        // whether fill cache put data
        bool put_fill_cache_;

        // for scan, MUST single-bucket everytime
        leveldb::Iterator* scan_it_;
        std::string scan_end_key_;
        bool still_have_;

        // statatics manager
        STAT_MANAGER_MAP* stat_manager_;
        // background task(compact)
        BgTask bg_task_;
        // gc cleared area and closed buckets
        LdbGcFactory gc_;
      };
    }
  }
}
#endif
