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

namespace tair
{
  class mdb_manager;
  class mput_record_vec;
  namespace storage
  {
    class storage_manager;
    namespace ldb
    {
      class LdbInstance
      {
        friend class LdbRemoteSyncLogReader;
      public:
        LdbInstance();
        explicit LdbInstance(int32_t index, bool db_version_care, storage_manager* cache);
        ~LdbInstance();

        typedef __gnu_cxx::hash_map<int32_t, stat_manager*> STAT_MANAGER_MAP;
        typedef STAT_MANAGER_MAP::iterator STAT_MANAGER_MAP_ITER;

        bool init_buckets(const std::vector<int32_t> buckets);
        void close_buckets(const std::vector<int32_t> buckets);

        void destroy();

        int put(int bucket_number, tair::common::data_entry& key,
                tair::common::data_entry& value,
                bool version_care, int expire_time);
        int batch_put(int bucket_number, int area, tair::common::mput_record_vec* record_vec, bool version_care);
        int get(int bucket_number, tair::common::data_entry& key, tair::common::data_entry& value);
        int remove(int bucket_number, tair::common::data_entry& key, bool version_care);

        int get_range(int bucket_number, tair::common::data_entry& key_start, tair::common::data_entry& end_key, int offset, int limit, int type, std::vector<tair::common::data_entry*>& result, bool &has_next);

        int op_cmd(ServerCmdType cmd, std::vector<std::string>& params);

        bool begin_scan(int bucket_number);
        bool end_scan();
        bool get_next_items(std::vector<item_data_info*>& list);
        void get_stats(tair_stat* stat);

        int stat_db();
        int backup_db();
        int set_config(std::vector<std::string>& params);

        int clear_area(int32_t area);
        bool exist(int32_t bucket_number);

        // stat util
        void stat_add(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count);
        void stat_sub(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count);

        void get_buckets(std::vector<int32_t>& buckets);

        const char* db_path() { return db_path_; }
        // use inner leveldb/gc_factory when compact
        leveldb::DB* db() { return db_; }
        LdbGcFactory* gc_factory() { return &gc_; }
        BgTask* bg_task() { return &bg_task_;}

      private:
        int do_cache_get(LdbKey& ldb_key, std::string& value, bool update_stat);
        int do_get(LdbKey& ldb_key, std::string& value, bool from_cache, bool fill_cache, bool update_stat = true);
        int do_put(LdbKey& ldb_key, LdbItem& ldb_item, bool fill_cache, bool synced);
        int do_remove(LdbKey& ldb_key, bool synced, tair::common::entry_tailer* tailer = NULL);
        bool is_mtime_care(const common::data_entry& key);
        bool is_synced(const common::data_entry& key);
        void add_prefix(LdbKey& ldb_key, int prefix_size);
        void fill_meta(tair::common::data_entry *data, LdbKey& key, LdbItem& item);

        bool init_db();
        void stop();
        void sanitize_option();
        tbsys::CThreadMutex* get_mutex(const tair::common::data_entry& key);

      private:
        // index of this instance
        int32_t index_;
        char db_path_[TAIR_MAX_PATH_LEN];
        // because version care strategy, we must "get" when "put", it's expensive to get an unexist item,
        // so this flag control whether we really must do it.
        // (statistics data may not be exact, as it is always.)
        bool db_version_care_;
        // init and write and read option to leveldb
        leveldb::Options options_;
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

        // for scan, MUST single-bucket everytime
        leveldb::Iterator* scan_it_;
        // the bucket migrating
        int scan_bucket_;
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
