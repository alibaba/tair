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

#ifndef TAIR_STORAGE_LDB_INSTANCE_H
#define TAIR_STORAGE_LDB_INSTANCE_H

#include "leveldb/db.h"

#include "common/data_entry.hpp"
#include "ldb_define.hpp"
#include "ldb_gc_factory.hpp"
#include "bg_task.hpp"
#include "stat_manager.hpp"

namespace tair {
class mdb_manager;

class mput_record_vec;

class operation_record;
namespace storage {
class storage_manager;
namespace ldb {
class LdbStatManager;

class LdbInstance {
    friend class LdbRemoteSyncLogReader;

public:
    LdbInstance();

    explicit LdbInstance(int32_t index, bool db_version_care, storage_manager *cache);

    ~LdbInstance();

    int initialize();

    typedef __gnu_cxx::hash_map<int32_t, int32_t> STAT_MANAGER_MAP;
    typedef STAT_MANAGER_MAP::iterator STAT_MANAGER_MAP_ITER;

    bool init_buckets(const std::vector<int32_t> &buckets);

    void close_buckets(const std::vector<int32_t> &buckets);

    void destroy();

    int put(int bucket_number, tair::common::data_entry &key,
            tair::common::data_entry &value,
            bool version_care, int expire_time, bool mc_ops = false);

    int direct_mupdate(int bucket_number, const std::vector<operation_record *> &kvs);

    int batch_put(int bucket_number, int area, tair::common::mput_record_vec *record_vec, bool version_care);

    int get(int bucket_number, tair::common::data_entry &key, tair::common::data_entry &value);

    int remove(int bucket_number, tair::common::data_entry &key, bool version_care);

    // for mc_proxy
    int update(int bucket_number, tair::common::data_entry &key, tair::common::data_entry &value,
               uint8_t ldb_update_type, bool version_care, int expire_time, bool mc_ops = true,
               tair::common::data_entry *new_data = NULL);

    int add(int bucket_number, tair::common::data_entry &key, tair::common::data_entry &value, bool version_care,
            int expire_time);

    int incr_decr(int bucket, tair::common::data_entry &key, uint64_t delta,
                  uint64_t init, bool is_incr, int expire, uint64_t &result, tair::common::data_entry *new_data = NULL);

    int get_range(int bucket_number, tair::common::data_entry &key_start, tair::common::data_entry &end_key, int offset,
                  int limit, int type, std::vector<tair::common::data_entry *> &result, bool &has_next);

    int op_cmd(ServerCmdType cmd, std::vector<std::string> &params, std::vector<std::string> &infos);

    int op_cmd_to_db(int32_t cmd, const std::vector<std::string> &params);

    bool begin_scan(int bucket_number);

    bool end_scan();

    bool get_next_items(std::vector<item_data_info *> &list);

    void get_stats(tair_stat *stat);

    int stat_db();

    int set_config(std::vector<std::string> &params);

    int get_config(std::vector<std::string> &infos);

    int clear_area(int32_t area);

    bool exist(int32_t bucket_number);

    // stat util
    void stat_add(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count);

    void stat_sub(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count);

    void get_buckets(std::vector<int32_t> &buckets);

    int32_t index() { return index_; }

    const char *db_path() { return db_path_; }

    // use inner leveldb/gc_factory when compact
    leveldb::DB *db() { return db_; }

    const leveldb::Comparator *comparator() { return options_.comparator; }

    LdbGcFactory *gc_factory() { return &gc_; }

    BgTask *bg_task() { return &bg_task_; }

    LdbStatManager *get_stat_manager() { return stat_; }

private:
    int do_cache_get(LdbKey &ldb_key, std::string &value, bool update_stat);

    int do_get(LdbKey &ldb_key, std::string &value, bool from_cache, bool fill_cache, bool update_stat = true,
               bool mc_ops = false);

    int do_put(LdbKey &ldb_key, LdbItem &ldb_item,
               bool fill_cache, bool synced, bool from_other_unit, bool mc_ops = false);

    int do_update(LdbKey &ldb_key, LdbItem &ldb_item, uint8_t ldb_update_type, bool fill_cache, bool synced);

    int do_remove(LdbKey &ldb_key, bool synced, bool from_other_unit, tair::common::entry_tailer *tailer = NULL);

    int do_put_and_set_meta(tair::common::data_entry &key, LdbKey &ldb_key, LdbItem &ldb_item, int bucket_number,
                            int data_size, int use_size, int item_count, int expire,
                            bool version_care, uint8_t ldb_update_type, tair::common::data_entry *value);

    bool is_mtime_care(const tair::common::data_entry &key);

    bool is_synced(const tair::common::data_entry &key);

    void add_prefix(LdbKey &ldb_key, int prefix_size);

    void fill_meta(tair::common::data_entry *data, LdbKey &key, LdbItem &item);

    uint64_t strntoul(const char *s, size_t n) {
        uint64_t ret = 0;
        for (size_t i = 0; i < n; ++i) {
            ret = ret * 10 + (s[i] - '0');
        }
        return ret;
    }

    bool init_db();

    void stop();

    void sanitize_option();

    tbsys::CThreadMutex *get_mutex(const tair::common::data_entry &key);

    int get_real_edate(int expire, int now, bool mc_ops);

    void append_specify_compact_status(std::vector<std::string> &infos);

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
    tbsys::CThreadMutex *mutex_;
    leveldb::DB *db_;
    // mdb cache
    mdb_manager *cache_;

    // for scan, MUST single-bucket everytime
    leveldb::Iterator *scan_it_;
    // the bucket migrating
    int scan_bucket_;
    bool still_have_;

    // statatics manager
    STAT_MANAGER_MAP *stat_manager_;
    // area stat_manager_;
    stat_manager all_area_stat_manager_;
    // stat
    LdbStatManager *stat_;
    // background task(compact)
    BgTask bg_task_;
    // gc cleared area and closed buckets
    LdbGcFactory gc_;
};
}
}
}
#endif
