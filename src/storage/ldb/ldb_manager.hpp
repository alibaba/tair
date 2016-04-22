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

#ifndef TAIR_STORAGE_LDB_MANAGER_H
#define TAIR_STORAGE_LDB_MANAGER_H

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)

#include <unordered_map>

#else
#include <tr1/unordered_map>
namespace std {
  using tr1::hash;
  using tr1::unordered_map;
}
#endif

#include <set>

#include "storage/storage_manager.hpp"
#include "common/data_entry.hpp"
#include "common/stat_info.hpp"
#include "common/stat_define.hpp"
#include "ldb_cache_stat.hpp"
#include "ldb_remote_sync_logger.hpp"
#include "local_binlog_queue.hpp"

namespace tair {
class operation_record;

class mdb_manager;

namespace common {
class RecordLogger;
}

namespace storage {
namespace ldb {
class LdbInstance;

class LdbBalancer;

////////////////////////////////
// UsingLdbManager
////////////////////////////////
static const int32_t DESTROY_LDB_MANGER_INTERVAL_S = 30; // 30s
static const int32_t FLUSH_LDB_MEM_INTERVAL_S = 60;      // 60s
class UsingLdbManager {
public:
    UsingLdbManager();

    ~UsingLdbManager();

    bool can_destroy();

    void destroy();

public:
    LdbInstance **ldb_instance_;
    int32_t db_count_;
    mdb_manager **cache_;
    int32_t cache_count_;
    std::string base_cache_path_;
    UsingLdbManager *next_;
    uint32_t time_;
};

// Bucket partition indexer
class BucketIndexer {
public:
    BucketIndexer() {};

    virtual ~BucketIndexer() {};


    // bucket => instance index
    typedef std::unordered_map<int32_t, int32_t> BUCKET_INDEX_MAP;
    // instance index => buckets
    typedef std::vector<std::vector<int32_t> > INDEX_BUCKET_MAP;

    virtual int sharding_bucket(int32_t total, const std::vector<int32_t> &buckets,
                                INDEX_BUCKET_MAP &index_map, bool &updated, bool close = false) = 0;

    virtual int32_t bucket_to_index(int32_t bucket_number, bool &recheck) = 0;

    virtual int reindex(int32_t bucket, int32_t from, int32_t to) = 0;

    virtual void get_index_map(INDEX_BUCKET_MAP &result) = 0;

    virtual void set_index_update_flag(bool can_update) = 0;

    virtual bool get_index_update_flag() = 0;

    virtual void set_final_balance(bool flag) { UNUSED(flag); }

    static void get_index_map(const BUCKET_INDEX_MAP &bucket_map, int32_t index_count, INDEX_BUCKET_MAP &index_map);

    static std::string to_string(const INDEX_BUCKET_MAP &index_map);

    static std::string to_string(int32_t index_count, const BUCKET_INDEX_MAP &bucket_map);

    static BucketIndexer *new_bucket_indexer(const char *strategy);
};

// bucket hash partition indexer
class HashBucketIndexer : public BucketIndexer {
public:
    HashBucketIndexer();

    virtual ~HashBucketIndexer();

    virtual int sharding_bucket(int32_t total, const std::vector<int> &buckets,
                                INDEX_BUCKET_MAP &index_map, bool &updated, bool close = false);

    virtual int32_t bucket_to_index(int32_t bucket_number, bool &recheck);

    virtual int reindex(int32_t bucket, int32_t from, int32_t to);

    virtual void get_index_map(INDEX_BUCKET_MAP &result);

    virtual void set_index_update_flag(bool can_update) {}

    virtual bool get_index_update_flag() { return false; }

private:
    int hash(int32_t key);

private:
    int32_t total_;
};

// bucket common map partition indexer
static const char BUCKET_INDEX_DELIM = ':';

class MapBucketIndexer : public BucketIndexer {
public:
    MapBucketIndexer();

    virtual ~MapBucketIndexer();

    virtual int sharding_bucket(int32_t total, const std::vector<int32_t> &buckets,
                                INDEX_BUCKET_MAP &index_map, bool &updated, bool close = false);

    virtual int32_t bucket_to_index(int32_t bucket_number, bool &recheck);

    virtual int reindex(int32_t bucket, int32_t from, int32_t to);

    virtual void get_index_map(INDEX_BUCKET_MAP &result);

    virtual void set_index_update_flag(bool can_update) { can_update_ = can_update; }

    virtual bool get_index_update_flag() { return can_update_; }

    virtual void set_final_balance(bool flag) { final_balance_ = flag; }

private:
    int close_sharding_bucket(int32_t total, const std::vector<int32_t> &buckets,
                              INDEX_BUCKET_MAP &index_map, bool &updated);

    int do_sharding_bucket(int32_t total, const std::vector<int32_t> &buckets,
                           INDEX_BUCKET_MAP &index_map, bool &updated);

    int update_bucket_index(int32_t total, BUCKET_INDEX_MAP *new_bucket_map);

    int save_bucket_index(int32_t total, BUCKET_INDEX_MAP &bucket_index_map);

    int load_bucket_index();

private:
    char bucket_index_file_path_[TAIR_MAX_PATH_LEN];
    BUCKET_INDEX_MAP *bucket_map_;
    int32_t total_;
    bool can_update_;
    bool final_balance_;
};


// manager hold buckets. single or multi leveldb instance based on config for test
// it works, maybe make it common manager level.
class LdbManager : public tair::storage::storage_manager {
    // ReviseStat can only used by LdbManager
private:
    class ReviseStat : public tbsys::CDefaultRunnable {
    public:
        ReviseStat(LdbManager *owner, bool is_revise_user_stat, int64_t sleep_interval, int32_t sleep_time,
                   std::set<int32_t> &instance_to_revise);

        ~ReviseStat();

        void run(tbsys::CThread *thread, void *arg);

    private:
        LdbManager *owner_;
        bool is_revise_user_stat_;
        int64_t sleep_interval_;
        int32_t sleep_time_;
        std::set<int32_t> instance_to_revise_;
    };

    friend class LdbRemoteSyncLogger;

    friend class LocalBinlogQueue;

    friend class ReviseStat;

public:
    LdbManager();

    ~LdbManager();

    int initialize();

    int put(int bucket_number, data_entry &key, data_entry &value,
            bool version_care, int expire_time);

    int direct_mupdate(int bucket_number, const std::vector<operation_record *> &kvs);

    int batch_put(int bucket_number, int area, tair::common::mput_record_vec *record_vec, bool version_care);

    int get(int bucket_number, data_entry &key, data_entry &value, bool stat);

    int remove(int bucket_number, data_entry &key, bool version_care);

    int clear(int area);

    int get_range(int bucket_number, data_entry &key_start, data_entry &end_key, int offset, int limit, int type,
                  std::vector<data_entry *> &result, bool &has_next);

    bool init_buckets(const std::vector<int> &buckets);

    void close_buckets(const std::vector<int> &buckets);

    void begin_scan(md_info &info);

    void end_scan(md_info &info);

    bool get_next_items(md_info &info, std::vector<item_data_info *> &list);

    void set_area_quota(int area, uint64_t quota);

    void set_area_quota(std::map<int, uint64_t> &quota_map);

    bool should_flush_data() { return true; }

    bool need_health_check() { return true; }

    int op_cmd(ServerCmdType cmd, std::vector<std::string> &params, std::vector<std::string> &infos);

    void get_stats(tair_stat *stat);

    void get_stats(char *&buf, int32_t &size, bool &alloc);

    const tair::common::StatSchema *get_stat_schema();

    void set_opstat(tair::common::OpStat *global_op_stat);

    void set_bucket_count(uint32_t bucket_count);

    //mc_ops
    int mc_set(int bucket_num, data_entry &key, data_entry &value, bool version_care, int expire);

    int add(int bucket_num, data_entry &key, data_entry &value, bool version_care, int expire);

    int replace(int bucket_num, data_entry &key, data_entry &value, bool version_care, int expire);

    int append(int bucket_num, data_entry &key, data_entry &value, bool version_care, data_entry *new_value = NULL);

    int prepend(int bucket_num, data_entry &key, data_entry &value, bool version_care, data_entry *new_value = NULL);

    int
    incr_decr(int bucket, data_entry &key, uint64_t delta, uint64_t init, bool is_incr, int expire, uint64_t &result,
              data_entry *new_value = NULL);

    int touch(int bucket_num, data_entry &key, int expire);

    // get bucket index map
    void get_index_map(BucketIndexer::INDEX_BUCKET_MAP &result);

    // reindex bucket from `from instance to `to instance
    int reindex_bucket(int32_t bucket, int32_t from, int32_t to);

    // pause service(mainly write) for the bucket
    void pause_service(int32_t bucket);

    void resume_service(int32_t bucket);

    LdbInstance *get_instance(int32_t index);

private:
    // revise stat
    int start_revise_stat(const std::vector<std::string> &params, std::vector<std::string> &infos);

    int stop_revise_stat(std::vector<std::string> &infos);

    int get_revise_status(std::vector<std::string> &infos);

private:
    int init_cache(mdb_manager **&new_cache, int32_t count);

    int destroy();

    int do_reset_db();

    int do_set_migrate_wait(int32_t cmd_wait_ms);

    int do_release_mem();

    void maybe_exec_cmd();

    int do_switch_db(LdbInstance **new_ldb_instance, mdb_manager **new_cache, std::string &base_cache_back_path);

    int do_reset_db_instance(LdbInstance **&new_ldb_instance, mdb_manager **new_cache);

    int do_reset_cache(mdb_manager **&new_cache, std::string &base_back_path);

    RecordLogger *get_remote_sync_logger(int version = 0);

    int get_instance_count();

    LdbInstance *get_db_instance(int bucket_number, bool write = true);

private:
    LdbInstance **ldb_instance_;
    int32_t db_count_;
    BucketIndexer *bucket_indexer_;

    // out-of-servicing bucket(maybe paused for a while)
    // only support one now.
    // TODO: maybe hashmap to support multiple ones
    int32_t frozen_bucket_;

    bool use_bloomfilter_;
    mdb_manager **cache_;
    int32_t cache_count_;
    // cache stat
    CacheStat cache_stat_;
    tbsys::CThreadMutex lock_;

    //for migrate
    std::vector<LdbInstance *> scan_ldb_set_; //scan one bucket need scan a list of LdbInstances
    std::vector<LdbInstance *>::const_iterator scan_it_;
    LdbInstance *scan_ldb_;
    uint32_t migrate_wait_us_;
    UsingLdbManager *using_head_;
    UsingLdbManager *using_tail_;
    //for froce release tcmalloc mem
    uint32_t last_release_time_;

    //for remote sync
    tair::common::RecordLogger *remote_sync_logger_;
    typedef std::unordered_map<std::string, tair::common::RecordLogger *> CLUSTER_REMOTE_SYNC_LOGGER_MAP;
    CLUSTER_REMOTE_SYNC_LOGGER_MAP cluster_remote_sync_loggers_;
    // balancer
    LdbBalancer *balancer_;

    // for sampling stat
    // we will serialize stat with area-level,
    // so need sampling all bucket-level first.
    // use MemStatStore here for total summary
    // area => stat
    tair::common::StatManager<tair::common::StatUnit<tair::common::StatStore>,
            tair::common::StatStore> *sampling_stat_;
    // all schema, if with cache, schema will include cache's schema
    tair::common::StatSchema schema_;

    //reivise stat
    // this lock only used in those revise function
    tbsys::CRWLock revise_stat_lock_;
    ReviseStat *revise_stat_;
    std::vector<time_t> user_last_revise_time_;
    std::vector<int32_t> user_already_revised_times_;
    std::vector<time_t> physical_last_revise_time_;
    std::vector<int32_t> physical_already_revised_times_;
};
}
}
}

#endif
