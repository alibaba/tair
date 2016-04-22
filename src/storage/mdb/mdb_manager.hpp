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

#ifndef __MDB_MANAGER_H
#define __MDB_MANAGER_H

#include "mem_pool.hpp"
#include "mem_cache.hpp"
#include "cache_hashmap.hpp"
#include "mdb_instance.hpp"

#include "define.hpp"
#include "stat_define.hpp"
#include "storage_manager.hpp"
#include "mdb_stat.hpp"
#include "stat_info.hpp"
#include "stat_helper.hpp"

namespace tair {

using namespace tair::storage;
using namespace tair::common;
namespace common {
class data_entry;
}

class mdb_manager : public storage::storage_manager, public tbsys::Runnable {
public:
    mdb_manager() {
        stopped = false;
        scan_index = 0;
        sampling_stat_ = NULL;
    }

    virtual ~ mdb_manager();

    bool initialize(bool use_share_mem = true);

    int put(int bucket_num, data_entry &key, data_entry &value,
            bool version_care, int expire_time);

    int get(int bucket_num, data_entry &key, data_entry &value, bool with_stat = true);

    int remove(int bucket_num, data_entry &key, bool version_care);

    int add_count(int bucket_num, data_entry &key, int count, int init_value,
                  int low_bound, int upper_bound, int expire_time, int &result_value);

    bool lookup(int bucket_num, data_entry &key);

    int mc_set(int bucket_num, data_entry &key, data_entry &value,
               bool version_care, int expire);

    int add(int bucket_num, data_entry &key, data_entry &value,
            bool version_care, int expire);

    int replace(int bucket_num, data_entry &key, data_entry &value,
                bool version_care, int expire);

    int append(int bucket_num, data_entry &key, data_entry &value,
               bool version_care, data_entry *new_value = NULL);

    int prepend(int bucket_num, data_entry &key, data_entry &value,
                bool version_care, data_entry *new_value = NULL);

    int incr_decr(int bucket, data_entry &key, uint64_t delta, uint64_t init,
                  bool is_incr, int expire, uint64_t &result, data_entry *new_value = NULL);

    // raw put/get/remove for embedded cache use
    // Not consider any meta, just key ==> value, cause operating those stuff is up to cache-user.
public:
    int raw_put(const char *key, int32_t key_len, const char *value,
                int32_t value_len, int flag, uint32_t expired, int32_t prefix_len, bool is_mc = false);

    int raw_get(const char *key, int32_t key_len, std::string &value, bool update);

    int raw_remove(const char *key, int32_t key_len);

    void raw_get_stats(mdb_area_stat *stat);

    void sampling_for_upper_storage(StatManager <StatUnit<StatStore>, StatStore> &upper_storage_sampling_stat,
                                    int32_t offset);

    void raw_update_stats(mdb_area_stat *stat);

public:
    int clear(int area);

    void begin_scan(md_info &info);

    bool get_next_items(md_info &info, std::vector<item_data_info *> &list);

    void end_scan(md_info &info) {
    }

    bool init_buckets(const std::vector<int> &buckets);

    void close_buckets(const std::vector<int> &buckets);

    void get_stats(tair_stat *stat);

    void get_stat(int area, mdb_area_stat *_stat);

    void get_stats(char *&buf, int32_t &size, bool &alloc);

    const StatSchema *get_stat_schema();

    void set_opstat(OpStat *stat);

    void add_self_op_stat_to_global(OpStat *global_op_stat);

    int get_meta(data_entry &key, item_meta_info &meta);

    int touch(int bucket_num, data_entry &key, int expire);

    void set_area_quota(int area, uint64_t quota);

    void set_area_quota(std::map<int, uint64_t> &quota_map);

    uint64_t get_area_quota(int area);

    int get_area_quota(std::map<int, uint64_t> &quota_map);

    bool is_quota_exceed(int area);

    void reload_config();

    int op_cmd(ServerCmdType cmd, std::vector<std::string> &params, std::vector<std::string> &infos);

    virtual void set_bucket_count(uint32_t bucket_count) {
        storage_manager::set_bucket_count(bucket_count);
        for (size_t i = 0; i < instance_list.size(); ++i) {
            instance_list[i]->set_bucket_count(bucket_count);
        }
    }

#ifdef TAIR_DEBUG
    std::map<int, int > get_slab_size();
    std::vector<int> get_area_size();
    std::vector< int> get_areas();
#endif

private:
    tbsys::CThread maintenance_thread;

    void run(tbsys::CThread *thread, void *arg);

    bool stopped;

    unsigned int hash(data_entry &key);

    unsigned int hash(const char *key, int len);

    mdb_instance *get_mdb_instance(unsigned int hv) {
        static unsigned int mask = (1 << mdb_param::inst_shift) - 1;
        return instance_list[hv & mask];
    }

    mdb_area_stat *init_area_stat(const char *path, bool use_shm);

private:
    std::vector<mdb_instance *> instance_list;
    size_t scan_index;
    mdb_area_stat *area_stat;
    bool use_share_mem;

    StatManager <StatUnit<StatStore>, StatStore> *sampling_stat_;
    StatSchema schema_;
    // protect smapling_stat_
    tbsys::CThreadMutex lock_samping_stat_;
};
}                                /* tair */
#endif
