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

#ifndef __MDB_INSTANCE_H
#define __MDB_INSTANCE_H

#include "mem_pool.hpp"
#include "mem_cache.hpp"
#include "cache_hashmap.hpp"

#include "define.hpp"
#include "storage_manager.hpp"
#include "mdb_stat.hpp"
#include "stat_info.hpp"
#include "stat_helper.hpp"

namespace tair {

//#define AREA(x) ((x) > TAIR_MAX_AREA_COUNT ? (TAIR_MAX_AREA_COUNT) : (x) )
using namespace tair::storage;
using namespace tair::common;
namespace storage { namespace mdb { class MdbStatManager; }}
namespace common { class data_entry; }
class mdb_instance {
public:
    mdb_instance(int instance_id) : instance_id_(instance_id), this_mem_pool(0), cache(0), hashmap(0),
                                    last_traversal_time(0), last_balance_time(0), last_expd_time(0),
                                    last_check_quota_time(0),
                                    force_run_remove_exprd_item_(false), pool(NULL), use_share_mem(false), stat_(NULL) {
    }

    virtual ~ mdb_instance();

    bool initialize(mdb_area_stat *stat, int32_t bucket_count, bool use_share_mem = true);
    //void stop() { stop_flag = true; }

    int put(int bucket_num, data_entry &key, unsigned int hv, data_entry &value,
            bool version_care, int expire_time);

    int get(int bucket_num, data_entry &key, unsigned int hv, data_entry &value, bool with_stat = true);

    int remove(int bucket_num, data_entry &key, unsigned int hv, bool version_care);

    int add_count(int bucket_num, data_entry &key, unsigned int hv, int count, int init_value,
                  int low_bound, int upper_bound, int expire_time, int &result_value);

    bool lookup(int bucket_num, data_entry &key, unsigned int hv);

    int mc_set(int bucket_num, data_entry &key, unsigned int hv, data_entry &value,
               bool version_care, int expire);

    int add(int bucket_num, data_entry &key, unsigned int hv, data_entry &value,
            bool version_care, int expire);

    int replace(int bucket_num, data_entry &key, unsigned int hv, data_entry &value,
                bool version_care, int expire);

    int append(int bucket_num, data_entry &key, unsigned int hv, data_entry &value,
               bool version_care, data_entry *new_value = NULL);

    int prepend(int bucket_num, data_entry &key, unsigned int hv, data_entry &value,
                bool version_care, data_entry *new_value = NULL);

    int incr_decr(int bucket, data_entry &key, unsigned int hv, uint64_t delta, uint64_t init,
                  bool is_incr, int expire, uint64_t &result, data_entry *new_value = NULL);

    // raw put/get/remove for embedded cache use
    // Not consider any meta, just key ==> value, cause operating those stuff is up to cache-user.
public:
    int raw_put(const char *key, int32_t key_len, unsigned int hv,
                const char *value, int32_t value_len, int flag, uint32_t expired, int32_t prefix_len,
                bool is_mc = false);

    int raw_get(const char *key, int32_t key_len, unsigned int hv, std::string &value, bool update);

    int raw_remove(const char *key, int32_t key_len, unsigned int hv);

    void run_checking();

    void run_mem_merge(volatile bool &stopped);

    int instance_id(void) {
        return instance_id_;
    }

    cache_hash_map *get_cache_hashmap() {
        return hashmap;
    }

private:
    bool raw_remove_if_exists(const char *key, int32_t key_len, unsigned int hv);

    bool raw_remove_if_expired(const char *key, int32_t key_len, mdb_item *&item, unsigned int hv);

    bool test_flag(uint8_t meta, int flag) {
        return (meta & flag) != 0;
    }

    void set_flag(uint8_t &meta, int flag) {
        meta |= flag;
    }

    void clear_flag(uint8_t &meta, int flag) {
        meta &= ~flag;
    }

public:
    mdb::MdbStatManager *get_stat_manager() {
        return stat_;
    }

    int clear(int area);

    void begin_scan(md_info &info);

    bool get_next_items(md_info &info, std::vector<item_data_info *> &list, uint32_t bucket_count);

    void end_scan(md_info &info) {
    }

    bool init_buckets(const std::vector<int> &buckets);

    void close_buckets(const std::vector<int> &buckets, uint32_t bucket_count);

    int get_meta(data_entry &key, unsigned int hv, item_meta_info &meta);

    int touch(int bucket_num, data_entry &key, unsigned int hv, int expire);

    void set_area_quota(int area, uint64_t quota);

    void set_area_quota(std::map<int, uint64_t> &quota_map);

    uint64_t get_area_quota(int area);

    int get_area_quota(std::map<int, uint64_t> &quota_map);

    void set_last_expd_time(int time_val) { last_expd_time = time_val; }

    // mdb manager reserve only 4 bits for flag, and
    // may use trick here when other flag is added.
    // Current flag:
    // TAIR_ITEM_FLAG_ADDCOUNT = 1,
    // TAIR_ITEM_FLAG_DELETED = 2,
    // TAIR_ITEM_FLAG_ITEM = 4,
    // TAIR_ITEM_FLAG_LOCKED=8
    // ADDCOUNT/ITEM/DELETED are mutexed flag, can use equal operation
    // to check, LOCKED can exist with other flag, so can check with bit
    // operation.

    bool is_quota_exceed(int area);

    void keep_area_quota(bool &stopped, const std::map<int, uint64_t> &exceeded_area_map);

    void set_bucket_count(uint32_t bucket_count);

public:
    void __remove(mdb_item *it);

    void __remove(mdb_item *it, unsigned int hv);

#ifdef TAIR_DEBUG
    std::map<int, int > get_slab_size();
    std::vector<int> get_area_size();
    std::vector< int> get_areas();
#endif

private:
    //do_put,do_get,do_remove should lock first.
    int do_put(int bucket_num, data_entry &key, unsigned int hv, data_entry &data, bool version_care,
               int expired, bool is_mc = false);

    int do_get(int bucket_num, data_entry &key, unsigned int hv, data_entry &data, bool with_stat = true);

    int do_remove(int bucket_num, data_entry &key, unsigned int hv, bool version_care);

    int do_add_count(int bucket_num, data_entry &key, unsigned int hv, int count, int init_value,
                     int low_bound, int upper_bound, int expired, int &result_value);

    bool do_lookup(int bucket_num, data_entry &key, unsigned int hv);

    int do_set(int bucket_num, data_entry &key, unsigned int hv, data_entry &value, bool version_care, int expire);

    int do_add(int bucket_num, data_entry &key, unsigned int hv, data_entry &value, bool version_care, int expire);

    int do_replace(int bucket_num, data_entry &key, unsigned int hv, data_entry &value, bool version_care, int expire);

    int do_pending(int bucket_num, data_entry &key, unsigned int hv, data_entry &value, bool version_care, bool pre,
                   data_entry *new_value);

    int do_incr_decr(int bucket, data_entry &key, unsigned int hv, uint64_t delta, uint64_t init,
                     bool is_incr, int expire, uint64_t &result, data_entry *new_value);

    mdb_item *alloc_counter_item(data_entry &key);

    bool remove_if_exists(data_entry &key, unsigned int hv);

    bool remove_if_expired(int bucket_num, data_entry &key, mdb_item *&mdb_item, unsigned int hv);

    bool remove_version_care(data_entry &key, unsigned int hv);

    bool is_chkexprd_time();

    bool is_chkslab_time();

    bool is_mem_merge_time();

    inline bool is_item_expired_by_area_clear(mdb_item *it) {
        return it->update_time < cache->get_area_timestamp(ITEM_AREA(it));
    }

    inline bool is_item_expired(mdb_item *it, uint32_t current_time) {
        return (it->exptime != 0 && it->exptime < current_time)
               || (it->update_time < cache->get_area_timestamp(ITEM_AREA(it)));
    }

    inline bool is_item_expired(mdb_item *it, uint32_t current_time, int bucket_num) {
        return (it->exptime != 0 && it->exptime < current_time)
               || (it->update_time < cache->get_area_timestamp(ITEM_AREA(it)))
               || (it->update_time < cache->get_bucket_timestamp(bucket_num));
    }

    int bucket_of_key(const char *key, size_t len, size_t prefix_size) {
        key += 2; //~ strip out area
        len -= 2;
        len = prefix_size == 0 ? len : prefix_size;
        return util::string_util::mur_mur_hash(key, len) % *this->bucket_count;
    }

    int bucket_of_key(mdb_item *item) {
        return bucket_of_key(ITEM_KEY(item), item->key_len, item->prefix_size);
    }

    void run_chkslab();

    void run_chkexprd_deleted();

    void balance_slab();

    void remove_deleted_item();

    void remove_exprd_item();

    void init_lock(bool pshared);

    // for logging
    int instance_id_;

    mem_pool *this_mem_pool;
    mem_cache *cache;
    cache_hash_map *hashmap;

    int *mdb_lock_inited;
    pthread_mutex_t *mdb_lock;
    //int m_hash_index; //is used to scan
    uint32_t last_traversal_time;        //record the last time of traversal
    uint32_t last_balance_time;
    uint32_t last_expd_time;
    uint32_t last_check_quota_time;
    volatile bool force_run_remove_exprd_item_;

    //area_stat m_stat;
    mdb_area_stat *area_stat;
    uint32_t *mdb_version;
    uint32_t *bucket_count;
    char instance_name[256];
    char *pool;
    bool use_share_mem;

    //new stat
    mdb::MdbStatManager *stat_;
};
}                                /* tair */
#endif
