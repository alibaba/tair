/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#ifndef __MDB_MANAGER_H
#define __MDB_MANAGER_H

#include "mem_pool.hpp"
#include "mem_cache.hpp"
#include "cache_hashmap.hpp"

#include "define.hpp"
#include "storage_manager.hpp"
#include "mdb_stat.hpp"
#include "data_entry.hpp"
#include "stat_info.hpp"
#include "stat_helper.hpp"

#include <boost/thread/mutex.hpp>
namespace tair {

//#define AREA(x) ((x) > TAIR_MAX_AREA_COUNT ? (TAIR_MAX_AREA_COUNT) : (x) )
  using namespace tair::storage;
  class mdb_manager:public storage::storage_manager, public tbsys::Runnable
  {
  public:
    mdb_manager():this_mem_pool(0), cache(0), hashmap(0),
      last_traversal_time(0), last_balance_time(0), last_expd_time(0), stopped(false)
    {
    }
    virtual ~ mdb_manager();

    bool initialize(bool use_share_mem = true);
    //void stop() { stop_flag = true; }

    int put(int bucket_num, data_entry & key, data_entry & value,
            bool version_care, int expire_time);

    int get(int bucket_num, data_entry & key, data_entry & value);

    int remove(int bucket_num, data_entry & key, bool version_care);
    int add_count(int bucket_num,data_entry &key, int count, int init_value,
            bool allow_negative,int expire_time,int &result_value);
    bool lookup(int bucket_num, data_entry &key);

    // raw put/get/remove for embedded cache use
    // Not consider any meta, just key ==> value, cause operating those stuff is up to cache-user.
  public:
    int raw_put(const char* key, int32_t key_len, const char* value, int32_t value_len, int flag, uint32_t expired);
    int raw_get(const char* key, int32_t key_len, std::string& value, bool update);
    int raw_remove(const char* key, int32_t key_len);
    void raw_get_stats(mdb_area_stat* stat);

  private:
    bool raw_remove_if_exists(const char* key, int32_t key_len);
    bool raw_remove_if_expired(const char* key, int32_t key_len, mdb_item*& item);

  public:
    int clear(int area);

    void begin_scan(md_info & info);
    bool get_next_items(md_info & info, std::vector<item_data_info *>&list);
    void end_scan(md_info & info)
    {
    }

    bool init_buckets(const std::vector<int> &buckets);

    void close_buckets(const std::vector<int> &buckets);

    void get_stats(tair_stat * stat);

    void get_stat(int area, mdb_area_stat *_stat);

    int get_meta(data_entry &key, item_meta_info &meta);

    void set_area_quota(int area, uint64_t quota);
    void set_area_quota(std::map<int, uint64_t> &quota_map);

    uint64_t get_area_quota(int area);
    int get_area_quota(std::map<int, uint64_t> &quota_map);

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
  public:
    void run(tbsys::CThread * thread, void *arg);
    void __remove(mdb_item * it);

#ifdef TAIR_DEBUG
    std::map<int, int > get_slab_size();
    std::vector<int> get_area_size();
    std::vector< int> get_areas();
#endif

  private:

    //do_put,do_get,do_remove should lock first.
    int do_put(data_entry & key, data_entry & data, bool version_care,
               int expired);
    int do_get(data_entry & key, data_entry & data);
    int do_remove(data_entry & key, bool version_care);
    int do_add_count(data_entry &key, int count, int init_value,
            bool not_negative,int expired ,int &result_value);
    bool do_lookup(data_entry &key);

    char *open_shared_mem(const char *path, int64_t size);
    bool remove_if_exists(data_entry & key);
    bool remove_if_expired(data_entry & key, mdb_item * &mdb_item);

    bool is_chkexprd_time();
    bool is_chkslab_time();

    inline bool is_item_expired(mdb_item* it, uint32_t current_time) {
      return (it->exptime != 0 && it->exptime < current_time)
             || (it->update_time < cache->get_area_timestamp(ITEM_AREA(it)));
    }

    void run_chkslab();
    void run_chkexprd_deleted();
    void balance_slab();
    void check_quota();
    void remove_deleted_item();
    void remove_exprd_item();
    mem_pool *this_mem_pool;
    mem_cache *cache;
    cache_hash_map *hashmap;
    boost::mutex mem_locker;

    //int m_hash_index; //is used to scan
    uint32_t last_traversal_time;        //record the last time of traversal
    uint32_t last_balance_time;
    uint32_t last_expd_time;

    tbsys::CThread chkexprd_thread;
    tbsys::CThread chkslab_thread;

    bool stopped;

    //area_stat m_stat;
    mdb_area_stat *area_stat[TAIR_MAX_AREA_COUNT];
    uint32_t* mdb_version;
  };
}                                /* tair */
#endif
