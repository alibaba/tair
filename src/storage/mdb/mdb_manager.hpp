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
      last_traversal_time(0), last_balance_time(0), stopped(false)
    {
    }
    virtual ~ mdb_manager();

    bool initialize(bool use_share_mem = true);
    //void stop() { stop_flag = true; }

    int put(int bucket_num, data_entry & key, data_entry & value,
            bool version_care, int expire_time);

    int get(int bucket_num, data_entry & key, data_entry & value);

    int remove(int bucket_num, data_entry & key, bool version_care);

    int clear(int area);

    void begin_scan(md_info & info);
    bool get_next_items(md_info & info, std::vector<item_data_info *>&list);
    void end_scan(md_info & info)
    {
    }

    bool init_buckets(const std::vector<int> &buckets);

    void close_buckets(const std::vector<int> &buckets);

    void get_stats(tair_stat * stat);

    void set_area_quota(int area, uint64_t quota);
    void set_area_quota(std::map<int, uint64_t> &quota_map);

    uint64_t get_area_quota(int area);
    int get_area_quota(std::map<int, uint64_t> &quota_map);

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

    int do_put(data_entry & key, data_entry & data, bool version_care,
               int expired);
    int do_get(data_entry & key, data_entry & data);
    int do_remove(data_entry & key, bool version_care);

    char *open_shared_mem(const char *path, int64_t size);
    bool remove_if_exists(data_entry & key);
    bool remove_if_expired(data_entry & key, mdb_item * &mdb_item);

    bool is_chkexprd_time();
    bool is_chkslab_time();
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

    tbsys::CThread chkexprd_thread;
    tbsys::CThread chkslab_thread;

    bool stopped;

    //area_stat m_stat;
    mdb_area_stat *area_stat[TAIR_MAX_AREA_COUNT];
  };
}                                /* tair */
#endif
