/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <iostream>
#include <list>
#include <boost/cast.hpp>

#include "mem_pool.hpp"
#include "mem_cache.hpp"
#include "cache_hashmap.hpp"
#include "mdb_manager.hpp"
#include "mdb_define.hpp"
#include "util.hpp"

using namespace tair;
using namespace std;
using namespace boost;


namespace tair {

  bool mdb_manager::initialize(bool use_share_mem /*=true*/ )
  {

    char *pool = 0;
    if(use_share_mem)
    {
      pool = open_shared_mem(mdb_param::mdb_path, mdb_param::size);
    }
    else
    {
      pool = static_cast<char *>(malloc(mdb_param::size));
      assert(pool != 0);
      //attention: if size is too large,memset will be a very time-consuming operation.
      memset(pool, 0, mdb_param::size);

    }
    if(pool == 0) {
      return false;
    }

    int meta_len = (1 << 20) + mem_cache::MEMCACHE_META_LEN;        //cachehashmap will alloc space by itself

    this_mem_pool =
      new mem_pool(pool, mdb_param::page_size,
                   mdb_param::size / mdb_param::page_size, meta_len);
    assert(this_mem_pool != 0);

    hashmap = new cache_hash_map(this_mem_pool, mdb_param::hash_shift);
    assert(hashmap != 0);

    cache =
      new mem_cache(this_mem_pool, this, TAIR_SLAB_LARGEST,
                    sizeof(mdb_item) + 16, mdb_param::factor);
    assert(cache != 0);

    for(int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
      area_stat[i] =
        reinterpret_cast<mdb_area_stat * >(this_mem_pool->get_pool_addr() +
                          mem_pool::MDB_STATINFO_START +
                          i * sizeof(mdb_area_stat));
    }
    chkexprd_thread.start(this, NULL);
    chkslab_thread.start(this, NULL);
    return true;
  }

  mdb_manager::~mdb_manager() {
    stopped = true;
    chkexprd_thread.join();
    chkslab_thread.join();
    delete hashmap;
    delete cache;
    delete this_mem_pool;
  }

  int mdb_manager::put(int bucket_num, data_entry & key, data_entry & value,
                       bool version_care, int expire_time)
  {
    return do_put(key, value, version_care, expire_time);
  }

  int mdb_manager::get(int bucket_num, data_entry & key, data_entry & value)
  {
    return do_get(key, value);
  }

  int mdb_manager::remove(int bucket_num, data_entry & key, bool version_care)
  {
    return do_remove(key, version_care);
  }


  void mdb_manager::set_area_quota(int area, uint64_t quota)
  {
    assert(area >= 0 && area < TAIR_MAX_AREA_COUNT);
    area_stat[area]->quota = quota;
  }

  void mdb_manager::set_area_quota(map<int, uint64_t> &quota_map)
  {
    for(map<int, uint64_t>::iterator it = quota_map.begin();
        it != quota_map.end(); ++it) {
      assert(it->first < TAIR_MAX_AREA_COUNT);
      area_stat[it->first]->quota = it->second;
    }
  }
  uint64_t mdb_manager::get_area_quota(int area)
  {
    assert(area >= 0 && area < TAIR_MAX_AREA_COUNT);
    return area_stat[area]->quota;
  }
  int mdb_manager::get_area_quota(std::map<int, uint64_t> &quota_map)
  {
    int i;
    for(i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
      quota_map.insert(make_pair(i, area_stat[i]->quota));
    }
    return i;
  }

  bool mdb_manager::is_quota_exceed(int area)
  {
    assert(area >= 0 && area < TAIR_MAX_AREA_COUNT);
    return area_stat[area]->quota <= area_stat[area]->data_size;
  }

  void mdb_manager::run(tbsys::CThread * thread, void *arg)
  {
    if(thread == &chkexprd_thread) {
      run_chkexprd_deleted();
    }
    else if(thread == &chkslab_thread) {
      run_chkslab();
    }
  }

  int mdb_manager::clear(int area)
  {
    TBSYS_LOG(INFO, "start clear : %d",area);
    assert(area >= 0 && area < TAIR_MAX_AREA_COUNT);
    for(int i = 0; i < cache->get_slabs_count(); ++i) {
      {
        boost::mutex::scoped_lock guard(mem_locker);
        uint64_t item_head = cache->get_item_head(i, area);
        while(item_head != 0) {
          mdb_item *item = id_to_item(item_head);
          if(item == 0) {
            //should not be here
            break;
          }
          item_head = item->next;
          //if(ITEM_AREA(item) != area){
          //       continue;
          //}
          __remove(item);
        }
      }
      usleep(100);
    }
    TBSYS_LOG(INFO, "end clear");
    return 0;
  }
  void mdb_manager::begin_scan(md_info & info)
  {
    info.hash_index = 0;
  }
#if 1
  bool mdb_manager::get_next_items(md_info & info,
                                   vector<item_data_info *> &list)
  {
    bool ret = true;

    boost::mutex::scoped_lock guard(mem_locker);
    uint64_t item_head = 0;
    while(true) {
      if(info.hash_index < 0
         || info.hash_index >= static_cast<uint32_t >(hashmap->get_bucket_size())) {
        return false;
      }
      uint64_t *hash_head = hashmap->get_hashmap() + info.hash_index;
      item_head = *hash_head;

      if(item_head != 0) {
        break;
      }
      ++info.hash_index;
    }

    uint32_t crrnt_time = static_cast<uint32_t> (time(NULL));

    while(item_head != 0) {
      mdb_item *it = id_to_item(item_head);

      //delete the mdb_item that have already migrated
      //if (ITEM_FLAGS(it->item_id) & TAIR_ITEM_FLAG_DELETED){
      //     item_head = it->h_next;
      //     __remove(it);
      //     continue;
      //}

      if(it->exptime != 0 && it->exptime <= crrnt_time) {
        item_head = it->h_next;
        __remove(it);
        continue;
      }

      uint32_t server_hash =
        util::string_util::mur_mur_hash(ITEM_KEY(it) + 2, it->key_len - 2);
      server_hash %= bucket_count;

      if(server_hash != info.db_id) {
        item_head = it->h_next;
        continue;
      }
      int size = it->key_len + it->data_len + sizeof(item_data_info);
      item_data_info *ait = (item_data_info *) malloc(size);        //meta + data
      memcpy(ait->m_data, it->data, it->key_len + it->data_len);        //copy data
      ait->header.keysize = it->key_len;
      ait->header.valsize = it->data_len;
      ait->header.version = it->version;
      ait->header.flag = ITEM_FLAGS(it->item_id);        //TODO
      ait->header.mdate = it->update_time;
      ait->header.edate = it->exptime;

      list.push_back(ait);
      item_head = it->h_next;

      //set deleted flag, we will remove it next turn.
      //if (remove)
      //     SET_ITEM_FLAGS(it->item_id,ITEM_FLAGS(it->item_id)|TAIR_ITEM_FLAG_DELETED);
    }
    if(++info.hash_index >= static_cast<uint32_t> (hashmap->get_bucket_size())) {
      ret = false;
      //if (remove)
      //     last_traversal_time = static_cast<uint32_t>(time(NULL));
    }
    return ret;
  }
#endif

  int mdb_manager::do_put(data_entry & key, data_entry & data,
                          bool version_care, int expired)
  {
    if(UNLIKELY(expired < 0))
      expired = 0;

    int total_size = key.get_size() + data.get_size() + sizeof(mdb_item);

    TBSYS_LOG(DEBUG, "start put: key:%u,area:%d,value:%u,flag:%d\n", key.get_size(),key.area,data.get_size(), data.data_meta.flag);
    if (key.area != KEY_AREA(key.get_data()))
    {
      TBSYS_LOG(INFO,"key.area[%d] != KEY_AREA(key.get_data())[%d]",key.area,KEY_AREA(key.get_data()));
      key.area =KEY_AREA( key.get_data());
    }


    uint32_t crrnt_time = static_cast<uint32_t> (time(NULL));
    boost::mutex::scoped_lock guard(mem_locker);
    mdb_item *it = hashmap->find(key.get_data(), key.get_size());

    uint16_t version = key.get_version();
    uint8_t old_flag = 0;
    if(it != 0) {                //exists
      if(IS_DELETED(it->item_id)) {        // in migrate
        old_flag = TAIR_ITEM_FLAG_DELETED;
      }
      if(it->exptime != 0 && it->exptime < crrnt_time) {        //expired
        __remove(it);
        version = 0;
        it = 0;
      }
      else if(version_care && version != 0
              && it->version != numeric_cast<uint32_t> (version)) {
        TBSYS_LOG(WARN, "it->version(%hu) != version(%hu)", it->version,
                  key.get_version());
        return TAIR_RETURN_VERSION_ERROR;

      }
      else {
        if(version_care) {
          version = it->version;
        }
        TBSYS_LOG(DEBUG, "%s:already exists,remove it", __FUNCTION__);
        __remove(it);
        it = 0;
      }
    }
    else {                        // doesn't exists
      version = 0;
    }
    int type = key.area;
    it = cache->alloc_item(total_size, type);        /* will be successful */
    assert(it != 0);
    if(type == ALLOC_EXPIRED || type == ALLOC_EVICT_SELF || type == ALLOC_EVICT_ANY) {        /* is evict */
      area_stat[ITEM_AREA(it)]->data_size -= (it->key_len + it->data_len);
      area_stat[ITEM_AREA(it)]->space_usage -= SLAB_SIZE(it->item_id);
      --(area_stat[ITEM_AREA(it)]->item_count);
      if(type == ALLOC_EVICT_ANY || type == ALLOC_EVICT_SELF) {
        ++(area_stat[ITEM_AREA(it)]->evict_count);
        TAIR_STAT.stat_evict(ITEM_AREA(it));
      }
      hashmap->remove(it);
    }
    /*write data into mdb_item */
    it->key_len = key.get_size();
    it->data_len = data.get_size();
    it->update_time = crrnt_time;
    it->version = version_care ? version + 1 : key.get_version();
    TBSYS_LOG(DEBUG, "actually put,version:%d,key.getVersion():%d\n",
              it->version, key.get_version());
    if(expired > 0) {
      it->exptime =
        static_cast<uint32_t> (expired) >
        crrnt_time ? expired : crrnt_time + expired;
    }

    SET_ITEM_FLAGS(it->item_id, data.data_meta.flag | old_flag);
    TBSYS_LOG(DEBUG, "ITEM_FLAGS(it->item_id):%u", ITEM_FLAGS(it->item_id));
    memcpy(ITEM_KEY(it), key.get_data(), it->key_len);
    memcpy(ITEM_DATA(it), data.get_data(), it->data_len);

    //set back
    key.set_version(it->version);
    key.data_meta.edate = it->exptime;
    key.data_meta.keysize = it->key_len;
    key.data_meta.valsize = it->data_len;
    key.data_meta.flag = ITEM_FLAGS(it->item_id);


    /*insert mdb_item into hashtable */
    hashmap->insert(it);

    /*update stat */
    area_stat[ITEM_AREA(it)]->data_size += (it->data_len + it->key_len);
    area_stat[ITEM_AREA(it)]->space_usage += SLAB_SIZE(it->item_id);
    ++(area_stat[ITEM_AREA(it)]->item_count);
    ++(area_stat[ITEM_AREA(it)]->put_count);
    return 0;
  }

  int mdb_manager::do_get(data_entry & key, data_entry & data)
  {

    TBSYS_LOG(DEBUG, "start get: area:%d,key size:%u", key.area,
              key.get_size());

    boost::mutex::scoped_lock guard(mem_locker);
    mdb_item *it = 0;
    int ret = TAIR_RETURN_DATA_NOT_EXIST;
    bool expired = false;
    if(!(expired = remove_if_expired(key, it)) && it != 0) {        //got it
      if (key.area != ITEM_AREA(it))
      {
        TBSYS_LOG(ERROR,"key.area != ITEM_AREA(it),why?");
        assert(0);
      }
      data.set_data(ITEM_DATA(it), it->data_len);
      data.set_version(it->version);
      data.data_meta.edate = it->exptime;
      key.set_version(it->version);
      key.data_meta.edate = it->exptime;
      key.data_meta.mdate = it->update_time;
      key.data_meta.flag = ITEM_FLAGS(it->item_id);

      key.data_meta.keysize = it->key_len;
      key.data_meta.valsize = it->data_len;
      data.data_meta.keysize = it->key_len;
      data.data_meta.valsize = it->data_len;
      key.data_meta.mdate = it->update_time;
      data.data_meta.flag = ITEM_FLAGS(it->item_id);

      cache->update_item(it);

      //++m_stat.hitCount;
      ++area_stat[key.area]->hit_count;
      ret = 0;
    }
    else if(expired) {
      ret = TAIR_RETURN_DATA_EXPIRED;
    }
    //++m_stat.getCount;
    ++area_stat[key.area]->get_count;
    return ret;
  }

  int mdb_manager::do_remove(data_entry & key, bool version_care)
  {
    TBSYS_LOG(DEBUG, "start remove: key size :%u", key.get_size());
    key.data_meta.keysize = key.get_size();        //FIXME:just set back
    boost::mutex::scoped_lock guard(mem_locker);
    bool ret = remove_if_exists(key);
    //++m_stat.removeCount;
    ++area_stat[key.area]->remove_count;
    return ret ? 0 : TAIR_RETURN_DATA_NOT_EXIST;
  }

  void mdb_manager::get_stats(tair_stat * stat)
  {
    if(stat == 0) {
      return;
    }
    for(int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
      stat[i].data_size_value = area_stat[i]->data_size;
      stat[i].use_size_value = area_stat[i]->space_usage;
      stat[i].item_count_value = area_stat[i]->item_count;
    }
  }

  bool mdb_manager::init_buckets(const vector<int> &buckets)
  {
    return true;
  }
  void mdb_manager::close_buckets(const vector<int> &buckets)
  {
    TBSYS_LOG(INFO,"start close_buckets");
    set<int>__buckets(buckets.begin(), buckets.end());

    for(int hash_index = 0; hash_index < hashmap->get_bucket_size();
        ++hash_index) {

      boost::mutex::scoped_lock guard(mem_locker);
      {
        uint64_t *hash_head = hashmap->get_hashmap() + hash_index;
        uint64_t item_head = *hash_head;

        if(item_head == 0) {
          continue;
        }
        uint32_t crrnt_time = static_cast<uint32_t> (time(NULL));

        while(item_head != 0) {
          mdb_item *it = id_to_item(item_head);
          item_head = it->h_next;

          if(it->exptime != 0 && it->exptime <= crrnt_time) {
            __remove(it);
            continue;
          }

          uint32_t server_hash =
            util::string_util::mur_mur_hash(ITEM_KEY(it) + 2,
                                            it->key_len - 2);
          server_hash %= bucket_count;
          if(__buckets.find(server_hash) != __buckets.end()) {
            __remove(it);
          }
        }
      }
    }
    TBSYS_LOG(INFO,"end close_buckets");
    return;
  }

  bool mdb_manager::remove_if_exists(data_entry & key)
  {
    mdb_item *it = hashmap->find(key.get_data(), key.get_size());
    bool ret = false;
    if(it != 0) {                //found
      __remove(it);
      it = 0;
      ret = true;
    }
    return ret;
  }

/**
 * @brief remove expired mdb_item,get lock before invoke this func
 *
 * @param area
 * @param key
 * @param mdb_item [OUT]
 *
 * @return  true -- removed ,false -- otherwise
 */
  bool mdb_manager::remove_if_expired(data_entry & key, mdb_item * &item)
  {
    mdb_item *it = hashmap->find(key.get_data(), key.get_size());
    bool ret = false;
    if(it != 0) {                //found
      if(it->exptime != 0
         && it->exptime<static_cast<uint32_t> (time(NULL))) {
        TBSYS_LOG(DEBUG, "this item is expired");
        __remove(it);
        ret = true;
        it = 0;
      }
      else {
        item = it;
      }
    }
    else {
      TBSYS_LOG(DEBUG, "item not found");
    }
    return ret;

  }

  void mdb_manager::__remove(mdb_item * it)
  {
//      m_stat.dataSize -= (it->key_len + it->data_len);
//
    /*update stat */
    area_stat[ITEM_AREA(it)]->data_size -= (it->key_len + it->data_len);
    area_stat[ITEM_AREA(it)]->space_usage -= SLAB_SIZE(it->item_id);
    if(UNLIKELY(area_stat[ITEM_AREA(it)]->data_size < 0)) {
      area_stat[ITEM_AREA(it)]->data_size = 0;
      area_stat[ITEM_AREA(it)]->space_usage = 0;
    }
    --area_stat[ITEM_AREA(it)]->item_count;
    hashmap->remove(it);
    CLEAR_FLAGS(it->item_id);        //clear all flag
    cache->free_item(it);
    //clear
    it->exptime = 0;
    it->data_len = 0;
    it->key_len = 0;
    it->version = 0;
    it->update_time = 0;
    TBSYS_LOG(DEBUG,"after free item [%p],id [%lu],h_next [%lu] next [%lu],prev[%lu],key_len[%d],data_len[%lu], version[%d]",it,it->item_id,it->h_next,it->next,it->prev,it->key_len,it->data_len,it->version);
  }


  char *mdb_manager::open_shared_mem(const char *path, int64_t size)
  {
    void *ptr = MAP_FAILED;
    int fd = -1;
    if((fd = shm_open(path, O_RDWR | O_CREAT, 0644)) < 0) {
      return 0;
    }
    ftruncate(fd, size);
    ptr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(ptr == MAP_FAILED) {
      return 0;
    }
    return static_cast<char *>(ptr);
  }


  void mdb_manager::remove_deleted_item()
  {
    for(int i = 0; i < hashmap->get_bucket_size(); ++i) {
      {
        boost::mutex::scoped_lock guard(mem_locker);
        uint64_t *hash_head = hashmap->get_hashmap() + i;
        uint64_t item_head = *hash_head;

        while(item_head != 0) {
          mdb_item *it = id_to_item(item_head);
          item_head = it->h_next;
          if(ITEM_FLAGS(it->item_id) & TAIR_ITEM_FLAG_DELETED) {
            __remove(it);
          }
        }
      }
    }
  }

  void mdb_manager::remove_exprd_item()
  {
    TBSYS_LOG(DEBUG, "start remove expired mdb_item ...");
    for(int i = 0; i < hashmap->get_bucket_size(); ++i) {
      {
        boost::mutex::scoped_lock guard(mem_locker);
        uint64_t *hash_head = hashmap->get_hashmap() + i;
        uint64_t item_head = *hash_head;
        uint32_t crrnt_time = static_cast<uint32_t> (time(NULL));


        while(item_head != 0) {
          mdb_item *it = id_to_item(item_head);
          item_head = it->h_next;
          if(it->exptime != 0 && it->exptime <= crrnt_time) {
            __remove(it);
          }
        }
      }
    }
  }


  void mdb_manager::balance_slab()
  {
    // 1. get slab info
    // 2. release page
    // 3. TODO allocate ?

    map<int, int> slab_info;
    cache->calc_slab_balance_info(slab_info);

    TBSYS_LOG(WARN, "slabinfo.size():%d", slab_info.size());

    for(map<int, int>::iterator it = slab_info.begin();
        it != slab_info.end(); ++it) {
      if(it->second < 0) {
        TBSYS_LOG(WARN, "from [%d] free  [%d] page(s)", it->first,
                  it->second);
        for(int i = 0; i < -it->second; ++i) {
          {
            boost::mutex::scoped_lock guard(mem_locker);
            if(cache->free_page(it->first) != 0) {
              TBSYS_LOG(WARN, "free page failed");
              break;
            }
          }
        }
      }
    }

    last_balance_time = static_cast<uint32_t> (time(NULL));
  }


  bool mdb_manager::is_chkexprd_time()
  {
    return hour_range(mdb_param::chkexprd_time_low,
                      mdb_param::chkexprd_time_high);
  }

  bool mdb_manager::is_chkslab_time()
  {
    return hour_range(mdb_param::chkslab_time_low,
                      mdb_param::chkslab_time_high);
  }

  void mdb_manager::run_chkexprd_deleted()
  {
    while(!stopped) {
      TAIR_SLEEP(stopped, 5);
      if(stopped)
        break;
      if(!is_chkexprd_time()) {
        continue;
      }
      //if (m_last_traversal_time != 0 && (time(NULL) - m_last_traversal_time > 60)){
      //     m_last_traversal_time = 0;
      //     remove_deleted_item();
      //}
      remove_exprd_item();
    }
  }

  void mdb_manager::run_chkslab()
  {
    TBSYS_LOG(WARN, "run_chkslab ....");
    while(!stopped) {
      TAIR_SLEEP(stopped, 10);
      if(stopped)
        break;
      if(!is_chkslab_time()) {
        continue;
      }

      if(last_balance_time != 0
         && time(NULL) - last_balance_time < (60 * 60 * 6)) {
        TBSYS_LOG(DEBUG, "don't check too often,%u", last_balance_time);
        continue;                //avoid to check too often
      }
      balance_slab();
      cache->balance_slab_done();
      check_quota();
    }

  }

  void mdb_manager::check_quota()
  {
    std::map<int, int> exceed_map;
    for(int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
      if(area_stat[i]->data_size == 0) {
        continue;                //this area is null
      }

      if(area_stat[i]->data_size > area_stat[i]->quota) {        //exceed
        {

          boost::mutex::scoped_lock guard(mem_locker);
          if(area_stat[i]->data_size > area_stat[i]->quota) {
            cache->keep_area_quota(i,
                                   area_stat[i]->data_size -
                                   area_stat[i]->quota);
          }
        }
      }
    }
    return;
  }


#ifdef TAIR_DEBUG
  map<int, int> mdb_manager::get_slab_size()
  {
    return cache->get_slab_size();
  }

  vector<int> mdb_manager::get_area_size()
  {
    return cache->get_area_size();
  }

  vector<int> mdb_manager::get_areas()
  {
    vector<int> areaS;
    for(int i = 0; i < TAIR_MAX_AREA_COUNT; i++) {
      areaS.push_back(area_stat[i]->data_size);
    }
    return areaS;
  }
#endif

}                                /* tair */
