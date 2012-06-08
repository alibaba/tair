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
#ifndef __MEM_CACHE_H
#define __MEM_CACHE_H
#include <iostream>
#include <stdint.h>
#include <stdlib.h>
#include <list>
#include <vector>
#include <map>
#include <cassert>
#include <boost/cast.hpp>
#include "data_dumpper.hpp"
#include "mem_pool.hpp"
#include "tblog.h"
#include "mdb_define.hpp"
namespace tair {
#pragma pack(1)
  struct mdb_item
  {
    uint64_t h_next;
    uint64_t prev;
    uint64_t next;
    uint32_t exptime;                /* expire time  */
    uint32_t key_len:12;        /* size of key    */
    uint32_t data_len:20;        /* size of data */
    uint16_t version;                /*  */
    uint32_t update_time;        /* the last update time */
    uint64_t item_id;                /* 0~19 slab_size,20~35 offset in page,36~51 page_id */
    /* 52~59 slab_id,60~63 flags(60 counter,61-63 free) */
    char data[0];                /* key+data */
  };
#pragma pack()

  enum alloc_type
  {
    ALLOC_NEW = 0,                /* new */
    ALLOC_EXPIRED,                /* find a expired mdb_item */
    ALLOC_EVICT_SELF,                /*  */
    ALLOC_EVICT_ANY,                /*  */
  };

#define SLAB_SIZE_MASK ((1<<20)-1)
#define OFFSET_MASK ((1<<16)-1)
#define PAGE_ID_MASK ((1<<16)-1)
#define SLAB_ID_MASK ((1<<8)-1)

#define ITEM_KEY(it) (&((it)->data[0]))
#define ITEM_DATA(it) (&((it)->data[0]) + (it)->key_len)
#define ITEM_AREA(it) (((it)->data[0]&0xff)|(((it)->data[1]<<8)&0xff00))
#define KEY_AREA(key) ((key[0]&0xff)|((key[1]<<8)&0xff00))


#define SLAB_SIZE(x) ((x) & SLAB_SIZE_MASK)
#define PAGE_OFFSET(x) (((x)>>20) & OFFSET_MASK)
#define PAGE_ID(x) (((x)>>36) & PAGE_ID_MASK)
#define SLAB_ID(x) (((x)>>52) & SLAB_ID_MASK)
#define TEST_COUNTER(x) ((x) & (1ULL<<60))
#define SET_COUNTER(x) ((x)|= (1ULL<<60))
#define CLEAR_COUNTER(x) ((x) &= (~(1ULL<<60)))
#define CLEAR_FLAGS(x) ((x) &= (~(0xFULL << 60)))
#define ITEM_FLAGS(x)                           \
   ({                                           \
      uint64_t __item_id = x;                   \
      ((__item_id >> 60) & 0xf);                \
   })
#define SET_ITEM_FLAGS(x,v)                     \
   ({                                           \
      uint64_t value = ( (v) & 0xf);            \
      (x) |= (value << 60);                     \
   })

#define ALIGN(x) ( ((x) + (mem_cache::ALIGN_SIZE-1)) & (~(mem_cache::ALIGN_SIZE-1)))
#define ITEM_ID(_slab_size,_offset,_page_id,_slab_id)   \
  ({                                                   \
   (((uint64_t)_slab_id) << 52                 \
    |((uint64_t)_page_id)<<36                  \
    |((uint64_t)_offset) << 20                  \
    |((uint64_t)_slab_size));})

#define ITEM_ADDR(base,item_id,page_size)                               \
   ({assert(item_id != 0);                                              \
      reinterpret_cast<mdb_item *>(base                                 \
                                   + PAGE_ID(item_id) * page_size       \
                                   + SLAB_SIZE(item_id) * PAGE_OFFSET(item_id)+ sizeof(mem_cache::page_info));})
#define id_to_item(id)                                                  \
   ITEM_ADDR(this_mem_pool->get_pool_addr(),id,this_mem_pool->get_page_size())


#define itemid_equal(lhs,rhs)                                           \
   ({                                                                   \
      ((SLAB_ID(lhs) == SLAB_ID(rhs)) &&                                \
       (PAGE_ID(lhs) == PAGE_ID(rhs)) &&                                \
       (PAGE_OFFSET(lhs) == PAGE_OFFSET(rhs)) && (SLAB_SIZE(lhs) == SLAB_SIZE(rhs))); \
   })

  class mdb_manager;

  class mem_cache {
  public:
    mem_cache(mem_pool * pool, mdb_manager * this_manager, int max_slab_id,
              int base_size, float factor):this_mem_pool(pool),
      manager(this_manager)
    {
      initialize(max_slab_id, base_size, factor);
    }
     ~mem_cache()
    {
    }

    //mdb_item* alloc_item(int size);
    mdb_item *alloc_item(int size, int &type);
    void update_item(mdb_item * mdb_item);
    void free_item(mdb_item * mdb_item);
    int free_page(int slabid);
    int get_slabs_count();
    uint64_t get_item_head(int slabid, int area);

    void display_statics();
    static const int ALIGN_SIZE = 8;
    struct page_info
    {
      uint32_t id;
      int free_nr;
      uint32_t next;
      uint32_t prev;
      uint64_t free_head;
    };



    bool is_quota_exceed(int area);
    void calc_slab_balance_info(std::map<int, int > &adjust_info);
    void balance_slab_done();
    void keep_area_quota(int area, int exceed);

    struct mdb_cache_info
    {
      int inited;
      int max_slab_id;
      int base_size;
      float factor;
    };

    struct slab_manager
    {
      slab_manager(mem_pool * pool,
                   mem_cache * this_cache):this_mem_pool(pool),
        cache(this_cache)
      {
      }
      mdb_item *alloc_new_item(int area);
      mdb_item *alloc_item(int &type);
      void update_item(mdb_item * item, int area);
      void free_item(mdb_item * mdb_item);

      mdb_item *evict_self(int &type);
      mdb_item *evict_any(int &type);
      void init_page(char *page, int index);
      void link_item(mdb_item * item, int area);
      void unlink_item(mdb_item * mdb_item);
      void unlink_page(page_info * info, uint32_t & page_head);
      void link_page(page_info * info, uint32_t & page_head);
      int pre_alloc(int pages = 1);
      void dump_item(mdb_item * mdb_item);
      void display_statics();

      const static int PARTIAL_PAGE_BUCKET = 10;        /* every 10 items */
      int partial_pages_bucket_no()
      {
        return partial_pages_bucket_num;
      }
      page_info *PAGE_INFO(char *page)
      {
        return reinterpret_cast<page_info *>(page);
      }
      uint32_t get_partial_page_id()
      {
        for(int i = 0; i < partial_pages_bucket_num; ++i) {
          if(partial_pages[i] != 0) {
            return partial_pages[i];
          }
        }
        return 0;
      }
      uint32_t get_the_most_free_items_of_partial_page_id()
      {
        //the name is too long,but don't worry about this,
        for(int i = partial_pages_bucket_num - 1; i >= 0; --i) {
          if(partial_pages[i] != 0) {
            return partial_pages[i];
          }
        }
        return 0;
      }
      uint64_t get_item_head(int area)
      {
        return this_item_list[area].item_head;
      }

      mem_pool *this_mem_pool;
      mem_cache *cache;
      uint32_t slab_id;
      int slab_size;
      int per_slab;
      int page_size;
      int partial_pages_bucket_num;

      struct item_list
      {
        uint64_t item_head;
        uint64_t item_tail;
      };

      item_list this_item_list[TAIR_MAX_AREA_COUNT];
      uint64_t item_count[TAIR_MAX_AREA_COUNT];
      uint64_t evict_count[TAIR_MAX_AREA_COUNT];

      uint64_t item_total_count;
      uint64_t evict_total_count;

      int full_pages_no;
      int partial_pages_no;
      int free_pages_no;
      int evict_index;

      uint32_t free_pages;
      uint32_t full_pages;
      uint32_t *partial_pages;

    };
    static const int MEMCACHE_META_LEN =
      sizeof(slab_manager) * TAIR_SLAB_LARGEST;

#ifdef TAIR_DEBUG
    std::map<int, int> get_slab_size();
    std::vector< int> get_area_size();
#endif
  private:
    slab_manager * get_slabmng(int size);
    void clear_page(slab_manager * slabmng, char *page);
    bool initialize(int max_slab_id, int base_size, float factor);
    bool get_slab_info();
    bool slab_initialize();
    mdb_item *evict_item(bool & evict);
    void init_page(char *page);


    mdb_cache_info *cache_info;
    std::vector<slab_manager *> slab_managers;
    mem_pool *this_mem_pool;
    mdb_manager *manager;
    static data_dumpper item_dump;
  };


}
#endif
