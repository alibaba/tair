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

#ifndef __MEM_CACHE_H
#define __MEM_CACHE_H

#include <iostream>
#include <stdint.h>
#include <stdlib.h>
#include <list>
#include <vector>
#include <map>
#include <cassert>
#include "data_dumpper.hpp"
#include "mem_pool.hpp"
#include "tblog.h"
#include "mdb_define.hpp"

namespace tair {
#pragma pack(1)
struct mdb_item {
    union {
        uint64_t item_id;             //~ holding info to locate this item, and other meta info, as below
        struct {
            uint64_t prefix_size: 10;   //~ prefix size of key
            uint64_t low_hash: 10;      //~ low bits of hash value
            uint64_t page_off: 16;      //~ offset in page, by items
            uint64_t page_id: 16;       //~ page id in mem_pool
            uint64_t slab_id: 8;        //~ slab id
            uint64_t flags: 4;          //~ counter: 0x1, hidden: 0x2, items(obsolete): 0x4, locked: 0x8
        };
    };
    uint64_t h_next;                //~ next in hash bucket, or next in free list
    uint64_t prev;                  //~ doubly-list used for LRU
    uint64_t next;                  //~ doubly-list used for LRU
    uint32_t exptime;               //~ time to be expired, in seconds, 0 for persistent
    uint32_t key_len:12;            //~ size of key
    uint32_t data_len:20;           //~ size of value
    uint32_t update_time;           //~ last modified time
    uint16_t version;               //~ CAS, an optimistic lock
    char data[0];                   //~ start of user data, with `area' in the leading two bytes
};
#pragma pack()

enum alloc_type {
    ALLOC_NEW = 0,                /* new */
    ALLOC_EXPIRED,                /* find a expired mdb_item */
    ALLOC_EVICT_SELF,                /*  */
    ALLOC_EVICT_ANY,                /*  */
};

#define OFFSET_MASK ((1<<16)-1)
#define PAGE_ID_MASK ((1<<16)-1)
#define SLAB_ID_MASK ((1<<8)-1)
#define LOW_HASH_MASK ((1<<10)-1)
#define ITEM_KEY(it) (&((it)->data[0]))
#define ITEM_DATA(it) (&((it)->data[0]) + (it)->key_len)
#define ITEM_AREA(it) (*reinterpret_cast<const uint16_t*>((it)->data))
#define KEY_AREA(key) (*reinterpret_cast<const uint16_t*>(key))


#define PAGE_OFFSET(x) (((x)>>20) & OFFSET_MASK)
#define PAGE_ID(x) (((x)>>36) & PAGE_ID_MASK)
#define SLAB_ID(x) (((x)>>52) & SLAB_ID_MASK)

#define ALIGN(x) ( ((x) + (mem_cache::ALIGN_SIZE-1)) & (~(mem_cache::ALIGN_SIZE-1)))
#define ITEM_ID(_offset, _page_id, _slab_id)   \
  ({                                                   \
   (((uint64_t)_slab_id) << 52                 \
    |((uint64_t)_page_id)<<36                  \
    |((uint64_t)_offset) << 20);})

//~ when we use `item_id' as a locator, we do not care the trival bits
#define CARED_FIELDS_MASK 0x0FFFFFFFFFF00000
#define itemid_equal(lhs, rhs) ( ((lhs) & CARED_FIELDS_MASK) == ((rhs) & CARED_FIELDS_MASK) )

class mdb_instance;

class mem_cache {
public:
    mem_cache(mem_pool *pool, mdb_instance *this_manager)
            : this_mem_pool(pool), instance(this_manager) {
    }

    ~mem_cache() {
    }

    bool initialize(int max_slab_id, int base_size, float factor);

    //mdb_item* alloc_item(int size);
    mdb_item *alloc_item(int size, int &type);

    bool is_item_fit(mdb_item *item, int size) {
        return find_target_slab(size) == (int) (SLAB_ID(item->item_id));
    }

    int find_target_slab(int size) {
        size_t slab_id = 0;
        while (slab_id < slab_size.size() && slab_size[slab_id++] < size);
        if (slab_id == slab_size.size() && slab_size.back() < size) {
            return (int) slab_id; //~ check effectiveness outside the door
        }
        return (int) --slab_id;
    }

    mdb_item *id_to_item(uint64_t id) {
        return slab_managers[SLAB_ID(id)]->id_to_item(id);
    }

    int slab_size_of_item(uint64_t id) {
        return slab_managers[SLAB_ID(id)]->slab_size;
    }

    void update_item(mdb_item *mdb_item);

    void free_item(mdb_item *mdb_item);

    int free_page(int slabid);

    int get_slabs_count();

    uint64_t get_item_head(int slabid, int area);

    //get the timestamp of the area
    inline uint32_t get_area_timestamp(int area) const {
        return *(area_timestamp + area);
    }

    //set the timestamp of the area
    inline void set_area_timestamp(int area, uint32_t current_time) {
        *(area_timestamp + area) = current_time;
    }

    //get the timestamp of the bucket
    inline uint32_t get_bucket_timestamp(int bucket) const {
        return *(bucket_timestamp + bucket);
    }

    //set the timestamp of the bucket
    inline void set_bucket_timestamp(int bucket, uint32_t current_time) {
        *(bucket_timestamp + bucket) = current_time;
    }

    void display_statics();

    static const int ALIGN_SIZE = 8;
    struct page_info {
        uint32_t id;
        int free_nr;
        uint32_t next;
        uint32_t prev;
        uint64_t free_head;
    };

    inline page_info *PAGE_INFO(char *page) {
        return reinterpret_cast<page_info *>(page);
    }

    void slab_memory_merge(volatile bool &stopped, pthread_mutex_t *mdb_lock);

    bool is_quota_exceed(int area);

    void calc_slab_balance_info(std::map<int, int> &adjust_info);

    void balance_slab_done();

    bool keep_area_quota(int area, uint64_t &exceed);

    struct mdb_cache_info {
        int inited;
        int max_slab_id;
        int base_size;
        float factor;
    };

#pragma pack(8)

    struct slab_manager {
        slab_manager(mem_pool *pool,
                     mem_cache *this_cache) : this_mem_pool(pool),
                                              cache(this_cache) {
        }

        mdb_item *id_to_item(uint64_t id) {
            char *addr = this->this_mem_pool->get_pool_addr();
            addr += PAGE_ID(id) * mdb_param::page_size;
            addr += sizeof(mem_cache::page_info);
            addr += PAGE_OFFSET(id) * this->slab_size;
            return reinterpret_cast<mdb_item *>(addr);
        }

        mdb_item *alloc_new_item(int area);

        mdb_item *alloc_item(int &type);

        void update_item(mdb_item *item, int area);

        void free_item(mdb_item *item);

        // for memory merge
        mdb_item *get_used_item_by_page_id(uint32_t page_id, uint32_t page_off_idx);

        mdb_item *alloc_item_from_page(int page_id, bool &is_link_full);

        int free_item_except_area_list(mdb_item *item);

        mdb_item *evict_self(int &type);

        mdb_item *evict_any(int &type);

        void init_page(char *page, int index);

        void link_item(mdb_item *item, int area);

        void unlink_item(mdb_item *mdb_item);

        void unlink_page(page_info *info, uint32_t &page_head);

        void link_page(page_info *info, uint32_t &page_head);

        int pre_alloc(int pages = 1);

        void dump_item(mdb_item *mdb_item);

        void display_statics();

        bool change_item_list_pointer(mdb_item *to_item, mdb_item *from_item);

        const static int PARTIAL_PAGE_BUCKET = 10;        /* every 10 items */
        int partial_pages_bucket_no() {
            return partial_pages_bucket_num;
        }

        page_info *PAGE_INFO(char *page) {
            return reinterpret_cast<page_info *>(page);
        }

        uint32_t get_partial_page_id() {
            return partial_pages[first_partial_page_index];
        }

        uint32_t get_the_most_free_items_of_partial_page_index() {
            for (int i = partial_pages_bucket_num - 1; i >= 0; --i) {
                if (partial_pages[i] != 0) {
                    return i;
                }
            }
            return 0;
        }

        uint32_t get_the_most_free_items_of_partial_page_id() {
            //the name is too long,but don't worry about this,
            for (int i = partial_pages_bucket_num - 1; i >= 0; --i) {
                if (partial_pages[i] != 0) {
                    return partial_pages[i];
                }
            }
            return 0;
        }

        uint64_t get_item_head(int area) {
            return this_item_list[area].item_head;
        }

        void link_partial_page(page_info *info) {
            int bucket = info->free_nr / PARTIAL_PAGE_BUCKET;
            if (partial_pages[bucket] == 0) {
                ++partial_bucket_count;
            }
            link_page(info, partial_pages[bucket]);
            if (bucket < first_partial_page_index ||
                (first_partial_page_index == 0 && partial_pages[0] == 0)) {
                first_partial_page_index = bucket;
            }
        }

        void unlink_partial_page(page_info *info) {
            int bucket = info->free_nr / PARTIAL_PAGE_BUCKET;
            unlink_page(info, partial_pages[bucket]);
            if (first_partial_page_index == bucket && partial_pages[bucket] == 0) {
                --partial_bucket_count;
                if (partial_bucket_count == 0) {
                    first_partial_page_index = 0;
                    return;
                }
                for (int i = bucket + 1; i < partial_pages_bucket_num; ++i) {
                    if (partial_pages[i] != 0) {
                        first_partial_page_index = i;
                        break;
                    }
                }
            } else if (partial_pages[bucket] == 0) {
                --partial_bucket_count;
            }
        }

        mem_pool *this_mem_pool;
        mem_cache *cache;
        uint32_t slab_id;
        int slab_size;
        int per_slab;
        int page_size;

        struct item_list {
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
        int partial_pages_bucket_num;
        int first_partial_page_index; //~ index of first bucket holding partial pages
        uint32_t partial_bucket_count; //~ actual number of buckets holding partial pages
    };

#pragma pack()
    static const int MEMCACHE_META_LEN =
            sizeof(slab_manager) * TAIR_SLAB_LARGEST;

#ifdef TAIR_DEBUG
    std::map<int, int> get_slab_size();
    std::vector< int> get_area_size();
#endif
private:
    slab_manager *get_slabmng(int size);

    void clear_page(slab_manager *slabmng, char *page);

    bool get_slab_info();

    bool slab_initialize();

    mdb_item *evict_item(bool &evict);

    void init_page(char *page);

    bool is_need_mem_merge(slab_manager *manager);

    void copy_item(mdb_item *to_item, mdb_item *from_item);

    bool do_slab_memory_merge(volatile bool &stopped, slab_manager *manager);

    mdb_cache_info *cache_info;
    std::vector<slab_manager *> slab_managers;
    std::vector<int> slab_size;
    //the timestamp of the area
    uint32_t *area_timestamp;
    //the timestamp of the buckets
    uint32_t *bucket_timestamp;

    mem_pool *this_mem_pool;
    mdb_instance *instance;
    static data_dumpper item_dump;
};


}
#endif
