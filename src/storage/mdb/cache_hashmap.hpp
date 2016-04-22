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

#ifndef __HASH_TABLE_H
#define __HASH_TABLE_H

#include "mem_pool.hpp"
#include "mem_cache.hpp"
#include "mdb_define.hpp"

#include <string.h>

namespace tair {

class mdb_instance;

class cache_hash_map {
public:
    cache_hash_map(mem_pool *pool, mdb_instance *instance,
                   int bucket_shift /*shift */ ) : this_mem_pool(pool), this_instance(instance) {
        assert(pool != 0);
        assert(pool->get_pool_addr() != 0);
        hashmng = reinterpret_cast <hash_manager * >(pool->get_pool_addr() + mem_pool::MEM_HASH_METADATA_START);
        if (hashmng->is_inited != 1) {
            hashmng->bucket_size = 1 << bucket_shift;

            int hash_table_pages = (hashmng->bucket_size * sizeof(uint64_t) + mdb_param::page_size - 1) / mdb_param::page_size;

            char *tmp_ret = pool->alloc_page(hashmng->start_page);
            assert(tmp_ret != NULL);        //save the index of the first page of hash table

            for (int i = 0; i < hash_table_pages - 1; ++i) {
                tmp_ret = pool->alloc_page();
                assert(tmp_ret != NULL);
            }
            UNUSED(tmp_ret);

            hashmng->item_count = 0;
        }
        hashtable = reinterpret_cast <uint64_t * >(pool->get_pool_addr() + hashmng->start_page * mdb_param::page_size);
        if (hashmng->is_inited != 1) {
            memset(hashtable, 0, hashmng->bucket_size * sizeof(uint64_t));
        }
        hashmng->is_inited = 1;
    }

    ~cache_hash_map() {
    }

    void insert(mdb_item *mdb_item);

    bool remove(mdb_item *mdb_item);

    mdb_item *find(const char *key, unsigned int key_len);

    void insert(mdb_item *mdb_item, unsigned int hv);

    bool remove(mdb_item *mdb_item, unsigned int hv);

    mdb_item *find_and_remove_expired(const char *key, unsigned int key_len, unsigned int hv);

    mdb_item *find(const char *key, unsigned int key_len, unsigned int hv);

    bool change_item_pointer(mdb_item *to_item, mdb_item *from_item);

    int get_bucket_size() {
        return hashmng->bucket_size;
    }

    uint64_t *get_hashmap() {
        return hashtable;
    }

    int get_item_count() {
        return hashmng->item_count;
    }

    void set_cache(mem_cache *cache) {
        this->cache = cache;
    }

    unsigned int hash(mdb_item *it) { return hash(ITEM_KEY(it), it->key_len); }

    unsigned int hash(const char *key, int len);

private:
    inline int get_bucket_index(unsigned int hv);

    mdb_item *__find(uint64_t head, const char *key, unsigned int key_len,
                     mdb_item **pprev, unsigned int hv, bool remove_expired);

    struct hash_manager {
        int is_inited;
        int bucket_size;
        int item_count;
        int start_page;
    };

    hash_manager *hashmng;
    mem_pool *this_mem_pool;
    mem_cache *cache;
    mdb_instance *this_instance;
    uint64_t *hashtable;
};

}                                /* tair */
#endif
