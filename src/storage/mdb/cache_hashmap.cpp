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

#include "cache_hashmap.hpp"
#include "tbsys.h"
#include "mdb_instance.hpp"

namespace tair {

void cache_hash_map::insert(mdb_item *item) {
    insert(item, hash(item));
}

void cache_hash_map::insert(mdb_item *item, unsigned int hv) {
    assert(item != 0);
    static unsigned int low_hash_shift = mdb_param::inst_shift + mdb_param::hash_shift;
    int idx = get_bucket_index(hv);
    item->h_next = hashtable[idx];
    item->low_hash = (hv >> low_hash_shift) & LOW_HASH_MASK;
    hashtable[idx] = item->item_id;
    ++(hashmng->item_count);
    log_debug("insert %lu [%p] next[%lu]into hash table[%d]", item->item_id, item, item->h_next, idx);
}

bool cache_hash_map::remove(mdb_item *item) {
    return remove(item, hash(item));
}

bool cache_hash_map::remove(mdb_item *item, unsigned int hv) {
    assert(item != 0);
    //assert(item->item_id != 0);
    int idx = get_bucket_index(hv);
    uint64_t pos = hashtable[idx];
    mdb_item *prev = 0;
    mdb_item *pprev = 0;

    if (pos == 0) {
        return false;
    }

    prev = __find(pos, ITEM_KEY(item), item->key_len, &pprev, hv, false);

    if (prev == 0) {                // not found
        return false;
    }

    --(hashmng->item_count);
    if (pprev) {
        pprev->h_next = prev->h_next;
    } else {
        hashtable[idx] = prev->h_next;
    }
    log_debug("remove %lu [%p,%p] from hash table[%d],item->h_next:%lu", item->item_id, prev, item, idx,
              item->h_next);
    prev->h_next = 0;
    return true;
}

bool cache_hash_map::change_item_pointer(mdb_item *to_item, mdb_item *from_item) {
    unsigned int hv = hash(from_item);
    int idx = get_bucket_index(hv);
    uint64_t pos = hashtable[idx];

    mdb_item *prev = 0;
    mdb_item *pprev = 0;

    if (pos == 0) {       // not found
        return false;
    }

    prev = __find(pos, ITEM_KEY(from_item), from_item->key_len, &pprev, hv, false);
    if (prev == 0) {      // not found
        return false;
    }

    if (pprev) {
        pprev->h_next = to_item->item_id;
    } else {
        hashtable[idx] = to_item->item_id;
    }

    log_debug("swap item %lu to %lu in hash table[%d]", from_item->item_id, to_item->item_id, idx);

    return true;
}

mdb_item *cache_hash_map::find(const char *key, unsigned int key_len) {
    return find(key, key_len, hash(key, key_len));
}

mdb_item *cache_hash_map::find_and_remove_expired(const char *key, unsigned int key_len, unsigned int hv) {
    assert(key != 0 && key_len > 0);
    log_debug("find: key,%u", key_len);
    int idx = get_bucket_index(hv);
    uint64_t pos = hashtable[idx];
    return pos == 0 ? NULL : __find(pos, key, key_len, NULL, hv, true);
}

mdb_item *cache_hash_map::find(const char *key, unsigned int key_len, unsigned int hv) {
    assert(key != 0 && key_len > 0);
    log_debug("find: key,%u", key_len);
    int idx = get_bucket_index(hv);
    uint64_t pos = hashtable[idx];
    return pos == 0 ? NULL : __find(pos, key, key_len, NULL, hv, false);
}

inline int cache_hash_map::get_bucket_index(unsigned int hv) {
    static int mask = hashmng->bucket_size - 1;
    /*drop bits used in selecting instance*/
    return ((hv >> mdb_param::inst_shift) & mask);
}

mdb_item *cache_hash_map::__find(uint64_t head, const char *key,
                                 unsigned int key_len,
                                 mdb_item **pprev,
                                 unsigned int hv,
                                 bool remove_expired) {
    static unsigned int low_hash_shift = mdb_param::inst_shift + mdb_param::hash_shift;
    unsigned int low_hash = ((hv >> low_hash_shift) & LOW_HASH_MASK);
    mdb_item *prev = 0;
    mdb_item *tmp_pprev = 0;
    uint64_t pos = head;
    uint32_t crrnt_time = static_cast<uint32_t>(time(NULL));

    while (pos != 0) {
        prev = cache->id_to_item(pos);
        assert(prev != 0);

        if (prev->key_len == key_len
            && prev->low_hash == low_hash
            && memcmp(key, ITEM_KEY(prev), key_len) == 0) {
            break;
        }
        pos = prev->h_next;
        tmp_pprev = prev;

        if (remove_expired) {
            if (prev->exptime != 0 && prev->exptime < crrnt_time) {
                this_instance->__remove(prev, hv);
            }
        }
    }
    if (pos == 0) {                //not found
        prev = 0;
    }
    if (pprev) {
        *pprev = tmp_pprev;
    }

    return prev;
}

/* http://murmurhash.googlepages.com/
 */
unsigned int cache_hash_map::hash(const char *key, int len) {
    //printf("%s %d\n", key, len);

    const unsigned int m = 0x5bd1e995;
    const int r = 24;
    const int seed = 97;
    unsigned int h = seed ^len;
    const unsigned char *data = (const unsigned char *) key;
    while (len >= 4) {
        unsigned int k = *(unsigned int *) data;
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
        data += 4;
        len -= 4;
    }
    switch (len) {
        case 3:
            h ^= data[2] << 16;
        case 2:
            h ^= data[1] << 8;
        case 1:
            h ^= data[0];
            h *= m;
    };
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}
}                                /* tair */
