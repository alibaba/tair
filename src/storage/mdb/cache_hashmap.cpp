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
#include "cache_hashmap.hpp"
#include "tbsys.h"
namespace tair {

  void cache_hash_map::insert(mdb_item * item)
  {
    assert(item != 0);
    int idx = get_bucket_index(item);
      item->h_next = hashtable[idx];
      hashtable[idx] = item->item_id;
    ++(hashmng->item_count);
    TBSYS_LOG(DEBUG,"insert %lu [%p] next[%lu]into hash table[%d]",item->item_id,item,item->h_next,idx);
  }

  bool cache_hash_map::remove(mdb_item * item)
  {
    assert(item != 0);
    //assert(item->item_id != 0);
    int idx = get_bucket_index(item);
    uint64_t pos = hashtable[idx];
    mdb_item *prev = 0;
    mdb_item *pprev = 0;

    prev = __find(pos, ITEM_KEY(item), item->key_len, &pprev);

    if(prev == 0) {                // not found
      return false;
    }

    --(hashmng->item_count);
    if(pprev) {
      pprev->h_next = prev->h_next;
    }
    else {
      hashtable[idx] = prev->h_next;
    }
    TBSYS_LOG(DEBUG,"remove %lu [%p,%p] from hash table[%d],item->h_next:%lu",item->item_id,prev,item,idx,item->h_next);
    prev->h_next = 0;
    return true;
  }

  mdb_item *cache_hash_map::find(const char *key, unsigned int key_len)
  {
    assert(key != 0 && key_len > 0);

    TBSYS_LOG(DEBUG, "find: key,%u", key_len);
    int idx = get_bucket_index(key, key_len);
    uint64_t pos = hashtable[idx];
    return __find(pos, key, key_len);
  }

  inline int cache_hash_map::get_bucket_index(mdb_item * item)
  {
    return get_bucket_index(ITEM_KEY(item), item->key_len);
  }

  inline int cache_hash_map::get_bucket_index(const char *key,
                                              unsigned int key_len)
  {
    unsigned int hv = hash(key, key_len);
    return (hv & (hashmng->bucket_size - 1));
  }

  mdb_item *cache_hash_map::__find(uint64_t head, const char *key,
                                   unsigned int key_len,
                                   mdb_item ** pprev /*= 0*/ )
  {
    mdb_item *prev = 0;
    mdb_item *tmp_pprev = 0;
    uint64_t pos = head;
    while(pos != 0) {
      prev = id_to_item(pos);
      assert(prev != 0);
      if((prev->key_len == key_len)
         && (memcmp(key, ITEM_KEY(prev), key_len) == 0)) {
        break;
      }
      pos = prev->h_next;
      tmp_pprev = prev;
    }
    if(pos == 0) {                //not found
      prev = 0;
    }
    if(pprev) {
      *pprev = tmp_pprev;
    }
    return prev;
  }

/* http://murmurhash.googlepages.com/
 */
  unsigned int cache_hash_map::hash(const char *key, int len)
  {
    //printf("%s %d\n", key, len);

    const unsigned int m = 0x5bd1e995;
    const int r = 24;
    const int seed = 97;
    unsigned int h = seed ^ len;
    const unsigned char *data = (const unsigned char *) key;
    while(len >= 4) {
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
