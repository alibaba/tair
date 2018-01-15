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

#ifndef LOCAL_CACHE_HPP
#define LOCAL_CACHE_HPP

#include "local_cache.h"

namespace tair {

#define LOCAL_CACHE_TEMPLATE template<class KeyT,  \
                                      class ValueT,\
                                      class Hash,  \
                                      class Pred>

#define LOCAL_CACHE_CLASS local_cache<KeyT, ValueT, Hash, Pred>


LOCAL_CACHE_TEMPLATE
LOCAL_CACHE_CLASS::~local_cache() {
    clear();
}

LOCAL_CACHE_TEMPLATE
LOCAL_CACHE_CLASS::local_cache(size_t cap) :
        capacity(cap > 0 ? cap : 0),
        expire(300) {
    tbsys::CThreadGuard guard(&mutex);
    lru.clear();
    cache.clear();
}

LOCAL_CACHE_TEMPLATE
void LOCAL_CACHE_CLASS::touch(const KeyT &key) {
    // lock
    tbsys::CThreadGuard guard(&mutex);
    // find
    entry_cache_iterator iter = cache.find(&key);
    if (iter == cache.end()) {
        // miss
        return;
    }
    // move to first
    lru.splice(lru.begin(), lru, iter->second);
    // check whether entry was expired
    // set_entry_utime_now(iter->second);
    return;
}


LOCAL_CACHE_TEMPLATE
void LOCAL_CACHE_CLASS::remove(const KeyT &key) {
    tbsys::CThreadGuard guard(&mutex);
    entry_cache_iterator iter = cache.find(&key);
    if (iter == cache.end()) {
        return;
    }
    internal_entry *entry = *(iter->second);
    lru.erase(iter->second);
    cache.erase(iter);
    drop_entry(entry);
}


LOCAL_CACHE_TEMPLATE
typename LOCAL_CACHE_CLASS::result
LOCAL_CACHE_CLASS::put_if_absent(const KeyT &key, const ValueT &val, ValueT &curtVal) {
    if (capacity < 1) {
        return MISS;
    }
    result res = MISS;
    // lock
    tbsys::CThreadGuard guard(&mutex);

    entry_cache_iterator iter = cache.find(&key);
    internal_entry *entry = NULL;
    if (iter == cache.end()) {
        res = MISS;
        // not found
        // evict entry
        while (cache.size() >= capacity) {
            assert(capacity >= 1);
            const internal_entry *evict = evict_one();
            assert(evict != NULL);
            // free entry
            drop_entry(evict);
        }

        // insert new one
        // allocate internal_entry, relase after evict or in clear()
        entry = make_entry(key, val);
        if (entry == NULL) {
            return SETERROR;
        }
        lru.push_front(entry);
        cache[entry->get_key()] = lru.begin();
        curtVal = val;
    } else {
        res = HIT;
        // found, already exists
        // adjust lru list
        lru.splice(lru.begin(), lru, iter->second);
        // update entry value and utime
        // update first, some meta info
        fill_value(iter, curtVal);
        uint64_t now = tbutil::Time::now().toMilliSeconds();
        assert(expire != 0);
        if (now - entry_utime(iter) > expire) {
            // expired
            set_entry_utime(iter->second, now);
            res = EXPIRED;
        }
    }
    return res;
}

LOCAL_CACHE_TEMPLATE
void LOCAL_CACHE_CLASS::put(const KeyT &key, const ValueT &val) {
    if (capacity < 1) {
        return;
    }
    // lock
    tbsys::CThreadGuard guard(&mutex);

    entry_cache_iterator iter = cache.find(&key);
    internal_entry *entry = NULL;
    if (iter == cache.end()) {
        // not found
        // evict entry
        while (cache.size() >= capacity) {
            assert(capacity >= 1);
            const internal_entry *evict = evict_one();
            assert(evict != NULL);
            // free entry
            drop_entry(evict);
        }

        // insert new one
        // allocate internal_entry, relase after evict or in clear()
        entry = make_entry(key, val);
        if (entry == NULL) {
            return;
        }
        lru.push_front(entry);
    } else {
        // found, already exists
        // adjust lru list
        lru.splice(lru.begin(), lru, iter->second);
        // update entry value and utime
        // update first, some meta info
        cache.erase(iter);
        entry_list_iterator elem = lru.begin();
        set_entry_key(elem, key);
        set_entry_value(elem, val);
        set_entry_utime_now(elem);
        entry = *elem;
    }
    cache[entry->get_key()] = lru.begin();
    return;
}

LOCAL_CACHE_TEMPLATE
void LOCAL_CACHE_CLASS::clear() {
    // lock
    tbsys::CThreadGuard guard(&mutex);
    // evict every entry
    while (cache.size() > 0) {
        const internal_entry *evict = evict_one();
        assert(evict != NULL);
        // free entry
        drop_entry(evict);
    }
    lru.clear();
    cache.clear();
}

LOCAL_CACHE_TEMPLATE
typename LOCAL_CACHE_CLASS::result
LOCAL_CACHE_CLASS::get(const KeyT &key, ValueT &value) {
    // lock
    if (capacity < 1) {
        return MISS;
    }
    tbsys::CThreadGuard guard(&mutex);

    entry_cache_iterator iter = cache.find(&key);
    if (iter == cache.end()) {
        return MISS;
    }
    // adjust lru list
    lru.splice(lru.begin(), lru, iter->second);
    // whatever, find entry, fill value
    fill_value(iter, value);
    // check whether entry was expired
    uint64_t now = tbutil::Time::now().toMilliSeconds();
    assert(expire != 0);
    if (now - entry_utime(iter) > expire) {
        // expired
        set_entry_utime(iter->second, now);
        return EXPIRED;
    } else {
        // fresh entry
        return HIT;
    }
}

#undef LOCAL_CACHE_TEMPLATE
#undef LOCAL_CACHE_CLASS

}

#endif
