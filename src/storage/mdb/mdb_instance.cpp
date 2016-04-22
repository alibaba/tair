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

#include <stdio.h>
#include <errno.h>
#include <iostream>
#include <list>
#include <map>

#include "mem_pool.hpp"
#include "mem_cache.hpp"
#include "cache_hashmap.hpp"
#include "mdb_instance.hpp"
#include "mdb_define.hpp"
#include "util.hpp"
#include "data_entry.hpp"
#include "lock_guard.hpp"
#include "mdb_stat_manager.hpp"

using namespace tair;
using namespace std;

namespace tair {

bool mdb_instance::initialize(mdb_area_stat *stat, int32_t bucket_count,
                              bool use_share_mem /*=true*/ ) {
    this->use_share_mem = use_share_mem;
    this->area_stat = stat;
    static int seq = 0;
    snprintf(instance_name, sizeof(instance_name), "%s.%03d", mdb_param::mdb_path, __sync_fetch_and_add(&seq, 1));
    this->stat_ = new storage::mdb::MdbStatManager();
    //"/dev/shm" hard code here
    int32_t ret = stat_->start("/dev/shm", instance_name);
    if (TAIR_RETURN_SUCCESS != ret) {
        log_error("start mdb stat fail, ret: %d", ret);
        return false;
    }

    if (use_share_mem) {
        pool = open_shared_mem(instance_name, mdb_param::size);
    } else {
        pool = static_cast < char *>(malloc(mdb_param::size));
        assert (pool != 0);
        //attention: if size is too large,memset will be a very time-consuming operation.
        memset(pool, 0, mdb_param::size);
    }
    if (pool == 0) {
        log_error("mdb instance init mem fail");
        return false;
    }

    int meta_len =
            mem_pool::MEM_POOL_METADATA_LEN + mem_cache::MEMCACHE_META_LEN;    //cachehashmap will alloc space by itself

    this_mem_pool = new mem_pool(pool, mdb_param::page_size, mdb_param::size / mdb_param::page_size, meta_len);
    assert (this_mem_pool != 0);

    hashmap = new cache_hash_map(this_mem_pool, this, mdb_param::hash_shift);
    assert (hashmap != 0);

    if (0 == mdb_param::slab_base_size) {
        mdb_param::slab_base_size = sizeof(mdb_item) + 16;
    }

    cache = new mem_cache(this_mem_pool, this);
    assert (cache != 0);
    if (!cache->initialize(TAIR_SLAB_LARGEST, mdb_param::slab_base_size, mdb_param::factor)) {
        log_error("initialize cache failed for %s", instance_name);
        //~ this_mem_pool/hashmap will be released in dtor
        return false;
    }

    hashmap->set_cache(cache);

    char *pool_base = this_mem_pool->get_pool_addr();

    //mdb's version
    mdb_version = reinterpret_cast <uint32_t * >(pool_base + mem_pool::MDB_VERSION_INFO_START);
    *mdb_version = 1;

    mdb_lock_inited = reinterpret_cast < int *>(pool_base + mem_pool::MDB_LOCK_INITED_START);
    mdb_lock = reinterpret_cast <pthread_mutex_t * >(pool_base + mem_pool::MDB_LOCK_START);
    init_lock(mdb_param::lock_pshared);

    this->bucket_count = reinterpret_cast <uint32_t * >(pool_base + mem_pool::MDB_BUCKET_COUNT_START);
    set_bucket_count(bucket_count);

    return true;
}

mdb_instance::~mdb_instance() {
    delete hashmap;
    delete cache;
    delete this_mem_pool;
    if (use_share_mem)
        munmap(pool, mdb_param::size);
    else
        free(pool);
    if (stat_ != NULL)
        delete stat_;
}

int mdb_instance::put(int bucket_num, data_entry &key, unsigned int hv, data_entry &value, bool version_care,
                      int expire_time) {
    lock_guard guard(mdb_lock);
    return do_put(bucket_num, key, hv, value, version_care, expire_time);
}

int mdb_instance::get(int bucket_num, data_entry &key, unsigned int hv, data_entry &value, bool with_stat) {
    lock_guard guard(mdb_lock);
    return do_get(bucket_num, key, hv, value, with_stat);
}

int mdb_instance::remove(int bucket_num, data_entry &key, unsigned int hv, bool version_care) {
    lock_guard guard(mdb_lock);
    return do_remove(bucket_num, key, hv, version_care);
}

int mdb_instance::raw_put(const char *key, int32_t key_len,
                          unsigned int hv, const char *value,
                          int32_t value_len, int flag, uint32_t expired,
                          int32_t prefix_size, bool is_mc) {
    int total_size = key_len + value_len + sizeof(mdb_item);
    int old_expire = -1;
    log_debug("start put: key:%u,area:%d,value:%u,flag:%d,exp:%u", key_len,
              KEY_AREA (key), value_len, flag, expired);

    uint32_t crrnt_time = static_cast < uint32_t > (time(NULL));
    PROFILER_BEGIN("mdb lock");
    lock_guard guard(mdb_lock);
    PROFILER_END();
    PROFILER_BEGIN("hashmap find");
    mdb_item *it = hashmap->find(key, key_len, hv);
    PROFILER_END();

    uint8_t old_flag = 0;
    if (it != 0) {       //exists
        if (IS_DELETED(it->item_id)) {   // in migrate
            old_flag = TAIR_ITEM_FLAG_DELETED;
        }
        log_debug("already exists,remove it");
        PROFILER_BEGIN("__remove");
        old_expire = it->exptime;
        __remove(it);

        PROFILER_END();
        it = 0;
    }

    int type = KEY_AREA (key);
    PROFILER_BEGIN("alloc item");
    it = cache->alloc_item(total_size, type);    /* will be successful */
    PROFILER_END();
    assert (it != 0);
    if (type == ALLOC_EXPIRED || type == ALLOC_EVICT_SELF || type == ALLOC_EVICT_ANY) {    /* is evict */
        area_stat[ITEM_AREA (it)].decr_data_size(it->key_len + it->data_len);
        area_stat[ITEM_AREA (it)].decr_space_usage(cache->slab_size_of_item(it->item_id));
        area_stat[ITEM_AREA (it)].decr_item_count();

        int32_t evict_count = 0;
        if (type == ALLOC_EVICT_ANY || type == ALLOC_EVICT_SELF) {
            area_stat[ITEM_AREA (it)].incr_evict_count();
            TAIR_STAT.stat_evict(ITEM_AREA (it));
            evict_count = 1;
        }
        if (LIKELY(is_item_expired_by_area_clear(it) == false))
            stat_->update(ITEM_AREA (it), -1, -(it->key_len + it->data_len),
                          -cache->slab_size_of_item(it->item_id),
                          evict_count);
        else
            stat_->update(ITEM_AREA (it), mdb::STAT_EVICT_COUNT, evict_count);

        PROFILER_BEGIN("hashmap remove");
        hashmap->remove(it);
        PROFILER_END();
    }
    /*write data into mdb_item */
    it->key_len = key_len;
    it->prefix_size = prefix_size;
    it->data_len = value_len;
    it->update_time = crrnt_time;
    it->version = 0;        // just ignore..

    if (expired > 0) {
        if (is_mc) {
            it->exptime = static_cast < uint32_t > (expired) > 30 * 24 * 60 * 60 ? expired : crrnt_time + expired;
        } else {
            // expired must be absolute time
            it->exptime = expired;
        }
    } else if (expired < 0) {
        it->exptime = old_expire < 0 ? 0 : old_expire;
    } else {
        it->exptime = 0;
    }

    it->flags = (flag | old_flag);
    log_debug("it->flags:%lx", it->flags);
    memcpy(ITEM_KEY (it), key, it->key_len);
    memcpy(ITEM_DATA (it), value, it->data_len);

    PROFILER_BEGIN("hashmap insert");
    /*insert mdb_item into hashtable */
    hashmap->insert(it, hv);
    PROFILER_END();

    /*update stat */
    area_stat[ITEM_AREA (it)].incr_data_size(it->data_len + it->key_len);
    area_stat[ITEM_AREA (it)].incr_space_usage(cache->slab_size_of_item(it->item_id));
    area_stat[ITEM_AREA (it)].incr_item_count();
    area_stat[ITEM_AREA (it)].incr_put_count();

    stat_->update(ITEM_AREA (it), 1, it->data_len + it->key_len, cache->slab_size_of_item(it->item_id));

    return TAIR_RETURN_SUCCESS;
}

int mdb_instance::raw_get(const char *key, int32_t key_len,
                          unsigned int hv, std::string &value,
                          bool update) {
    log_debug("start get: area:%d,key size:%d", KEY_AREA (key), key_len);

    PROFILER_BEGIN("mdb lock");
    lock_guard guard(mdb_lock);
    PROFILER_END();
    mdb_item *it = 0;
    int ret = TAIR_RETURN_DATA_NOT_EXIST;
    bool expired = false;
    int area = KEY_AREA (key);

    if (!(expired = raw_remove_if_expired(key, key_len, it, hv)) && it != 0) {
        value.assign(ITEM_DATA (it), it->data_len);    // just get value.

        PROFILER_BEGIN("cache update");
        cache->update_item(it);
        PROFILER_END();
        //++m_stat.hitCount;
        if (update) {
            area_stat[area].incr_hit_count();
        }
        ret = TAIR_RETURN_SUCCESS;
    } else if (expired) {
        ret = TAIR_RETURN_DATA_NOT_EXIST;
    }

    if (update) {
        area_stat[area].incr_get_count();
    }
    return ret;
}

int mdb_instance::raw_remove(const char *key, int32_t key_len,
                             unsigned int hv) {
    log_debug("start remove: key size :%d", key_len);
    PROFILER_BEGIN("mdb lock");
    lock_guard guard(mdb_lock);
    PROFILER_END();
    bool ret = raw_remove_if_exists(key, key_len, hv);
    //++m_stat.removeCount;
    area_stat[KEY_AREA (key)].incr_remove_count();
    return ret ? TAIR_RETURN_SUCCESS : TAIR_RETURN_DATA_NOT_EXIST;
}

bool mdb_instance::raw_remove_if_exists(const char *key, int32_t key_len,
                                        unsigned int hv) {
    PROFILER_BEGIN("hashmap find");
    mdb_item *it = hashmap->find(key, key_len, hv);
    PROFILER_END();
    bool ret = false;
    if (it != 0)        // found
    {
        PROFILER_BEGIN("__remove");
        __remove(it, hv);
        PROFILER_END();
        ret = true;
    }
    return ret;
}

bool mdb_instance::raw_remove_if_expired(const char *key, int32_t key_len,
                                         mdb_item *&item, unsigned int hv) {
    PROFILER_BEGIN("hashmap find");
    mdb_item *it = hashmap->find(key, key_len, hv);
    PROFILER_END();
    bool ret = false;
    if (it != 0) {
        if (is_item_expired(it, static_cast < uint32_t > (time(NULL)), bucket_of_key(key, key_len, it->prefix_size))) {
            log_debug("this item is expired");
            PROFILER_BEGIN("__remove");
            __remove(it, hv);
            PROFILER_END();
            ret = true;
        } else {
            item = it;
        }
    } else {
        log_debug("item not found");
    }
    return ret;
}

int mdb_instance::add_count(int bucket_num, data_entry &key,
                            unsigned int hv, int count, int init_value,
                            int low_bound, int upper_bound,
                            int expire_time, int &result_value) {
    lock_guard guard(mdb_lock);
    return do_add_count(bucket_num, key, hv, count, init_value, low_bound, upper_bound, expire_time, result_value);
}

bool mdb_instance::lookup(int bucket_num, data_entry &key, unsigned int hv) {
    lock_guard guard(mdb_lock);
    return do_lookup(bucket_num, key, hv);
}

int mdb_instance::mc_set(int bucket_num, data_entry &key, unsigned int hv, data_entry &value, bool version_care, int expire) {
    lock_guard guard(mdb_lock);
    return do_set(bucket_num, key, hv, value, version_care, expire);
}

int mdb_instance::add(int bucket_num, data_entry &key, unsigned int hv, data_entry &value, bool version_care, int expire) {
    lock_guard guard(mdb_lock);
    return do_add(bucket_num, key, hv, value, version_care, expire);
}

int mdb_instance::replace(int bucket_num, data_entry &key,
                          unsigned int hv, data_entry &value,
                          bool version_care, int expire) {
    lock_guard guard(mdb_lock);
    return do_replace(bucket_num, key, hv, value, version_care, expire);
}

int mdb_instance::append(int bucket_num, data_entry &key, unsigned int hv,
                         data_entry &value, bool version_care,
                         data_entry *new_value) {
    lock_guard guard(mdb_lock);
    return do_pending(bucket_num, key, hv, value, version_care, false, new_value);
}

int mdb_instance::prepend(int bucket_num, data_entry &key,
                          unsigned int hv, data_entry &value,
                          bool version_care, data_entry *new_value) {
    lock_guard guard(mdb_lock);
    return do_pending(bucket_num, key, hv, value, version_care, true, new_value);
}

int mdb_instance::incr_decr(int bucket, data_entry &key, unsigned int hv,
                            uint64_t delta, uint64_t init, bool is_incr,
                            int expire, uint64_t &result,
                            data_entry *new_value) {
    lock_guard guard(mdb_lock);
    return do_incr_decr(bucket, key, hv, delta, init, is_incr, expire, result, new_value);
}

void mdb_instance::set_area_quota(int area, uint64_t quota) {
    assert (area >= 0 && area < TAIR_MAX_AREA_COUNT);
    // new stat, if quota == 0, and stat_->get_quota == 0, not set
    // if (stat_->stat_->stats_[area] == NULL)
    //   set 0 here will make stat_->stat_->stats_[area] alloc a all 0 stat array, it make no sense
    if (!(0 == quota && 0 == stat_->get_quota(area)))
        stat_->set_quota(area, (int64_t) quota);
}

void mdb_instance::set_area_quota(map<int, uint64_t> &quota_map) {
    for (map<int, uint64_t>::iterator it = quota_map.begin();
         it != quota_map.end(); ++it) {
        assert (it->first < TAIR_MAX_AREA_COUNT);
        // new stat
        if (!(0 == it->second && 0 == stat_->get_quota(it->first)))
            stat_->set_quota(it->first, (int64_t) it->second);
    }
}

uint64_t mdb_instance::get_area_quota(int area) {
    assert (area >= 0 && area < TAIR_MAX_AREA_COUNT);
    return (uint64_t) stat_->get_quota(area);
}

int mdb_instance::get_area_quota(std::map<int, uint64_t> &quota_map) {
    int i;
    for (i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
        quota_map.insert(make_pair(i, stat_->get_quota(i)));
    }
    return i;
}

bool mdb_instance::is_quota_exceed(int area) {
    assert (area >= 0 && area < TAIR_MAX_AREA_COUNT);
//    return area_stat[area].get_quota() <= area_stat[area].get_data_size();
    return stat_->is_quota_exceed(area);
}

void mdb_instance::run_checking() {
    run_chkexprd_deleted();
    run_chkslab();
}

void mdb_instance::run_mem_merge(volatile bool &stopped) {
    if (stopped || !is_mem_merge_time()) {
        return;
    }
    cache->slab_memory_merge(stopped, mdb_lock);
}

int mdb_instance::clear(int area) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        log_error("invalid area: %d", area);
        return -1;
    }
    {
        lock_guard guard(mdb_lock);
        cache->set_area_timestamp(area,
                                  static_cast < uint32_t > (time(NULL)));
        vector<int32_t> types;
        // don't reset quota info here
        types.push_back(storage::mdb::STAT_ITEM_COUNT);
        types.push_back(storage::mdb::STAT_DATA_SIZE);
        types.push_back(storage::mdb::STAT_USE_SIZE);
        stat_->reset_area(area, 0, &types);
    }
    return 0;
}

void mdb_instance::begin_scan(md_info &info) {
    info.hash_index = 0;
}

#if 1

bool mdb_instance::get_next_items(md_info &info, vector<item_data_info *> &list, uint32_t bucket_count) {
    bool ret = true;

    lock_guard guard(mdb_lock);
    uint64_t item_head = 0;
    while (true) {
        if (info.hash_index < 0 || info.hash_index >= static_cast < uint32_t >(hashmap->get_bucket_size())) {
            return false;
        }
        uint64_t *hash_head = hashmap->get_hashmap() + info.hash_index;
        item_head = *hash_head;

        if (item_head != 0) {
            break;
        }
        ++info.hash_index;
    }

    uint32_t crrnt_time = static_cast < uint32_t > (time(NULL));

    while (item_head != 0) {
        mdb_item *it = cache->id_to_item(item_head);

        if (is_item_expired(it, crrnt_time)) {
            item_head = it->h_next;
            __remove(it);
            continue;
        }

        uint32_t server_hash = bucket_of_key(it);

        if (server_hash != info.db_id) {
            item_head = it->h_next;
            continue;
        }

        if (is_item_expired(it, crrnt_time, server_hash)) {
            item_head = it->h_next;
            __remove(it);
            continue;
        }

        int size = it->key_len + it->data_len + sizeof(item_data_info);
        item_data_info *ait = (item_data_info *) malloc(size);    //meta + data
        memcpy(ait->m_data, it->data, it->key_len + it->data_len);    //copy data
        ait->header.keysize = it->key_len;
        ait->header.valsize = it->data_len;
        ait->header.prefixsize = it->prefix_size;
        ait->header.version = it->version;
        ait->header.flag = it->flags;
        ait->header.mdate = it->update_time;
        ait->header.edate = it->exptime;
        ait->header.cdate = 0;
        ait->header.magic = 0;
        ait->header.checksum = 0;

        list.push_back(ait);
        item_head = it->h_next;
    }
    if (++info.hash_index >= static_cast < uint32_t >(hashmap->get_bucket_size())) {
        ret = false;
    }
    return ret;
}

#endif

int mdb_instance::do_put(int bucket_num, data_entry &key, unsigned int hv,
                         data_entry &data, bool version_care, int expire,
                         bool is_mc) {
    int total_size = key.get_size() + data.get_size() + sizeof(mdb_item);
    int old_expire = -1;

    log_debug("start put: key:%u,area:%d,value:%u,flag:%d\n", key.get_size(), key.area, data.get_size(), data.data_meta.flag);
    if (key.area != KEY_AREA (key.get_data())) {
        log_info("key.area[%d] != KEY_AREA(key.get_data())[%d]", key.area, KEY_AREA (key.get_data()));
        key.area = KEY_AREA (key.get_data());
    }

    uint32_t crrnt_time = static_cast < uint32_t > (time(NULL));

    mdb_item *it = NULL;

    if (mdb_param::enable_put_remove_expired) {
        it =
                hashmap->find_and_remove_expired(key.get_data(), key.get_size(),
                                                 hv);
    } else {
        it = hashmap->find(key.get_data(), key.get_size(), hv);
    }

    uint16_t version = key.get_version();
    if (it != 0) {                //exists
        bool expired = is_item_expired(it, crrnt_time, bucket_num);
        // test lock.
        if ((data.server_flag & TAIR_OPERATION_UNLOCK) == 0 &&
            test_flag(it->flags, TAIR_ITEM_FLAG_LOCKED) &&
            version_care && !expired) {
            return TAIR_RETURN_LOCK_EXIST;
        }

        //if(test_flag(it->item_id, TAIR_ITEM_FLAG_DELETED)) {        // in migrate
        //old_flag = TAIR_ITEM_FLAG_DELETED;
        //}
        if (expired) {
            if (is_mc && version_care && key.get_version() != 0) {
                return TAIR_RETURN_VERSION_ERROR;
            }
            version = 0;
        } else if (version_care && version != 0
                   && it->version != static_cast < uint32_t > (version)) {
            log_debug("it->version(%hu) != version(%hu)", it->version, key.get_version());
            return TAIR_RETURN_VERSION_ERROR;
        } else {
            if (version_care) {
                version = it->version;
            }
            if (test_flag(it->flags, TAIR_ITEM_FLAG_DELETED)) {
                version = 0;
            }
            old_expire = it->exptime;
        }
        if (!cache->is_item_fit(it, total_size)) {
            log_debug("item size not fit, remove the old");
            __remove(it, hv);
            it = NULL;
        } else {
            area_stat[ITEM_AREA (it)].decr_data_size(it->key_len +
                                                     it->data_len);
            area_stat[ITEM_AREA (it)].decr_space_usage(cache->slab_size_of_item(it->item_id));
            area_stat[ITEM_AREA (it)].decr_item_count();
            if (LIKELY(is_item_expired_by_area_clear(it) == false))
                stat_->update(ITEM_AREA (it), -1, -(it->key_len + it->data_len), -cache->slab_size_of_item(it->item_id));
        }
    } else {                // doesn't exists
        if (is_mc && version_care && key.get_version() != 0) {
            return TAIR_RETURN_VERSION_ERROR;
        }
        version = 0;
    }

    if (it == NULL) {
        int type = key.area;
        it = cache->alloc_item(total_size, type);    /* will be successful */
        assert (it != 0);
        if (type == ALLOC_EXPIRED || type == ALLOC_EVICT_SELF
            || type == ALLOC_EVICT_ANY) {            /* is evict */
            area_stat[ITEM_AREA (it)].decr_data_size(it->key_len + it->data_len);
            area_stat[ITEM_AREA (it)].decr_space_usage(cache->slab_size_of_item(it->item_id));
            area_stat[ITEM_AREA (it)].decr_item_count();

            int32_t evict_count = 0;
            if (type == ALLOC_EVICT_ANY || type == ALLOC_EVICT_SELF) {
                area_stat[ITEM_AREA (it)].incr_evict_count();
                TAIR_STAT.stat_evict(ITEM_AREA (it));
                evict_count = 1;
            }
            if (LIKELY(is_item_expired_by_area_clear(it) == false))
                stat_->update(ITEM_AREA (it), -1, -(it->key_len + it->data_len), -cache->slab_size_of_item(it->item_id), evict_count);
            else
                stat_->update(ITEM_AREA (it), mdb::STAT_EVICT_COUNT, evict_count);

            hashmap->remove(it);
        }
        it->key_len = key.get_size();
        memcpy(ITEM_KEY (it), key.get_data(), it->key_len);
        /*insert mdb_item into hashtable */
        hashmap->insert(it, hv);
    }
    /*write data into mdb_item */
    it->data_len = data.get_size();
    it->update_time = crrnt_time;
    it->prefix_size = key.get_prefix_size();
    if (version_care) {
        it->version = UNLIKELY(it->version == TAIR_DATA_MAX_VERSION - 1) ? 1 : version + 1;
    } else {
        it->version = key.get_version();
    }
    log_debug("actually put,version:%d,key.getVersion():%d\n", it->version, key.get_version());
    if (expire > 0) {
        if (!is_mc) {
            it->exptime = static_cast < uint32_t > (expire) >= crrnt_time - 30 * 24 * 60 * 60 ? expire : crrnt_time + expire;
        } else {
            it->exptime = static_cast < uint32_t > (expire) > 30 * 24 * 60 * 60 ? expire : crrnt_time + expire;
        }
    } else if (expire < 0) {
        it->exptime = old_expire < 0 ? 0 : old_expire;
    } else {
        it->exptime = 0;
    }

    //set_flag(old_flag, data.data_meta.flag);
    it->flags = data.data_meta.flag;
    log_debug("it->flags:%lx", it->flags);
    memcpy(ITEM_DATA (it), data.get_data(), it->data_len);

    //set back
    key.set_version(it->version);
    key.data_meta.edate = it->exptime;
    key.data_meta.keysize = it->key_len;
    key.data_meta.valsize = it->data_len;
    key.data_meta.prefixsize = it->prefix_size;
    key.data_meta.flag = it->flags;

    /*update stat */
    area_stat[ITEM_AREA (it)].incr_data_size(it->data_len + it->key_len);
    area_stat[ITEM_AREA (it)].incr_space_usage(cache->slab_size_of_item(it->item_id));
    area_stat[ITEM_AREA (it)].incr_item_count();
    area_stat[ITEM_AREA (it)].incr_put_count();
    stat_->update(ITEM_AREA (it), 1, it->key_len + it->data_len, cache->slab_size_of_item(it->item_id));

    return 0;
}

int mdb_instance::do_get(int bucket_num, data_entry &key, unsigned int hv,
                         data_entry &data, bool with_stat) {

    log_debug("start get: area:%d,key size:%u", key.area, key.get_size());

    mdb_item *it = 0;
    int ret = TAIR_RETURN_DATA_NOT_EXIST;
    bool expired = false;
    if (!(expired = remove_if_expired(bucket_num, key, it, hv)) && it != 0) {                // got it
        if (key.area != ITEM_AREA (it)) {
            log_error("key.area != ITEM_AREA(it),why?");
            assert (0);
        }
        data.set_data(ITEM_DATA (it), it->data_len);
        data.set_version(it->version);
        data.data_meta.edate = it->exptime;
        key.set_version(it->version);
        key.data_meta.edate = it->exptime;
        key.data_meta.mdate = it->update_time;
        key.data_meta.flag = it->flags;

        key.data_meta.keysize = it->key_len;
        key.data_meta.valsize = it->data_len;
        key.data_meta.prefixsize = it->prefix_size;
        data.data_meta.keysize = it->key_len;
        data.data_meta.valsize = it->data_len;
        data.data_meta.prefixsize = it->prefix_size;
        key.data_meta.mdate = it->update_time;
        data.data_meta.flag = it->flags;

        cache->update_item(it);

        //++m_stat.hitCount;
        if (with_stat)
            area_stat[key.area].incr_hit_count();
        ret = 0;
    } else if (expired) {
        ret = TAIR_RETURN_DATA_NOT_EXIST;
    }
    //++m_stat.getCount;
    if (with_stat)
        area_stat[key.area].incr_get_count();
    return ret;
}

int mdb_instance::do_remove(int bucket_num, data_entry &key,
                            unsigned int hv, bool version_care) {
    log_debug("start remove: key size :%u", key.get_size());
    area_stat[key.area].incr_remove_count();
    key.data_meta.keysize = key.get_size();    //FIXME:just set back
    bool ret;
    if (version_care && key.get_version() != 0) {                //~ For OCS/Proxy::delete
        ret = remove_version_care(key, hv);
        return ret ? TAIR_RETURN_SUCCESS : TAIR_RETURN_VERSION_ERROR;
    } else {
        ret = remove_if_exists(key, hv);
        return ret ? TAIR_RETURN_SUCCESS : TAIR_RETURN_DATA_NOT_EXIST;
    }
}

int mdb_instance::do_add_count(int bucket_num, data_entry &key,
                               unsigned int hv, int count, int init_value,
                               int low_bound, int upper_bound, int expired,
                               int &result_value) {
    data_entry old_value;

    //todo:: get and just replace it.it should be done with new mdb version.
    int rc = do_get(bucket_num, key, hv, old_value);
    if (rc == TAIR_RETURN_SUCCESS) {
        //the counter is exist
        if (test_flag(key.data_meta.flag, TAIR_ITEM_FLAG_ADDCOUNT)) {
            //exists && iscounter && not hidden
            if (!test_flag(key.data_meta.flag, TAIR_ITEM_FLAG_DELETED)) {            //! exists && iscounter && not hidden
                //old value exist
                int32_t *v =
                        (int32_t *) (old_value.get_data() + ITEM_HEAD_LENGTH);
                log_debug("old count: %d, new count: %d, init value: %d", (*v), count, init_value);
                if (util::
                boundary_available((*v) + count, low_bound, upper_bound)) {
                    //ok, available
                    *v += count;
                    result_value = *v;
                } else {
                    //out of range, do noting but return error code.
                    return TAIR_RETURN_COUNTER_OUT_OF_RANGE;
                }
            }
                //the counter was exist and hidden, overwrite it
            else {
                if (util::boundary_available(init_value + count, low_bound, upper_bound)) {
                    result_value = init_value + count;
                    clear_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_DELETED);
                } else {
                    return TAIR_RETURN_COUNTER_OUT_OF_RANGE;
                }
            }
        }
            //the key is exist, but not counter, return error;
        else {
            //exist,but is not add_count,return error;
            log_debug("cann't override old value");
            return TAIR_RETURN_CANNOT_OVERRIDE;
        }
    }
        //the counter is not exist
    else {
        if (util::
        boundary_available(init_value + count, low_bound, upper_bound)) {
            result_value = init_value + count;
        } else {
            return TAIR_RETURN_COUNTER_OUT_OF_RANGE;
        }
    }

    char fv[INCR_DATA_SIZE];    // 2 + sizeof(int)
    SET_INCR_DATA_COUNT(fv, result_value);
    old_value.set_data(fv, INCR_DATA_SIZE);
    set_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_ADDCOUNT);
    rc = do_put(bucket_num, key, hv, old_value, true, expired);

    return rc;
}

bool mdb_instance::do_lookup(int bucket_num, data_entry &key,
                             unsigned int hv) {
    if (key.area != KEY_AREA (key.get_data())) {
        log_error("key.area[%d] != KEY_AREA(key.get_data())[%d]", key.area, KEY_AREA (key.get_data()));
        return false;
    }
    mdb_item *it = hashmap->find(key.get_data(), key.get_size(), hv);
    if (it == NULL) {
        return false;
    }
    if (is_item_expired
            (it, static_cast < uint32_t > (time(NULL)), bucket_num)) {
        __remove(it, hv);
        return false;
    }
    return true;
}

int mdb_instance::do_set(int bucket_num, data_entry &key, unsigned int hv,
                         data_entry &value, bool version_care, int expire) {
    //TODO: reuse do_put for now
    return do_put(bucket_num, key, hv, value, version_care, expire, true);
}

int mdb_instance::do_add(int bucket_num, data_entry &key, unsigned int hv,
                         data_entry &value, bool version_care, int expire) {
    //TODO: reuse do_put for now
    mdb_item *it = NULL;
    remove_if_expired(bucket_num, key, it, hv);
    if (it != NULL && version_care /*not mig/dup */ )
        return TAIR_RETURN_KEY_EXISTS;
    return do_put(bucket_num, key, hv, value, version_care, expire, true);
}

int mdb_instance::do_replace(int bucket_num, data_entry &key,
                             unsigned int hv, data_entry &value,
                             bool version_care, int expire) {
    //TODO: reuse do_put for now
    mdb_item *it = NULL;
    remove_if_expired(bucket_num, key, it, hv);
    if (it == NULL && version_care /*not mig/dup */ )
        return TAIR_RETURN_DATA_NOT_EXIST;
    return do_put(bucket_num, key, hv, value, version_care, expire, true);
}

int mdb_instance::do_pending(int bucket_num, data_entry &key,
                             unsigned int hv, data_entry &value,
                             bool version_care, bool pre,
                             data_entry *new_value) {
    time_t current_time = time(NULL);
    mdb_item *item = NULL;
    remove_if_expired(bucket_num, key, item, hv);
    if (item == NULL) {
        return TAIR_RETURN_DATA_NOT_EXIST;
    }
    if (version_care && key.get_version() != item->version
        && key.get_version() != 0) {
        return TAIR_RETURN_VERSION_ERROR;
    }

    int total_size = key.get_size() + value.get_size() + sizeof(mdb_item);
    total_size += item->key_len + item->data_len;
    if (total_size > TAIR_MAX_DATA_SIZE)
        return TAIR_RETURN_ITEMSIZE_ERROR;

    int type = key.area;
    mdb_item *new_item = cache->alloc_item(total_size, type);
    if (type == ALLOC_EXPIRED || type == ALLOC_EVICT_SELF
        || type == ALLOC_EVICT_ANY) {
        area_stat[ITEM_AREA (new_item)].decr_data_size(new_item->key_len +
                                                       new_item->data_len);
        area_stat[ITEM_AREA (new_item)].decr_space_usage(cache->
                slab_size_of_item
                (new_item->
                        item_id));
        area_stat[ITEM_AREA (new_item)].decr_item_count();
        int32_t evict_count = 0;
        if (type == ALLOC_EVICT_ANY || type == ALLOC_EVICT_SELF) {
            area_stat[ITEM_AREA (new_item)].incr_evict_count();
            TAIR_STAT.stat_evict(ITEM_AREA (new_item));
            evict_count = 1;
        }
        if (LIKELY(is_item_expired_by_area_clear(new_item) == false))
            stat_->update(ITEM_AREA (new_item), -1,
                          -(new_item->key_len + new_item->data_len),
                          -cache->slab_size_of_item(new_item->item_id),
                          evict_count);
        else
            stat_->update(ITEM_AREA (item), mdb::STAT_EVICT_COUNT,
                          evict_count);
        hashmap->remove(new_item);
    }

    new_item->key_len = key.get_size();
    new_item->prefix_size = key.get_prefix_size();
    new_item->data_len = value.get_size() + item->data_len;
    new_item->update_time = current_time;
    int version = item->version;
    new_item->version = version_care ? version + 1 : key.get_version();
    new_item->exptime = item->exptime;

    memcpy(ITEM_KEY (new_item), key.get_data(), key.get_size());
    if (pre) {
        memcpy(ITEM_DATA (new_item), ITEM_DATA (item), sizeof(int32_t));    // flags
        memcpy(ITEM_DATA (new_item) + sizeof(int32_t), value.get_data(), value.get_size());    // appendant
        memcpy(ITEM_DATA (new_item) + sizeof(int32_t) + value.get_size(), ITEM_DATA (item) + sizeof(int32_t),
               item->data_len - sizeof(int32_t));    // origin
    } else {
        memcpy(ITEM_DATA (new_item), ITEM_DATA (item), item->data_len);
        memcpy(ITEM_DATA (new_item) + item->data_len, value.get_data(),
               value.get_size());
    }

    key.set_version(new_item->version);
    key.data_meta.edate = new_item->exptime;
    key.data_meta.keysize = new_item->key_len;
    key.data_meta.valsize = new_item->data_len;
    key.data_meta.flag = new_item->flags;

    __remove(item, hv);
    hashmap->insert(new_item, hv);
    area_stat[ITEM_AREA (new_item)].incr_data_size(new_item->data_len +
                                                   new_item->key_len);
    area_stat[ITEM_AREA (new_item)].incr_space_usage(cache->
            slab_size_of_item
            (new_item->item_id));
    area_stat[ITEM_AREA (new_item)].incr_item_count();
    area_stat[ITEM_AREA (new_item)].incr_put_count();

    stat_->update(ITEM_AREA (new_item), 1,
                  new_item->data_len + new_item->key_len,
                  cache->slab_size_of_item(new_item->item_id));

    if (new_value != NULL) {
        char *data = new char[new_item->data_len];
        memcpy(data, ITEM_DATA (new_item), new_item->data_len);
        new_value->set_alloced_data(data, new_item->data_len);
        new_value->data_meta.edate = new_item->exptime;
        new_value->data_meta.keysize = new_item->key_len;
        new_value->data_meta.valsize = new_item->data_len;
        new_value->data_meta.flag = new_item->flags;
    }
    return TAIR_RETURN_SUCCESS;
}

mdb_item *mdb_instance::alloc_counter_item(data_entry &key) {
    static const int max_ascii_counter_len = 21;    // 1 + max length of ascii representation of uint64
    int total_size = sizeof(int32_t)    // flags
                     + max_ascii_counter_len + sizeof(mdb_item) + key.get_size();
    int type = key.area;
    mdb_item *item = cache->alloc_item(total_size, type);
    if (type == ALLOC_EXPIRED || type == ALLOC_EVICT_SELF || type == ALLOC_EVICT_ANY) {
        area_stat[ITEM_AREA (item)].decr_data_size(item->key_len +
                                                   item->data_len);
        area_stat[ITEM_AREA (item)].decr_space_usage(cache->slab_size_of_item(item->item_id));
        area_stat[ITEM_AREA (item)].decr_item_count();

        int evict_count = 0;
        if (type == ALLOC_EVICT_ANY || type == ALLOC_EVICT_SELF) {
            area_stat[ITEM_AREA (item)].incr_evict_count();
            TAIR_STAT.stat_evict(ITEM_AREA (item));
            evict_count = 1;
        }
        if (LIKELY(is_item_expired_by_area_clear(item) == false))
            stat_->update(ITEM_AREA (item), -1,
                          -(item->key_len + item->data_len),
                          -cache->slab_size_of_item(item->item_id),
                          evict_count);
        else
            stat_->update(ITEM_AREA (item), mdb::STAT_EVICT_COUNT,
                          evict_count);
        hashmap->remove(item);
    }
    item->key_len = key.get_size();
    item->prefix_size = key.get_prefix_size();
    item->data_len = sizeof(int32_t) + max_ascii_counter_len;
    memcpy(ITEM_KEY (item), key.get_data(), key.get_size());
    memset(ITEM_DATA (item), 0, item->data_len);
    item->flags = TAIR_ITEM_FLAG_ADDCOUNT;

    return item;
}

int mdb_instance::do_incr_decr(int bucket, data_entry &key,
                               unsigned int hv, uint64_t delta,
                               uint64_t init, bool is_incr, int expire,
                               uint64_t &result, data_entry *new_value) {
    mdb_item *new_item = NULL;
    mdb_item *item = NULL;
    uint64_t curr_val = 0;
    remove_if_expired(bucket, key, item, hv);

    if (item == NULL) {
        //TODO whether to proceed, if so, alloc & init & return
        if (expire == -1) {
            return TAIR_RETURN_DATA_NOT_EXIST;
        }
        new_item = alloc_counter_item(key);
        assert (new_item != NULL);
        new_item->data_len = snprintf(ITEM_DATA (new_item) + sizeof(int32_t), new_item->data_len - sizeof(int32_t), "%lu", init);
        new_item->data_len += sizeof(int32_t);
        result = init;
        new_item->version = 1;    // initial version
        hashmap->insert(new_item, hv);
        area_stat[ITEM_AREA (new_item)].incr_data_size(new_item->data_len + new_item->key_len);
        area_stat[ITEM_AREA (new_item)].incr_space_usage(cache->slab_size_of_item(new_item->item_id));
        area_stat[ITEM_AREA (new_item)].incr_item_count();
        area_stat[ITEM_AREA (new_item)].incr_put_count();
        stat_->update(ITEM_AREA (new_item), 1, new_item->data_len + new_item->key_len, cache->slab_size_of_item(new_item->item_id));
    } else {                // already exist
        // check the item's type, if not counter, return
        bool is_numeric = true;
        if ((item->flags & TAIR_ITEM_FLAG_ADDCOUNT) == 0) {
            for (uint32_t i = sizeof(int32_t); i < item->data_len; ++i) {
                char c = *(ITEM_DATA (item) + i);
                if (c < '0' || c > '9') {
                    is_numeric = false;
                }
            }
            if (!is_numeric) {
                return TAIR_RETURN_CANNOT_OVERRIDE;
            }
            // check if the result would be negative, if so, return the current value
            curr_val = tair::util::string_util::strntoul(ITEM_DATA (item) + sizeof(int32_t), item->data_len - sizeof(int32_t));
            new_item = alloc_counter_item(key);
            new_item->exptime = item->exptime;
            new_item->version = UNLIKELY(item->version == TAIR_DATA_MAX_VERSION - 1) ? 1 : item->version + 1;
            __remove(item);
            hashmap->insert(new_item);
            area_stat[ITEM_AREA (new_item)].incr_data_size(new_item->data_len + new_item->key_len);
            area_stat[ITEM_AREA (new_item)].incr_space_usage(cache->slab_size_of_item(new_item->item_id));
            area_stat[ITEM_AREA (new_item)].incr_item_count();
            area_stat[ITEM_AREA (new_item)].incr_put_count();
            stat_->update(ITEM_AREA (new_item), 1, new_item->data_len + new_item->key_len, cache->slab_size_of_item(new_item->item_id));
        } else {
            curr_val = tair::util::string_util::strntoul(ITEM_DATA (item) + sizeof(int32_t), item->data_len - sizeof(int32_t));
            new_item = item;
            new_item->version = UNLIKELY(item->version == TAIR_DATA_MAX_VERSION - 1) ? 1 : item->version + 1;
        }
        if (is_incr) {
            curr_val += delta;
        } else {
            curr_val = delta > curr_val ? 0 : curr_val - delta;
        }
        result = curr_val;

        int32_t data_size_diff = 0;
        area_stat[ITEM_AREA (new_item)].decr_data_size(new_item->data_len + new_item->key_len);
        data_size_diff -= (new_item->data_len + new_item->key_len);
        new_item->data_len = sprintf(ITEM_DATA (new_item) + sizeof(int32_t), "%lu", curr_val);
        new_item->data_len += sizeof(int32_t);
        area_stat[ITEM_AREA (new_item)].incr_data_size(new_item->data_len + new_item->key_len);
        data_size_diff += (new_item->data_len + new_item->key_len);

        stat_->update(ITEM_AREA (new_item), 0, data_size_diff, 0);
        cache->update_item(new_item);
    }

    time_t current_time = time(NULL);
    if (expire != -1) {
        new_item->exptime = expire > 30 * 24 * 60 * 60 ? expire : (expire == 0 ? 0 : current_time + expire);
    }                /* if expire == -1, don't modify existed key's expire */
    new_item->update_time = current_time;
    if (new_value != NULL) {
        char *data = new char[new_item->data_len];
        memcpy(data, ITEM_DATA (new_item), new_item->data_len);
        new_value->set_alloced_data(data, new_item->data_len);
        new_value->data_meta.edate = new_item->exptime;
        new_value->data_meta.keysize = new_item->key_len;
        new_value->data_meta.valsize = new_item->data_len;
        new_value->data_meta.flag = new_item->flags;
        new_value->data_meta.version = new_item->version;
    }
    key.set_version(new_item->version);
    return TAIR_RETURN_SUCCESS;
}

int mdb_instance::get_meta(data_entry &key, unsigned int hv, item_meta_info &meta) {
    lock_guard guard(mdb_lock);
    int ret = TAIR_RETURN_DATA_NOT_EXIST;
    mdb_item *it = NULL;
    bool expired = false;
    int bucket = bucket_of_key(key.get_data(), key.get_size(), key.get_prefix_size());
    if (!(expired = remove_if_expired(bucket, key, it, hv)) && it != NULL) {
        meta.keysize = it->key_len;
        meta.valsize = it->data_len;
        meta.version = it->version;
        meta.edate = it->exptime;
        meta.mdate = it->update_time;
        meta.flag = it->flags;
        ret = TAIR_RETURN_SUCCESS;
    } else if (expired) {
        ret = TAIR_RETURN_DATA_NOT_EXIST;
    }

    return ret;
}

int mdb_instance::touch(int bucket_num, data_entry &key, unsigned int hv,
                        int expire) {
    lock_guard guard(mdb_lock);
    int ret = TAIR_RETURN_DATA_NOT_EXIST;
    mdb_item *it = NULL;
    bool expired = false;
    int crrnt_time = time(NULL);
    if (!(expired = remove_if_expired(bucket_num, key, it, hv))
        && it != NULL) {
        it->update_time = time(NULL);
        if (expire > 0) {
            it->exptime = static_cast < uint32_t > (expire) > 30 * 24 * 60 * 60 ? expire : crrnt_time + expire;
        } else if (expire == 0) {
            it->exptime = 0;
        }
        key.data_meta.version = it->version;
        ret = TAIR_RETURN_SUCCESS;
    }
    return ret;
}

bool mdb_instance::init_buckets(const vector<int> &buckets) {
    return true;
}

void mdb_instance::close_buckets(const vector<int> &buckets,
                                 uint32_t bucket_count) {
    log_warn("start close buckets");
    set<int> bucket_set(buckets.begin(), buckets.end());

    uint32_t current_time = (uint32_t) time(NULL);
    {
        lock_guard guard(mdb_lock);
        for (set<int>::iterator itr = bucket_set.begin(); itr != bucket_set.end(); ++itr) {
            this->cache->set_bucket_timestamp(*itr, current_time);
        }
    }
    // this assign without lock is Ok
    this->force_run_remove_exprd_item_ = true;

    log_warn("end close buckets");
    return;
}

bool mdb_instance::remove_version_care(data_entry &key, unsigned int hv) {
    mdb_item *it = hashmap->find(key.get_data(), key.get_size(), hv);
    if (it != NULL) {
        if (key.get_version() != it->version) {
            return false;
        }
        __remove(it, hv);
    }
    return true;
}

bool mdb_instance::remove_if_exists(data_entry &key, unsigned int hv) {
    mdb_item *it = hashmap->find(key.get_data(), key.get_size(), hv);
    bool ret = false;
    if (it != 0) {                //found
        __remove(it, hv);
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
bool mdb_instance::remove_if_expired(int bucket_num, data_entry &key,
                                     mdb_item *&item, unsigned int hv) {
    mdb_item *it = hashmap->find(key.get_data(), key.get_size(), hv);
    bool ret = false;
    if (it != 0) {                //found
        uint32_t current_time = static_cast < uint32_t > (time(NULL));
        if (is_item_expired(it, current_time, bucket_num)) {
            log_debug("this item is expired");
            __remove(it, hv);
            ret = true;
            it = 0;
        } else {
            item = it;
        }
    } else {
        log_debug("item not found");
    }
    return ret;

}

void mdb_instance::__remove(mdb_item *it) {
    __remove(it, hashmap->hash(it));
}

void mdb_instance::__remove(mdb_item *it, unsigned int hv) {
    //      m_stat.dataSize -= (it->key_len + it->data_len);
    //
    /*update stat */
    area_stat[ITEM_AREA (it)].decr_data_size(it->key_len + it->data_len);
    area_stat[ITEM_AREA (it)].decr_space_usage(cache->slab_size_of_item(it->item_id));
    if (UNLIKELY(area_stat[ITEM_AREA (it)].get_data_size() < 0)) {
        area_stat[ITEM_AREA (it)].set_data_size(0);
        area_stat[ITEM_AREA (it)].set_space_usage(0);
    }
    area_stat[ITEM_AREA (it)].decr_item_count();
    if (LIKELY(is_item_expired_by_area_clear(it) == false))
        stat_->update(ITEM_AREA (it), -1, -(it->key_len + it->data_len), -cache->slab_size_of_item(it->item_id));

    PROFILER_BEGIN("hashmap remove");
    hashmap->remove(it, hv);
    PROFILER_END();
    it->flags = 0;
    PROFILER_BEGIN("cache free");
    cache->free_item(it);
    PROFILER_END();
    //clear
    it->exptime = 0;
    it->data_len = 0;
    it->key_len = 0;
    it->prefix_size = 0;
    it->low_hash = 0;
    it->version = 0;
    it->update_time = 0;
    log_debug("after free item [%p],id [%lu],h_next [%lu] next [%lu],prev[%lu],key_len[%d],data_len[%u], version[%d]",
              it, it->item_id, it->h_next, it->next, it->prev, it->key_len, it->data_len, it->version);
}

void mdb_instance::remove_deleted_item() {
    for (int i = 0; i < hashmap->get_bucket_size(); ++i) {
        {
            lock_guard guard(mdb_lock);
            uint64_t *hash_head = hashmap->get_hashmap() + i;
            uint64_t item_head = *hash_head;

            while (item_head != 0) {
                mdb_item *it = cache->id_to_item(item_head);
                item_head = it->h_next;
                if (it->flags & TAIR_ITEM_FLAG_DELETED) {
                    __remove(it);
                }
            }
        }
    }
}

void mdb_instance::remove_exprd_item() {
    log_warn("%s: start removing expired items ...", instance_name);
    int64_t items_removed = 0;
    int64_t space_released = 0;
    double sum_duration = 0.0;
    double max_duration = 0.0;
    double min_duration = 999.999;
    double granularity = 0.0;

    for (int i = 0; i < hashmap->get_bucket_size(); ++i) {
        double start = get_timespec();
        {
            lock_guard guard(mdb_lock);
            uint64_t *hash_head = hashmap->get_hashmap() + i;
            uint64_t item_head = *hash_head;
            uint32_t crrnt_time = static_cast < uint32_t > (time(NULL));

            while (item_head != 0) {
                mdb_item *it = cache->id_to_item(item_head);
                item_head = it->h_next;
                if (is_item_expired(it, crrnt_time, bucket_of_key(it))) {
                    ++items_removed;
                    space_released += cache->slab_size_of_item(it->item_id);
                    __remove(it);
                }
            }
        }
        double end = get_timespec();
        double per_duration = end - start;
        sum_duration += per_duration;
        if (per_duration > max_duration) {
            max_duration = per_duration;
        }
        if (per_duration < min_duration) {
            min_duration = per_duration;
        }

        granularity += per_duration;
        if (granularity * 1e3 > mdb_param::check_granularity /* ms */ ) {
            long time_to_usleep = (long) (granularity * 1e6);
            time_to_usleep *= mdb_param::check_granularity_factor;
            usleep(time_to_usleep);
            granularity = 0.0;
        }
    }
    log_warn("%s: end removing expired items, %ld items %ld bytes released",
             instance_name, items_removed, space_released);
    log_warn
            ("%.9lf seconds consumed, duration per bucket:  max %.9lf, min %.9lf, avg %.9lf",
             sum_duration, max_duration, min_duration,
             sum_duration / hashmap->get_bucket_size());
    last_expd_time = static_cast < uint32_t > (time(NULL));
}


void mdb_instance::balance_slab() {
    // 1. get slab info
    // 2. release page
    // 3. TODO allocate ?

    log_warn("start balancing");
    map<int, int> slab_info;
    {
        lock_guard guard(mdb_lock);
        cache->calc_slab_balance_info(slab_info);
    }

    double sum_duration = 0.0;
    double max_duration = 0.0;
    double min_duration = 999.999;
    int pages_released = 0;
    bool balance_happened = false;
    double granularity = 0.0;

    for (map<int, int>::iterator it = slab_info.begin();
         it != slab_info.end(); ++it) {
        if (it->second < 0) {
            log_warn("from slab %d, to release %d page(s)", it->first, -it->second);
            for (int i = 0; i < -it->second; ++i) {
                double start = get_timespec();
                {
                    lock_guard guard(mdb_lock);
                    if (cache->free_page(it->first) != 0) {
                        log_warn("no pages could be released in this slab");
                        break;
                    }
                    ++pages_released;
                }
                double end = get_timespec();
                double per_duration = end - start;
                sum_duration += per_duration;
                if (per_duration > max_duration) {
                    max_duration = per_duration;
                }
                if (per_duration < min_duration) {
                    min_duration = per_duration;
                }

                granularity += per_duration;
                if (granularity * 1e3 >
                    mdb_param::check_granularity /* ms */ ) {
                    long time_to_usleep = (long) (granularity * 1e6);
                    time_to_usleep *= mdb_param::check_granularity_factor;
                    usleep(time_to_usleep);
                    granularity = 0.0;
                }
                balance_happened = true;
            }
        }
    }
    log_warn("%s: end balancing, %d pages released", instance_name, pages_released);
    if (balance_happened) {
        log_warn("%.9lf seconds consumed, duration per page:  max %.9lf, min %.9lf, avg %.9lf",
                 sum_duration, max_duration, min_duration, sum_duration / pages_released);
    }

    last_balance_time = static_cast < uint32_t > (time(NULL));
}


bool mdb_instance::is_chkexprd_time() {
    return hour_range(mdb_param::chkexprd_time_low,
                      mdb_param::chkexprd_time_high);
}

bool mdb_instance::is_chkslab_time() {
    return hour_range(mdb_param::chkslab_time_low,
                      mdb_param::chkslab_time_high);
}

bool mdb_instance::is_mem_merge_time() {
    // mem_merge_time_low == mem_merge_time_high means to disable the memory merge
    return (mdb_param::mem_merge_time_low != mdb_param::mem_merge_time_high &&
            hour_range(mdb_param::mem_merge_time_low,
                       mdb_param::mem_merge_time_high));
}

void mdb_instance::run_chkexprd_deleted() {
    // if force_run_remove_exprd_item_ is true, run remove_exprd_item() without check time,
    // and set force_run_remove_exprd_item to false.
    if (this->force_run_remove_exprd_item_) {
        this->force_run_remove_exprd_item_ = false;
    } else {
        if (!is_chkexprd_time())
            return;
        //avoid to check too often
        if (last_expd_time != 0
            && time(NULL) - last_expd_time < (60 * 60 * 6)) {
            log_debug("don't exprd too often, last_expd_time: %u", last_expd_time);
            return;
        }
    }
    remove_exprd_item();
}

void mdb_instance::run_chkslab() {
    if (!is_chkslab_time()) {
        return;
    }

    if (last_balance_time != 0
        && time(NULL) - last_balance_time < (60 * 60 * 6)) {
        log_debug("don't check too often,%u", last_balance_time);
        return;
    }
    balance_slab();
    cache->balance_slab_done();
}

void mdb_instance::keep_area_quota(bool &stopped, const map<int,
        uint64_t> &exceeded_area_map) {
    if (last_check_quota_time != 0 && time(NULL) - last_check_quota_time < (60 * 60 * 6)) {
        return;
    }

    log_warn("%s: start keeping quota ...", instance_name);

    double sum_duration = 0.0;

    map<int, uint64_t>::const_iterator itr = exceeded_area_map.begin();
    while (!stopped && itr != exceeded_area_map.end()) {
        int area = itr->first;
        uint64_t exceeded_bytes = itr->second;
        double granularity = 0.0;
        while (!stopped && exceeded_bytes > 0) {
            double start = get_timespec();
            {
                lock_guard guard(mdb_lock);
                if (!cache->keep_area_quota(area, exceeded_bytes) || exceeded_bytes == 0) {        //~ rarely happens
                    //NOTE not that accurate, since data among instances not 100% balanced
                    break;
                }
            }
            double end = get_timespec();
            double duration = end - start;
            sum_duration += duration;

            granularity += duration;
            if (granularity * 1e3 > mdb_param::check_granularity /* ms */ ) {
                long time_to_usleep = (long) (granularity * 1e6);
                time_to_usleep *= mdb_param::check_granularity_factor;
                usleep(time_to_usleep);
                granularity = 0.0;
            }
        }
        log_warn
                ("keeping quota of area %d, %lu bytes released, %.9fs consumed",
                 area, itr->second - exceeded_bytes, sum_duration);
        ++itr;
    }
    log_warn("%s: end keeping area quota", instance_name);

    last_check_quota_time = time(NULL);
    return;
}

void mdb_instance::set_bucket_count(uint32_t bucket_count) {
    lock_guard guard(mdb_lock);
    if (*this->bucket_count == 0) {
        *this->bucket_count = bucket_count;
    }
}

void mdb_instance::init_lock(bool pshared) {
    //! if coredump occured with `mdb_lock held,
    //! we should have cleared the shm space.
    if (*mdb_lock_inited) {
        return;
    }
    memset(mdb_lock, 0, sizeof(pthread_mutex_t));
    if (!pshared) {
        pthread_mutex_init(mdb_lock, NULL);
    } else {
        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        pthread_mutex_init(mdb_lock, &attr);
    }
    *mdb_lock_inited = 1;
}

#ifdef TAIR_DEBUG
map < int, int >mdb_instance::get_slab_size ()
{
  return cache->get_slab_size ();
}

vector < int >mdb_instance::get_area_size ()
{
  return cache->get_area_size ();
}

vector < int >mdb_instance::get_areas ()
{
  vector < int >areaS;
  for (int i = 0; i < TAIR_MAX_AREA_COUNT; i++)
    {
  areaS.push_back (area_stat[i].get_data_size ());
    }
  return areaS;
}
#endif

}                /* tair */
