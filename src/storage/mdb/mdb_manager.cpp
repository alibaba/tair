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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <iostream>
#include <list>
#include <map>

#include "mem_pool.hpp"
#include "mem_cache.hpp"
#include "cache_hashmap.hpp"
#include "mdb_manager.hpp"
#include "mdb_define.hpp"
#include "mdb_stat_manager.hpp"
#include "util.hpp"
#include "data_entry.hpp"

namespace tair {

using namespace common;
using namespace std;

bool mdb_manager::initialize(bool use_share_mem /*=true*/ ) {
    char area_stat_path[128];
    snprintf(area_stat_path, sizeof(area_stat_path), "%s.stat", mdb_param::mdb_path);
    area_stat = init_area_stat(area_stat_path, use_share_mem);
    if (area_stat == NULL) {
        log_error("init area stat shm failed: %m");
        return false;
    }
    scan_index = 0;
    instance_list.reserve(1 << mdb_param::inst_shift);
    for (int32_t i = 0; i < (1 << mdb_param::inst_shift); ++i) {
        mdb_instance *instance = new mdb_instance(i);
        if (!instance->initialize(area_stat, this->bucket_count, use_share_mem)) {
            log_error("mdb instance: %d init fail", i);
            delete instance;
            for (size_t j = 0; j < instance_list.size(); ++j) {
                delete instance_list[j];
            }
            return false;
        }
        instance_list.push_back(instance);
    }

    //when here instance_list is no empty
    schema_.merge(*instance_list[0]->get_stat_manager()->get_schema());
    sampling_stat_ = new StatManager <StatUnit<StatStore>, StatStore>(new MemStatStore(&schema_));

    if (mdb_param::use_check_thread) {
        maintenance_thread.start(this, NULL);
    }
    return true;
}

mdb_area_stat *mdb_manager::init_area_stat(const char *path, bool use_share_mem) {
    this->use_share_mem = use_share_mem;
    char *addr = NULL;
    size_t size = TAIR_MAX_AREA_COUNT * sizeof(mdb_area_stat);
    if (use_share_mem) {
        addr = open_shared_mem(path, size);
    } else {
        addr = static_cast<char *>(malloc(size));
        memset(addr, 0, size);
    }
    if (addr == NULL) {
        return NULL;
    }
    mdb_area_stat *stat = reinterpret_cast<mdb_area_stat *> (addr);
    map<uint32_t, uint64_t>::const_iterator it = mdb_param::default_area_capacity.begin();
    for (; it != mdb_param::default_area_capacity.end(); ++it) {
        stat[it->first].set_quota(it->second);
    }
    return stat;
}

mdb_manager::~mdb_manager() {
    stopped = true;
    if (mdb_param::use_check_thread) {
        maintenance_thread.join();
    }
    for (size_t i = 0; i < instance_list.size(); ++i) {
        delete instance_list[i];
    }
    char *addr = reinterpret_cast<char *>(area_stat);
    if (use_share_mem) {
        munmap(addr, TAIR_MAX_AREA_COUNT * sizeof(mdb_area_stat));
    } else {
        free(addr);
    }
    if (sampling_stat_ != NULL)
        delete sampling_stat_;
}

int mdb_manager::put(int bucket_num, data_entry &key, data_entry &value,
                     bool version_care, int expire_time) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->put(bucket_num, key, hv, value, version_care, expire_time);
}

int mdb_manager::get(int bucket_num, data_entry &key, data_entry &value, bool with_stat) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->get(bucket_num, key, hv, value, with_stat);
}

int mdb_manager::remove(int bucket_num, data_entry &key, bool version_care) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->remove(bucket_num, key, hv, version_care);
}

int mdb_manager::raw_put(const char *key, int32_t key_len, const char *value,
                         int32_t value_len, int flag, uint32_t expired, int32_t prefix_len, bool is_mc) {
    unsigned int hv = hash(key, key_len);
    return get_mdb_instance(hv)->raw_put(key, key_len, hv, value, value_len, flag, expired, prefix_len, is_mc);
}

int mdb_manager::raw_get(const char *key, int32_t key_len, std::string &value, bool update) {
    unsigned int hv = hash(key, key_len);
    return get_mdb_instance(hv)->raw_get(key, key_len, hv, value, update);
}

int mdb_manager::raw_remove(const char *key, int32_t key_len) {
    unsigned int hv = hash(key, key_len);
    return get_mdb_instance(hv)->raw_remove(key, key_len, hv);
}

void mdb_manager::raw_get_stats(mdb_area_stat *stat) {
    if (stat != NULL) {
        memset(stat, 0, sizeof(mdb_area_stat) * TAIR_MAX_AREA_COUNT);
        for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
            stat[i].add(&area_stat[i], true);
        }
    }
}

void mdb_manager::raw_update_stats(mdb_area_stat *stat) {
    if (stat != NULL) {
        for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
            stat[i].add(&area_stat[i], true);
        }
    }
}

int mdb_manager::add_count(int bucket_num, data_entry &key, int count, int init_value,
                           int low_bound, int upper_bound, int expire_time, int &result_value) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->add_count(bucket_num, key, hv, count, init_value,
                                           low_bound, upper_bound, expire_time, result_value);
}

bool mdb_manager::lookup(int bucket_num, data_entry &key) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->lookup(bucket_num, key, hv);
}

int mdb_manager::mc_set(int bucket_num, data_entry &key, data_entry &value,
                        bool version_care, int expire) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->mc_set(bucket_num, key, hv, value, version_care, expire);
}

int mdb_manager::add(int bucket_num, data_entry &key, data_entry &value,
                     bool version_care, int expire) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->add(bucket_num, key, hv, value, version_care, expire);
}

int mdb_manager::replace(int bucket_num, data_entry &key, data_entry &value,
                         bool version_care, int expire) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->replace(bucket_num, key, hv, value, version_care, expire);
}

int mdb_manager::append(int bucket_num, data_entry &key, data_entry &value,
                        bool version_care, data_entry *new_value) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->append(bucket_num, key, hv, value, version_care, new_value);
}

int mdb_manager::prepend(int bucket_num, data_entry &key, data_entry &value,
                         bool version_care, data_entry *new_value) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->prepend(bucket_num, key, hv, value, version_care, new_value);
}

int mdb_manager::incr_decr(int bucket, data_entry &key, uint64_t delta, uint64_t init,
                           bool is_incr, int expire, uint64_t &result, data_entry *new_value) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->incr_decr(bucket, key, hv, delta, init, is_incr, expire, result, new_value);
}

void mdb_manager::set_area_quota(int area, uint64_t quota) {
    area_stat[area].set_quota(quota);
    // new stat
    uint64_t instance_quota = quota / instance_list.size();
    if (quota % instance_list.size() != 0)
        ++instance_quota;
    for (vector<mdb_instance *>::const_iterator it = instance_list.begin(); it != instance_list.end(); ++it)
        (*it)->set_area_quota(area, instance_quota);
}

void mdb_manager::set_area_quota(map<int, uint64_t> &quota_map) {
    map<int, uint64_t>::iterator it = quota_map.begin();
    while (it != quota_map.end()) {
        area_stat[it->first].set_quota(it->second);
        ++it;
        //new stat
        uint64_t instance_quota = it->second / instance_list.size();
        if (it->second % instance_list.size() != 0)
            ++instance_quota;
        for (vector<mdb_instance *>::const_iterator ins = instance_list.begin(); ins != instance_list.end(); ++ins)
            (*ins)->set_area_quota(it->first, instance_quota);
    }
}

uint64_t mdb_manager::get_area_quota(int area) {
    // return area_stat[area].get_quota();
    //when here, instance_list never be empty()
    return instance_list.size() * instance_list[0]->get_area_quota(area);
}

int mdb_manager::get_area_quota(std::map<int, uint64_t> &quota_map) {
    for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
        quota_map[i] = instance_list.size() * instance_list[0]->get_area_quota(i);
    }
    return TAIR_MAX_AREA_COUNT;
}

bool mdb_manager::is_quota_exceed(int area) {
    for (vector<mdb_instance *>::const_iterator it = instance_list.begin(); it != instance_list.end(); ++it)
        if ((*it)->is_quota_exceed(area) == false)
            return false;
    return true;
}

void mdb_manager::reload_config() {
    mdb_param::check_granularity = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, "mdb_check_granularity", 15);
    log_warn("mdb_param::check_granularity = %d", mdb_param::check_granularity);
    mdb_param::check_granularity_factor = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, "mdb_check_granularity_factor", 10);
    log_warn("mdb_param::check_granularity_factor = %d", mdb_param::check_granularity_factor);

    const char *hour_range = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_CHECK_EXPIRED_HOUR_RANGE, "2-4");
    log_warn("check expired hour range: %s", hour_range);
    assert(hour_range != 0);
    if (sscanf(hour_range, "%d-%d", &mdb_param::chkexprd_time_low, &mdb_param::chkexprd_time_high) != 2) {
        mdb_param::chkexprd_time_low = 2;
        mdb_param::chkexprd_time_high = 4;
    }

    hour_range = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_CHECK_SLAB_HOUR_RANGE, "5-7");
    log_warn("check slab hour range: %s", hour_range);
    assert(hour_range != 0);
    if (sscanf(hour_range, "%d-%d", &mdb_param::chkslab_time_low, &mdb_param::chkslab_time_high) != 2) {
        mdb_param::chkslab_time_low = 5;
        mdb_param::chkslab_time_high = 7;
    }

    hour_range = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_MEM_MERGE_HOUR_RANGE, "7-7");
    log_warn("memory merge hour range: %s", hour_range);
    assert(hour_range != 0);
    if (sscanf(hour_range, "%d-%d", &mdb_param::mem_merge_time_low, &mdb_param::mem_merge_time_high) != 2) {
        mdb_param::mem_merge_time_low = 7;
        mdb_param::mem_merge_time_high = 7;
    }
}

int mdb_manager::clear(int area) {
    log_warn("mdb::clear area %d", area);
    for (size_t i = 0; i < instance_list.size(); ++i) {
        instance_list[i]->clear(area);
    }
    return 0;
}

void mdb_manager::begin_scan(md_info &info) {
    info.hash_index = 0;
    scan_index = 0;
}

#if 1

bool mdb_manager::get_next_items(md_info &info,
                                 vector<item_data_info *> &list) {
    bool has_next = instance_list[scan_index]->get_next_items(info, list, bucket_count);
    if (!has_next) {
        if (scan_index < instance_list.size() - 1) {
            info.hash_index = 0;
            has_next = true;
            ++scan_index;
        } else {
            scan_index = 0;
        }
    }
    return has_next;
}

#endif

void mdb_manager::get_stats(tair_stat *stat) {
    if (stat == NULL) {
        return;
    }
    for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
        stat[i].data_size_value += area_stat[i].get_data_size();
        stat[i].use_size_value += area_stat[i].get_space_usage();
        stat[i].item_count_value += area_stat[i].get_item_count();
    }
}

void mdb_manager::get_stat(int area, mdb_area_stat *stat) {
    stat->add(&area_stat[area], true);
}

const StatSchema *mdb_manager::get_stat_schema() {
    return &schema_;
}

void mdb_manager::set_opstat(OpStat *global_op_stat) {
    for (size_t i = 0; i < instance_list.size(); ++i)
        instance_list[i]->get_stat_manager()->set_opstat(global_op_stat);

    // because it is also invoke by ldb_manager::set_opstat many times.
    static bool already_add = false;
    if (false == already_add && !instance_list.empty()) {
        global_op_stat->add(*instance_list[0]->get_stat_manager()->get_op_schema());
        already_add = true;
    }
}

void mdb_manager::get_stats(char *&buf, int32_t &size, bool &alloc) {
    // this routie should be invode by only one thread at anytime
    // this lock doesn't not do harm to all performace
    tbsys::CThreadGuard guard(&lock_samping_stat_);
    sampling_stat_->reset(0);
    for (vector<mdb_instance *>::const_iterator it = instance_list.begin(); it != instance_list.end(); ++it)
        (*it)->get_stat_manager()->sampling(*sampling_stat_);
    sampling_stat_->serialize(buf, size, alloc);
}

void mdb_manager::sampling_for_upper_storage(StatManager <StatUnit<StatStore>, StatStore> &upper_storage_sampling_stat,
                                             int32_t offset) {
    for (vector<mdb_instance *>::const_iterator it = instance_list.begin(); it != instance_list.end(); ++it)
        (*it)->get_stat_manager()->sampling(upper_storage_sampling_stat, offset);
}

int mdb_manager::get_meta(data_entry &key, item_meta_info &meta) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->get_meta(key, hv, meta);
}

int mdb_manager::touch(int bucket_num, data_entry &key, int expire) {
    unsigned int hv = hash(key);
    return get_mdb_instance(hv)->touch(bucket_num, key, hv, expire);
}

bool mdb_manager::init_buckets(const vector<int> &buckets) {
    bool success = true;
    for (size_t i = 0; i < instance_list.size(); ++i) {
        success = success && instance_list[i]->init_buckets(buckets);
    }
    return success;
}

void mdb_manager::close_buckets(const vector<int> &buckets) {
    for (size_t i = 0; i < instance_list.size(); ++i) {
        instance_list[i]->close_buckets(buckets, bucket_count);
    }
}

int mdb_manager::op_cmd(ServerCmdType cmd, std::vector<std::string> &params, std::vector<std::string> &infos) {
    if (cmd == TAIR_SERVER_CMD_MODIFY_CHKEXPIR_HOUR_RANGE) {
        char const *const hour_range = params[0].c_str();
        int time_low = 2, time_high = 4;
        if (sscanf(hour_range, "%d-%d", &time_low, &time_high) != 2) {
            infos.push_back("check expired hour range error!");
        } else {
            if (time_low < 0 || time_low > 23 || time_high < 0 || time_high > 23 || time_low > time_high) {
                infos.push_back("check expired hour range error!");
            } else {
                char result[50];
                snprintf(result, 49, "successful! %d-%d -> %d-%d",
                         mdb_param::chkexprd_time_low, mdb_param::chkexprd_time_high, time_low, time_high);
                infos.push_back(result);
                mdb_param::chkexprd_time_low = time_low;
                mdb_param::chkexprd_time_high = time_high;
                for (size_t i = 0; i < instance_list.size(); ++i) {
                    instance_list[i]->set_last_expd_time(0);
                }
                log_warn("check expired hour range change to: %d-%d",
                         mdb_param::chkexprd_time_low, mdb_param::chkexprd_time_high);
            }
        }
    } else if (cmd == TAIR_SERVER_CMD_MODIFY_MDB_CHECK_GRANULARITY) {
        int check_granularity = -1;
        if (sscanf(params[0].c_str(), "%d", &check_granularity) != 1) {
            infos.push_back("mdb_check_granularity value error!");
        } else {
            if (check_granularity < 0) {
                infos.push_back("mdb_check_granularity value error!");
            } else {
                char result[50];
                snprintf(result, 49, "successful! %d -> %d", mdb_param::check_granularity, check_granularity);
                infos.push_back(result);
                mdb_param::check_granularity = check_granularity;
                log_warn("mdb::check_granularity change to %d", mdb_param::check_granularity);
            }
        }
    } else if (cmd == TAIR_SERVER_CMD_MODIFY_SLAB_MERGE_HOUR_RANGE) {
        char const *const hour_range = params[0].c_str();
        int time_low = 2, time_high = 4;
        if (sscanf(hour_range, "%d-%d", &time_low, &time_high) != 2) {
            infos.push_back("slab merge hour range error!");
        } else {
            if (time_low < 0 || time_low > 23 || time_high < 0 || time_high > 23 || time_low > time_high) {
                infos.push_back("slab merge hour range error!");
            } else {
                char result[50];
                snprintf(result, 49, "successful! %d-%d -> %d-%d",
                         mdb_param::mem_merge_time_low, mdb_param::mem_merge_time_high, time_low, time_high);
                infos.push_back(result);
                mdb_param::mem_merge_time_low = time_low;
                mdb_param::mem_merge_time_high = time_high;
                log_warn("check slab memory merge hour range change to: %d-%d",
                         mdb_param::mem_merge_time_low, mdb_param::mem_merge_time_high);
            }
        }
    } else if (cmd == TAIR_SERVER_CMD_MODIFY_PUT_REMOVE_EXPIRED) {
        int put_remove_expired = -1;
        if (sscanf(params[0].c_str(), "%d", &put_remove_expired) != 1) {
            infos.push_back("enable_put_remove_expired value error!");
        } else {
            if (put_remove_expired != 0 && put_remove_expired != 1) {
                infos.push_back("enable_put_remove_expired value error!");
            } else {
                char result[50];
                snprintf(result, 49, "successful! %d -> %d", (int) mdb_param::enable_put_remove_expired,
                         put_remove_expired);
                infos.push_back(result);
                mdb_param::enable_put_remove_expired = (put_remove_expired == 1) ? true : false;
                log_warn("enable_put_remove_expired change to %d", mdb_param::enable_put_remove_expired);
            }
        }

    }

    return TAIR_RETURN_SUCCESS;
}

void mdb_manager::run(tbsys::CThread *, void *) {
    while (!stopped) {
        TAIR_SLEEP(stopped, 5);
        if (stopped) break;

        if (hour_range(mdb_param::chkexprd_time_low, mdb_param::chkexprd_time_high)) {
            map<int, uint64_t> exceeded_area_map;
            for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
                if (area_stat[i].get_data_size() > area_stat[i].get_quota()) {
                    if (area_stat[i].get_quota() == 0) {
                        this->clear(i);
                        continue;
                    }
                    exceeded_area_map[i] =
                            (area_stat[i].get_data_size() - area_stat[i].get_quota()) / instance_list.size();
                }
            }

            if (!exceeded_area_map.empty()) {
                for (size_t i = 0; i < instance_list.size(); ++i) {
                    instance_list[i]->keep_area_quota(stopped, exceeded_area_map);
                    if (stopped) break;
                }
            }
        }

        if (stopped) break;
        for (size_t i = 0; i < instance_list.size(); ++i) {
            instance_list[i]->run_checking();
        }

        if (stopped) break;

        for (size_t i = 0; i < instance_list.size(); ++i) {
            instance_list[i]->run_mem_merge(stopped);
        }

        if (stopped) break;
    }
}

#ifdef TAIR_DEBUG
map<int, int> mdb_manager::get_slab_size()
{
    //TODO
    return map<int,int>();
}

vector<int> mdb_manager::get_area_size()
{
    //TODO
    return vector<int>();
}

vector<int> mdb_manager::get_areas()
{
    //TODO
    return vector<int>();
}
#endif

unsigned int mdb_manager::hash(data_entry &key) {
    return hash(key.get_data(), key.get_size());
}

unsigned int mdb_manager::hash(const char *key, int len) {
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
