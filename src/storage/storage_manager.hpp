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

#ifndef TAIR_STORAGE_MANAGER_H
#define TAIR_STORAGE_MANAGER_H

#include <stdint.h>
#include <map>
#include "util.hpp"
#include "define.hpp"
#include "stat_info.hpp"

namespace tair {
const int ITEM_HEAD_LENGTH = 2;
typedef struct _migrate_dump_index {
    uint32_t hash_index;
    uint32_t db_id;
    bool is_migrate;
} md_info;

class operation_record;
namespace common {
class StatSchema;

class OpStat;

class RecordLogger;

class data_entry;

class mput_record;
}
class _ItemData;

typedef _ItemData item_data_info;
struct _item_meta;
typedef _item_meta item_meta_info;

class tair_stat;
namespace storage {
using namespace tair::util;
using namespace tair::common;

class storage_manager {
public:
    storage_manager() : bucket_count(0) {
    }

    virtual ~ storage_manager() {
    }

    virtual int put(int bucket_number, data_entry &key, data_entry &value,
                    bool version_care, int expire_time) = 0;

    virtual int
    direct_mupdate(int bucket_number, const std::vector<operation_record *> &kvs) { return TAIR_RETURN_NOT_SUPPORTED; }

    virtual int batch_put(int bucket_number, int area, std::vector<mput_record *> *record_vec,
                          bool version_care) { return TAIR_RETURN_NOT_SUPPORTED; }

    virtual int get(int bucket_number, data_entry &key,
                    data_entry &value, bool with_stat = true) = 0;

    virtual int remove(int bucket_number, data_entry &key,
                       bool version_care) = 0;

    virtual int add_count(int bucket_num, data_entry &key, int count, int init_value,
                          int low_bound, int upper_bound, int expire_time, int &result_value) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int touch(int bucket_num, data_entry &key, int expire) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int mc_set(int bucket_num, data_entry &key, data_entry &value,
                       bool version_care, int expire) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int add(int bucket_num, data_entry &key, data_entry &value,
                    bool version_care, int expire) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int replace(int bucket_num, data_entry &key, data_entry &value,
                        bool version_care, int expire) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int append(int bucket_num, data_entry &key, data_entry &value,
                       bool version_care, data_entry *new_value = NULL) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int prepend(int bucket_num, data_entry &key, data_entry &value,
                        bool version_care, data_entry *new_value = NULL) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int incr_decr(int bucket, data_entry &key, uint64_t delta, uint64_t init,
                          bool is_incr, int expire, uint64_t &result, data_entry *new_value = NULL) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int
    get_range(int bucket_number, data_entry &key_start, data_entry &key_end, int offset, int limit, int type,
              std::vector<data_entry *> &result, bool &has_next) { return TAIR_RETURN_NOT_SUPPORTED; }

    virtual int
    del_range(int bucket_number, data_entry &key_start, data_entry &key_end, int offset, int limit, int type,
              std::vector<data_entry *> &result, bool &has_next) { return TAIR_RETURN_NOT_SUPPORTED; }

    virtual int clear(int area) = 0;

    virtual bool init_buckets(const std::vector<int> &buckets) = 0;

    virtual void close_buckets(const std::vector<int> &buckets) = 0;

    virtual bool get_next_items(md_info &info,
                                std::vector<item_data_info *> &list) = 0;

    virtual void begin_scan(md_info &info) = 0;

    virtual void end_scan(md_info &info) = 0;

    virtual void get_stats(tair_stat *stat) = 0;

    virtual void get_stats(char *&buf, int32_t &size, bool &alloc) {}

    virtual const tair::common::StatSchema *get_stat_schema() { return NULL; }

    virtual void set_opstat(tair::common::OpStat *stat) {}

    virtual int get_meta(data_entry &key, item_meta_info &meta) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int get_instance_count() {
        return 1;
    }

    // Every storage engine can implement own remote synchronization logger to
    // make use of its specific feature(eg, cache can have less strict data consistency
    // where memory queue can be afford; permanence storage engine can direct use its
    // bigLog).
    virtual tair::common::RecordLogger *get_remote_sync_logger(int version = 0) {
        return NULL;
    }

    virtual void set_area_quota(int area, uint64_t quota) = 0;

    virtual void set_area_quota(std::map<int, uint64_t> &quota_map) = 0;

    // Different storage engine reserve different amount of bits for value flag,
    // we don't want to depend on the shortest bucket, so storage engine should
    // implement its flag operation(may use some trick etc.) based on own bit resource.
    // If one engine allocs too few bits to hold all flags, it is just deserved that this engine
    // can not support all storage feature.

    // use bit operation default
    virtual bool test_flag(uint8_t meta, int flag) {
        return (meta & flag) != 0;
    }

    virtual void set_flag(uint8_t &meta, int flag) {
        meta |= flag;
    }

    virtual void clear_flag(uint8_t &meta, int flag) {
        meta &= ~flag;
    }

    virtual bool should_flush_data() { return false; }

    // whether tair_serve need health check before send heartbeat
    virtual bool need_health_check() { return false; }

    virtual int op_cmd(ServerCmdType cmd, std::vector<std::string> &params, std::vector<std::string> &infos) {
        return TAIR_RETURN_SUCCESS;
    }

    virtual void set_bucket_count(uint32_t bucket_count) {
        if (this->bucket_count != 0)
            return;                //can not rest bucket count
        this->bucket_count = bucket_count;
        return;
    }

    virtual void reload_config() {
    }

protected:
    uint32_t bucket_count;

};

}
}

#endif
