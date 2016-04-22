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

#include "mdb_stat_manager.hpp"

namespace tair {
namespace storage {
namespace mdb {
// WARNING: MUST update this version(just hit) when modify stat_descs_
const int32_t MdbStatManager::MDB_STAT_VERSION = 0x6d6462/*base version*/ + 1/*hit me*/;
const tair::common::StatDesc MdbStatManager::stat_descs_[] =
        {
                {STAT_ITEM_COUNT, 8, "itemcount"},
                {STAT_DATA_SIZE,  8, "datasize"},
                {STAT_USE_SIZE,   8, "usesize"},
                {STAT_QUOTA_SIZE, 8, "quota"}
        };
const tair::common::StatDesc MdbStatManager::opstat_descs_[] =
        {
                {STAT_EVICT_COUNT, 4, "evict"},
        };

MdbStatManager::MdbStatManager() :
        schema_(stat_descs_, OP_STAT_TYPE_START),
        op_schema_(opstat_descs_, MDB_STAT_MAX_TYPE - OP_STAT_TYPE_START, OP_STAT_TYPE_START),
        stat_(NULL), op_stat_(NULL), base_op_type_(0) {
}

MdbStatManager::~MdbStatManager() {
    if (stat_ != NULL) {
        delete stat_;
        stat_ = NULL;
    }
}

void MdbStatManager::set_opstat(tair::common::OpStat *stat) {
    op_stat_ = stat;
    base_op_type_ = op_stat_->get_type_size() - OP_STAT_TYPE_START;
}

int MdbStatManager::start(const char *path, const char *prefix) {
    static const char *name_prefix = "mdbstat";
    std::string pathname = std::string(path) + "/" +
                           (prefix != NULL && *prefix != '\0' ? std::string(prefix) + "." : "") + name_prefix;

    // DiskStatStore here to persistence
    tair::common::StatStore *stat_store = new tair::common::
    DiskStatStore(&schema_, MDB_STAT_VERSION, TAIR_MAX_AREA_COUNT, pathname.c_str());
    // this stat_store_ need start()
    int ret = stat_store->start();
    if (ret != TAIR_RETURN_SUCCESS) {
        log_error("start statstore fail: %d", ret);
        delete stat_store;
    } else {
        stat_ = new tair::common::
        StatManager<tair::common::StatUnit<tair::common::StatStore>,
                tair::common::StatStore>(stat_store);
        // need start(), load statistics
        stat_->start();
    }

    return ret;
}

void MdbStatManager::update(int32_t area, int32_t itemcount, int32_t datasize, int32_t usesize, int32_t evictcount) {
    tair::common::Stat *s = stat_->get_stat(area);
    if (LIKELY(s != NULL)) {
        if (LIKELY(itemcount != 0)) {
            schema_.update(s, STAT_ITEM_COUNT, itemcount);
        }
        if (LIKELY(datasize != 0)) {
            schema_.update(s, STAT_DATA_SIZE, datasize);
        }
        if (LIKELY(usesize != 0)) {
            schema_.update(s, STAT_USE_SIZE, usesize);
        }
    }

    if (UNLIKELY(evictcount != 0 && op_stat_ != NULL)) {
        op_stat_->update(area, STAT_EVICT_COUNT + base_op_type_, evictcount);
    }
}

void MdbStatManager::update(int32_t area, int32_t type, int32_t value) {
    if (LIKELY(value != 0)) {
        if (type >= OP_STAT_TYPE_START) {
            if (LIKELY(op_stat_ != NULL)) {
                op_stat_->update(area, type + base_op_type_, value);
            }
        } else {
            stat_->update(area, type, value);
        }
    }
}

void MdbStatManager::set_quota(int32_t area, int64_t quota) {
    stat_->set_value(area, STAT_QUOTA_SIZE, quota);
}

int64_t MdbStatManager::get_quota(int32_t area) {
    return stat_->get_value(area, STAT_QUOTA_SIZE);
}

void MdbStatManager::sampling(tair::common::StatManager<tair::common::StatUnit<tair::common::StatStore>,
        tair::common::StatStore> &sampling_stat, int32_t offset) {
    stat_->sampling(sampling_stat, offset);
}

void MdbStatManager::reset_area(int32_t area, int32_t score, const std::vector <int32_t> *types) {
    stat_->reset_key(area, score, types, &schema_);
}

}
}
}
