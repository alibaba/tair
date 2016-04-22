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

#include <set>
#include <string>
#include <snappy.h>

#include "storage/storage_manager.hpp"

#include "define.hpp"
#include "tair_stat.hpp"

namespace tair {
namespace common {
//////////////////// DataStat
DataStat::DataStat(storage::storage_manager *storage_mgr)
        : storage_manager_(storage_mgr) {
}

DataStat::~DataStat() {
}

void DataStat::serialize(char *&buf, int32_t &size, bool &alloc) {
    storage_manager_->get_stats(buf, size, alloc);
}

const StatSchema *DataStat::get_schema() {
    return storage_manager_->get_stat_schema();
}
//////////////////////////////

////////// TairStat //////////
// WARNING: MUST update this version when modify
//          OpStat::stat_descs_,
//          LdbStatManager::stat_descs_, LdbStatManager::opstat_descs_,
//          MdbStatManager::stat_descs_, or MdbStatManager::opstat_descs_;
const int32_t TairStat::TAIR_STAT_VERSION = 0 /*base version*/ + 1/*hit me*/;

TairStat::TairStat(storage::storage_manager *storage_mgr) :
        op_stat_(new OpStat()),
        data_stat_(new DataStat(storage_mgr)) {
    storage_mgr->set_opstat(op_stat_);

    // op_stat + data_stat
    schema_.merge(*op_stat_->get_schema());
    schema_.merge(*data_stat_->get_schema());

    op_schema_ = op_stat_->get_schema();

    stat_data_ = NULL;
    stat_data_buffer_size_ = 0;
    stat_data_size_ = 0;

    compressed_stat_data_ = NULL;
    compressed_stat_data_buffer_size_ = 0;
    compressed_stat_data_size_ = 0;

    lock_ = new tbsys::CRWLock();
    sampling_interval_s_ = 10; // default 10s
}

TairStat::~TairStat() {
    stop();
    wait();
    if (op_stat_) {
        delete op_stat_;
        op_stat_ = NULL;
    }
    if (data_stat_) {
        delete data_stat_;
        data_stat_ = NULL;
    }
    if (stat_data_) {
        delete[] stat_data_;
        stat_data_ = NULL;
        stat_data_size_ = 0;
    }
    if (compressed_stat_data_) {
        delete[] compressed_stat_data_;
        compressed_stat_data_ = NULL;
        compressed_stat_data_buffer_size_ = 0;
        compressed_stat_data_size_ = 0;
    }
    if (lock_) {
        delete lock_;
        lock_ = NULL;
    }
}

void TairStat::update_get(int32_t area, int rc, int32_t count) {
    op_stat_->update_or_not(area, OP_GET_KEY, OP_HIT, rc, count);
}

void TairStat::update_prefix_get(int32_t area, int rc, int32_t count) {
    op_stat_->update_or_not(area, OP_PF_GET_KEY, OP_PF_HIT, rc, count);
}

void TairStat::update_flow(int32_t area, int32_t in, int32_t out) {
    Stat *s = op_stat_->get_stat(area);
    if (LIKELY(s != NULL)) {
        op_schema_->update(s, OP_FC_IN, in);
        op_schema_->update(s, OP_FC_OUT, out);
    }
}

void TairStat::update(int32_t area, int32_t type, int32_t count) {
    // Actually, all update is to op_stat_,
    // data_stat_'s update is up to storage_manager now.
    op_stat_->update(area, type, count);
}

void TairStat::get_stats(char *&buf, int32_t &size) {
    // serialize
    if (stat_data_size_ == 0) {
        tbsys::CWLockGuard guard(*lock_);
        if (stat_data_size_ == 0) {
            serialize();
        }
    }

    tbsys::CRLockGuard guard(*lock_);
    if (stat_data_size_ != 0) {
        buf = new char[compressed_stat_data_size_];
        size = compressed_stat_data_size_;
        memcpy(buf, compressed_stat_data_, compressed_stat_data_size_);
    }
}

void TairStat::get_schema_str(char *&buf, int32_t &size) {
    size = schema_.to_string().size();
    buf = new char[size];
    memcpy(buf, schema_.to_string().c_str(), size);
}

void TairStat::run(tbsys::CThread *thread, void *arg) {
    while (!_stop) {
        sampling();
        // just hitchhike
        TRY_DESTROY_TRASH();
        TAIR_SLEEP(_stop, sampling_interval_s_);
    }
}

void TairStat::sampling() {
    {
        tbsys::CWLockGuard guard(*lock_);
        // sampling op stat
        // data stat need no sampling
        op_stat_->sampling(sampling_interval_s_);
        // trigger next serialize
        stat_data_size_ = 0;
    }
    /*
    // lazily serialize
    // debug @@@@@@@@@@@@
    char* buf = NULL;
    int32_t size = 0;
    get_stats(buf, size);
    if (buf != NULL)
    {
    delete buf;
    }
    // @@@@@@@@@@@@
    */
}

void TairStat::serialize() {
    char *op_stat_buf = NULL, *data_stat_buf = NULL;
    int32_t op_stat_size = 0, data_stat_size = 0;
    bool op_stat_alloc = false, data_stat_alloc = false;

    op_stat_->serialize(op_stat_buf, op_stat_size, op_stat_alloc);
    data_stat_->serialize(data_stat_buf, data_stat_size, data_stat_alloc);

    // merge disjointed opstat/datastat.
    // this is really not what I want to do...
    StatSchema::merge_stats(TAIR_MAX_AREA_COUNT,
                            *op_stat_->get_schema(), op_stat_buf, op_stat_size,
                            *data_stat_->get_schema(), data_stat_buf, data_stat_size,
                            schema_, stat_data_, stat_data_buffer_size_, stat_data_size_);
    // get compressed data
    if (UNLIKELY((size_t) compressed_stat_data_buffer_size_ < snappy::MaxCompressedLength(stat_data_size_))) {
        compressed_stat_data_buffer_size_ = (int32_t) snappy::MaxCompressedLength(stat_data_size_);
        delete[] compressed_stat_data_; // delete NULL is safe
        compressed_stat_data_ = new char[compressed_stat_data_buffer_size_];
    }
    size_t dest_size = compressed_stat_data_buffer_size_;
    snappy::RawCompress(stat_data_, stat_data_size_, compressed_stat_data_, &dest_size);
    compressed_stat_data_size_ = dest_size;

    if (UNLIKELY(op_stat_buf != NULL && op_stat_alloc)) {
        delete op_stat_buf;
    }
    if (UNLIKELY(data_stat_buf != NULL && data_stat_alloc)) {
        delete data_stat_buf;
    }
}
}
}
