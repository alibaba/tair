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

#ifndef TAIR_COMMON_TAIR_STAT_H
#define TAIR_COMMON_TAIR_STAT_H

#include <stdint.h>
#include <string>

#include "stat_define.hpp"

namespace tbsys {
class CDefaultRunnable;

class CThread;

class CRWLock;
}

namespace tair {
namespace storage {
class storage_manager;
}

namespace common {
//////////////////// DataStat
// data statistics depends on storage engine
class DataStat {
public:
    DataStat(storage::storage_manager *storage_mgr);

    ~DataStat();

    const StatSchema *get_schema();

    void serialize(char *&buf, int32_t &size, bool &alloc);

private:
    storage::storage_manager *storage_manager_;
};

// Tair Statistics
class TairStat : public tbsys::CDefaultRunnable {
public:
    TairStat(storage::storage_manager *storage_mgr);

    ~TairStat();

    // update statistics based on `type and `area.
    void update(int32_t area, int32_t type, int32_t count = 1);

    // update get based on `type and `area.
    void update_get(int32_t area, int rc, int32_t count = 1);

    void update_prefix_get(int32_t area, int rc, int32_t count = 1);

    void update_flow(int32_t area, int32_t in, int32_t out);

    // get statistics data to `buf/`size.
    // caller should release `buf.
    void get_stats(char *&buf, int32_t &size);

    void get_schema_str(char *&buf, int32_t &size);

    int32_t get_version() { return TAIR_STAT_VERSION; }

    void run(tbsys::CThread *thread, void *arg);

private:
    // sampling statistics
    void sampling();

    void serialize();

    // merge disjointed opstat/datastat
    bool merge_stats(const char *stat_buf_a, int32_t stat_size_a, int32_t type_count_a,
                     const char *stat_buf_b, int32_t stat_size_b, int32_t type_count_b);

    int cmp_stat(const char *key_a, int32_t size_a, const char *key_b, int32_t size_b);

    std::string get_placeholder(int32_t column);

private:
    // operation statistics
    OpStat *op_stat_;
    const StatSchema *op_schema_;
    // current data statistics.
    // datastat need no sampling.
    DataStat *data_stat_;

    // all schema
    StatSchema schema_;
    // last serialized stat data (lazy serialize)
    char *stat_data_;
    int32_t stat_data_buffer_size_; // stat data buffer size
    int32_t stat_data_size_;        // real stat data size
    char *compressed_stat_data_;
    int32_t compressed_stat_data_buffer_size_; // compressed stat data buffer size
    int32_t compressed_stat_data_size_;       // compressed stat data size

    // sampling interval (second unit)
    int32_t sampling_interval_s_;

    // rw lock guard when sampling/serializing
    tbsys::CRWLock *lock_;

    // version for tair stat schema info
    // WARNING: MUST update this version when modify
    //          OpStat::stat_descs_,
    //          LdbStatManager::stat_descs_, LdbStatManager::opstat_descs_,
    //          MdbStatManager::stat_descs_, or MdbStatManager::opstat_descs_;
    static const int32_t TAIR_STAT_VERSION;
};

}
}
#endif
