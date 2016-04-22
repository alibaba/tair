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

#ifndef TAIR_STORAGE_MDB_STAT_MANAGER_H
#define TAIR_STORAGE_MDB_STAT_MANAGER_H

#include "common/stat_define.hpp"

namespace tair {
namespace storage {
namespace mdb {
enum MdbStatType {
    STAT_ITEM_COUNT = 0,
    STAT_DATA_SIZE,
    STAT_USE_SIZE,
    STAT_QUOTA_SIZE,
    // op stat type
            STAT_EVICT_COUNT,
    // all type MUST be less than MDB_STAT_MAX_TYPE
            MDB_STAT_MAX_TYPE,
};
static const int32_t OP_STAT_TYPE_START = STAT_EVICT_COUNT;

class MdbStatManager {
public:
    MdbStatManager();

    ~MdbStatManager();

    int start(const char *path, const char *prefix = NULL);

    void update(int32_t area, int32_t itemcount, int32_t datasize, int32_t usesize, int32_t evictcount = 0);

    void update(int32_t area, int32_t type, int32_t value);

    void set_quota(int32_t area, int64_t quota);

    int64_t get_quota(int32_t area);

    bool is_quota_exceed(int32_t area) {
        tair::common::Stat *s = stat_->get_stat(area);
        return schema_.get_value(s, STAT_DATA_SIZE) >= schema_.get_value(s, STAT_QUOTA_SIZE);
    }

    void sampling(tair::common::StatManager<tair::common::StatUnit<tair::common::StatStore>,
            tair::common::StatStore> &sampling_stat, int32_t offset = 0);

    void serialize(char *&buf, int32_t &size, bool &alloc);

    void reset_area(int32_t area, int32_t score = 0, const std::vector <int32_t> *types = NULL);

    const tair::common::StatSchema *get_schema() { return &schema_; }

    const tair::common::StatSchema *get_op_schema() { return &op_schema_; }

    void set_opstat(tair::common::OpStat *stat);

private:
    tair::common::StatSchema schema_;
    tair::common::StatSchema op_schema_;

    // just area => stat
    tair::common::StatManager<tair::common::StatUnit<tair::common::StatStore>,
            tair::common::StatStore> *stat_;

    // opstat
    tair::common::OpStat *op_stat_;
    int32_t base_op_type_;

    // version for local statistics store
    // WARNING: MUST update this version when modify stat_descs_
    static const int32_t MDB_STAT_VERSION;
    static const tair::common::StatDesc stat_descs_[];
    static const tair::common::StatDesc opstat_descs_[];
};

}
}
}

#endif
