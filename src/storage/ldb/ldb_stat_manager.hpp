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

#ifndef TAIR_STORAGE_LDB_STAT_MANAGER_H
#define TAIR_STORAGE_LDB_STAT_MANAGER_H

#include "common/stat_define.hpp"

namespace tair {
namespace storage {
namespace ldb {
enum LdbStatType {
    STAT_USER_ITEM_COUNT = 0,
    STAT_USER_DATA_SIZE,
    STAT_PHYSICAL_ITEM_COUNT,
    STAT_PHYSICAL_DATA_SIZE,
    // op stat type
            STAT_BF_GET,            // bloomfilter get
    STAT_BF_HIT,            // bloomfilter hit
    STAT_CA_GET,            // cache get
    STAT_CA_HIT,            // cache hit
    // all type MUST be less than LDB_STAT_MAX_TYPE
            LDB_STAT_MAX_TYPE,
};
static const int32_t OP_STAT_TYPE_START = STAT_BF_GET;
static const int32_t OP_STAT_BF_COUNT = 2;
static const int32_t OP_STAT_CA_COUNT = 2;

class LdbInstance;

class LdbStatManager {
public:
    // just area => stat
    typedef tair::common::
    StatManager<tair::common::StatUnit<tair::common::StatStore>,
            tair::common::StatStore> LDB_STAT;

public:
    LdbStatManager(bool with_bf, bool with_cache);

    ~LdbStatManager();

    int start(const char *path, const char *prefix = NULL);

    void update_physical_stat(int32_t area, int32_t physical_itemcount, int32_t physical_datasize);

    void update_cache_get(int32_t area, int rc, int32_t value = 1);

    void update_bf_get(int32_t area, int rc, int32_t value = 1);

    // decode data(LdbKey format) to update stat
    void drop(const char *key, int32_t kv_size);

    void clear(int32_t bucket);

    void clear(int32_t bucket, int32_t area);

    void sampling(tair::common::StatManager<tair::common::StatUnit<tair::common::StatStore>,
            tair::common::StatStore> &sampling_stat);

    const tair::common::StatSchema *get_schema() { return &schema_; }

    const tair::common::StatSchema *get_op_schema() { return &op_schema_; }

    void set_opstat(tair::common::OpStat *stat);

    inline static void update(const tair::common::StatSchema &schema, LDB_STAT *stat, int32_t area,
                              int32_t physical_itemcount, int32_t physical_datasize);

    inline static void update(const tair::common::StatSchema &schema, LDB_STAT *stat, int32_t area,
                              int32_t physical_itemcount, int32_t physical_datasize,
                              int32_t user_itemcount, int32_t user_datasize);

    void reset_area(int32_t area, int32_t score = 0, const std::vector<int32_t> *types = NULL);

    // if is_revise_user_stat == true , revsie user stat,or fasle, revise physical stat
    int start_revise(volatile bool &stop, LdbInstance *db, bool is_revise_user_stat,
                     int64_t sleep_interval, int32_t sleep_time);

private:
    int revise_phyiscal_stat(volatile bool &stop, LdbInstance *db, LDB_STAT &stat, int64_t sleep_interval,
                             int32_t sleep_time);

    int
    revise_user_stat(volatile bool &stop, LdbInstance *db, LDB_STAT &stat, int64_t sleep_interval, int32_t sleep_time);

private:
    tair::common::StatSchema schema_;
    tair::common::StatSchema op_schema_;

    // just area => stat
    LDB_STAT *stat_;

    tair::common::OpStat *op_stat_;
    int32_t base_op_type_;

    // version for local statistics store
    // WARNING: MUST update this version when modify stat_descs_
    static const int32_t LDB_STAT_VERSION;
    static const tair::common::StatDesc stat_descs_[];
    static const tair::common::StatDesc opstat_descs_[];
};

}
}
}

#endif
