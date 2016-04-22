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

#include "db/db_impl.h"
// Just don't want to modify the original file. God forgive me..
#include "db/db_iter.h"

#include "common/tair_atomic.hpp"

#include "ldb_instance.hpp"
#include "ldb_define.hpp"
#include "ldb_stat_manager.hpp"

namespace tair {
namespace storage {
namespace ldb {
// WARNING: MUST update this version(just hit) when modify stat_descs_
const int32_t LdbStatManager::LDB_STAT_VERSION = 0x6c6462/*base version*/ + 1/*hit me*/;
const tair::common::StatDesc LdbStatManager::stat_descs_[] =
        {
                {STAT_USER_ITEM_COUNT,     8, "user_itemcount"},
                {STAT_USER_DATA_SIZE,      8, "user_datasize"},
                {STAT_PHYSICAL_ITEM_COUNT, 8, "physical_itemcount"},
                {STAT_PHYSICAL_DATA_SIZE,  8, "physical_datasize"},
        };
const tair::common::StatDesc LdbStatManager::opstat_descs_[] =
        {
                {STAT_BF_GET, 4, "bf_get"},
                {STAT_BF_HIT, 4, "bf_hit"},
                {STAT_CA_GET, 4, "ca_get"},
                {STAT_CA_HIT, 4, "ca_hit"},
        };

LdbStatManager::LdbStatManager(bool with_bf, bool with_cache) :
        schema_(stat_descs_, OP_STAT_TYPE_START),
        op_schema_(opstat_descs_ + (with_bf ? 0 : OP_STAT_BF_COUNT), // real descs
                   (with_bf ? OP_STAT_BF_COUNT : 0) + (with_cache ? OP_STAT_CA_COUNT : 0), // real type count
                   OP_STAT_TYPE_START + (with_bf ? 0 : OP_STAT_BF_COUNT)),
        stat_(NULL), op_stat_(NULL),
        base_op_type_(OP_STAT_TYPE_START + (with_bf ? 0 : OP_STAT_BF_COUNT)) {
}

LdbStatManager::~LdbStatManager() {
    if (stat_ != NULL) {
        delete stat_;
    }
}

void LdbStatManager::set_opstat(tair::common::OpStat *stat) {
    op_stat_ = stat;
    base_op_type_ = op_stat_->get_type_size() - base_op_type_;
}

int LdbStatManager::start(const char *path, const char *prefix) {
    static const char *name_prefix = "ldbstat";
    std::string pathname = std::string(path) + "/" +
                           (prefix != NULL && *prefix != '\0' ? std::string(prefix) + "." : "") + name_prefix;

    // DiskStatStore here to persistence
    tair::common::StatStore *stat_store = new tair::common::
    DiskStatStore(&schema_, LDB_STAT_VERSION, TAIR_MAX_AREA_COUNT, pathname.c_str());
    // this stat_store_ need start()
    int ret = stat_store->start();
    if (ret != TAIR_RETURN_SUCCESS) {
        log_error("start statstore fail: %d", ret);
        delete stat_store;
    } else {
        stat_ = new LDB_STAT(stat_store);
        // need start(), load statistics
        stat_->start();
    }
    return ret;
}

void LdbStatManager::update_physical_stat(int32_t area, int32_t physical_itemcount, int32_t physical_datasize) {
    update(schema_, stat_, area, physical_itemcount, physical_datasize);
}

void LdbStatManager::update_cache_get(int32_t area, int rc, int32_t value) {
    if (LIKELY(op_stat_ != NULL)) {
        op_stat_->update_or_not(area, STAT_CA_GET + base_op_type_, STAT_CA_HIT + base_op_type_, rc, value);
    }
}

void LdbStatManager::update_bf_get(int32_t area, int rc, int32_t value) {
    if (LIKELY(op_stat_ != NULL)) {
        op_stat_->update_or_not(area, STAT_BF_GET + base_op_type_, STAT_BF_HIT + base_op_type_, rc, value);
    }
}

void LdbStatManager::drop(const char *key, int32_t kv_size) {
    update(schema_, stat_, LdbKey::decode_area_with_key(key), -1, -kv_size);
}

void LdbStatManager::clear(int32_t area) {
    stat_->clear(area);
}

void LdbStatManager::sampling(tair::common::StatManager<tair::common::StatUnit<tair::common::StatStore>,
        tair::common::StatStore> &sampling_stat) {
    if (stat_ != NULL) {
        stat_->sampling(sampling_stat);
    }
}

void LdbStatManager::update(const tair::common::StatSchema &schema, LDB_STAT *stat, int32_t area,
                            int32_t physical_itemcount, int32_t physical_datasize) {
    tair::common::Stat *s = stat->get_stat(area);
    if (LIKELY(s != NULL)) {
        if (LIKELY(physical_itemcount != 0)) {
            schema.update(s, STAT_PHYSICAL_ITEM_COUNT, physical_itemcount);
        }
        if (LIKELY(physical_datasize != 0)) {
            schema.update(s, STAT_PHYSICAL_DATA_SIZE, physical_datasize);
        }
    }
}

void LdbStatManager::update(const tair::common::StatSchema &schema, LDB_STAT *stat, int32_t area,
                            int32_t physical_itemcount, int32_t physical_datasize,
                            int32_t user_itemcount, int32_t user_datasize) {
    tair::common::Stat *s = stat->get_stat(area);
    if (LIKELY(s != NULL)) {
        if (LIKELY(physical_itemcount != 0)) {
            schema.update(s, STAT_PHYSICAL_ITEM_COUNT, physical_itemcount);
        }
        if (LIKELY(physical_datasize != 0)) {
            schema.update(s, STAT_PHYSICAL_DATA_SIZE, physical_datasize);
        }
        if (LIKELY(user_itemcount != 0)) {
            schema.update(s, STAT_USER_ITEM_COUNT, user_itemcount);
        }
        if (LIKELY(user_datasize != 0)) {
            schema.update(s, STAT_USER_DATA_SIZE, user_datasize);
        }
    }
}

void LdbStatManager::reset_area(int32_t area, int32_t score, const std::vector<int32_t> *types) {
    stat_->reset_key(area, score, types, &schema_);
}

int LdbStatManager::start_revise(volatile bool &stop, LdbInstance *db, bool is_revise_user_stat,
                                 int64_t sleep_interval, int32_t sleep_time) {
    //simple check
    if (sleep_interval < 0 || !(0 < sleep_time && sleep_time < 1000000))
        return TAIR_RETURN_FAILED;

    int32_t start_time = time(NULL);

    int ret = TAIR_RETURN_SUCCESS;
    LDB_STAT *base_stat = new LDB_STAT(new tair::common::MemStatStore(&schema_));
    LDB_STAT *revise_stat = new LDB_STAT(new tair::common::MemStatStore(&schema_));

    // not perfect accurate actually, update between this sampling() and NewIterator() is lost.
    stat_->sampling(*base_stat);

    std::vector<int32_t> revise_items;
    if (is_revise_user_stat) {
        // only revise user stat
        revise_items.push_back(STAT_USER_ITEM_COUNT);
        revise_items.push_back(STAT_USER_DATA_SIZE);
        ret = revise_user_stat(stop, db, *revise_stat, sleep_interval, sleep_time);
    } else {
        // only revise phyiscal stat
        revise_items.push_back(STAT_PHYSICAL_ITEM_COUNT);
        revise_items.push_back(STAT_PHYSICAL_DATA_SIZE);
        ret = revise_phyiscal_stat(stop, db, *revise_stat, sleep_interval, sleep_time);
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        // diff stat
        base_stat->reset(-1);
        revise_stat->update(*base_stat);
        // update
        stat_->update(*revise_stat, &revise_items);
    }

    log_warn("finish stat revise: success: %s, cost: %ld(s)",
             ret == TAIR_RETURN_SUCCESS ? "yes" : "no", time(NULL) - start_time);

    delete base_stat;
    delete revise_stat;

    return ret;
}

int LdbStatManager::revise_phyiscal_stat(volatile bool &stop, LdbInstance *db, LDB_STAT &stat,
                                         int64_t sleep_interval, int32_t sleep_time) {
    leveldb::ReadOptions options;
    options.fill_cache = false;
    options.hold_for_long = true;
    leveldb::Iterator *db_it = reinterpret_cast<leveldb::DBImpl *>(db->db())->NewRawIterator(options);

    const tair::common::StatSchema &schema = schema_;

    if (NULL == db_it) {
        log_error("get ldb iterator fail");
        return TAIR_RETURN_FAILED;
    }

    std::string last_key;
    leveldb::Slice key, value;
    int32_t size = 0;
    int64_t number = 0;

    for (db_it->SeekToFirst(); db_it->Valid() && !stop; db_it->Next()) {
        ++number;
        if (UNLIKELY(number == sleep_interval)) {
            usleep(sleep_time);
            number = 0;
        }
        key = db_it->key();
        value = db_it->value();
        size = key.size() + value.size();

        LdbStatManager::update(schema, &stat,
                               LdbKey::decode_area_with_key(key.data()),
                               1,
                               size
        );
    }

    int ret = TAIR_RETURN_SUCCESS;
    // it is stopped, so this stat result should be droped
    if (db_it->Valid())
        ret = TAIR_RETURN_FAILED;

    delete db_it;
    return ret;
}

int LdbStatManager::revise_user_stat(volatile bool &stop, LdbInstance *db, LDB_STAT &stat,
                                     int64_t sleep_interval, int32_t sleep_time) {
    leveldb::ReadOptions options;
    options.fill_cache = false;
    options.hold_for_long = true;
    leveldb::Iterator *db_it = db->db()->NewIterator(options);
    if (NULL == db_it) {
        log_error("get ldb iterator fail");
        return TAIR_RETURN_FAILED;
    }

    const tair::common::StatSchema &schema = schema_;
    leveldb::Slice key;
    int32_t size;
    int64_t number = 0;
    for (db_it->SeekToFirst(); db_it->Valid() && !stop; db_it->Next()) {
        ++number;
        if (UNLIKELY(number == sleep_interval)) {
            usleep(sleep_time);
            number = 0;
        }

        key = db_it->key();
        size = key.size() + db_it->value().size();

        LdbStatManager::update(schema, &stat,
                               LdbKey::decode_area_with_key(key.data()),
                               0,
                               0,
                               1,
                               size - LDB_META_SIZE
        );
    }

    int ret = TAIR_RETURN_SUCCESS;
    // it is stopped, so this stat result should be droped
    if (db_it->Valid())
        ret = TAIR_RETURN_FAILED;

    delete db_it;
    return ret;
}
}
}
}
