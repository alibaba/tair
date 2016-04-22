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
#include "mdb_manager.hpp"
#include "libmdb_c.hpp"
#include "data_entry.hpp"
#include <limits>

using namespace tair;

static void mdb_assign_static_param(const mdb_param_t *param);

mdb_t mdb_init(const mdb_param_t *param) {
    mdb_assign_static_param(param);
    bool use_shm = true;
    if (strcmp(mdb_param::mdb_type, "mdb_shm") == 0) {
        use_shm = true;
    } else if (strcmp(mdb_param::mdb_type, "mdb") == 0) {
        use_shm = false;
    } else {
        log_error("invalid mdb memory type: %s", mdb_param::mdb_type);
        return NULL;
    }
    mdb_manager *db = new mdb_manager;
    if (db == NULL) {
        return NULL;
    }
    if (!db->initialize(use_shm)) {
        delete db;
        return NULL;
    }
    db->set_bucket_count(1023);
    //TAIR_STAT.set_storage_manager(db);
    //TAIR_STAT.start();
    return db;
}

void mdb_destroy(mdb_t db) {
    if (db == NULL) {
        return;
    }
    //~ not multiple inheritance class, no problem
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    if (_db != NULL) {
        delete _db;
    }
}

int mdb_put(mdb_t db, int area, const data_entry_t *key, const data_entry_t *value,
            int version, bool version_care, int expire) {
    data_entry mkey(key->data, key->size, false);
    mkey.merge_area(area);
    mkey.set_version(version);
    data_entry mvalue(value->data, value->size, false);
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    return _db->put(0, mkey, mvalue, version_care, expire);
}

int mdb_get(mdb_t db, int area, const data_entry_t *key, data_entry_t *value, int *version, int *expire) {
    data_entry mkey(key->data, key->size, false);
    mkey.merge_area(area);
    data_entry mvalue;
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    int rc = _db->get(0, mkey, mvalue);
    if (rc == TAIR_RETURN_SUCCESS) {
        //~ we cannot reuse space of mvalue, because of the two merged-area bytes
        value->data = (char *) malloc(mvalue.get_size());
        value->size = mvalue.get_size();
        memcpy(value->data, mvalue.get_data(), value->size);
        if (version != NULL) {
            *version = mkey.data_meta.version;
        }
        if (expire != NULL) {
            *expire = mkey.data_meta.edate;
        }
    }
    return rc;
}

int mdb_del(mdb_t db, int area, const data_entry_t *key, bool version_care) {
    data_entry mkey(key->data, key->size, false);
    mkey.merge_area(area);
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    return _db->remove(0, mkey, version_care);
}

int mdb_add_count(mdb_t db, int area, const data_entry_t *key, int count,
                  int init_value, int expire, int *result) {
    data_entry mkey(key->data, key->size, false);
    mkey.merge_area(area);
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    return _db->add_count(0, mkey, count, init_value, std::numeric_limits<int>::min(),
                          std::numeric_limits<int>::max(), expire, *result);
}

bool mdb_lookup(mdb_t db, int area, const data_entry_t *key) {
    data_entry mkey(key->data, key->size, false);
    mkey.merge_area(area);
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    return _db->lookup(0, mkey);
}

int mdb_clear(mdb_t db, int area) {
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    return _db->clear(area);
}

void mdb_get_stat(mdb_t db, int area, mdb_stat_t *stat) {
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    mdb_area_stat _stat;
    _db->get_stat(area, &_stat);
    stat->quota = _stat.get_quota();
    stat->data_size = _stat.get_data_size();
    stat->space_usage = _stat.get_space_usage();
    stat->item_count = _stat.get_item_count();
    stat->hit_count = _stat.get_hit_count();
    stat->get_count = _stat.get_get_count();
    stat->put_count = _stat.get_put_count();
    stat->remove_count = _stat.get_remove_count();
    stat->evict_count = _stat.get_evict_count();
}

void mdb_set_quota(mdb_t db, int area, uint64_t quota) {
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    _db->set_area_quota(area, quota);
}

uint64_t mdb_get_quota(mdb_t db, int area) {
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    return _db->get_area_quota(area);
}

bool mdb_is_quota_exceed(mdb_t db, int area) {
    mdb_manager *_db = reinterpret_cast<mdb_manager *>(db);
    return _db->is_quota_exceed(area);
}

void mdb_log_file(const char *log) {
    if (log == NULL) return;
    TBSYS_LOGGER.setFileName(log);
}

void mdb_log_level(const char *level) {
    if (level == NULL) return;
    TBSYS_LOGGER.setLogLevel(level);
}

void mdb_assign_static_param(const mdb_param_t *param) {
    if (param->use_check_thread) {
        mdb_param::use_check_thread = true;
    } else {
        mdb_param::use_check_thread = false;
    }
    mdb_param::inst_shift = param->inst_shift;
    if (param->lock_pshared) {
        mdb_param::lock_pshared = true;
    }
    if (param->mdb_type != NULL) {
        mdb_param::mdb_type = param->mdb_type;
    }
    if (param->mdb_path != NULL) {
        mdb_param::mdb_path = param->mdb_path;
    }
    if (param->size != 0) {
        mdb_param::size = param->size;
        mdb_param::size >>= mdb_param::inst_shift;
    }
    if (param->slab_base_size != 0) {
        mdb_param::slab_base_size = param->slab_base_size;
    }
    if (param->page_size != 0) {
        mdb_param::page_size = param->page_size;
    }
    if (param->factor != 0) {
        mdb_param::factor = param->factor;
    }
    if (param->hash_shift != 0) {
        mdb_param::hash_shift = param->hash_shift;
        mdb_param::hash_shift -= mdb_param::inst_shift;
    }
    if (param->chkexprd_time_low != 0) {
        mdb_param::chkexprd_time_low = param->chkexprd_time_low;
    }
    if (param->chkexprd_time_high != 0) {
        mdb_param::chkexprd_time_high = param->chkexprd_time_high;
    }
    if (param->chkslab_time_low != 0) {
        mdb_param::chkslab_time_low = param->chkslab_time_low;
    }
    if (param->chkslab_time_high != 0) {
        mdb_param::chkslab_time_high = param->chkslab_time_high;
    }
    if (param->check_granularity != 0) {
        mdb_param::check_granularity = param->check_granularity;
    }
    if (param->check_granularity_factor != 0) {
        mdb_param::check_granularity_factor = param->check_granularity_factor;
    }
}
