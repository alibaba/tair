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

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)

#include <unordered_map>

#else
#include <tr1/unordered_map>
namespace std {
  using tr1::hash;
  using tr1::unordered_map;
}
#endif

#include <leveldb/env.h>
#include <leveldb/write_batch.h>
#include <util/config.h>

#include <db/db_impl.h>

#include "common/define.hpp"
#include "common/util.hpp"
#include "packets/put_packet.hpp"
#include "packets/mupdate_packet.hpp"
#include "storage/storage_manager.hpp"
#include "storage/mdb/mdb_manager.hpp"
#include "ldb_instance.hpp"
#include "ldb_comparator.hpp"
#include "ldb_bloom.cpp"
#include "ldb_stat_manager.hpp"
#include "rt_profiler.hpp"

namespace tair {
namespace storage {
namespace ldb {
using namespace tair::common;

LdbInstance::LdbInstance()
        : index_(0), db_version_care_(true), mutex_(NULL), db_(NULL), cache_(NULL),
          scan_it_(NULL), scan_bucket_(-1), still_have_(true) {
    db_path_[0] = '\0';
    stat_manager_ = new STAT_MANAGER_MAP();
}

LdbInstance::LdbInstance(int32_t index, bool db_version_care,
                         storage::storage_manager *cache)
        : index_(index), db_version_care_(db_version_care), mutex_(NULL), db_(NULL),
          cache_(dynamic_cast<tair::mdb_manager *>(cache)),
          scan_it_(NULL), scan_bucket_(-1), still_have_(true) {
    if (cache_ != NULL) {
        mutex_ = new tbsys::CThreadMutex[LOCKER_SIZE];
    }
    db_path_[0] = '\0';
    stat_manager_ = new STAT_MANAGER_MAP();
    stat_ = new LdbStatManager(TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USE_BLOOMFILTER, 0) > 0, cache_ != NULL);
}

LdbInstance::~LdbInstance() {
    stop();
    if (mutex_ != NULL) {
        delete[] mutex_;
    }

    if (stat_ != NULL) {
        delete stat_;
    }

    if (stat_manager_ != NULL) {
        delete stat_manager_;
    }

    // delete allocated env.
    if (options_.env != NULL) {
        delete options_.env;
    }
    // delete allocated comparator
    if (options_.comparator != NULL) {
        delete options_.comparator;
    }
    // delete allocated filter policy
    if (options_.filter_policy != NULL) {
        delete options_.filter_policy;
    }
}

int LdbInstance::initialize() {
    // enable to config multi path.. ?
    const char *data_dir = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_DATA_DIR, LDB_DEFAULT_DATA_DIR);

    // because there a third parma  LDB_DEFAULT_DATA_DIR "data/ldb", data_dir is never be NULL.
    /*
    if (NULL == data_dir)
    {
      log_error("ldb data dir path not config, item: %s.%s", TAIRLDB_SECTION, LDB_DATA_DIR);
      ret = TAIR_RETURN_FAILED;
    }
    */
    // leveldb data path
    snprintf(db_path_, sizeof(db_path_), "%s%d/ldb", data_dir, index_ + 1/*TODO: change to index*/);

    if (tbsys::CFileUtil::mkdirs(db_path_) == false) {
        log_error("mkdir fail: %s", db_path_);
        return TAIR_RETURN_FAILED;
    }

    if (stat_->start(db_path_) != TAIR_RETURN_SUCCESS) {
        log_error("start stat fail");
        return TAIR_RETURN_FAILED;
    }

    if (all_area_stat_manager_.start(db_path_) == false) {
        log_error("start old stat fail");
        return TAIR_RETURN_FAILED;
    }

    return TAIR_RETURN_SUCCESS;

}

bool LdbInstance::init_db() {
    bool ret = true;

    if (NULL == db_) {
        sanitize_option();

        log_warn("init ldb %d: table_cache_size: %lu, write_buffer: %zu, use_bloomfilter: %s, use_mmap: %s",
                 index_, options_.table_cache_size, options_.write_buffer_size,
                 options_.filter_policy != NULL ? "yes" : "no",
                 options_.kUseMmapRandomAccess ? "yes" : "no");
        leveldb::Status status = leveldb::DB::Open(options_, db_path_, &db_);

        if (!status.ok()) {
            log_error("ldb init database fail, error: %s", status.ToString().c_str());
            ret = false;
        } else {
            if (!(ret = bg_task_.start(this))) {
                log_error("start bg task fail. destroy db");
            } else if (!(ret = gc_.start(this))) {
                log_error("start gc factory fail. destroy db");
            }

            if (!ret) {
                destroy();
            }
        }
    }
    return ret;
}

bool LdbInstance::init_buckets(const std::vector<int32_t> &buckets) {
    bool ret = init_db();
    if (ret) {
        STAT_MANAGER_MAP *tmp_stat_manager = new STAT_MANAGER_MAP(*stat_manager_);

        for (std::vector<int32_t>::const_iterator it = buckets.begin(); it != buckets.end(); ++it) {
            STAT_MANAGER_MAP_ITER stat_it = tmp_stat_manager->find(*it);
            if (stat_it != tmp_stat_manager->end()) {
                log_debug("bucket %d already inited.", *it);
            } else {
                (*tmp_stat_manager)[*it] = 1;
                log_debug("bucket %d init ok.", *it);
            }
        }

        STAT_MANAGER_MAP *old_stat = stat_manager_;
        stat_manager_ = tmp_stat_manager;
        usleep(100);
        delete old_stat;
    }
    return ret;
}

void LdbInstance::close_buckets(const std::vector<int32_t> &buckets) {
    STAT_MANAGER_MAP *tmp_stat_manager = new STAT_MANAGER_MAP(*stat_manager_);
    std::vector<int32_t> gc_buckets;

    leveldb::Status status = db_->ForceFlush();
    if (!status.ok()) {
        // force flush fail, ignore it anyway
        log_error("ForceFlush fail: %s in close_buckets", status.ToString().c_str());
    }

    // for gc. when close, bucket can't write (migrate maybe)
    // so it is ok to get sequence and file_number one time.
    for (std::vector<int32_t>::const_iterator it = buckets.begin(); it != buckets.end(); ++it) {
        STAT_MANAGER_MAP_ITER stat_it = tmp_stat_manager->find(*it);

        if (stat_it == tmp_stat_manager->end()) {
            log_warn("close bucket %d fail: not exist.", *it);
        } else {
            gc_buckets.push_back(*it);
            tmp_stat_manager->erase(stat_it);
        }
    }

    STAT_MANAGER_MAP *old_stat = stat_manager_;
    stat_manager_ = tmp_stat_manager;
    usleep(40);
    // add gc
    gc_.add(gc_buckets, GC_BUCKET);

    delete old_stat;

    // no bucket exist. destory db
    if (stat_manager_->empty()) {
        log_warn("ldb instance %d own none bucket now.", index_);
        // This instance will not service any more, just not destroy now
        // destroy();
    }
}

void LdbInstance::stop() {
    gc_.stop();
    bg_task_.stop();

    log_info("stop ldb %s", db_path_);
    if (db_ != NULL) {
        delete db_;
        db_ = NULL;
    }
    all_area_stat_manager_.stop();
}

void LdbInstance::destroy() {
    stop();
    // not delete all file now
#if 0
    gc_.destroy();

    leveldb::Status status = leveldb::DestroyDB(db_path_, options_);
    if (!status.ok())
    {
      log_error("remove ldb database fail. path: %s, error: %s", db_path_, status.ToString().c_str());
    }
    else
    {
      log_debug("destroy ldb database ok: %s", db_path_);
    }
#endif
}

int LdbInstance::put(int bucket_number, tair::common::data_entry &key,
                     tair::common::data_entry &value,
                     bool version_care, int expire_time, bool mc_ops) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    uint32_t cdate = 0, mdate = 0, edate = 0;
    int32_t stat_data_size = 0, stat_use_size = 0, item_count = 1;
    if (key.data_meta.cdate == 0 || version_care) {
        cdate = time(NULL);
        mdate = cdate;
        edate = get_real_edate(expire_time, mdate, mc_ops);
    } else {
        cdate = key.data_meta.cdate;
        mdate = key.data_meta.mdate;
        edate = key.data_meta.edate;
    }

    LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number, edate);
    LdbItem ldb_item;
    std::string db_value;
    bool mtime_care = is_mtime_care(key);
    bool need_get = (db_version_care_ && version_care) || mtime_care;
    bool need_op = true;    // need do operation
    int rc = TAIR_RETURN_SUCCESS;
    PROFILER_BEGIN("db lock");
    RTProbe probe_mtx;
    probe_mtx.enter(PSN_DB_MUTEX_LOCK);
    tbsys::CThreadGuard mutex_guard(get_mutex(key));
    probe_mtx.leave();
    PROFILER_END();

    // version care or mtime_care
    if (need_get) {
        PROFILER_BEGIN("db get");
        rc = do_get(ldb_key, db_value, true/* get from cache */,
                    false/* not fill cache */, false/* not update cache stat */);
        PROFILER_END();
        if (TAIR_RETURN_SUCCESS == rc) {
            ldb_item.assign(const_cast<char *>(db_value.data()), db_value.size());
            if (expire_time < 0) {
                // may the key get from mdb, it wiil lost edate, so...
                ldb_key.build_key_meta(bucket_number, ldb_item.edate());
            }
            // db mtime is later than request, then need not do operation any more
            need_op = !(mtime_care && ldb_item.mdate() > key.data_meta.mdate);
            log_debug("@@ mtime care: %d,np:%d, %d <> %d, ldb_item.version:%d,key.data_meta.version:%d",
                      mtime_care, need_op, ldb_item.mdate(), key.data_meta.mdate, ldb_item.version(),
                      key.data_meta.version);
            if (!need_op) {
                rc = TAIR_RETURN_MTIME_EARLY;
            } else {
                // ldb already check expired. no need here.
                cdate = ldb_item.cdate(); // set back the create time
                if (version_care) {
                    // item care version, check version
                    if (key.data_meta.version != 0
                        && key.data_meta.version != ldb_item.version()) {
                        rc = TAIR_RETURN_VERSION_ERROR;
                    }
                }

                // NOTE:
                //    1. all updates to data with expiretime are considered as
                //       a new put statistics. real statistics will be update later.
                //    2. stat_use_size is increased all the way, stat will be updated
                //       when data is really merged.
                if (rc == TAIR_RETURN_SUCCESS && ldb_item.edate() == 0) {
                    stat_data_size -= ldb_key.key_size() + ldb_item.value_size();
                    item_count = 0;
                }
            }
        } else if (key.get_version() == 0 || mc_ops == false) {
            rc = TAIR_RETURN_SUCCESS; // get fail does not matter, because this is normal set
        }
        // else if(key.get_version() != 0)  //This is CAS(Check and Set) function, if key is not existed, must return NOT FOUND.
        //Define in memcached_session.cpp:CastTairReturnCode
        //This return code is return from do_get, we needn't reset it.
    }

    if (need_op && rc == TAIR_RETURN_SUCCESS) {
        ldb_item.meta().base_.meta_version_ = META_VER_PREFIX;
        ldb_item.meta().base_.flag_ = value.data_meta.flag | TAIR_ITEM_FLAG_NEWMETA;
        ldb_item.meta().base_.cdate_ = cdate;
        ldb_item.meta().base_.mdate_ = mdate;
        if (expire_time >= 0) {
            ldb_item.meta().base_.edate_ = edate;
        }
        ldb_item.set_prefix_size(key.get_prefix_size());
        if (version_care) {
            ldb_item.meta().base_.version_ =
                    UNLIKELY(ldb_item.version() == TAIR_DATA_MAX_VERSION - 1) ? 1 : ldb_item.version() + 1;
        } else {
            ldb_item.meta().base_.version_ = key.data_meta.version;
        }

        ldb_item.set(value.get_data(), value.get_size());
        log_debug("meta_version:%u ,prefix_size %u, total_size:%u, valuesize:%u edate:%u",
                  ldb_item.meta().base_.meta_version_, key.get_prefix_size(), ldb_item.size(), value.get_size(),
                  ldb_item.edate());

        PROFILER_BEGIN("db put");
        rc = do_put(ldb_key, ldb_item,
                    SHOULD_PUT_FILL_CACHE(key.data_meta.flag),
                    is_synced(key),
                    is_meta_flag_unit(key.data_meta.flag),
                    mc_ops);
        PROFILER_END();

        if (TAIR_RETURN_SUCCESS == rc) {
            if (!need_get && (key.data_meta.flag & TAIR_SERVER_WITH_STAT)) {
                // get stat information.
                item_count = value.data_meta.keysize;
                stat_data_size = value.data_meta.valsize;
            } else {
                stat_data_size += ldb_key.key_size() + ldb_item.value_size();
                // need stat data
                // WARNING: We need duplicate stat information but I'm exhausted
                //          to find a room for them.
                //          Lash me for this...
                value.data_meta.keysize = item_count;
                value.data_meta.valsize = stat_data_size;
            }
            stat_use_size += ldb_key.size() + ldb_item.size();

            // stat_data_size > 0 then stat_use_size > 0
            if (stat_data_size > 0) {
                stat_add(bucket_number, key.area, stat_data_size, stat_use_size, item_count);
            } else if (stat_data_size < 0) {
                stat_sub(bucket_number, key.area, -stat_data_size, -stat_use_size, item_count);
            }
        }

        //update key's meta info
        key.data_meta.flag = ldb_item.flag();
        key.data_meta.cdate = ldb_item.cdate();
        key.data_meta.edate = ldb_item.edate();
        key.data_meta.mdate = ldb_item.mdate();
        key.data_meta.version = ldb_item.version();
        key.data_meta.keysize = key.get_size();
        key.data_meta.valsize = value.get_size();
    }

    log_debug("ldb::put %d, key len: %d, key prefix len:%d , value len: %d", rc, key.get_size(), key.get_prefix_size(),
              value.get_size());
    return rc;
}

//update data if this key existed: for mc_ops replace, append, prepend
int LdbInstance::update(int bucket_number, tair::common::data_entry &key, tair::common::data_entry &value,
                        uint8_t ldb_update_type, bool version_care, int expire_time, bool mc_ops,
                        data_entry *p_new_value) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = TAIR_RETURN_SUCCESS;
    uint32_t cdate = 0, mdate = 0, edate = 0;
    int stat_data_size = 0, stat_use_size = 0, item_count = 0;
    mdate = time(NULL);
    edate = get_real_edate(expire_time, mdate, mc_ops);
    LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number, edate);
    LdbItem ldb_item;
    std::string db_value;

    tbsys::CThreadGuard mutex_guard(get_mutex(key));
    rc = do_get(ldb_key, db_value, true/* get from cache */,
                false/* not fill cache */, false/* not update cache stat */, true/*mc ops*/);
    if (TAIR_RETURN_SUCCESS == rc) {
        ldb_item.assign(const_cast<char *>(db_value.data()), db_value.size());
        if (version_care) {
            // item care version, check version
            if (key.data_meta.version != 0
                && key.data_meta.version != ldb_item.version()) {
                rc = TAIR_RETURN_VERSION_ERROR;
                return rc;
            }
        }
        // set value
        static int mc_header_size = mc_ops ? sizeof(int32_t) : 0;
        switch (ldb_update_type) {
            {
                case LDB_TYPE_UPDATE :
                    ldb_item.meta().base_.version_ =
                            UNLIKELY(ldb_item.version() == TAIR_DATA_MAX_VERSION - 1) ? 1 : ldb_item.version() + 1;
                ldb_item.set(const_cast<char *>(value.get_data()), value.get_size());
                stat_use_size = stat_data_size = (value.get_size() - ldb_item.value_size());
                ldb_item.meta().base_.edate_ = edate;
                break;
            }
            {
                case LDB_TYPE_TOUCH :
                    //  do nothing just write back old value with new expire
                    //edate = tair::util::coding_util::decode_fixed32(key.get_data());
                    ldb_item.meta().base_.edate_ = edate;
                break;
            }
            {
                case LDB_TYPE_APPEND :
#ifdef INTERNALTEST
                    // run below just after configure --with-internal-test=yes
                    do {
                      char* copy_key = (char*)malloc(sizeof(char) * (key.get_size() - 1));
                      char* copy_val = (char*)malloc(sizeof(char) * (value.get_size() + 1));
                      memcpy(copy_key, key.get_data() + 2, key.get_size() - 2);
                      memcpy(copy_val, value.get_data(), value.get_size());
                      copy_key[key.get_size() - 2] = '\0';
                      copy_val[value.get_size()] = '\0';
                      log_error("RSYNC_ORDER_TEST(%s %s)", copy_key, copy_val);
                      free(copy_key);
                      free(copy_val);
                    } while(false);
#endif
                    char *new_value = (char *) malloc(db_value.size() + value.get_size());
                memcpy(new_value, db_value.data(), db_value.size());
                memcpy(new_value + db_value.size(), value.get_data(), value.get_size()); //skip mc_header
                //value.get_data() + mc_header_size, value.get_size() - mc_header_size); //skip mc_header
                ldb_item.delegate(const_cast<char *>(new_value), db_value.size() + value.get_size());
                stat_use_size = stat_data_size = value.get_size();
                break;
            }
            {
                case LDB_TYPE_PREPEND :
                    char *new_value = (char *) malloc(db_value.size() + value.get_size());
                int meta_size = ldb_item.size() - ldb_item.value_size();
                memcpy(new_value, ldb_item.data(), meta_size + mc_header_size); //set META
                memcpy(new_value + meta_size + mc_header_size, value.get_data(), value.get_size());
                memcpy(new_value + meta_size + mc_header_size + value.get_size(),
                       ldb_item.value() + mc_header_size, ldb_item.value_size() - mc_header_size);
                //memcpy(new_value, ldb_item.data(), meta_size); //set META
                //memcpy(new_value + meta_size, value.get_data(), value.get_size());
                //memcpy(new_value + meta_size + value.get_size(),
                //       ldb_item.value() + mc_header_size, ldb_item.value_size() - mc_header_size);
                ldb_item.delegate(const_cast<char *>(new_value), db_value.size() + value.get_size());
                stat_use_size = stat_data_size = value.get_size();
                break;
            }
        }
        //do not change cdate
        ldb_item.meta().base_.mdate_ = mdate;
        ldb_item.set_prefix_size(key.get_prefix_size()); // may be useless in mc_ops
        // set meta

        if (version_care) {
            ldb_item.meta().base_.version_ =
                    UNLIKELY(ldb_item.version() == TAIR_DATA_MAX_VERSION - 1) ? 1 : ldb_item.version() + 1;
        }

        log_debug(
                "ldb::update %d, key len: %d, key prefix len:%d , value len: %u, update_type:%d version:%d expire:%d\n",
                rc, key.get_size(), key.get_prefix_size(), ldb_item.size(), ldb_update_type,
                ldb_item.meta().base_.version_, ldb_item.meta().base_.edate_);
        PROFILER_BEGIN("db put:update");
        rc = do_put(ldb_key, ldb_item,
                    SHOULD_PUT_FILL_CACHE(key.data_meta.flag),
                    is_synced(key),
                    is_meta_flag_unit(key.data_meta.flag),
                    true);
        PROFILER_END();

        if (rc == TAIR_RETURN_SUCCESS) {
            if (stat_data_size > 0) {
                stat_add(bucket_number, key.area, stat_data_size, stat_use_size, item_count);
            } else if (stat_data_size < 0) {
                stat_sub(bucket_number, key.area, -stat_data_size, -stat_use_size, item_count);
            }
            //update key's meta info
            key.data_meta.flag = ldb_item.flag();
            key.data_meta.cdate = cdate;
            key.data_meta.edate = edate;
            key.data_meta.mdate = mdate;
            //key.data_meta.version = ldb_item.version();
            key.data_meta.version = ldb_item.version();
            key.data_meta.keysize = key.get_size();
            key.data_meta.valsize = value.get_size();

            if (p_new_value != NULL) {
                char *buf = new char[ldb_item.value_size()];
                memcpy(buf, ldb_item.value(), ldb_item.value_size());
                p_new_value->set_alloced_data(buf, ldb_item.value_size());
                p_new_value->data_meta.flag = ldb_item.flag();
                p_new_value->data_meta.cdate = cdate;
                p_new_value->data_meta.edate = edate;
                p_new_value->data_meta.mdate = mdate;
                p_new_value->data_meta.version = ldb_item.version();
                p_new_value->data_meta.keysize = key.get_size();
                p_new_value->data_meta.valsize = ldb_item.value_size();
            }
        }
    } else {
        log_warn("update failed rc:%d", rc);
        //ldb_item.set(value.get_data(), value.get_size());
        //data not exist or error. do nothing. just return
    }
    return rc;
}

int LdbInstance::add(int bucket_number, data_entry &key, data_entry &value, bool version_care, int expire_time) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = TAIR_RETURN_SUCCESS;
    LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number, get_real_edate(expire_time, time(NULL), true));
    LdbItem ldb_item;
    std::string db_value;

    tbsys::CThreadGuard mutex_guard(get_mutex(key));
    rc = do_get(ldb_key, db_value, true/* get from cache */,
                false/* not fill cache */, false/* not update cache stat */, true/*mc ops*/);
    log_debug("do_get rc:%d db_value size:%lu", rc, db_value.size());
    if (TAIR_RETURN_DATA_NOT_EXIST == rc) {
        int stat_use_size, stat_data_size;
        stat_data_size = ldb_key.key_size() + ldb_item.value_size();
        stat_use_size = ldb_key.size() + ldb_item.size();
        PROFILER_BEGIN("db put:add");
        rc = do_put_and_set_meta(key, ldb_key, ldb_item, bucket_number, stat_data_size, stat_use_size, 1, expire_time,
                                 version_care, LDB_TYPE_ADD, &value);
        PROFILER_END();
    } else if (TAIR_RETURN_SUCCESS == rc) {
        rc = TAIR_RETURN_KEY_EXISTS;
    }
    return rc;
}

// memcached-style incr-decr
int LdbInstance::incr_decr(int bucket_number, data_entry &key, uint64_t delta, uint64_t init,
                           bool is_incr, int expire, uint64_t &result, data_entry *p_new_value) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = TAIR_RETURN_SUCCESS;
    int stat_data_size = 0, stat_use_size = 0, item_count = 0;
    uint64_t curr_val = 0;
    LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number, get_real_edate(expire, time(NULL), true));
    LdbItem ldb_item;
    std::string db_value;
    static const int mc_header_size = sizeof(int32_t);
    static const int max_ascii_counter_len = 21; // 1 + max length of ascii representation of uint64
    uint8_t type = LDB_TYPE_UPDATE;

    tbsys::CThreadGuard mutex_guard(get_mutex(key));
    rc = do_get(ldb_key, db_value, true/* get from cache */,
                false/* not fill cache */, false/* not update cache stat */, true/*mc ops*/);
    if (TAIR_RETURN_SUCCESS == rc) {
        // data exists, update this counter
        bool is_numeric = true;
        ldb_item.assign(const_cast<char *>(db_value.data()), db_value.size());
        if (IS_ADDCOUNT_TYPE(ldb_item.flag())) {
            curr_val = tair::util::string_util::strntoul(ldb_item.value() + mc_header_size,
                                                         ldb_item.value_size() - mc_header_size);
        } else
            // revert string to int
        {
            for (int i = mc_header_size; i < ldb_item.value_size(); ++i) {
                char c = *(ldb_item.value() + i);
                if (c < '0' || c > '9') {
                    is_numeric = false;
                    return TAIR_RETURN_CANNOT_OVERRIDE;
                }
            }
            // check if the result would be negative, if so, return the current value
            curr_val = is_numeric ?
                       tair::util::string_util::strntoul(ldb_item.value() + mc_header_size,
                                                         ldb_item.value_size() - mc_header_size) : 0;
            // calculate datasize delta
            stat_data_size = max_ascii_counter_len - ldb_item.value_size(); //not exactlly: FIXME
            stat_use_size = stat_data_size;
        }
        if (is_incr) {
            curr_val += delta;
        } else {
            curr_val = delta > curr_val ? 0 : curr_val - delta;
        }
    } else if (TAIR_RETURN_DATA_NOT_EXIST == rc)
        // data not exist. add this counter
    {
        if (expire == -1) {
            return TAIR_RETURN_DATA_NOT_EXIST;
        }
        type = LDB_TYPE_ADD;
        item_count = 1;
        curr_val = init;
        //not exactlly: FIXME
        stat_data_size = key.get_size() + max_ascii_counter_len;
        stat_use_size = key.get_size() + sizeof(LdbItemMeta) + mc_header_size + max_ascii_counter_len;
    }
    char *new_value = (char *) malloc(mc_header_size + max_ascii_counter_len);
    memset(new_value, 0, mc_header_size + max_ascii_counter_len);
    int counter_size = sprintf(new_value + mc_header_size, "%lu", curr_val);
    ldb_item.set(const_cast<char *>(new_value), mc_header_size + counter_size);
    free(new_value);

    rc = do_put_and_set_meta(key, ldb_key, ldb_item, bucket_number, stat_data_size, stat_use_size, item_count, expire,
                             true, type, NULL);
    if (rc == TAIR_RETURN_SUCCESS) {
        result = curr_val;
    }
    if (p_new_value != NULL) {
        char *buf = new char[ldb_item.value_size()];
        memcpy(buf, ldb_item.value(), ldb_item.value_size());
        p_new_value->set_alloced_data(buf, ldb_item.value_size());
        p_new_value->data_meta.flag = ldb_item.flag();
        p_new_value->data_meta.cdate = ldb_item.cdate();
        p_new_value->data_meta.edate = ldb_item.edate();
        p_new_value->data_meta.mdate = ldb_item.mdate();
        p_new_value->data_meta.version = ldb_item.version();
        p_new_value->data_meta.keysize = key.get_size();
        p_new_value->data_meta.valsize = ldb_item.value_size();
    }

    return rc;
}

// after do_get, update meta to ldb_item and key
// key : passed from user (key's meta will updating)
// ldb_item : get from ldb, update and put it into ldb.
int
LdbInstance::do_put_and_set_meta(data_entry &key, LdbKey &ldb_key, LdbItem &ldb_item, int bucket_number, int data_size,
                                 int use_size, int item_count, int expire, bool version_care, uint8_t ldb_update_type,
                                 data_entry *value) {
    int rc = TAIR_RETURN_FAILED;

    uint32_t cdate = 0, mdate = 0, edate = 0;
    mdate = time(NULL);
    if (expire == -1)
        edate = ldb_item.edate();
    else
        edate = get_real_edate(expire, mdate, true);

    LdbKey::build_key_meta(ldb_key.data(), bucket_number, edate);

    // set meta to ldb_item
    if (version_care) {
        ldb_item.meta().base_.version_ =
                UNLIKELY(ldb_item.version() == TAIR_DATA_MAX_VERSION - 1) ? 1 : ldb_item.version() + 1;
    }
    if (ldb_update_type == LDB_TYPE_ADD) {
        ldb_item.meta().base_.cdate_ = mdate;
    }
    ldb_item.meta().base_.mdate_ = mdate;
    ldb_item.meta().base_.edate_ = edate;
    ldb_item.set_prefix_size(key.get_prefix_size()); // may be useless in mc_ops
    if (value != NULL) {
        ldb_item.set(value->get_data(), value->get_size());
    } else {
        //ldb_item.set(ldb_item.value(), ldb_item.value_size());// ERROR code
    }

    PROFILER_BEGIN("db put");
    log_debug("ldb::put, key len: %d, key prefix len:%d , value len: %u, version:%d expire:%d, mdate:%d, cdate:%d\n",
              key.get_size(), key.get_prefix_size(), ldb_item.size(),
              ldb_item.meta().base_.version_, ldb_item.meta().base_.edate_, ldb_item.meta().base_.mdate_,
              ldb_item.meta().base_.cdate_);
    rc = do_put(ldb_key, ldb_item,
                SHOULD_PUT_FILL_CACHE(key.data_meta.flag),
                is_synced(key),
                is_meta_flag_unit(key.data_meta.flag),
                true);
    PROFILER_END();

    if (rc == TAIR_RETURN_SUCCESS) {
        if (data_size > 0) {
            stat_add(bucket_number, key.area, data_size, use_size, item_count);
        } else if (data_size < 0) {
            stat_sub(bucket_number, key.area, -data_size, -use_size, item_count);
        }
        //update key's meta info
        key.data_meta.flag = ldb_item.flag();
        key.data_meta.cdate = cdate;
        key.data_meta.edate = edate;
        key.data_meta.mdate = mdate;
        key.data_meta.version = ldb_item.version();
        key.data_meta.keysize = key.get_size();
        key.data_meta.valsize = ldb_item.value_size();
    }
    return rc;
}

void LdbInstance::add_prefix(LdbKey &ldb_key, int prefix_size) {
    ldb_key.incr_key(prefix_size);
}

void LdbInstance::fill_meta(data_entry *data, LdbKey &key, LdbItem &item) {
    data->data_meta.flag = item.flag();
    data->data_meta.mdate = item.mdate();
    data->data_meta.cdate = item.cdate();
    data->data_meta.edate = item.edate();
    data->data_meta.version = item.version();
    data->data_meta.valsize = item.value_size();
    data->data_meta.keysize = key.key_size();
}

typedef struct UpdateStat {
    UpdateStat() : item_count_(0), data_size_(0), use_size_(0) {}

    int32_t item_count_;
    int32_t data_size_;
    int32_t use_size_;
} UpdateStat;
typedef struct OneUpdateStat {
    OneUpdateStat() : physical_itemcount_(0), physical_datasize_(0) {}

    int32_t physical_itemcount_;
    int32_t physical_datasize_;
} OneUpdateStat;

int LdbInstance::direct_mupdate(int32_t bucket_number, const tair_operc_vector &kvs) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    leveldb::WriteBatch batch;
    const bool synced = true;
    unordered_map <int32_t, UpdateStat> stats;
    unordered_map <int32_t, OneUpdateStat> new_stats;

    for (size_t i = 0; i < kvs.size(); ++i) {
        uint8_t type = kvs[i]->operation_type;
        data_entry &key = *kvs[i]->key;
        LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number,
                       kvs[i]->value != NULL ? kvs[i]->value->data_meta.edate : 0);
        LdbItem ldb_item;
        int32_t area = key.get_area();

        unordered_map<int32_t, UpdateStat>::iterator it = stats.find(area);
        if (it == stats.end()) {
            it = stats.insert(std::pair<int32_t, UpdateStat>(area, UpdateStat())).first;
        }
        UpdateStat &ustat = it->second;

        //new stat
        OneUpdateStat &one_new_stats = new_stats[area];

        if (type == 1) {
            data_entry &value = *kvs[i]->value;
            ldb_item.meta().base_.meta_version_ = META_VER_PREFIX;
            ldb_item.meta().base_.flag_ = value.data_meta.flag | TAIR_ITEM_FLAG_NEWMETA;
            ldb_item.meta().base_.cdate_ = value.data_meta.cdate;
            ldb_item.meta().base_.mdate_ = value.data_meta.mdate;
            ldb_item.meta().base_.edate_ = value.data_meta.edate;
            ldb_item.meta().base_.version_ = value.data_meta.version;
            ldb_item.set_prefix_size(key.get_prefix_size());
            ldb_item.set(value.get_data(), value.get_size());

            batch.Put(leveldb::Slice(ldb_key.data(), ldb_key.size()),
                      leveldb::Slice(ldb_item.data(), ldb_item.size()), synced);

            ++ustat.item_count_;
            ustat.data_size_ += ldb_key.key_size() + ldb_item.value_size();
            ustat.use_size_ += ldb_key.size() + ldb_item.size();

            //new stat
            ++one_new_stats.physical_itemcount_;
            one_new_stats.physical_datasize_ += ldb_key.size() + ldb_item.size();
        } else if (type == 2) {
            // synced, no tailer
            batch.Delete(leveldb::Slice(ldb_key.data(), ldb_key.size()), synced);
            // not correct
            --ustat.item_count_;
            ustat.data_size_ -= ldb_key.key_size() + ldb_item.value_size();
            ustat.use_size_ -= ldb_key.size() + ldb_item.size();

            //new stat
            ++one_new_stats.physical_itemcount_;
            one_new_stats.physical_datasize_ += ldb_key.size() + ldb_item.size();
        } else {
            log_error("unkown mupdate type: %d", type);
        }
    }

    leveldb::Status status = db_->Write(write_options_, &batch, bucket_number);
    if (!status.ok()) {
        log_error("direct update fail. %s", status.ToString().c_str());
    } else {
        // update stat
        for (unordered_map<int32_t, UpdateStat>::iterator it = stats.begin(); it != stats.end(); ++it) {
            stat_add(bucket_number, it->first, it->second.data_size_, it->second.use_size_, it->second.item_count_);
        }
        // update new stat
        for (unordered_map<int32_t, OneUpdateStat>::const_iterator it = new_stats.begin(); it != new_stats.end(); ++it)
            stat_->update_physical_stat(it->first, it->second.physical_itemcount_, it->second.physical_datasize_);
    }
    return status.ok() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
}

// batch_put is for importing data as fast as possible, so lock and cache/db consistency is ignored,
// once used when db is running, it may have the risk of data inconsistency.
int LdbInstance::batch_put(int bucket_number, int area, tair::common::mput_record_vec *record_vec, bool version_care) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (record_vec->size() <= 0) {
        return TAIR_RETURN_FAILED;
    }

    leveldb::WriteBatch batch;
    // old statistics code, will be removed in future.
    int stat_data_size = 0, stat_use_size = 0, item_count = record_vec->size();
    // new statistics code
    int32_t new_stat_use_item_count = 0, new_stat_use_size = 0;

    int rc = TAIR_RETURN_SUCCESS;

    // NO lock here, cache maybe dirty
    for (tair::common::mput_record_vec::iterator it = record_vec->begin(); it != record_vec->end(); ++it) {
        data_entry &key = *((*it)->key);
        data_entry &value = (*it)->value->get_d_entry();
        uint32_t cdate = 0, mdate = 0, edate = 0;
        if (key.data_meta.cdate == 0 || version_care) {
            cdate = time(NULL);
            mdate = cdate;
            edate = (*it)->value->get_expire();
            if (edate > 0 && edate < static_cast<uint32_t>(mdate)) {
                edate += mdate;
            }
        } else {
            cdate = key.data_meta.cdate;
            mdate = key.data_meta.mdate;
            edate = key.data_meta.edate;
        }

        data_entry mkey = key;
        // merge area
        mkey.merge_area(area);

        LdbKey ldb_key(mkey.get_data(), mkey.get_size(), bucket_number, edate);
        LdbItem ldb_item;
        // db version care
        if (db_version_care_ && version_care) {
            std::string db_value;
            rc = do_get(ldb_key, db_value, false, false); // not fill cache or update cache stat

            if (TAIR_RETURN_SUCCESS == rc) {
                ldb_item.assign(const_cast<char *>(db_value.data()), db_value.size());
                // ldb already check expired. no need here.
                cdate = ldb_item.cdate(); // set back the create time
                if (version_care) {
                    // item care version, check version
                    if (key.data_meta.version != 0
                        && key.data_meta.version != ldb_item.version()) {
                        rc = TAIR_RETURN_VERSION_ERROR;
                    }
                }

                if (rc == TAIR_RETURN_SUCCESS) {
                    stat_data_size -= ldb_key.key_size() + ldb_item.value_size();
                    stat_use_size -= ldb_key.size() + ldb_item.size();
                    item_count--;
                }
            } else {
                rc = TAIR_RETURN_SUCCESS; // get fail does not matter
            }
        }

        if (TAIR_RETURN_SUCCESS != rc) {
            break;
        }

        // just remove cache.
        // avoid cache lock, we can clear all cache when dump over
        // if (cache_ != NULL)
        // {
        //   cache_->raw_remove(ldb_key.key(), ldb_key.key_size());
        // }

        ldb_item.set_prefix_size(key.get_prefix_size());
        ldb_item.meta().base_.meta_version_ = META_VER_PREFIX;
        ldb_item.meta().base_.flag_ = value.data_meta.flag | TAIR_ITEM_FLAG_NEWMETA;
        ldb_item.meta().base_.cdate_ = cdate;
        ldb_item.meta().base_.mdate_ = mdate;
        ldb_item.meta().base_.edate_ = edate;

        if (version_care) {
            ldb_item.meta().base_.version_++;
        } else {
            ldb_item.meta().base_.version_ = key.data_meta.version;
        }

        ldb_item.set(value.get_data(), value.get_size());
        batch.Put(leveldb::Slice(ldb_key.data(), ldb_key.size()),
                  leveldb::Slice(ldb_item.data(), ldb_item.size()));

        new_stat_use_size += ldb_key.size() + ldb_item.size();
        ++new_stat_use_item_count;

        stat_data_size += ldb_key.key_size() + ldb_item.value_size();
        stat_use_size += ldb_key.size() + ldb_item.size();

        //update key's meta info
        key.data_meta.flag = ldb_item.flag();
        key.data_meta.cdate = ldb_item.cdate();
        key.data_meta.edate = edate;
        key.data_meta.mdate = ldb_item.mdate();
        key.data_meta.version = ldb_item.version();
        key.data_meta.keysize = key.get_size();
        key.data_meta.valsize = value.get_size();
    }

    if (TAIR_RETURN_SUCCESS == rc) {
        leveldb::Status status = db_->Write(write_options_, &batch, bucket_number);

        if (!status.ok()) {
            log_error("update batch ldb fail. %s", status.ToString().c_str());
            rc = TAIR_RETURN_FAILED;
        } else {
            stat_add(bucket_number, area, stat_data_size, stat_use_size, item_count);

            stat_->update_physical_stat(area, new_stat_use_item_count, new_stat_use_size);
        }
    }

    return rc;
}

int
LdbInstance::get_range(int bucket_number, data_entry &key_start, data_entry &key_end, int offset, int limit, int type,
                       std::vector<data_entry *> &result, bool &has_next) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }
    int max_retcnt = std::min(leveldb::config::kLimitGetRange, limit);

    int total_size = 0;
    bool end_break = false, scan_overflow = false;

    leveldb::Iterator *iter;
    int rc = TAIR_RETURN_SUCCESS;
    int count = 0;
    LdbKey ldb_key;
    LdbItem ldb_item;
    static int range_max_size = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_RANGE_MAX_SIZE, 1 << 20); // 1M
    data_entry *nkey = NULL, *nvalue = NULL;

    if (CMD_RANGE_ALL != type && CMD_RANGE_VALUE_ONLY != type && CMD_RANGE_KEY_ONLY != type) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (RANGE_FROM_CUR != offset && RANGE_FROM_NEXT != offset) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    log_debug("start range. startkey size:%d prefixsize:%d key:%s,%s.", key_start.get_size(),
              key_start.get_prefix_size(), key_start.get_data() + 4,
              key_start.get_data() + 2 + key_start.get_prefix_size());

    leveldb::ReadOptions scan_read_options = read_options_;
    scan_read_options.fill_cache = false;
    iter = db_->NewIterator(scan_read_options);

    LdbKey ldbkey(key_start.get_data(), key_start.get_size(), bucket_number);
    LdbKey ldbendkey(key_end.get_data(), key_end.get_size(), bucket_number);

    // if key_end's skey is "",  add_prefix to key_end for scan all prefix.
    if (key_end.get_size() - key_end.get_prefix_size() == TAIR_AREA_ENCODE_SIZE) {
        add_prefix(ldbendkey, key_end.get_prefix_size());
    }
    leveldb::Slice slice_key_start(ldbkey.data(), ldbkey.size());
    leveldb::Slice slice_key_end(ldbendkey.data(), ldbendkey.size());
    iter->SetBrake(&slice_key_end, leveldb::config::kLimitGetRange);
    iter->EnableStat();

    //iterate all keys
    for (iter->Seek(slice_key_start); iter->Valid() && count < max_retcnt; iter->Next()) {
        leveldb::Slice slice_iter_key(iter->key().data(), iter->key().size());
        if (options_.comparator->Compare(slice_iter_key, slice_key_end) >= 0) {
            end_break = true;
            break;
        }

        if (iter->ScanTimes() > leveldb::config::kLimitRangeScanTimes) {
            scan_overflow = true;
            break;
        }

        ldb_key.assign(const_cast<char *>(iter->key().data()), iter->key().size());
        ldb_item.assign(const_cast<char *>(iter->value().data()), iter->value().size());
        if (ldb_item.prefix_size() != key_start.get_prefix_size()) {
            log_debug("iter key skip. size:%d prefixsize:%d key:%s,%s. iter prefixsize:%d", key_start.get_size(),
                      key_start.get_prefix_size(), key_start.get_data() + 4,
                      key_start.get_data() + 2 + key_start.get_prefix_size(), ldb_item.prefix_size());
            continue;
        }

        if (offset > 0) {
            offset--;
            continue;
        }

        if (CMD_RANGE_ALL == type || CMD_RANGE_KEY_ONLY == type) {
            nkey = new data_entry(ldb_key.key() + key_start.get_prefix_size() + LDB_KEY_AREA_SIZE,
                                  ldb_key.key_size() - key_start.get_prefix_size() - LDB_KEY_AREA_SIZE, true);
            fill_meta(nkey, ldb_key, ldb_item);
            result.push_back(nkey);
            total_size += ldb_key.size();
        }
        if (CMD_RANGE_ALL == type || CMD_RANGE_VALUE_ONLY == type) {
            nvalue = new data_entry(ldb_item.value(), ldb_item.value_size(), true);
            fill_meta(nvalue, ldb_key, ldb_item);
            result.push_back(nvalue);
            total_size += ldb_item.size();
        }

        count++;

        if (total_size > range_max_size)
            break;
    }

    // has_next will set to true when satisfy either of the following two conditions
    // 1. total_size exceed
    // 2. kLimitGetRange is smaller than limit, retcnt reach kLimitGetRange
    if (limit != count && !end_break && iter->Valid()) {
        has_next = true;
    } else {
        has_next = false;
    }

    if (0 == count) {
        rc = TAIR_RETURN_DATA_NOT_EXIST;
    }
    if (!iter->status().ok()) {
        //~ brake-limit or other
        rc = TAIR_RETURN_FAILED;
        log_error("get_range iter error: %s, area: %d, bucket %d",
                  iter->status().ToString().c_str(), key_start.get_area(), bucket_number);
    }
    if (scan_overflow) {
        //~ scan overflow
        rc = TAIR_RETURN_FAILED;
        log_error("get_range reach scan-limit: area: %d, bucket %d",
                  key_start.get_area(), bucket_number);
    }
    if (NULL != iter) {
        delete iter;
    }
    return rc;
}

int LdbInstance::get(int bucket_number, tair::common::data_entry &key, tair::common::data_entry &value) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number);
    LdbItem ldb_item;
    std::string db_value;

    // first get from cache, no need lock here
    PROFILER_BEGIN("direct cache get");
    int rc = do_cache_get(ldb_key, db_value, true/* update stat */);
    PROFILER_END();
    // cache miss, even expired, get from the db
    // as sync bucket cmd invalid content in mdb, should get from leveldb even expired
    if (rc != TAIR_RETURN_SUCCESS) {
        PROFILER_BEGIN("db lock");
        RTProbe probe_mtx;
        probe_mtx.enter(PSN_DB_MUTEX_LOCK);
        tbsys::CThreadGuard mutex_guard(get_mutex(key));
        probe_mtx.leave();
        PROFILER_END();
        PROFILER_BEGIN("db get");
        rc = do_get(ldb_key, db_value, false/* not get from cache */, true/* fill cache */);
        PROFILER_END();
    }

    if (TAIR_RETURN_SUCCESS == rc) {
        ldb_item.assign(const_cast<char *>(db_value.data()), db_value.size());
        // already check expired. no need here.
        value.set_data(ldb_item.value(), ldb_item.value_size());
        log_debug("@@value: size %zd, ldb_item: size %d prefix %d, cdate %d edate %d mdate %d", db_value.size(),
                  ldb_item.value_size(), ldb_item.prefix_size(), ldb_item.cdate(), ldb_item.edate(), ldb_item.mdate());
        // update meta info
        key.data_meta.flag = value.data_meta.flag = ldb_item.flag();
        key.data_meta.cdate = value.data_meta.cdate = ldb_item.cdate();
        key.data_meta.edate = value.data_meta.edate = ldb_item.edate();
        key.data_meta.mdate = value.data_meta.mdate = ldb_item.mdate();
        key.data_meta.version = value.data_meta.version = ldb_item.version();
        key.data_meta.keysize = value.data_meta.keysize = key.get_size();
        key.data_meta.valsize = value.data_meta.valsize = ldb_item.value_size();
        key.data_meta.prefixsize = value.data_meta.prefixsize = ldb_item.prefix_size();
        key.set_prefix_size(ldb_item.prefix_size());
    }

    log_debug("ldb::get rc: %d, key len: %d, key prefix len:%d , value len: %d, meta_version:%u ", rc, key.get_size(),
              key.get_prefix_size(), value.get_size(), ldb_item.meta().base_.meta_version_);

    return rc;
}


int LdbInstance::remove(int bucket_number, tair::common::data_entry &key, bool version_care) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = TAIR_RETURN_SUCCESS;

    LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number);
    LdbItem ldb_item;
    std::string db_value;
    bool mtime_care = is_mtime_care(key);
    bool need_op = true;
    tbsys::CThreadGuard mutex_guard(get_mutex(key));

    if ((db_version_care_ && version_care) || mtime_care) {
        rc = do_get(ldb_key, db_value, true/* get from cache */,
                    false/* not fill cache */, false/* not update cache stat */);
        if (TAIR_RETURN_SUCCESS == rc) {
            ldb_item.assign(const_cast<char *>(db_value.data()), db_value.size());
            need_op = !(mtime_care && ldb_item.mdate() > key.data_meta.mdate);
            if (!need_op) {
                rc = TAIR_RETURN_MTIME_EARLY;
            } else if (version_care &&
                       key.data_meta.version != 0 &&
                       key.data_meta.version != ldb_item.version()) {
                rc = TAIR_RETURN_VERSION_ERROR;
            }
        }
    }

    if (need_op && rc == TAIR_RETURN_SUCCESS) {
        bool synced = is_synced(key);
        bool from_other_unit = is_meta_flag_unit(key.data_meta.flag);
        // only not synced data need tailer now
        if (!synced && tair::common::entry_tailer::need_entry_tailer(key)) {
            tair::common::entry_tailer tailer(key);
            rc = do_remove(ldb_key, synced, from_other_unit, &tailer);
        } else {
            rc = do_remove(ldb_key, synced, from_other_unit);
        }

        if (TAIR_RETURN_SUCCESS == rc) {
            stat_sub(bucket_number, key.area, ldb_key.key_size() + ldb_item.value_size(),
                     ldb_key.size() + ldb_item.size(), 1);
        }
    }

    log_debug("ldb::remove %d, key len: %d", rc, key.get_size());

    return rc;
}

int LdbInstance::op_cmd(ServerCmdType cmd, std::vector<std::string> &params, std::vector<std::string> &infos) {
    if (db_ == NULL) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int ret = TAIR_RETURN_SUCCESS;
    log_warn("op cmd %d, param size: %zu", cmd, params.size());

    switch (cmd) {
        case TAIR_SERVER_CMD_FLUSH_MMT:
        case TAIR_SERVER_CMD_SYNC_BUCKET: {
            leveldb::Status status = db_->ForceCompactMemTable();
            if (!status.ok()) {
                log_error("op cmd flush mem fail: %s", status.ToString().c_str());
                ret = TAIR_RETURN_FAILED;
            }
            break;
        }
        case TAIR_SERVER_CMD_RESET_DB: // just rename here
        {
            // just rename to back db path
            std::string back_db_path = get_back_path(db_path_);
            if (::access(db_path_, F_OK) != 0) {
                // just consider it's ok. maybe ::access fail because other reason.
                log_warn("resetdb but orignal db path is not exist: %s, ignore it.", db_path_);
            } else if (::rename(db_path_, back_db_path.c_str()) != 0) {
                log_error("rename db %s to back db %s fail. error: %s", db_path_, back_db_path.c_str(),
                          strerror(errno));
                ret = TAIR_RETURN_FAILED;
            }

            db_->ResetDbName(back_db_path);
            break;
        }
        case TAIR_SERVER_CMD_PAUSE_GC: {
            gc_.pause_gc();
            break;
        }
        case TAIR_SERVER_CMD_RESUME_GC: {
            gc_.resume_gc();
            break;
        }
        case TAIR_SERVER_CMD_STAT_DB: {
            ret = stat_db();
            break;
        }
        case TAIR_SERVER_CMD_BACKUP_DB:
        case TAIR_SERVER_CMD_UNLOAD_BACKUPED_DB: {
            // TODO: all db cmd should use op_cmd_to_db(),
            //       FLUSH_MMT/STAT_DB eg.
            ret = op_cmd_to_db(cmd, params);
            break;
        }
        case TAIR_SERVER_CMD_SET_CONFIG:
        case TAIR_SERVER_CMD_LDB_INSTANCE_CONFIG: {
            ret = set_config(params);
            break;
        }
        case TAIR_SERVER_CMD_GET_CONFIG: {
            ret = get_config(infos);
            break;
        }
        default: {
            break;
        }
    }
    return ret;
}

bool LdbInstance::begin_scan(int bucket_number) {
    log_info("begin scan");
    bool ret = true;
    if (scan_it_ != NULL)   // not close previous scan
    {
        delete scan_it_;
        scan_it_ = NULL;
    }

    char scan_key[LDB_KEY_META_SIZE];
    LdbKey::build_key_meta(scan_key, bucket_number);

    leveldb::ReadOptions scan_read_options = read_options_;
    scan_read_options.fill_cache = false; // not fill cache
    scan_read_options.hold_for_long = true;
    scan_it_ = db_->NewIterator(scan_read_options);
    if (NULL == scan_it_) {
        log_error("get ldb scan iterator fail");
        ret = false;
    } else {
        scan_it_->Seek(leveldb::Slice(scan_key, sizeof(scan_key)));
    }

    if (ret) {
        scan_bucket_ = bucket_number;
        still_have_ = true;
    }
    return ret;
}

bool LdbInstance::end_scan() {
    if (scan_it_ != NULL) {
        delete scan_it_;
        scan_it_ = NULL;
        scan_bucket_ = -1;
    }
    return true;
}

bool LdbInstance::get_next_items(std::vector<item_data_info *> &list) {
    list.clear();

    static const int32_t migrate_batch_size =
            TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MIGRATE_BATCH_SIZE, 1048576); // 1M default
    static const int32_t migrate_batch_count =
            TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MIGRATE_BATCH_COUNT, 2000); // 2000 default

    if (NULL == scan_it_) {
        log_error("not begin_scan");
    } else if (still_have_) {
        LdbKey ldb_key;       // reuse is ok.
        LdbItem ldb_item;
        int32_t key_size = 0, value_size = 0, total_size = 0, batch_size = 0, batch_count = 0;

        while (batch_size < migrate_batch_size && batch_count < migrate_batch_count && scan_it_->Valid()) {
            // match bucket
            if (LdbKey::decode_bucket_number(scan_it_->key().data() + LDB_EXPIRED_TIME_SIZE) == scan_bucket_) {
                ldb_key.assign(const_cast<char *>(scan_it_->key().data()), scan_it_->key().size());
                ldb_item.assign(const_cast<char *>(scan_it_->value().data()), scan_it_->value().size());

                key_size = ldb_key.key_size();
                value_size = ldb_item.value_size();
                if (key_size >= TAIR_MAX_KEY_SIZE_WITH_AREA || key_size < 1 || value_size >= TAIR_MAX_DATA_SIZE ||
                    value_size < 1) {
                    log_error("kv size invalid: k: %d, v: %d [%zu %zu] [%x %x %s]", key_size, value_size,
                              scan_it_->key().size(), scan_it_->value().size(), *(ldb_key.key()), *(ldb_key.key() + 1),
                              ldb_key.key() + 2);
                    ::sleep(2);
                }
                total_size = ITEM_HEADER_LEN + key_size + value_size;
                item_data_info * data = (item_data_info * )
                new char[total_size];
                data->header.magic = MAGIC_ITEM_META_LDB_PREFIX;
                data->header.keysize = key_size;
                data->header.version = ldb_item.version();
                data->header.valsize = value_size;
                data->header.flag = ldb_item.flag();
                data->header.cdate = ldb_item.cdate();
                data->header.mdate = ldb_item.mdate();
                data->header.edate = ldb_item.edate();
                data->header.prefixsize = ldb_item.prefix_size();

                memcpy(data->m_data, ldb_key.key(), key_size);
                memcpy(data->m_data + key_size, ldb_item.value(), value_size);

                list.push_back(data);
                scan_it_->Next();
                batch_size += total_size;
                ++batch_count;
            } else {
                still_have_ = false;
                break;
            }
        }
        log_debug("migrate count: %zu, size: %d", list.size(), batch_size);
        if (list.empty()) {
            still_have_ = false;
        }
    }

    return still_have_;
}

void LdbInstance::get_stats(tair_stat *stat) {
    if (NULL != db_)        // not init now, no stat
    {
        if (TBSYS_LOGGER._level > TBSYS_LOG_LEVEL_WARN) {
            log_debug("ldb bucket get stat %p", stat);
            std::string stat_value;
            if (get_db_stat(db_, stat_value, "stats")) {
                log_info("ldb status: %s", stat_value.c_str());
            } else {
                log_error("get ldb status fail, uncompleted status: %s", stat_value.c_str());
            }
        }

        if (stat != NULL) {
            // get all stat information
            tair_pstat *pstat = all_area_stat_manager_.get_stat();
            for (int i = 0; i < TAIR_MAX_AREA_COUNT; i++) {
                stat[i].data_size_value += pstat[i].data_size();
                stat[i].use_size_value += pstat[i].use_size();
                stat[i].item_count_value += pstat[i].item_count();
            }
        }
    }
}

// get all sstable's range infomation etc.
int LdbInstance::stat_db() {
    int ret = TAIR_RETURN_SUCCESS;
    std::string stat_value;
    if (get_db_stat(db_, stat_value, "ranges")) {
        fprintf(stderr, "==== statdb instance %d ====\n%s", index_, stat_value.c_str());
    } else {
        log_error("get db range stat fail");
        ret = TAIR_RETURN_FAILED;
    }
    return ret;
}

int LdbInstance::op_cmd_to_db(int32_t cmd, const std::vector<std::string> &params) {
    leveldb::Status s;
    const char *desc = NULL;

    switch (cmd) {
        case TAIR_SERVER_CMD_BACKUP_DB: {
            desc = "backup db";
            s = db_->OpCmd(leveldb::kCmdBackupDB, &params);
            break;
        }
        case TAIR_SERVER_CMD_UNLOAD_BACKUPED_DB: {
            desc = "unload backuped db";
            s = db_->OpCmd(leveldb::kCmdUnloadBackupedDB, &params);
            break;
        }
        default: {
            desc = "unknown cmd";
            s = leveldb::Status::InvalidArgument("unkonwn cmd");
            break;
        }
    }

    if (!s.ok()) {
        log_error("%s fail, cmd: %d, error: %s", desc, cmd, s.ToString().c_str());
    }

    return s.ok() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
}

void LdbInstance::append_specify_compact_status(std::vector<std::string> &infos) {
    leveldb::DBImpl *impl = dynamic_cast<leveldb::DBImpl *>(db_);
    if (impl == NULL) return;

    {
        // 0 finish specify compact completely
        // 1 doing specify compact
        // 2 finish specify compact for reaching time end
        // 3 finish specify compact for stoping specify compact by someone
        static const char *const SpecifyStatusDescription[] = {
                "FinishCompletely",
                "Doing",
                "FinishByDeadLine",
                "FinishByHumanStop"
        };
        std::ostringstream os;
        int status = impl->SpecifyCompactStatus();
        assert(status >= 0 && status <= 3);
        os << "kNowSpecifyCompactStatus = " << SpecifyStatusDescription[status];
        infos.push_back(os.str());
    }
    {
        std::ostringstream os;
        struct tm t;
        char buf[50];
        time_t time = impl->LastFinishSpecifyCompactTime();
        strftime(buf, 50, "%Y-%m-%d %H:%M:%S", localtime_r(&time, &t));
        os << "kLastFinishSpecifyCompactTime = " << buf;
        infos.push_back(os.str());
    }
    {
        std::ostringstream os;
        os << "kNowSpecifyCompactLevel = " << impl->NowSpecifyCompactLevel();
        infos.push_back(os.str());
    }
}

int LdbInstance::get_config(std::vector<std::string> &infos) {
    int ret = TAIR_RETURN_SUCCESS;
    infos.clear();

    leveldb::config::getConfig(infos);

    append_specify_compact_status(infos);

    std::string t = bg_task_.get_need_repair();
    if (t.size() != 0) {
        infos.push_back(t);
    }
    t = bg_task_.get_time_range();;
    if (t.size() != 0) {
        infos.push_back(t);
    }

    return ret;
}

int LdbInstance::set_config(std::vector<std::string> &params) {
    int ret = TAIR_RETURN_SUCCESS;
    if (params.size() < 2) {
        log_error("set config but invalid params size: %zu", params.size());
        ret = TAIR_RETURN_FAILED;
    }
        // leveldb::config is global static actually
    else {
        std::string &config_key = params[0];
        std::string &config_value = params[1];
        log_warn("set config: %s = %s", config_key.c_str(), config_value.c_str());
        if (config_key == LDB_LIMIT_COMPACT_LEVEL_COUNT) {
            int count = atoi(config_value.c_str());
            if (count >= leveldb::config::kNumLevels) {
                log_error("invalid value %s to set %s, ignore it.", config_value.c_str(), config_key.c_str());
                ret = TAIR_RETURN_FAILED;
            } else {
                leveldb::config::kLimitCompactLevelCount = count;
            }
        } else if (config_key == LDB_COMPACT_GC_RANGE) {
            bg_task_.reset_compact_gc_range(config_value.c_str());
        } else if (config_key == LDB_LIMIT_COMPACT_COUNT_INTERVAL) {
            int count = atoi(config_value.c_str());
            if (count < 0) {
                log_error("invalid value %s to set %s, ignore it.", config_value.c_str(), config_key.c_str());
                ret = TAIR_RETURN_FAILED;
            } else {
                leveldb::config::kLimitCompactCountInterval = count;
            }
        } else if (config_key == LDB_LIMIT_COMPACT_TIME_INTERVAL) {
            leveldb::config::kLimitCompactTimeInterval = atoi(config_value.c_str());;
        } else if (config_key == LDB_LIMIT_COMPACT_TIME_RANGE) {
            int time_start = 0, time_end = 0;
            util::time_util::get_time_range(config_value.c_str(),
                                            time_start,
                                            time_end);
            leveldb::config::SetLimitCompactTimeRange(time_start, time_end);
        } else if (config_key == LDB_LIMIT_DELETE_OBSOLETE_FILE_INTERVAL) {
            leveldb::config::kLimitDeleteObsoleteFileInterval = atoi(config_value.c_str());
        } else if (config_key == LDB_DO_SEEK_COMPACTION) {
            leveldb::config::kDoSeekCompaction = (atoi(config_value.c_str()) > 0);
        } else if (config_key == LDB_LOG_FILE_KEEP_INTERVAL) {
            leveldb::config::kLogFileKeepInterval = atoi(config_value.c_str());
        } else if (config_key == LDB_DO_BACKUP_SST_FILE) {
            leveldb::config::kDoBackUpSSTFile = (atoi(config_value.c_str()) > 0);
        } else if (config_key == LDB_DO_SPLIT_MMT_COMPACTION) {
            leveldb::config::kDoSplitMmtCompaction = (atoi(config_value.c_str()) > 0);
        } else if (config_key == LDB_SPECIFY_COMPACT_MAX_THRESHOLD) {
            int maxthreshold = atoi(config_value.c_str());
            maxthreshold = (maxthreshold <= 0 ? 10000 : maxthreshold);
            leveldb::config::kSpecifyCompactMaxThreshold = maxthreshold;
        } else if (config_key == LDB_SPECIFY_COMPACT_THRESHOLD) {
            int threshold = atoi(config_value.c_str());
            threshold = (threshold > leveldb::config::kSpecifyCompactMaxThreshold ?
                         leveldb::config::kSpecifyCompactMaxThreshold : threshold);
            threshold = (threshold < 0 ? 0 : threshold);
            leveldb::config::kSpecifyCompactThreshold = threshold;
        } else if (config_key == LDB_SPECIFY_COMPACT_SCORE_THRESHOLD) {
            int score_threshold = atoi(config_value.c_str());
            leveldb::config::kSpecifyCompactScoreThreshold = score_threshold;
        } else if (config_key == LDB_SPECIFY_COMPACT_TIME_RANGE) {
            int time_start = 0, time_end = 0;
            util::time_util::get_time_range(config_value.c_str(),
                                            time_start,
                                            time_end);
            leveldb::config::SetSpecifyCompactTimeRange(time_start, time_end);
        } else if (config_key == LDB_BUCKET_INDEX_CAN_UPDATE) {
            // do it in ldb_manager, just skip it
            ret = TAIR_RETURN_SUCCESS;
        } else if (config_key == TAIR_ADD_GC_BUCKET) {
            ret = gc_.add(config_value, GC_BUCKET);
        } else if (config_key == TAIR_REMOVE_GC_BUCKET) {
            ret = gc_.remove(config_value, GC_BUCKET);
        } else if (config_key == TAIR_ADD_GC_AREA) {
            ret = gc_.add(config_value, GC_AREA);
        } else if (config_key == TAIR_REMOVE_GC_AREA) {
            ret = gc_.remove(config_value, GC_AREA);
        } else if (config_key == LDB_REMOVE_EXTRA_SST) {
            bg_task_.start_repair_compact(leveldb::KCompactSelfRepairRemoveExtraSSTFile, config_value, true);
        } else if (config_key == LDB_REMOVE_EXTRA_SST_NO_CHECK) {
            bg_task_.start_repair_compact(leveldb::KCompactSelfRepairRemoveExtraSSTFile, config_value, false);
        } else if (config_key == LDB_REMOVE_EXTRA_SST_STOP) {
            bg_task_.stop_repair_compact(leveldb::KCompactSelfRepairRemoveExtraSSTFile);
        } else if (config_key == LDB_REMOVE_GC_SST) {
            bg_task_.start_repair_compact(leveldb::KCompactSelfRepairRemoveGCSSTFile, config_value, true);
        } else if (config_key == LDB_REMOVE_GC_SST_NO_CHECK) {
            bg_task_.start_repair_compact(leveldb::KCompactSelfRepairRemoveGCSSTFile, config_value, false);
        } else if (config_key == LDB_REMOVE_GC_SST_STOP) {
            bg_task_.stop_repair_compact(leveldb::KCompactSelfRepairRemoveGCSSTFile);
        } else if (config_key == LDB_REMOVE_CORRUPTION_SST) {
            bg_task_.start_repair_compact(leveldb::KCompactSelfRepairRemoveCorruptionSSTFile, config_value, true);
        } else if (config_key == LDB_REMOVE_CORRUPTION_SST_NO_CHECK) {
            bg_task_.start_repair_compact(leveldb::KCompactSelfRepairRemoveCorruptionSSTFile, config_value, false);
        } else if (config_key == LDB_REMOVE_CORRUPTION_SST_STOP) {
            bg_task_.stop_repair_compact(leveldb::KCompactSelfRepairRemoveCorruptionSSTFile);
        } else if (config_key == LDB_BACKUP_MAX_SIZE) {
            bg_task_.set_backup_max_size(atoi(config_value.c_str()));
        } else if (config_key == LDB_CHECK_CLEAN_INTERVAL) {
            bg_task_.set_check_clean_interval(atoi(config_value.c_str()));
        } else if (config_key == LDB_GET_RANGE_LIMIT) {
            leveldb::config::kLimitGetRange = atoi(config_value.c_str());
        } else if (config_key == LDB_RANGE_SCAN_TIMES_LIMIT) {
            leveldb::config::kLimitRangeScanTimes = atoi(config_value.c_str());
        } else {
            ret = TAIR_RETURN_FAILED;
            log_error("invalid value %s to set %s, ignore it.", config_value.c_str(), config_key.c_str());
        }

        // ignore unknown config
    }
    return ret;
}

int LdbInstance::clear_area(int32_t area) {
    int ret = TAIR_RETURN_SUCCESS;
    if (area >= 0) {
        all_area_stat_manager_.stat_reset(area);

        std::vector<int32_t> reset_types;
        reset_types.push_back(STAT_USER_ITEM_COUNT);
        reset_types.push_back(STAT_USER_DATA_SIZE);
        stat_->reset_area(area, 0, &reset_types);

        ret = gc_.add(area, GC_AREA);
    }
    return ret;
}

bool LdbInstance::exist(int32_t bucket_number) {
    STAT_MANAGER_MAP *tmp_stat_manager = stat_manager_;
    return tmp_stat_manager->find(bucket_number) != tmp_stat_manager->end();
}

int LdbInstance::do_put(LdbKey &ldb_key, LdbItem &ldb_item,
                        bool fill_cache, bool synced, bool from_other_unit, bool mc_ops) {
    RTProbe probe(PSN_DO_PUT);
    int rc = TAIR_RETURN_SUCCESS;
    PROFILER_BEGIN("db db put");
    leveldb::Status status = db_->Put(write_options_, leveldb::Slice(ldb_key.data(), ldb_key.size()),
                                      leveldb::Slice(ldb_item.data(), ldb_item.size()),
                                      synced, from_other_unit);
    PROFILER_END();
    if (UNLIKELY(!status.ok())) {
        log_error("update ldb item fail. %s", status.ToString().c_str());
        rc = TAIR_RETURN_FAILED;
    } else {
        //put sucess, update new stat
        stat_->update_physical_stat(LdbKey::decode_area_with_key(ldb_key.data()), 1, ldb_key.size() + ldb_item.size());

        if (cache_ != NULL) {
            // client's requsting fill_cache
            if (fill_cache) {
                log_debug("fill cache");
                PROFILER_BEGIN("db cache put");
                // raw_put key to mdb with prefix_size
                {
                    RTProbe probe_cache(PSN_CACHE_PUT);
                    rc = cache_->raw_put(ldb_key.key(), ldb_key.key_size(), ldb_item.data(), ldb_item.size(),
                                         ldb_item.flag(), ldb_item.edate(), ldb_item.prefix_size(), mc_ops);
                }
                PROFILER_END();
                if (rc != TAIR_RETURN_SUCCESS) // what happend
                {
                    log_error("::put. put cache fail, rc: %d", rc);
                }
            } else {
                log_debug("not fill cache");
                PROFILER_BEGIN("db cache remove");
                {
                    RTProbe probe_cache(PSN_CACHE_DEL);
                    rc = cache_->raw_remove(ldb_key.key(), ldb_key.key_size());
                }
                PROFILER_END();
                if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_DATA_NOT_EXIST) // what happened ?
                {
                    log_error("::put. remove cache fail: %d", rc);
                } else {
                    rc = TAIR_RETURN_SUCCESS;
                }
            }
        }
    }

    return rc;
}

int LdbInstance::do_cache_get(LdbKey &ldb_key, std::string &value, bool update_stat) {
    RTProbe probe(PSN_CACHE_GET);
    int rc = TAIR_RETURN_FAILED;
    if (cache_ != NULL) {
        PROFILER_BEGIN("db cache get");
        rc = cache_->raw_get(ldb_key.key(), ldb_key.key_size(), value, update_stat);
        PROFILER_END();
        if (TAIR_RETURN_SUCCESS == rc) // cache hit
        {
            log_debug("ldb cache hit");
        }

        stat_->update_cache_get(LdbKey::decode_area_with_key(ldb_key.data()), rc);
    }
    return rc;
}

int LdbInstance::do_get(LdbKey &ldb_key, std::string &value, bool from_cache, bool fill_cache, bool update_stat,
                        bool mc_ops) {
    RTProbe probe(PSN_DO_GET);
    int rc = from_cache ? do_cache_get(ldb_key, value, update_stat) : TAIR_RETURN_FAILED;
    // cache miss, even expired, get from the db
    // as sync bucket cmd invalid content in mdb, should get from leveldb even expired
    if (rc != TAIR_RETURN_SUCCESS) {
        PROFILER_BEGIN("db db get");
        leveldb::Status status = db_->Get(read_options_, leveldb::Slice(ldb_key.data(), ldb_key.size()),
                                          &value);
        PROFILER_END();
        if (status.ok()) {
            rc = TAIR_RETURN_SUCCESS;
            if (fill_cache && cache_ != NULL)     // fill cache
            {
                PROFILER_BEGIN("db cache put");
                LdbItem ldb_item;
                ldb_item.assign(const_cast<char *>(value.data()), value.size());
                RTProbe probe_cache;
                probe_cache.enter(PSN_CACHE_PUT);
                int tmp_rc = cache_->raw_put(ldb_key.key(), ldb_key.key_size(), ldb_item.data(), ldb_item.size(),
                                             ldb_item.flag(), ldb_item.edate(), ldb_item.prefix_size(), mc_ops);
                probe_cache.leave();
                PROFILER_END();
                if (tmp_rc != TAIR_RETURN_SUCCESS) // ignore return value.
                {
                    log_debug("::get. put cache fail, rc: %d", tmp_rc);
                }
            }
        } else {
            log_debug("get ldb item not found");
            rc = status.IsNotFound() ? TAIR_RETURN_DATA_NOT_EXIST : TAIR_RETURN_FAILED;
        }
    }

    return rc;
}

int LdbInstance::do_remove(LdbKey &ldb_key,
                           bool synced, bool from_other_unit, tair::common::entry_tailer *tailer) {
    // first remvoe db, then cache
    int rc = TAIR_RETURN_SUCCESS;
    leveldb::Status status;
    if (NULL == tailer) {
        status = db_->Delete(write_options_, leveldb::Slice(ldb_key.data(), ldb_key.size()),
                             synced, from_other_unit);
    } else {
        status = db_->Delete(write_options_, leveldb::Slice(ldb_key.data(), ldb_key.size()),
                             leveldb::Slice(tailer->data(), tailer->size()), synced, from_other_unit);
    }

    if (UNLIKELY(!status.ok())) {
        log_error("remove ldb item fail: %s", status.ToString().c_str()); // ignore return status
        rc = status.IsNotFound() ? TAIR_RETURN_DATA_NOT_EXIST : TAIR_RETURN_FAILED;
    } else
        stat_->update_physical_stat(LdbKey::decode_area_with_key(ldb_key.data()), 1,
                                    ldb_key.size()); // remove() actually add one key use_size


    if (TAIR_RETURN_SUCCESS == rc && cache_ != NULL) {
        rc = cache_->raw_remove(ldb_key.key(), ldb_key.key_size()); // should always succeed
        if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_DATA_NOT_EXIST) // what happened ?
        {
            log_error("remove cache fail. rc: %d", rc);
        } else {
            rc = TAIR_RETURN_SUCCESS; // data not exist mean remove successfully
        }
    }
    return rc;
}

void
LdbInstance::stat_add(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count) {
    UNUSED(bucket_number);
    all_area_stat_manager_.stat_add(area, data_size, use_size, item_count);
}

void
LdbInstance::stat_sub(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count) {
    UNUSED(bucket_number);
    all_area_stat_manager_.stat_sub(area, data_size, use_size, item_count);
}

void LdbInstance::get_buckets(std::vector<int32_t> &buckets) {
    STAT_MANAGER_MAP *tmp_stat_manager = stat_manager_;
    for (STAT_MANAGER_MAP_ITER it = tmp_stat_manager->begin(); it != tmp_stat_manager->end(); ++it) {
        buckets.push_back(it->first);
    }
}

bool LdbInstance::is_mtime_care(const data_entry &key) {
    // client request and mtime care and data's mtime is valid
    return (key.server_flag != TAIR_SERVERFLAG_DUPLICATE) && (key.server_flag != TAIR_SERVERFLAG_MIGRATE) &&
           (key.data_meta.flag & TAIR_CLIENT_DATA_MTIME_CARE) && (key.data_meta.mdate > 0);
}

bool LdbInstance::is_synced(const data_entry &key) {
    // rsync server_flag is TAIR_SERVERFLAG_RSYNC
    // duplicate server_flag is TAIR_SERVERFLAG_DUPLICATE
    // rsync then duplicate server_flag is TAIR_SERVERFLAG_DUPLICATE
    return (TAIR_SERVERFLAG_CLIENT != key.server_flag && TAIR_SERVERFLAG_PROXY != key.server_flag);
}

void LdbInstance::sanitize_option() {
    options_.error_if_exists = false; // exist is ok
    options_.create_if_missing = true; // create if not exist
    options_.comparator = LdbComparator(&gc_);// self-defined comparator
    // can use one static filterpolicy instance
    options_.filter_policy = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USE_BLOOMFILTER, 0) > 0 ?
                             new LdbBloomFilterPolicy(
                                     TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BLOOMFILTER_BITS_PER_KEY, 10), stat_)
                                                                                              : NULL;
    options_.paranoid_checks = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_PARANOID_CHECK, 0) > 0;
    options_.max_open_files = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MAX_OPEN_FILES, 655350);
    options_.max_mem_usage_for_memtable = atoll(
            TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_MAX_MEM_USAGE_FOR_MEMTABLE, "1073741824"));
    options_.write_buffer_size = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_WRITE_BUFFER_SIZE, 4 << 20); // 4M
    options_.block_size = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BLOCK_SIZE, 4 << 10); // 4K
    options_.table_cache_size = atoll(
            TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_TABLE_CACHE_SIZE, "5368709120")); // 5G
    options_.block_cache_size = atoll(TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_BLOCK_CACHE_SIZE, "8388608")); // 8M
    options_.block_restart_interval = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BLOCK_RESTART_INTERVAL, 16); // 16
    options_.compression = static_cast<leveldb::CompressionType>(TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_COMPRESSION,
                                                                                     leveldb::kSnappyCompression));
    // need reserve binlog when doing remote sync
    options_.reserve_log = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_DO_RSYNC, 0) > 0;
    options_.load_backup_version = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_LOAD_BACKUP_VERSION, 0) > 0;
    options_.kL0_CompactionTrigger = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_L0_COMPACTION_TRIGGER, 4);
    options_.kL0_SlowdownWritesTrigger = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_L0_SLOWDOWN_WRITE_TRIGGER, 8);
    options_.kL0_StopWritesTrigger = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_L0_STOP_WRITE_TRIGGER, 12);
    options_.kMaxMemCompactLevel = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MAX_MEMCOMPACT_LEVEL, 2);
    options_.kTargetFileSize = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_TARGET_FILE_SIZE, 2 << 20); // 2M
    options_.kArenaBlockSize = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_ARENABLOCK_SIZE, 4 << 10); // 4K
    options_.kBaseLevelSize = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BASE_LEVEL_SIZE, 10 << 20); // 10M
    options_.kFilterBaseLg = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_FILTER_BASE_LOGARITHM, 12); // 1<<12 == block_size
    options_.kUseMmapRandomAccess = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USE_MMAP_RANDOM_ACCESS, 0) > 0; // false
    options_.kLimitCompactLevelCount = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_LIMIT_COMPACT_LEVEL_COUNT, 0);
    options_.kLimitCompactCountInterval = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_LIMIT_COMPACT_COUNT_INTERVAL, 0);
    options_.kLimitCompactTimeInterval = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_LIMIT_COMPACT_TIME_INTERVAL, 0);
    std::string time_range = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_LIMIT_COMPACT_TIME_RANGE, "0-0");
    util::time_util::get_time_range(time_range.c_str(),
                                    options_.kLimitCompactTimeStart,
                                    options_.kLimitCompactTimeEnd);
    options_.kLimitDeleteObsoleteFileInterval = TBSYS_CONFIG.getInt(TAIRLDB_SECTION,
                                                                    LDB_LIMIT_DELETE_OBSOLETE_FILE_INTERVAL, 0);
    options_.kDoSeekCompaction = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DO_SEEK_COMPACTION, 0) > 0;
    options_.kLogFileKeepInterval = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_LOG_FILE_KEEP_INTERVAL, 0);
    options_.kDoSplitMmtCompaction = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DO_SPLIT_MMT_COMPACTION, 0) > 0;
    options_.kDoBackUpSSTFile = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DO_BACKUP_SST_FILE, 0) > 0;

    std::string time_range_for_specify_compact = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_SPECIFY_COMPACT_TIME_RANGE,
                                                                        "0-0");
    util::time_util::get_time_range(time_range_for_specify_compact.c_str(),
                                    options_.kSpecifyCompactTimeStart,
                                    options_.kSpecifyCompactTimeEnd);
    options_.kSpecifyCompactMaxThreshold = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_SPECIFY_COMPACT_MAX_THRESHOLD,
                                                               10000);
    options_.kSpecifyCompactThreshold = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_SPECIFY_COMPACT_THRESHOLD,
                                                            options_.kSpecifyCompactMaxThreshold);
    options_.kSpecifyCompactScoreThreshold = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_SPECIFY_COMPACT_SCORE_THRESHOLD,
                                                                 1);

    // Env::Default() is a global static instance.
    // We allocate one env to one leveldb instance here.
    options_.env = leveldb::Env::Instance();
    options_.fileRepairCheckFunc = do_file_repair_check;
    options_.stat_ = stat_;
    read_options_.verify_checksums = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_READ_VERIFY_CHECKSUMS, 0) != 0;
    read_options_.fill_cache = true;
    write_options_.sync = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_WRITE_SYNC, 0) != 0;
    // remainning avaliable config: comparator, env, block cache.
}

tbsys::CThreadMutex *LdbInstance::get_mutex(const tair::common::data_entry &key) {
    tbsys::CThreadMutex *ret = NULL;
    if (mutex_ != NULL)     // no cache, no lock.
    {
        ret = mutex_ + util::string_util::mur_mur_hash(key.get_data(), key.get_size()) % LOCKER_SIZE;
    }
    return ret;
}

int LdbInstance::get_real_edate(int expire_time, int now, bool mc_ops) {
    int edate;
    if (expire_time > 0) {
        if (mc_ops) {
            edate = static_cast<uint32_t> (expire_time) > 30 * 24 * 60 * 60 ? expire_time : now + expire_time;
        } else {
            edate = static_cast<uint32_t>(expire_time) >= (uint32_t) now - 30 * 24 * 60 * 60 ? expire_time : now +
                                                                                                             expire_time;
        }
    } else {
        edate = 0;
    }
    return edate;
}
}
}
}
