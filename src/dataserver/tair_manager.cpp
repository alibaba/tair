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

#include "storage/ldb/ldb_manager.hpp"
#include "storage/mdb/mdb_define.hpp"

#include "tair_manager.hpp"
#include "migrate_manager.hpp"
#include "define.hpp"
#include "mdb_factory.hpp"
#include "duplicate_manager.hpp"
#include "dup_sync_manager.hpp"
#include "i_remote_sync_manager.hpp"
#include "new_remote_sync_manager.hpp"
#include "remote_sync_manager.hpp"
#include "prefix_incdec_bounded_packet.hpp"
#include "common/tair_stat.hpp"

#include "dup_depot.hpp"
#include "recovery_manager.hpp"

namespace tair {
tair_manager::tair_manager(easy_io_t *eio)
        : migrate_done_set(0), recovery_done_set(0), current_bucket_count(1), total_bucket_count(1),
          supported_admin(false) {
    status = STATUS_NOT_INITED;
    localmode = false;
    storage_mgr = NULL;
    table_mgr = new table_manager();
    duplicator = NULL;
    migrate_mgr = NULL;
    migrate_log = NULL;
    dump_mgr = NULL;
    remote_sync_mgr = NULL;
    this->eio = eio;

    dup_depot = NULL;
    recovery_mgr = NULL;
    stat_ = NULL;
    bzero(area_total_quotas, sizeof(area_total_quotas));
}

tair_manager::~tair_manager() {
    tbsys::CThreadGuard update_table_guard(&update_server_table_mutex);

    if (recovery_mgr != NULL) {
        delete recovery_mgr;
        recovery_mgr = NULL;
    }

    if (dup_depot != NULL) {
        delete dup_depot;
        dup_depot = NULL;
    }

    if (remote_sync_mgr != NULL) {
        delete remote_sync_mgr;
        remote_sync_mgr = NULL;
    }

    if (migrate_mgr != NULL) {
        delete migrate_mgr;
        migrate_mgr = NULL;
    }

    if (duplicator != NULL) {
        delete duplicator;
        duplicator = NULL;
    }

    if (migrate_log != NULL) {
        migrate_log->close();
        migrate_log = NULL;
    }

    if (dump_mgr != NULL) {
        delete dump_mgr;
        dump_mgr = NULL;
    }

    if (storage_mgr != NULL) {
        delete storage_mgr;
        storage_mgr = NULL;
    }

    if (stat_ != NULL) {
        delete stat_;
        stat_ = NULL;
    }

    delete table_mgr;
    table_mgr = NULL;
}

bool tair_manager::initialize(tair::stat::FlowController *flow_ctrl, RTCollector *rtc) {
    if (status != STATUS_NOT_INITED) {
        return true;
    }

    const char *se_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_SENGINE, NULL);
    if (se_name == NULL || (strcmp(se_name, "mdb") == 0)) {
        storage_mgr = mdb_factory::create_mdb_manager();
    } else if (strcmp(se_name, "ldb") == 0) {
        log_info("init storage engine ldb");
        storage_mgr = new tair::storage::ldb::LdbManager();
        if (dynamic_cast<tair::storage::ldb::LdbManager *>(storage_mgr)->initialize() != TAIR_RETURN_SUCCESS)
            return false;
    } else {
        storage_mgr = NULL;
    }

    if (storage_mgr == NULL) {
        log_error("init storage engine failed, storage engine name: %s", se_name);
        return false;
    }
    this->supported_admin = (bool) TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, "supported_admin", 0);
    bool do_rsync = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_DO_RSYNC, 0) > 0;
    int new_version = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_RSYNC_VERSION, 0);
    // remote synchronization init
    if (do_rsync) {
        if (new_version) {
            remote_sync_mgr = new NewRemoteSyncManager(this);
        } else {
            remote_sync_mgr = new RemoteSyncManager(this);
        }
        int ret = remote_sync_mgr->init();
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("init remote sync manager fail: %d", ret);
            delete remote_sync_mgr;
            remote_sync_mgr = NULL;
            return false;
        }
    }

    // duplicate depot
    bool do_dup_depot = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_DO_DUP_DEPOT, 0) > 0;
    if (do_dup_depot) {
        dup_depot = new DupDepot(this);
        int ret = dup_depot->init();
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("init duplicate depot fail: %d", ret);
            delete dup_depot;
            dup_depot = NULL;
            return false;
        }
    }

    stat_ = new tair::common::TairStat(storage_mgr);
    stat_->start();

    // init the storage manager for stat helper
    TAIR_STAT.set_storage_manager(storage_mgr);
    // is dataserver use localmode
    localmode = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_LOCAL_MODE, 0);
    localmode_bucket = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_LOCAL_MODE_BUCKET, 1023);

    // init dupicator
    int dup_sync_mode = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_DUP_SYNC, TAIR_DUP_SYNC_MODE);
    int dup_timeout = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_DUP_TIMEOUT, 500);
    if (0 == dup_sync_mode) {
        duplicator = new duplicate_sender_manager(table_mgr);
    } else {
        uint32_t dup_ioth_count = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_DUP_IO_THREAD_COUNT, 1);
        log_info("run duplicator with sync mode ");
        duplicator = new dup_sync_sender_manager(flow_ctrl, rtc, dup_ioth_count);
    }
    duplicator->set_dup_timeout(dup_timeout);

    migrate_mgr = new migrate_manager(duplicator, this, storage_mgr);

    // for recovery
    if (do_dup_depot) {
        recovery_mgr = new recovery_manager(duplicator, this, storage_mgr);
    }

    dump_mgr = new tair::storage::dump_manager(storage_mgr);

    status = STATUS_INITED;

    // if localmode, init the table with bucket_count = 1, force set the status to STATUS_CAN_WORK
    if (localmode) {
        table_mgr->set_table_for_localmode();
        // init storage manager buckets
        std::vector<int32_t> buckets;
        for (int32_t i = 0; i < localmode_bucket; ++i) {
            buckets.push_back(i);
        }
        storage_mgr->set_bucket_count(localmode_bucket);
        if (!storage_mgr->init_buckets(buckets)) {
            log_error("localmode init buckets fail.");
            status = STATUS_NOT_INITED;
            return false;
        }
        update_quota_on_bucket_changed();

        log_warn("localmode init success, buckets: %d", localmode_bucket);
        status = STATUS_CAN_WORK;
    }

    return true;
}

int tair_manager::prefix_puts(request_prefix_puts *request, int heart_version, uint32_t &resp_size) {
    int rc = TAIR_RETURN_SUCCESS;
    tair_keyvalue_map *kvmap = request->kvmap;
    tair_keyvalue_map::iterator it = kvmap->begin();
    uint32_t ndone = 0;
    response_mreturn *resp = NULL;

    int bucket_number = get_bucket_number(*request->kvmap->begin()->first);
    if (request->server_flag & TAIR_SERVERFLAG_DUPLICATE) {
        response_mreturn_dup *resp_dup = new response_mreturn_dup();
        resp_dup->bucket_id = bucket_number;
        resp_dup->server_id = local_server_ip::ip;
        resp_dup->packet_id = request->packet_id;
        resp = resp_dup;
    } else {
        resp = new response_mreturn();
    }

    if (!(request->server_flag & TAIR_SERVERFLAG_DUPLICATE)) {
        while (it != kvmap->end()) {
            data_entry *key = it->first;
            data_entry value;
            int version = key->data_meta.version;
            item_meta_info meta;
            int ret = get_meta(request->area, *key, meta, bucket_number);
            if (version != 0 && ret == TAIR_RETURN_SUCCESS && version != meta.version) {
                log_warn("version not match, old: %d, new: %d", meta.version, version);
                rc = TAIR_RETURN_VERSION_ERROR;
                data_entry *skey = new data_entry();
                int prefix_size = key->get_prefix_size();
                skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                resp->add_key_code(skey, rc);
            }
            ++it;
        }
    }
    it = kvmap->begin();
    if (rc != TAIR_RETURN_VERSION_ERROR) {
        while (it != kvmap->end()) {
            data_entry *key = it->first;
            data_entry *value = it->second;
            key->server_flag = request->server_flag;

            //item_meta_info kmeta = key->data_meta;
            //item_meta_info vmeta = value->data_meta;
            rc = put(request->area, *key, *value, key->data_meta.edate, NULL, heart_version);
            //key->data_meta = kmeta;
            //value->data_meta = vmeta;
            if (rc == TAIR_RETURN_SUCCESS) {
                ++ndone;
            } else {
                data_entry *skey = new data_entry();
                int prefix_size = key->get_prefix_size();
                skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                resp->add_key_code(skey, rc);
            }
            ++it;
        }
    }

    if (ndone > 0 && ndone < request->key_count) {
        rc = TAIR_RETURN_PARTIAL_SUCCESS;
    } else if (ndone == 0) {
        //rc = TAIR_RETURN_FAILED;
    }
    resp->set_code(rc);
    resp->config_version = heart_version;
    resp->setChannelId(request->getChannelId());

    if (rc == TAIR_RETURN_SUCCESS) {
        if ((request->server_flag & TAIR_SERVERFLAG_DUPLICATE) == 0) {
            vector<uint64_t> slaves;
            get_slaves(request->server_flag, bucket_number, slaves);
            if (!slaves.empty()) {
                delete resp;
                resp = NULL;
                log_debug("duplicate prefix_puts request to bucket %d", bucket_number);
                rc = duplicator->duplicate_data(bucket_number, request->r, slaves);
            }
        }
    }
    if (resp != NULL) {
        resp_size = resp->size();
        request->r->opacket = resp;
    }

    // client:prefix_put->put ---------->server:put
    // client:prefix_puts     ---------->server:prefix_puts--N-->put
    // OP_PF_PUT:     prefix_put + prefix_puts
    // OP_PF_PUT_KEY: prefix_put + nr(prefix_puts)
    // to count the number of prefix_puts, in put, OP_PF_PUT is added nr(prefix_puts)，so minus nr(prefix_puts)-1 here.
    stat_->update(request->area, OP_PF_PUT, -(kvmap->size() - 1));
    return rc;
}

int tair_manager::prefix_removes(request_prefix_removes *request, int heart_version, uint32_t &resp_size) {
    int rc = TAIR_RETURN_SUCCESS;

    data_entry *key = request->key == NULL ?
                      *request->key_list->begin() : request->key;
    int32_t bucket_number = get_bucket_number(*key);

    response_mreturn *resp = NULL;
    if (request->server_flag & TAIR_SERVERFLAG_DUPLICATE) {
        response_mreturn_dup *resp_dup = new response_mreturn_dup();
        resp_dup->bucket_id = bucket_number;
        resp_dup->server_id = local_server_ip::ip;
        resp_dup->packet_id = request->packet_id;
        resp = resp_dup;
    } else {
        resp = new response_mreturn();
    }

    uint64_t target_server_id = 0L;
    if (request->key != NULL) {
        key->server_flag = request->server_flag;
        do {
            if (should_proxy(*key, target_server_id)) {
                rc = TAIR_RETURN_SHOULD_PROXY;
                break; //~ would not do proxy
            }
            int32_t op_flag = get_op_flag(bucket_number, key->server_flag);
            if (!should_write_local(bucket_number, key->server_flag, op_flag, rc)) {
                rc = TAIR_RETURN_REMOVE_NOT_ON_MASTER;
                break;
            }

            item_meta_info meta = key->data_meta;
            rc = remove(request->area, *key, NULL, heart_version);
            key->data_meta = meta;
            if (rc != TAIR_RETURN_SUCCESS) {
                data_entry *skey = new data_entry();
                int32_t prefix_size = key->get_prefix_size();
                skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                resp->add_key_code(skey, rc);
            }
        } while (false);
    } else if (request->key_list != NULL) {
        tair_dataentry_set::iterator itr = request->key_list->begin();
        size_t ndone = 0;
        while (itr != request->key_list->end()) {
            data_entry *key = *itr;
            key->server_flag = request->server_flag;
            if (should_proxy(*key, target_server_id)) {
                rc = TAIR_RETURN_SHOULD_PROXY;
                break;
            } else {
                int32_t op_flag = get_op_flag(bucket_number, key->server_flag);
                if (!should_write_local(bucket_number, key->server_flag, op_flag, rc)) {
                    rc = TAIR_RETURN_REMOVE_NOT_ON_MASTER;
                    break;
                }

                item_meta_info meta = key->data_meta;
                rc = remove(request->area, *key, NULL, heart_version);
                key->data_meta = meta;
            }
            if (rc == TAIR_RETURN_SUCCESS) {
                ++ndone;
            } else {
                data_entry *skey = new data_entry();
                int32_t prefix_size = key->get_prefix_size();
                skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                resp->add_key_code(skey, rc);
            }
            ++itr;
        }
        if (ndone > 0 && ndone < request->key_count) {
            rc = TAIR_RETURN_PARTIAL_SUCCESS;
        } else if (ndone == 0) {
            //rc = TAIR_RETURN_FAILED;
        }
    }

    resp->set_code(rc);
    resp->config_version = heart_version;
    resp->setChannelId(request->getChannelId());

    if (rc == TAIR_RETURN_SUCCESS) {
        if ((request->server_flag & TAIR_SERVERFLAG_DUPLICATE) == 0) {
            vector<uint64_t> slaves;
            get_slaves(request->server_flag, bucket_number, slaves);
            if (!slaves.empty()) {
                delete resp;
                resp = NULL;
                log_debug("duplicate prefix removes request to bucket %d", bucket_number);
                rc = duplicator->duplicate_data(bucket_number, request->r, slaves);
            }
        }
    }
    if (resp != NULL) {
        resp_size = resp->size();
        request->r->opacket = resp;
    }
    return rc;
}

int tair_manager::prefix_hides(request_prefix_hides *request, int heart_version, uint32_t &resp_size) {
    int rc = TAIR_RETURN_SUCCESS;

    data_entry *key = request->key == NULL ?
                      *request->key_list->begin() : request->key;
    int32_t bucket_number = get_bucket_number(*key);

    response_mreturn *resp = NULL;
    if (request->server_flag & TAIR_SERVERFLAG_DUPLICATE) {
        response_mreturn_dup *resp_dup = new response_mreturn_dup();
        resp_dup->bucket_id = bucket_number;
        resp_dup->server_id = local_server_ip::ip;
        resp_dup->packet_id = request->packet_id;
        resp = resp_dup;
    } else {
        resp = new response_mreturn();
    }

    uint64_t target_server_id = 0L;
    if (request->key != NULL) {
        key->server_flag = request->server_flag;
        do {
            if (should_proxy(*key, target_server_id)) {
                rc = TAIR_RETURN_SHOULD_PROXY;
                break; //~ would not do proxy
            }
            int32_t op_flag = get_op_flag(bucket_number, key->server_flag);
            if (!should_write_local(bucket_number, key->server_flag, op_flag, rc)) {
                rc = TAIR_RETURN_REMOVE_NOT_ON_MASTER;
                break;
            }

            item_meta_info meta = key->data_meta;
            rc = hide(request->area, *key, NULL, heart_version);
            key->data_meta = meta;
            if (rc != TAIR_RETURN_SUCCESS) {
                data_entry *skey = new data_entry();
                int32_t prefix_size = key->get_prefix_size();
                skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                resp->add_key_code(skey, rc);
            }
        } while (false);
    } else if (request->key_list != NULL) {
        tair_dataentry_set::iterator itr = request->key_list->begin();
        size_t ndone = 0;
        while (itr != request->key_list->end()) {
            data_entry *key = *itr;
            key->server_flag = request->server_flag;
            if (should_proxy(*key, target_server_id)) {
                rc = TAIR_RETURN_SHOULD_PROXY;
                break;
            } else {
                int32_t op_flag = get_op_flag(bucket_number, key->server_flag);
                if (!should_write_local(bucket_number, key->server_flag, op_flag, rc)) {
                    rc = TAIR_RETURN_REMOVE_NOT_ON_MASTER;
                    break;
                }

                item_meta_info meta = key->data_meta;
                rc = hide(request->area, *key, NULL, heart_version);
                key->data_meta = meta;
            }
            if (rc == TAIR_RETURN_SUCCESS) {
                ++ndone;
            } else {
                data_entry *skey = new data_entry();
                int32_t prefix_size = key->get_prefix_size();
                skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                resp->add_key_code(skey, rc);
            }
            ++itr;
        }
        if (ndone > 0 && ndone < request->key_count) {
            rc = TAIR_RETURN_PARTIAL_SUCCESS;
        } else if (ndone == 0) {
            //rc = TAIR_RETURN_FAILED;
        }
    }

    resp->set_code(rc);
    resp->config_version = heart_version;
    resp->setChannelId(request->getChannelId());

    if (rc == TAIR_RETURN_SUCCESS) {
        if ((request->server_flag & TAIR_SERVERFLAG_DUPLICATE) == 0) {
            vector<uint64_t> slaves;
            get_slaves(request->server_flag, bucket_number, slaves);
            if (!slaves.empty()) {
                delete resp;
                resp = NULL;
                log_debug("duplicate prefix removes request to bucket %d", bucket_number);
                rc = duplicator->duplicate_data(bucket_number, request->r, slaves);
            }
        }
    }
    if (resp != NULL) {
        resp_size = resp->size();
        request->r->opacket = resp;
    }
    return rc;
}

int tair_manager::prefix_incdec(request_prefix_incdec *request, int heart_version,
                                int low_bound, int upper_bound, bool bound_care, response_prefix_incdec *resp) {
    int rc = TAIR_RETURN_SUCCESS;
    size_t ndone = 0;
    request_prefix_incdec::key_counter_map_t *key_counter_map = request->key_counter_map;
    request_prefix_incdec::key_counter_map_t::iterator it = key_counter_map->begin();
    int32_t bucket_number = 0;
    while (it != key_counter_map->end()) {
        data_entry *key = it->first;
        key->server_flag = request->server_flag;
        counter_wrapper *wrapper = it->second;
        int ret_value;
        bucket_number = get_bucket_number(*key);
        int32_t op_flag = get_op_flag(bucket_number, key->server_flag);
        if (!should_write_local(bucket_number, key->server_flag, op_flag, rc)) {
            rc = TAIR_RETURN_WRITE_NOT_ON_MASTER;
            break;
        }
        rc = add_count(request->area, *key, wrapper->count,
                       wrapper->init_value, low_bound, upper_bound, &ret_value, wrapper->expire, NULL, heart_version);

        data_entry *skey = new data_entry();
        int prefix_size = key->get_prefix_size();
        skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
        if (rc == TAIR_RETURN_SUCCESS) {
            resp->add_key_value(skey, ret_value);
            ++ndone;
        } else {
            resp->add_key_code(skey, rc);
        }
        ++it;
    }

    if (request->key_count == 0) {
        rc = TAIR_RETURN_FAILED;
    } else if (ndone == request->key_count) {
        rc = TAIR_RETURN_SUCCESS;
    } else if (ndone > 0) {
        rc = TAIR_RETURN_PARTIAL_SUCCESS;
    } else {
        rc = TAIR_RETURN_FAILED;
    }

    if (rc == TAIR_RETURN_SUCCESS) {
        if ((request->server_flag & TAIR_SERVERFLAG_DUPLICATE) == 0) {
            vector<uint64_t> slaves;
            get_slaves(request->server_flag, bucket_number, slaves);
            if (!slaves.empty()) {
                log_debug("duplicate prefix inc/dec request to bucekt %d", bucket_number);
                rc = duplicator->duplicate_data(bucket_number, request->r, slaves);
            }
        }
        // migrate_log is recorded by put
    }
    return rc;
}

int
tair_manager::put(int area, data_entry &key, data_entry &value, int expire_time, base_packet *request, int heart_vesion,
                  bool with_stat) {
    if (verify_key(key) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (value.get_size() >= TAIR_MAX_DATA_SIZE || value.get_size() < 1) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    data_entry mkey = key; // key merged with area
    mkey.merge_area(area);

    int bucket_number = get_bucket_number(key);
    log_debug("put request will server in bucket: %d key =%s ", bucket_number, key.get_data());
    key.data_meta.log_self();
    int op_flag = get_op_flag(bucket_number, key.server_flag);

    int rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    PROFILER_BEGIN("should write local?");
    if (should_write_local(bucket_number, key.server_flag, op_flag, rc) == false) {
        PROFILER_END();
        return rc;
    }
    PROFILER_END();

    // for client PUT_CACHE_FLAG etc.
    int old_flag = key.data_meta.flag;

    // save into the storage engine
    bool version_care = op_flag & TAIR_OPERATION_VERSION;
    if (is_meta_flag_unit(key.data_meta.flag)) {
        version_care = false;
    }

    PROFILER_BEGIN("put into storage");
#ifdef INTERNALTEST
    // run below just after configure --with-internal-test=yes
    if (key.server_flag == TAIR_SERVERFLAG_RSYNC) {
      char* copy_key = (char*)malloc(sizeof(char) * (mkey.get_size() - 1));
      char* temp_value = value.get_data();
      int sharp_index = value.get_size() - 1;
      for (; sharp_index >= 0 && temp_value[sharp_index] != '#'; sharp_index--);
      if (sharp_index < 0) sharp_index = 0;
      char* copy_val = (char*)malloc(sizeof(char) * (value.get_size() - sharp_index + 1));
      memcpy(copy_key, (char*)mkey.get_data() + 2, mkey.get_size() - 2);
      memcpy(copy_val, value.get_data() + sharp_index, value.get_size() - sharp_index);
      copy_key[mkey.get_size() - 2] = '\0';
      copy_val[value.get_size() - sharp_index] = '\0';
      log_error("RSYNC_ORDER_TEST(%s %s)", copy_key, copy_val);
      free(copy_key);
      free(copy_val);
    }
#endif
    rc = storage_mgr->put(bucket_number, mkey, value, version_care, expire_time);
    PROFILER_END();

    if (rc == TAIR_RETURN_SUCCESS) {
        key.data_meta = mkey.data_meta;
        // for duplicate
        key.data_meta.flag = old_flag;

        if (migrate_log != NULL && need_do_migrate_log(bucket_number)) {
            PROFILER_BEGIN("do migrate log");
            migrate_log->log(SN_PUT, mkey, value, bucket_number);
            PROFILER_END();
        }

        if (op_flag & TAIR_OPERATION_DUPLICATE) {
            vector<uint64_t> slaves;
            bool is_failover = get_slaves(key.server_flag, bucket_number, slaves);
            if (slaves.empty() == false) {
                PROFILER_BEGIN("do duplicate");
                // with stat data
                key.data_meta.flag |= TAIR_SERVER_WITH_STAT;
                //duplicator->(area, &key, &value, bucket_number, slaves);
                if (request)
                    rc = duplicator->duplicate_data(area, &key, &value, bucket_number,
                                                    slaves, request == NULL ? NULL : request->r);
                PROFILER_END();
            } else if (is_failover && dup_depot != NULL) {
                // ignore fail ?
                // dup_depot->add(TAIR_OP_TYPE_PUT, &mkey, &value);
                dup_depot->add(SN_PUT, mkey, value, bucket_number);
            }
        }

        do_remote_sync(TAIR_REMOTE_SYNC_TYPE_PUT, &mkey, &value, rc, op_flag);
    }
    TAIR_STAT.stat_put(area);
    if (LIKELY(with_stat)) {
        if (key.get_prefix_size() == 0) {
            // this packet comes from tair::tair_client_impl::put()
            stat_->update(area, OP_PUT);
            stat_->update(area, OP_PUT_KEY);
        } else {
            // this packet comes from tair::tair_client_impl::prefix_put() or prefix_puts()
            stat_->update(area, OP_PF_PUT_KEY);
            stat_->update(area, OP_PF_PUT);
        }
    }
    return rc;
}

int tair_manager::direct_put(data_entry &key, data_entry &value, bool update_stat) {
    if (key.get_size() >= TAIR_MAX_KEY_SIZE_WITH_AREA || key.get_size() < 1) {
        log_debug("key size invalid : %d", key.get_size());
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (value.get_size() >= TAIR_MAX_DATA_SIZE || value.get_size() < 1) {
        log_debug("value size invalid : %d", value.get_size());
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry akey = key;
    int area = akey.decode_area();
    key.area = area;

    int bucket_number = get_bucket_number(akey);

    int rc = storage_mgr->put(bucket_number, key, value, false, key.data_meta.edate);

    if (rc == TAIR_RETURN_LOCK_EXIST) {
        rc = TAIR_RETURN_SUCCESS;
    }

    TAIR_STAT.stat_put(area);
    if (UNLIKELY(update_stat)) {
        stat_->update(area, OP_PUT);
        stat_->update(area, OP_PUT_KEY);
    }

    return rc;
}

int tair_manager::direct_remove(data_entry &key, bool update_stat) {
    if (key.get_size() >= TAIR_MAX_KEY_SIZE_WITH_AREA || key.get_size() < 1) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    data_entry akey = key;
    int area = akey.decode_area();

    int bucket_number = get_bucket_number(akey);


    int rc = storage_mgr->remove(bucket_number, key, false);

    if (rc == TAIR_RETURN_DATA_NOT_EXIST) {
        // for migrate, return SUCCESS
        rc = TAIR_RETURN_SUCCESS;
    }

    TAIR_STAT.stat_remove(area);
    if (UNLIKELY(update_stat))
        stat_->update(area, OP_REMOVE_KEY);

    return rc;
}

int tair_manager::add_count(int area, data_entry &key, int count, int init_value, int low_bound, int upper_bound,
                            int *result_value, int expire_time, base_packet *request, int heart_version) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (verify_key(key) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    data_entry old_value;
    data_entry mkey = key;
    mkey.merge_area(area);
    int bucket_number = get_bucket_number(key);
    int op_flag = get_op_flag(bucket_number, key.server_flag);
    bool need_duplicate = (op_flag & TAIR_OPERATION_DUPLICATE);
    bool need_log = (migrate_log != NULL && need_do_migrate_log(bucket_number));

    int rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    PROFILER_BEGIN("should write local?");
    if (should_write_local(bucket_number, key.server_flag, op_flag, rc) == false) {
        PROFILER_END();
        return rc;
    }
    PROFILER_END();

    //check if it's support add_count or it's a mdb enginer.
    rc = storage_mgr->add_count(bucket_number, mkey, count, init_value, low_bound, upper_bound, expire_time,
                                *result_value);

    stat_->update(area, OP_ADD);

    if (TAIR_RETURN_NOT_SUPPORTED != rc) {
        //if add_count success,now just mdb supported add_count.
        if (rc == TAIR_RETURN_SUCCESS) {
            if (need_duplicate || need_log) {
                char buf[INCR_DATA_SIZE];
                SET_INCR_DATA_COUNT(buf, *result_value);
                old_value.set_data(buf, INCR_DATA_SIZE);
                storage_mgr->set_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_ADDCOUNT);
            }
            if (need_duplicate) {
                key.data_meta = mkey.data_meta;
                rc = do_duplicate(area, key, old_value, bucket_number, request, heart_version);

            }
            //log it
            if (need_log) {
                PROFILER_BEGIN("do migrate log");
                migrate_log->log(SN_PUT, mkey, old_value, bucket_number);
                PROFILER_END();
            }
        }
        return rc;
    }

    tbsys::CThreadGuard guard(&counter_mutex[get_mutex_index(key)]);
    // reserve client key flag(maybe skip cache flag)
    int old_flag = key.data_meta.flag;
    // get from storage engine
    PROFILER_BEGIN("get from storage");
    rc = get(area, key, old_value, false);
    PROFILER_END();
    log_debug("get result: %d, flag: %d", rc, key.data_meta.flag);
    key.data_meta.log_self();
    //the exist counter
    if (rc == TAIR_RETURN_SUCCESS && storage_mgr->test_flag(key.data_meta.flag, TAIR_ITEM_FLAG_ADDCOUNT)) {
        // old value exist
        int32_t *v = (int32_t *) (old_value.get_data() + ITEM_HEAD_LENGTH);
        log_debug("old count: %d, new count: %d, init value: %d, low bound: %d, upper bound; %d",
                  (*v), count, init_value, low_bound, upper_bound);

        if (util::boundary_available(*v + count, low_bound, upper_bound)) {
            //the sum of `(*v) and `count was belong to the range [low_bound, upper_bound].
            *v += count;
            *result_value = *v;
        } else {
            return TAIR_RETURN_COUNTER_OUT_OF_RANGE;
        }
    }
        //key was exist, with out TAIR_ITEM_FLAG_ADDCOUNT
    else if (rc == TAIR_RETURN_SUCCESS) {
        log_debug("cann't override old value");
        return TAIR_RETURN_CANNOT_OVERRIDE;
    }
        //key was not exist, new counter will be created if the value boundary is available,
        //otherwise, return TAIR_RETURN_COUNTER_OUT_OF_RANGE.
    else {
        if (util::boundary_available(init_value + count, low_bound, upper_bound)) {
            char fv[INCR_DATA_SIZE];
            *result_value = init_value + count;
            SET_INCR_DATA_COUNT(fv, *result_value);
            old_value.set_data(fv, INCR_DATA_SIZE);
        } else {
            //out of range
            return TAIR_RETURN_COUNTER_OUT_OF_RANGE;
        }
    }

    storage_mgr->set_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_ADDCOUNT);
    // reassign key's meta flag
    key.data_meta.flag = old_flag;
    log_debug("before put flag: %d", old_value.data_meta.flag);
    PROFILER_BEGIN("save count into storage");
    int result = put(area, key, old_value, expire_time, request, heart_version, false);
    PROFILER_END();
    return result;
}

int tair_manager::get(int area, data_entry &key, data_entry &value, bool with_stat) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (verify_key(key) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    data_entry mkey = key;
    mkey.merge_area(area);

    int bucket_number = get_bucket_number(key);
    log_debug("get request will server in bucket: %d", bucket_number);
    PROFILER_BEGIN("get from storage engine");
    int rc = storage_mgr->get(bucket_number, mkey, value, with_stat);
    PROFILER_END();
    key.data_meta = mkey.data_meta;
    if (LIKELY(with_stat)) {
        TAIR_STAT.stat_get(area, rc);
        if (key.get_prefix_size() == 0)
            stat_->update_get(area, rc);
        else
            stat_->update_prefix_get(area, rc);
    }
    if (rc == TAIR_RETURN_SUCCESS) {
        rc = (value.data_meta.flag & TAIR_ITEM_FLAG_DELETED) ?
             TAIR_RETURN_HIDDEN : TAIR_RETURN_SUCCESS;
    }
    //! even if rc==TAIR_RETURN_HIDDEN, 'value' still hold the value
    return rc;
}

int tair_manager::get_range(int32_t area, data_entry &key_start, data_entry &key_end, int offset, int limit, int type,
                            std::vector<data_entry *> &result, bool &has_next) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (verify_key(key_start) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int bucket_number = get_bucket_number(key_start);

    key_start.merge_area(area);
    key_end.merge_area(area);

    PROFILER_BEGIN("get range from storage engine");
    int rc = storage_mgr->get_range(bucket_number, key_start, key_end, offset, limit, type, result, has_next);
    PROFILER_END();

    //TAIR_STAT.stat_get_range(area, rc);
    stat_->update(area, OP_GET_RANGE);
    // rc will not be TAIR_HAS_MORE_DATA here
    // TAIR_RETURN_SUCCESS success, otherwise fail.
    int32_t key_num = (int32_t) result.size();
    // if CMD_RANGE_ALL, vector contains key and value, so key_num = vector.size()/2;
    if (CMD_RANGE_ALL == type)
        key_num = (key_num >> 1);

    stat_->update(area, OP_GET_RANGE_KEY, key_num);
    return rc;
}

int tair_manager::del_range(int32_t area, data_entry &key_start, data_entry &key_end, int offset, int limit, int type,
                            std::vector<data_entry *> &result, bool &has_next) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (verify_key(key_start) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int bucket_number = get_bucket_number(key_start);

    key_start.merge_area(area);
    key_end.merge_area(area);

    PROFILER_BEGIN("del range from storage engine");
    int rc = storage_mgr->del_range(bucket_number, key_start, key_end, offset, limit, type, result, has_next);
    stat_->update(area, OP_PF_REMOVE_KEY, result.size());
    PROFILER_END();

    //TAIR_STAT.stat_remove(area);
    return rc;
}

int tair_manager::hide(int area, data_entry &key, base_packet *request, int heart_version) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (verify_key(key) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    tbsys::CThreadGuard guard(&counter_mutex[get_mutex_index(key)]);
    // get from storage engine
    data_entry old_value;
    PROFILER_BEGIN("hide::get");
    int rc = get(area, key, old_value, false);
    PROFILER_END();
    log_debug("hide::get result: %d, flag: %d", rc, key.data_meta.flag);
    key.data_meta.log_self();

    if (rc == TAIR_RETURN_SUCCESS) {
        old_value.data_meta.flag |= TAIR_ITEM_FLAG_DELETED;
        PROFILER_BEGIN("hide::put");
        rc = put(area, key, old_value, old_value.data_meta.edate, request, heart_version);
        PROFILER_END();
    } else if (rc == TAIR_RETURN_HIDDEN) {
        rc = TAIR_RETURN_SUCCESS;
    }

    TAIR_STAT.stat_remove(area);
    if (key.get_prefix_size() == 0)
        stat_->update(area, OP_HIDE);
    else
        stat_->update(area, OP_PF_HIDE);
    return rc;
}

int tair_manager::get_hidden(int area, data_entry &key, data_entry &value) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (verify_key(key) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    data_entry mkey = key;
    mkey.merge_area(area);

    int bucket_number = get_bucket_number(key);
    log_debug("get_hidden request will server in bucket: %d", bucket_number);
    PROFILER_BEGIN("get_hidden from storage engine");
    int rc = storage_mgr->get(bucket_number, mkey, value);
    PROFILER_END();
    if (rc == TAIR_RETURN_SUCCESS) {
        rc = (value.data_meta.flag & TAIR_ITEM_FLAG_DELETED) ?
             TAIR_RETURN_HIDDEN : TAIR_RETURN_SUCCESS;
    }
    key.data_meta = mkey.data_meta;
    TAIR_STAT.stat_get(area, rc);
    if (key.get_prefix_size() == 0)
        stat_->update_get(area, rc);
    else
        stat_->update_prefix_get(area, rc);

    return rc;
}

int tair_manager::remove(int area, data_entry &key, request_remove *request, int heart_version) {
    if (verify_key(key) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    data_entry mkey = key;
    mkey.merge_area(area);

    int bucket_number = get_bucket_number(key);

    int op_flag = get_op_flag(bucket_number, key.server_flag);
    int rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    PROFILER_BEGIN("should write local?");
    if (should_write_local(bucket_number, key.server_flag, op_flag, rc) == false) {
        PROFILER_END();
        return rc;
    }
    PROFILER_END();
    bool version_care = op_flag & TAIR_OPERATION_VERSION;
    if (is_meta_flag_unit(key.data_meta.flag)) {
        version_care = false;
    }

    PROFILER_BEGIN("remove from storage engine");
    rc = storage_mgr->remove(bucket_number, mkey, version_care);
    PROFILER_END();

    if (rc == TAIR_RETURN_SUCCESS || rc == TAIR_RETURN_DATA_NOT_EXIST) {
        if (op_flag & TAIR_OPERATION_DUPLICATE) {
            vector<uint64_t> slaves;
            bool is_failover = get_slaves(key.server_flag, bucket_number, slaves);
            if (slaves.empty() == false) {
                PROFILER_BEGIN("do duplicate");
                int rc1 = duplicator->duplicate_data(area, &key, NULL, bucket_number, slaves,
                                                     (rc == TAIR_RETURN_DATA_NOT_EXIST) ? NULL : (request == NULL ? NULL
                                                                                                                  : request->r));
                if (TAIR_RETURN_SUCCESS == rc) rc = rc1;
                PROFILER_END();
            } else if (is_failover && dup_depot != NULL) {
                dup_depot->add(SN_REMOVE, mkey, mkey, bucket_number);
            }
        }

        do_remote_sync(TAIR_REMOTE_SYNC_TYPE_DELETE, &mkey, NULL, rc, op_flag);

        if (migrate_log != NULL && need_do_migrate_log(bucket_number)) {
            PROFILER_BEGIN("do migrate log");
            migrate_log->log(SN_REMOVE, mkey, mkey, bucket_number);
            PROFILER_END();
        }

    }
    TAIR_STAT.stat_remove(area);
    if (key.get_prefix_size() == 0)
        stat_->update(area, OP_REMOVE_KEY);
    else
        stat_->update(area, OP_PF_REMOVE_KEY);
    return rc;
}

int tair_manager::uniq_remove(int area, int bucket, data_entry &key) {
    if (verify_key(key) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (bucket < 0 || bucket >= TAIR_MAX_BUCKET_NUMBER) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    data_entry mkey = key;
    mkey.merge_area(area);

    int bucket_number = bucket;

    int op_flag = get_op_flag(bucket_number, key.server_flag);
    int rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    PROFILER_BEGIN("should write local?");
    if (should_write_local(bucket_number, key.server_flag, op_flag, rc) == false) {
        PROFILER_END();
        return rc;
    }
    PROFILER_END();
    bool version_care = op_flag & TAIR_OPERATION_VERSION;

    PROFILER_BEGIN("remove from storage engine");
    rc = storage_mgr->remove(bucket_number, mkey, version_care);
    PROFILER_END();

    log_debug("uniq_remove rc is %d, bucket num is %d, area is %d, key size is %d", rc, bucket_number, area,
              key.get_size());

    TAIR_STAT.stat_remove(area);

    return rc;
}

int tair_manager::batch_put(int area, mput_record_vec *record_vec, request_mput *request, int heart_version) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT || record_vec == NULL) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (record_vec->size() <= 0) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    // we consider mput_record_vec are all one bucket's kv
    int bucket_number = get_bucket_number(*((*record_vec->begin())->key));
    int op_flag = get_op_flag(bucket_number, request->server_flag);

    if (should_write_local(bucket_number, request->server_flag, op_flag, rc) == false) {
        return TAIR_RETURN_REMOVE_NOT_ON_MASTER;
    }

    bool version_care = op_flag & TAIR_OPERATION_VERSION;
    PROFILER_BEGIN("do batch put");
    rc = storage_mgr->batch_put(bucket_number, area, record_vec, version_care);
    PROFILER_END();

    TAIR_STAT.stat_put(area, record_vec->size());
    stat_->update(area, OP_PUT);
    stat_->update(area, OP_PUT_KEY, record_vec->size());

    if (TAIR_RETURN_SUCCESS == rc && migrate_log != NULL && need_do_migrate_log(bucket_number)) {
        // do migrate
        for (mput_record_vec::const_iterator it = record_vec->begin(); it != record_vec->end(); ++it) {
            if (migrate_log != NULL && need_do_migrate_log(bucket_number)) {
                // TODO. mkey everaywhere is NOT really necessary.
                // Once get_bucket_number() check data_entry.is_merged(),
                // key can be used no matter merged or not, so mkey can be demised.
                data_entry mkey = *((*it)->key);
                mkey.merge_area(area);
                // migrate no flag here.
                migrate_log->log(SN_PUT, mkey, (*it)->value->get_d_entry(), bucket_number);
            } else {
                // migrate status change, we don't handle this now
                rc = TAIR_RETURN_MIGRATE_BUSY;
                break;
            }
        }
    }

    if (TAIR_RETURN_SUCCESS == rc) {
        // do duplicate
        if (request->server_flag != TAIR_SERVERFLAG_DUPLICATE) {
            vector<uint64_t> slaves;
            bool is_failover = get_slaves(request->server_flag, bucket_number, slaves);
            if (!slaves.empty()) {
                rc = duplicator->duplicate_batch_data(bucket_number, record_vec, slaves, request);
            } else if (is_failover && dup_depot != NULL) {
                for (mput_record_vec::const_iterator it = record_vec->begin(); it != record_vec->end(); ++it) {
                    data_entry mkey = *((*it)->key);
                    mkey.merge_area(area);
                    dup_depot->add(SN_PUT, mkey, (*it)->value->get_d_entry(), bucket_number);
                }
            }
        } else {
            log_debug("bp send return packet to duplicate source");
            response_duplicate *dresp = new response_duplicate();
            dresp->setChannelId(request->getChannelId());
            dresp->server_id = local_server_ip::ip;
            dresp->packet_id = request->packet_id;
            dresp->bucket_id = bucket_number;
            request->r->opacket = dresp;
        }
    }

    return rc;
}

int
tair_manager::batch_remove(int area, const tair_dataentry_set *key_list, request_remove *request, int heart_version) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;

    set<data_entry *, data_entry_comparator>::iterator it;
    //check param first, one fail,all fail.
    for (it = key_list->begin(); it != key_list->end(); ++it) {
        uint64_t target_server_id = 0;

        data_entry *pkey = (*it);
        data_entry key = *pkey; //for same code.
        key.server_flag = request->server_flag;

        if (verify_key(key) == false) {
            return TAIR_RETURN_ITEMSIZE_ERROR;
        }

        if (should_proxy(key, target_server_id)) {
            // proxyed, we will not touch it
            // the previon assume it as a EXIT_FAILURE;
            log_error("batch remove,but have key not proxy.");
            return TAIR_RETURN_REMOVE_NOT_ON_MASTER;
        }
        //if (count != request->key_list->size()) rc = EXIT_FAILURE;

        data_entry mkey = key;
        mkey.merge_area(area);
        int bucket_number = get_bucket_number(key);

        int op_flag = get_op_flag(bucket_number, key.server_flag);

        int rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
        if (should_write_local(bucket_number, key.server_flag, op_flag, rc) == false) {
            return TAIR_RETURN_REMOVE_NOT_ON_MASTER;
        }

    }

    //the storage_mgr should has batch interface
    for (it = key_list->begin(); it != key_list->end(); ++it) {
        data_entry *pkey = (*it);
        data_entry key = *pkey; //for same code.
        key.server_flag = request->server_flag;

        data_entry mkey = key;
        mkey.merge_area(area);

        int bucket_number = get_bucket_number(key);
        int op_flag = get_op_flag(bucket_number, key.server_flag);

        bool version_care = op_flag & TAIR_OPERATION_VERSION;
        rc = storage_mgr->remove(bucket_number, mkey, version_care);

        if (rc == TAIR_RETURN_SUCCESS || rc == TAIR_RETURN_DATA_NOT_EXIST) {
            if (migrate_log != NULL && need_do_migrate_log(bucket_number)) {
                migrate_log->log(SN_REMOVE, mkey, mkey, bucket_number);
            }
            if (op_flag & TAIR_OPERATION_DUPLICATE) {
                log_error("mdelete shouldn't called in multi-copy cluster: area=%d, bucket_number=%d", area,
                          bucket_number);
            }
        } else {
            //one fail,all fail
            return TAIR_RETURN_REMOVE_ONE_FAILED;
        }
        TAIR_STAT.stat_remove(area);
        stat_->update(area, OP_REMOVE_KEY);
    }

    //if(_need_dup)
    return rc;
}

int tair_manager::clear(int area) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    return storage_mgr->clear(area);
}

int tair_manager::sync(int32_t bucket, std::vector<common::Record *> *records,
                       base_packet *request, int heart_version) {
    int ret = TAIR_RETURN_SUCCESS;
    std::vector<common::Record *> &rhs = *records;
    for (size_t i = 0; i < rhs.size(); i++) {
        common::KVRecord *rh = static_cast<common::KVRecord *>(rhs[i]);
        if (rh->operator_type_ == TAIR_REMOTE_SYNC_TYPE_PUT) {
            ret = put(rh->key_->get_area(), *(rh->key_), *(rh->value_),
                      rh->key_->data_meta.edate, NULL, heart_version);
        } else if (rh->operator_type_ == TAIR_REMOTE_SYNC_TYPE_DELETE) {
            ret = remove(rh->key_->get_area(), *(rh->key_), NULL, heart_version);
            if (ret == TAIR_RETURN_DATA_NOT_EXIST) {
                ret = TAIR_RETURN_SUCCESS;
            }
        }
        if (ret == TAIR_RETURN_MTIME_EARLY) {
            ret = TAIR_RETURN_SUCCESS;
        }
        if (ret != TAIR_RETURN_SUCCESS) {
            return ret;
        }
    }

    // now op_flag == TAIR_OPERATION_DUPLICATE that mean sync data here should duplicate it to slave
    int op_flag = get_op_flag(bucket, rhs[0]->key_->server_flag);
    // to do duplicate
    if (op_flag & TAIR_OPERATION_DUPLICATE) {
        std::vector<uint64_t> slaves;
        get_slaves(rhs[0]->key_->server_flag, bucket, slaves);
        if (slaves.empty() == false) {
            PROFILER_BEGIN("do duplicate");
            if (request) {
                ret = duplicator->duplicate_data(bucket, records, slaves, request->r);
            }
            PROFILER_END();
        }
        // did by each sub key request, put, remove
        // else if (is_failover && dup_depot != NULL) {
        //  // ignore fail ?
        //  // dup_depot->add(TAIR_OP_TYPE_PUT, &mkey, &value);
        //  dup_depot->add(SN_PUT, mkey, value, bucket_number);
        //}
    }

    return ret;
}

int tair_manager::op_cmd(ServerCmdType cmd, std::vector<std::string> &params, std::vector<std::string> &infos) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }
    if (cmd <= TAIR_SERVER_CMD_MIN_TYPE || cmd >= TAIR_SERVER_CMD_MAX_TYPE) {
        log_error("unknown cmd type: %d", cmd);
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    int ret = TAIR_RETURN_SUCCESS;
    bool need_storage_ops = true;
    switch (cmd) {
        case TAIR_SERVER_CMD_PAUSE_RSYNC:
            if (remote_sync_mgr != NULL) {
                if (params.size() >= 1) {
                    if (params[0] == "all") {
                        remote_sync_mgr->pause(true);
                    } else {
                        // no this unit group will return TAIR_RETURN_FAILED
                        vector<string> values;
                        tair::util::string_util::split_str(params[0].c_str(), "@", values);
                        if (values.size() != 2) {
                            return TAIR_RETURN_FAILED;
                        }
                        return remote_sync_mgr->pause(true, values[1], values[0]);
                    }
                } else {
                    remote_sync_mgr->pause(true);
                }
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_RESUME_RSYNC:
            if (remote_sync_mgr != NULL) {
                if (params.size() >= 1) {
                    if (params[0] == "all") {
                        remote_sync_mgr->pause(false);
                    } else {
                        // no this unit group will return TAIR_RETURN_FAILED
                        vector<string> values;
                        tair::util::string_util::split_str(params[0].c_str(), "@", values);
                        if (values.size() != 2) {
                            return TAIR_RETURN_FAILED;
                        }
                        return remote_sync_mgr->pause(false, values[1], values[0]);
                    }
                } else {
                    remote_sync_mgr->pause(false);
                }
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_WATCH_RSYNC:
            if (remote_sync_mgr != NULL) {
                if (params.size() == 1) {
                    if (params[0] == "all") {
                        return remote_sync_mgr->watch("all", "");
                    } else {
                        vector<string> values;
                        tair::util::string_util::split_str(params[0].c_str(), "@", values);
                        if (values.size() != 2) {
                            return TAIR_RETURN_FAILED;
                        }
                        return remote_sync_mgr->watch(values[1], values[0]);
                    }
                } else {
                    return TAIR_RETURN_FAILED;
                }
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_STAT_RSYNC:
            if (remote_sync_mgr != NULL) {
                if (params.size() == 1) {
                    if (params[0] == "all") {
                        return remote_sync_mgr->statistics("all", "");
                    } else {
                        vector<string> values;
                        tair::util::string_util::split_str(params[0].c_str(), "@", values);
                        if (values.size() != 2) {
                            return TAIR_RETURN_FAILED;
                        }
                        return remote_sync_mgr->statistics(values[1], values[0]);
                    }
                } else {
                    return TAIR_RETURN_FAILED;
                }
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_RSYNC_CONNECTION_STAT:
            if (remote_sync_mgr != NULL) {
                infos.push_back(remote_sync_mgr->connection_stat());
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_OPTIONS_RSYNC:
            if (remote_sync_mgr != NULL) {
                if (params.size() == 1) {
                    if (params[0] == "all") {
                        infos.push_back(remote_sync_mgr->options("all", ""));
                        return TAIR_RETURN_SUCCESS;
                    } else {
                        vector<string> values;
                        tair::util::string_util::split_str(params[0].c_str(), "@", values);
                        if (values.size() != 2) {
                            return TAIR_RETURN_FAILED;
                        }
                        infos.push_back(remote_sync_mgr->options(values[1], values[0]));
                        return TAIR_RETURN_SUCCESS;
                    }
                } else if (params.size() == 3) {
                    int val = atoi(params[2].c_str());
                    std::map<std::string, int> m;
                    m.insert(std::make_pair<std::string, int>(params[1], val));
                    if (params[0] == "all") {
                        return remote_sync_mgr->options("all", "", m);
                    } else {
                        vector<string> values;
                        tair::util::string_util::split_str(params[0].c_str(), "@", values);
                        if (values.size() != 2) {
                            return TAIR_RETURN_FAILED;
                        }
                        return remote_sync_mgr->options(values[1], values[0], m);
                    }
                } else {
                    return TAIR_RETURN_FAILED;
                }
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_SET_RSYNC_CONFIG_SERVICE:
            if (remote_sync_mgr != NULL && params.size() > 0) {
                remote_sync_mgr->set_rsync_config_service(params[0]);
            } else {
                return TAIR_RETURN_FAILED;
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_SET_RSYNC_CONFIG_INTERVAL:
            if (remote_sync_mgr != NULL && params.size() > 0) {
                remote_sync_mgr->set_rsync_config_update_interval(atoi(params[0].c_str()));
            } else {
                return TAIR_RETURN_FAILED;
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_GET_RSYNC_STAT:
            if (remote_sync_mgr != NULL) {
                infos.push_back(remote_sync_mgr->get_rsync_info());
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_GET_CLUSTER_INFO:
            if (remote_sync_mgr != NULL) {
                infos.push_back(remote_sync_mgr->get_cluster_info());
            }
            return TAIR_RETURN_SUCCESS;
        case TAIR_SERVER_CMD_SET_CONFIG:
            ret = params.size() >= 2 ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
            if (ret != TAIR_RETURN_SUCCESS) {
                break;
            }
            // TODO: maybe config center to do all config modify.
            if (params[0] == TAIR_RSYNC_MTIME_CARE && remote_sync_mgr != NULL) {
                remote_sync_mgr->set_mtime_care(atoi(params[1].c_str()) > 0);
                return TAIR_RETURN_SUCCESS;
            } else if (params[0] == TAIR_RSYNC_WAIT_US && remote_sync_mgr != NULL) {
                // TODO now unit is us, change to ms
                remote_sync_mgr->set_wait_us(atoi(params[1].c_str()));
                return TAIR_RETURN_SUCCESS;
            }
            break;
        case TAIR_SERVER_CMD_LDB_INSTANCE_CONFIG:
            ret = params.size() >= 3 ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
            break;
        case TAIR_SERVER_CMD_CLOSE_UNUESD_BUCKETS: {
            if (params.size() >= 1) {
                vector<char *> buckets;
                vector<int> close_buckets;
                char bucket_list[params[0].size() + 1];
                strncpy(bucket_list, params[0].c_str(), params[0].size() + 1);
                tbsys::CStringUtil::split(bucket_list, ",", buckets);
                for (vector<char *>::iterator iter = buckets.begin(); iter != buckets.end(); iter++) {
                    if (!table_mgr->bucket_is_held(atoi(*iter))) {
                        log_error("add bucket %d to close_bucket", atoi(*iter));
                        close_buckets.push_back(atoi(*iter));
                    } else {
                        log_error("bucket[%d] is held by this server. close failed", atoi(*iter));
                    }
                }
                if (close_buckets.size() != 0) {
                    storage_mgr->close_buckets(close_buckets);
                }
            }
        }
            break;
        case TAIR_ADMIN_SERVER_CMD_MODIFY_STAT_HIGN_OPS: {
            need_storage_ops = false;
            int high_ops = atoi(params[0].c_str());
            char buf[21];
            snprintf(buf, 20, "%d", stat_helper::stat_high_ops_count);
            string response_info;
            response_info = tbsys::CNetUtil::addrToString(local_server_ip::ip) + ", value: " + buf;
            if (high_ops != 0 && high_ops != -1) {
                log_warn("modify stat_helper::stat_high_ops_count, %d -> %d", stat_helper::stat_high_ops_count,
                         high_ops);
                stat_helper::stat_high_ops_count = high_ops;
                response_info = response_info + " -> " + params[0];
            }
            infos.push_back(response_info);
            ret = TAIR_RETURN_SUCCESS;
        }
            break;

        default:
            break;
    }

    // op cmd to storage manager
    if (need_storage_ops && ret == TAIR_RETURN_SUCCESS) {
        ret = storage_mgr->op_cmd(cmd, params, infos);
    }

    if (cmd == TAIR_SERVER_CMD_GET_CONFIG) {
        if (remote_sync_mgr != NULL) {
            {
                std::ostringstream os;
                os << "kRsyncWaitUs = " << remote_sync_mgr->get_wait_us();
                infos.push_back(os.str());
            }
            {
                std::ostringstream os;
                os << "kRsyncConfigService = " << remote_sync_mgr->get_rsync_config_service();
                infos.push_back(os.str());
            }
            {
                std::ostringstream os;
                os << "kRsyncConfigUpdateInterval = " << remote_sync_mgr->get_rsync_config_update_interval();
                infos.push_back(os.str());
            }
        }
    }

    return ret;
}

int tair_manager::lock(int area, LockType lock_type, data_entry &key, base_packet *request, int heart_version) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (verify_key(key) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    tbsys::CThreadGuard guard(&counter_mutex[get_mutex_index(key)]);
    // get from storage engine
    data_entry old_value;
    PROFILER_BEGIN("lock::get");
    int rc = get(area, key, old_value);
    PROFILER_END();
    log_debug("lock::get result: %d, flag: %d", rc, key.data_meta.flag);
    key.data_meta.log_self();

    if (rc == TAIR_RETURN_SUCCESS) {
        rc = storage_mgr->test_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_LOCKED) ?
             TAIR_RETURN_LOCK_EXIST : TAIR_RETURN_LOCK_NOT_EXIST;
        switch (lock_type) {
            case LOCK_STATUS:
                break;
            case LOCK_VALUE:
                if (rc != TAIR_RETURN_LOCK_EXIST) {
                    storage_mgr->set_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_LOCKED);
                    rc = TAIR_RETURN_SUCCESS;
                }
                break;
            case UNLOCK_VALUE:
                if (rc != TAIR_RETURN_LOCK_NOT_EXIST) {
                    storage_mgr->clear_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_LOCKED);
                    // unlock operation will skip lock flag check when put.
                    // little ugly..
                    old_value.server_flag |= TAIR_OPERATION_UNLOCK;
                    rc = TAIR_RETURN_SUCCESS;
                }
                break;
            case PUT_AND_LOCK_VALUE: // TODO: put_and_lock
            default:
                log_debug("invalid lock type: %d", lock_type);
                rc = TAIR_RETURN_FAILED;
                break;
        }
    } // TODO: else if (put_and_lock)

    if (TAIR_RETURN_SUCCESS == rc) {
        PROFILER_BEGIN("lock::put");
        rc = put(area, key, old_value, old_value.data_meta.edate, request, heart_version);
        PROFILER_END();
    }

    return rc;
}

void tair_manager::do_dump(set<dump_meta_info> dump_meta_infos) {
    return;// NOT_FIXED_ITEM_FUNC
    log_debug("receive dump request, size: %lu", dump_meta_infos.size());
    if (dump_meta_infos.size() == 0) return;

    // cancal all previous task
    dump_mgr->cancle_all();

    // make sure all task has been canceled
    while (dump_mgr->is_all_stoped() == false) usleep(100);

    set<dump_meta_info>::iterator it;
    set<tair::storage::dump_filter> dump_filters;

    // dump directory
    const string dir(TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_DUMP_DIR, TAIR_DEFAULT_DUMP_DIR));
    set<uint32_t> buckets;
    for (uint32_t bucket_no = 0; bucket_no < table_mgr->get_bucket_count(); bucket_no++) {
        if (table_mgr->is_master(bucket_no, TAIR_SERVERFLAG_CLIENT)) {
            buckets.insert(bucket_no);
        }
    }

    for (it = dump_meta_infos.begin(); it != dump_meta_infos.end(); it++) {
        dump_meta_info info = *it;
        tair::storage::dump_filter filter;
        filter.set_parameter(info.start_time, info.end_time, info.area, dir);
        log_debug("add dump: {startTime:%u, endTime:%u, area:%d, dir:%s }", info.start_time, info.end_time, info.area,
                  dir.c_str());
        dump_filters.insert(filter);
    }

    dump_mgr->do_dump(dump_filters, buckets, time(NULL));

}

int tair_manager::get_meta(int area, data_entry &key, item_meta_info &meta, int bucket) {
    bool has_merged = key.has_merged;
    if (!has_merged) {
        key.merge_area(area);
    }

    int rc = storage_mgr->get_meta(key, meta);
    if (rc != TAIR_RETURN_NOT_SUPPORTED) {
        if (!has_merged) {
            key.decode_area();
        }
        return rc;
    }
    //~ following would be expensive
    data_entry data;
    item_meta_info kmeta = key.data_meta;
    rc = storage_mgr->get(bucket, key, data);
    meta = key.data_meta;
    key.data_meta = kmeta;
    if (!has_merged) {
        key.decode_area();
    }

    return rc;
}

// used by add_count
int tair_manager::do_duplicate(int area, data_entry &key, data_entry &value,
                               int bucket_number, base_packet *request, int heart_vesion) {
    int rc = 0;
    vector<uint64_t> slaves;
    bool is_failover = get_slaves(key.server_flag, bucket_number, slaves);
    if (slaves.empty() == false) {
        PROFILER_BEGIN("do duplicate");
        if (request) {
            rc = duplicator->duplicate_data(area, &key, &value, bucket_number, slaves, request->r);
        }
        PROFILER_END();
    } else if (is_failover && dup_depot != NULL) {
        key.area = area;
        // dup_depot->add(TAIR_OP_TYPE_PUT, &key, &value);
        dup_depot->add(SN_PUT, key, value, bucket_number);
    }
    return rc;
}

int tair_manager::do_remote_sync(TairRemoteSyncType type, common::data_entry *key, common::data_entry *value,
                                 int rc, int op_flag) {
    int ret = TAIR_RETURN_SUCCESS;
    // We only do remote sync when duplication succeeds,
    // so asynchronous duplicator will do remote sync logic self.
    // Here, only check remote sync operation flag and
    // NO asynchronous duplication(asynchronous duplication success will return TAIR_DUP_WAIT_RSP).
    if (remote_sync_mgr != NULL && (op_flag & TAIR_OPERATION_RSYNC) && TAIR_RETURN_SUCCESS == rc) {
        ret = remote_sync_mgr->add_record(type, key, value);
        // local cluster can ignore remote sync failure.
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("add record for remote sync fail: %d", ret);
        }
    }
    return ret;
}

bool tair_manager::is_migrating() {
    return migrate_mgr->is_migrating();
}

bool tair_manager::is_local(data_entry &key) {
    int bucket_number = get_bucket_number(key);
    return table_mgr->is_master(bucket_number, TAIR_SERVERFLAG_CLIENT);
}

bool tair_manager::should_proxy(data_entry &key, uint64_t &target_server_id) {
    if (key.server_flag == TAIR_SERVERFLAG_PROXY || key.server_flag == TAIR_SERVERFLAG_RSYNC_PROXY || localmode)
        return false; // if this is proxy, dont proxy

    if (localmode)
        return false;

    int bucket_number = get_bucket_number(key);
    bool is_migrated = migrate_done_set.test(bucket_number);
    // check if bucket is recovered
    bool is_recovered = recovery_done_set.test(bucket_number);
    if (is_migrated || is_recovered) {
        target_server_id = table_mgr->get_migrate_target(bucket_number);
        if (target_server_id == local_server_ip::ip) // target is myself, do not proxy
            target_server_id = 0;
    }

    return (is_migrated || is_recovered) && target_server_id != 0;
}

bool tair_manager::need_do_migrate_log(int bucket_number) {
    assert (migrate_mgr != NULL);
    return migrate_mgr->is_bucket_migrating(bucket_number);
}

void tair_manager::set_migrate_done(int bucket_number) {
    assert (migrate_mgr != NULL);
    migrate_done_set.set(bucket_number, true);
}

void tair_manager::set_area_quota(int area, int64_t quota) {
    if (supported_admin == false)
        storage_mgr->set_area_quota(area, quota);
}

int64_t tair_manager::get_area_quota(int area) {
    assert(area >= 0);
    assert(area < TAIR_MAX_AREA_COUNT);
    return this->area_total_quotas[area];
}

void tair_manager::set_all_area_total_quota(const std::map<int, int64_t> &quotas) {
    if (supported_admin == false)
        return;
    tbsys::CThreadGuard update_table_guard(&update_server_table_mutex);
    int last = 0;
    for (std::map<int, int64_t>::const_iterator iter = quotas.begin();
         iter != quotas.end(); ++iter, ++last) {
        for (; last < iter->first; ++last) {
            this->area_total_quotas[last] = 0;
            set_area_total_quota_internal(last, 0);
        }
        this->area_total_quotas[iter->first] = iter->second;
        set_area_total_quota_internal(iter->first, iter->second);
    }
    for (; last < TAIR_MAX_AREA_COUNT; ++last) {
        this->area_total_quotas[last] = 0;
        set_area_total_quota_internal(last, 0);
    }
}

void tair_manager::set_area_total_quota(int area, int64_t quota) {
    assert(area >= 0);
    assert(area < TAIR_MAX_AREA_COUNT);
    assert(quota >= 0);
    tbsys::CThreadGuard update_table_guard(&update_server_table_mutex);
    this->area_total_quotas[area] = quota;
    set_area_total_quota_internal(area, quota);
}

void tair_manager::set_area_total_quota_internal(int32_t area, int64_t quota) {
    quota = quota * this->current_bucket_count / this->total_bucket_count;
    log_info("set area(%d) quota(%ld) for this dataserver", area, quota);
    storage_mgr->set_area_quota(area, quota);
}

void tair_manager::update_quota_on_bucket_changed() {
    for (int32_t area = 0; area < TAIR_MAX_AREA_COUNT; ++area) {
        int64_t total_quota = this->area_total_quotas[area];
        if (total_quota <= 0) continue;
        set_area_total_quota_internal(area, total_quota);
    }
}

void tair_manager::update_server_table(uint64_t *server_table, int server_table_size, uint32_t server_table_version,
                                       int32_t data_need_remove,
                                       vector<uint64_t> &current_state_table, uint32_t copy_count,
                                       uint32_t bucket_count, bool calc_migrate) {
    if (localmode == true) {
        localmode_bucket = bucket_count;
        std::vector<int32_t> buckets;
        for (int32_t i = 0; i < localmode_bucket; ++i) {
            buckets.push_back(i);
        }
        storage_mgr->set_bucket_count(localmode_bucket);
        storage_mgr->init_buckets(buckets);
        return;
    }

    tbsys::CThreadGuard update_table_guard(&update_server_table_mutex);

    log_warn("updateServerTable, size: %d, calc migrate: %d", server_table_size, calc_migrate);
    table_mgr->do_update_table(server_table, server_table_size, server_table_version, copy_count, bucket_count,
                               calc_migrate);
    duplicator->set_max_queue_size(10 * ((table_mgr->get_copy_count() - 1) * 3 * 3 + 1));
    storage_mgr->set_bucket_count(table_mgr->get_bucket_count());

    migrate_done_set.resize(table_mgr->get_bucket_count());
    migrate_done_set.reset();
    if (status != STATUS_CAN_WORK) {
        if (data_need_remove == TAIR_DATA_NEED_MIGRATE)
            init_migrate_log();
        table_mgr->init_migrate_done_set(migrate_done_set, current_state_table);
        if (table_mgr->get_holding_buckets().empty() == false || table_mgr->get_padding_buckets().empty() == false)
            status = STATUS_CAN_WORK; // set inited status to true after init buckets
    }

    vector<int> release_buckets(table_mgr->get_release_buckets());
    if (release_buckets.empty() == false && localmode == false) {
        storage_mgr->close_buckets(release_buckets);
    }

    set<int> current_buckets;
    vector<int> holding_buckets(table_mgr->get_holding_buckets());
    if (holding_buckets.empty() == false) {
        // remove already migrated buckets
        vector<int>::reverse_iterator rit = holding_buckets.rbegin();
        while (rit != holding_buckets.rend()) {
            if (migrate_done_set.test(*rit))
                holding_buckets.erase((++rit).base());
            else
                ++rit;
        }
        storage_mgr->init_buckets(holding_buckets);
        current_buckets.insert(holding_buckets.begin(), holding_buckets.end());
    }

    vector<int> padding_buckets(table_mgr->get_padding_buckets());
    if (padding_buckets.empty() == false) {
        storage_mgr->init_buckets(padding_buckets);
        current_buckets.insert(padding_buckets.begin(), padding_buckets.end());
    }

    if (supported_admin) {
        int64_t current_bucket_count = current_buckets.size();
        int64_t total_bucket_count = copy_count * bucket_count;
        if (current_bucket_count != this->current_bucket_count ||
            total_bucket_count != this->total_bucket_count) {
            this->current_bucket_count = current_bucket_count < 1 ? 1 : current_bucket_count;
            this->total_bucket_count = total_bucket_count < 1 ? 1 : total_bucket_count;
            update_quota_on_bucket_changed();
        }
    }
    // clear dump task
    dump_mgr->cancle_all();
    assert (migrate_mgr != NULL);
    migrate_mgr->do_server_list_changed();

    bucket_server_map migrates(table_mgr->get_migrates());

    if (migrates.empty() == false) {
        // remove already migrated buckets
        bucket_server_map::iterator it = migrates.begin();
        while (it != migrates.end()) {
            if (migrate_done_set.test((*it).first))
                migrates.erase(it++);
            else
                ++it;
        }
        migrate_mgr->set_migrate_server_list(migrates, table_mgr->get_version());
    }

    // recovery related ops

    // stop the recovery_manager
    reset_recovery_task();
    // delete fail log if needed
    if (calc_migrate) {
        usleep(50000);
        clear_dup_depot();
    }


    duplicator->do_hash_table_changed();
}

void tair_manager::direct_update_table(uint64_t *server_table, uint32_t server_list_count, uint32_t copy_count,
                                       uint32_t bucket_count, bool is_failover,
                                       bool version_changed, uint32_t server_table_version) {
    if (localmode == true)
        return;
    // direct_update_table is invoked when transition diff or recovery version diff
    table_mgr->direct_update_table(server_table, server_list_count, copy_count, bucket_count, is_failover,
                                   version_changed, server_table_version);
}

bool tair_manager::is_recovering() {
    return recovery_mgr->is_recovering();
}

void tair_manager::set_recovery_done(int bucket_number) {
    assert (migrate_mgr != NULL);
    recovery_done_set.set(bucket_number, true);
}

void tair_manager::update_recovery_bucket(vector<uint64_t> recovery_ds, uint32_t transition_version) {
    if (localmode == true)
        return;

    tbsys::CThreadGuard update_table_guard(&update_server_table_mutex);

    // need to :
    // 1. stop the current recovery task
    // 2. update recoverys
    // 2. init new recovery task

    // init recovery_done_set
    recovery_done_set.resize(table_mgr->get_bucket_count());
    recovery_done_set.reset();

    if (recovery_mgr == NULL) {
        log_error("recovery_mgr is NULL, check the configuration");
        return;
    }

    // stop the current recovery task
    recovery_mgr->do_server_list_changed();

    // calculate table_mgr->recoverys, contains info that recovery_task need
    bucket_server_map recoverys;
    table_mgr->update_recoverys(recovery_ds, recoverys);

    if (recoverys.empty() == false) {
        recovery_mgr->set_log(dup_depot->get_failover_log());
        recovery_mgr->set_recovery_server_list(recoverys, table_mgr->get_version(), transition_version);
    }
}

void tair_manager::reset_recovery_task() {
    log_debug("reset recovery_done_set and stop recovery task");
    recovery_done_set.reset();
    if (recovery_mgr != NULL) {
        recovery_mgr->do_server_list_changed();
    }
}

void tair_manager::clear_dup_depot() {
    log_warn("clear dup_depot");
    if (dup_depot != NULL) {
        dup_depot->clear();
    }
}

// private methods
uint32_t tair_manager::get_bucket_number(const data_entry &key) {
    uint32_t hashcode = key.get_hashcode();
    log_debug("hashcode: %u, bucket count: %d", hashcode, table_mgr->get_bucket_count());
    return hashcode % (localmode ? localmode_bucket : table_mgr->get_bucket_count());
}

bool tair_manager::should_write_local(int bucket_number, int server_flag, int op_flag, int &rc) {

    if (localmode == true) {
        return true;
    }

    if (status != STATUS_CAN_WORK) {
        log_debug("server can not work now...");
        rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
        return false;
    }

    if (migrate_mgr->is_bucket_available(bucket_number) == false) {
        log_debug("bucket is migrating, request reject");
        rc = TAIR_RETURN_MIGRATE_BUSY;
        return false;
    }

    if (recovery_mgr && recovery_mgr->is_bucket_available(bucket_number) == false) {
        log_debug("bucket is migrating, request reject");
        rc = TAIR_RETURN_MIGRATE_BUSY;
        return false;
    }

    PROFILER_BEGIN("migrate is done?");
    if ((server_flag == TAIR_SERVERFLAG_CLIENT || server_flag == TAIR_SERVERFLAG_PROXY ||
         server_flag == TAIR_SERVERFLAG_RSYNC || server_flag == TAIR_SERVERFLAG_RSYNC_PROXY)
        && (migrate_done_set.test(bucket_number) || recovery_done_set.test(bucket_number))
        && table_mgr->is_master(bucket_number, TAIR_SERVERFLAG_PROXY) == false) {
        // didn't detect migrate/recovery done in request_processor, but here in tair_manager
        rc = TAIR_RETURN_MIGRATE_BUSY;
        PROFILER_END();
        return false;
    }
    PROFILER_END();

    log_debug("bucket number: %d, serverFlag: %d, client const: %d", bucket_number, server_flag,
              TAIR_SERVERFLAG_CLIENT);
    if ((server_flag == TAIR_SERVERFLAG_CLIENT || server_flag == TAIR_SERVERFLAG_PROXY ||
         server_flag == TAIR_SERVERFLAG_RSYNC || server_flag == TAIR_SERVERFLAG_RSYNC_PROXY)
        && table_mgr->is_master(bucket_number, server_flag) == false) {
        log_debug("request rejected...");
        rc = TAIR_RETURN_WRITE_NOT_ON_MASTER;
        return false;
    }
    //not need check  duplicate's list size any more.
    return true;

    //return true;

    //if (op_flag & TAIR_OPERATION_DUPLICATE) {
    //   bool is_available = false;
    //   for (int i=0; i<TAIR_DUPLICATE_BUSY_RETRY_COUNT; ++i) {
    //      is_available = duplicator->is_bucket_available(bucket_number);
    //      if (is_available)
    //         break;

    //      usleep(1000);
    //   }
    //   if (is_available == false) {
    //      log_debug("bucket is not avaliable, reject request");
    //      rc = TAIR_RETURN_DUPLICATE_BUSY;
    //      return false;
    //   }
    //}

    //return true;
}

void tair_manager::init_migrate_log() {
    if (migrate_log != NULL) {
        log_info("migrateLog already inited, quit");
        return;
    }
    // init migrate update log
    const char *mlog_dir = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_ULOG_DIR, NULL);
    if (mlog_dir == NULL) {
        log_error("migrate log directory can not empty");
        exit(1);
    }
    if (!tbsys::CFileUtil::mkdirs((char *) mlog_dir)) {
        log_error("mkdir migrate log dir failed: %s", mlog_dir);
        exit(1);
    }

    const char *log_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_ULOG_MIGRATE_BASENAME,
                                                       TAIR_ULOG_MIGRATE_DEFAULT_BASENAME);
    int32_t log_file_number = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_ULOG_FILENUM, TAIR_ULOG_DEFAULT_FILENUM);
    int32_t log_file_size = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_ULOG_FILESIZE, TAIR_ULOG_DEFAULT_FILESIZE);
    log_file_size *= MB_SIZE;

    migrate_log = update_log::open(mlog_dir, log_file_name, log_file_number, true, log_file_size);
    log_info("migrate log opened: %s/%s", mlog_dir, log_file_name);
    migrate_mgr->set_log(migrate_log);
}

void tair_manager::get_proxying_buckets(vector<uint32_t> &buckets) {
    for (uint32_t i = 0; i < migrate_done_set.size(); ++i) {
        if (migrate_done_set.test(i))
            buckets.push_back(i);
    }
}

void tair_manager::set_solitary() {
    if (status == STATUS_CAN_WORK && localmode == false) {
        status = STATUS_INITED;
        sleep(1);
        table_mgr->clear_available_server();
        duplicator->do_hash_table_changed();
        migrate_mgr->do_server_list_changed();
        log_warn("serverVersion is 1, set tairManager to solitary");
    }
}

bool tair_manager::is_working() {
    return status == STATUS_CAN_WORK || localmode;
}

int tair_manager::get_mutex_index(data_entry &key) {
    uint32_t hashcode = util::string_util::mur_mur_hash(key.get_data(), key.get_size());
    return hashcode % mutex_array_size;
}

bool tair_manager::get_slaves(int server_flag, int bucket_number, vector<uint64_t> &slaves) {
    bool is_failover = false;
    if (localmode || table_mgr->get_copy_count() == 1) return is_failover;

    if (server_flag == TAIR_SERVERFLAG_PROXY || server_flag == TAIR_SERVERFLAG_RSYNC_PROXY) {
        table_mgr->get_slaves(bucket_number, true, is_failover).swap(slaves);
    } else {
        table_mgr->get_slaves(bucket_number,
                              (migrate_done_set.test(bucket_number) || recovery_done_set.test(bucket_number)),
                              is_failover).swap(slaves);;
    }
    return is_failover;
}

int tair_manager::get_op_flag(int bucket_number, int server_flag) {
    int flag = 0;
    if (server_flag == TAIR_SERVERFLAG_CLIENT ||
        server_flag == TAIR_SERVERFLAG_PROXY) {
        flag |= TAIR_OPERATION_VERSION | TAIR_OPERATION_RSYNC;
        if (table_mgr->get_copy_count() > 1) {
            flag |= TAIR_OPERATION_DUPLICATE;
        }
    } else if (server_flag == TAIR_SERVERFLAG_RSYNC_PROXY) { // proxied rsync data need no rsync again
        flag |= TAIR_OPERATION_VERSION;
        if (table_mgr->get_copy_count() > 1) {
            flag |= TAIR_OPERATION_DUPLICATE;
        }
    } else if (server_flag == TAIR_SERVERFLAG_RSYNC) { // rsynced data need no version care or rsync again
        if (table_mgr->get_copy_count() > 1) {
            flag |= TAIR_OPERATION_DUPLICATE;
        }
    }
    return flag;
}

int tair_manager::expire(int area, data_entry &key, int expire_time, base_packet *request, int heart_version) {
    if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (verify_key(key) == false) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    tbsys::CThreadGuard guard(&counter_mutex[get_mutex_index(key)]);
    // get from storage engine
    data_entry old_value;
    PROFILER_BEGIN("expire::get");
    int rc = get(area, key, old_value);
    PROFILER_END();
    log_debug("expire::get result: %d, flag: %d", rc, key.data_meta.flag);
    key.data_meta.log_self();

    if (TAIR_RETURN_SUCCESS == rc) {
        PROFILER_BEGIN("expire::put");
        rc = put(area, key, old_value, expire_time, request, heart_version);
        PROFILER_END();
    }

    return rc;
}

int tair_manager::do_mc_ops(request_mc_ops *req, response_mc_ops *&resp, int heart_version) {
    int rc = TAIR_RETURN_SUCCESS;
    int bucket = get_bucket_number(req->key);
    int op_flag = get_op_flag(bucket, req->server_flag);
    if (should_write_local(bucket, req->server_flag, op_flag, rc) == false) {
        resp = new response_mc_ops;
        resp->code = rc;
        resp->setChannelId(req->getChannelId());
        resp->config_version = heart_version;
        return rc;
    }

    vector<uint64_t> slaves;
    get_slaves(req->server_flag, bucket, slaves);
    //TODO we need such a global flag, which being set when receiving server table
    bool need_duplicate = !slaves.empty();

    data_entry mkey = req->key;
    mkey.merge_area(req->area);
    mkey.data_meta.version = req->version;
    bool version_care = op_flag & TAIR_OPERATION_VERSION;

    char *key_buf = NULL;
    int32_t key_len = 0;
    char *val_buf = NULL;
    int32_t val_len = 0;
    data_entry new_value;
    data_entry *p_new_value = NULL;

    uint64_t counter_result = 0;

    switch (req->mc_opcode) {
        case 0x01: // SET
        {
            PROFILER_BEGIN("SET");
            rc = storage_mgr->mc_set(bucket, mkey, req->value, version_care, req->expire);
            TAIR_STAT.stat_put(req->area);

            stat_->update(req->area, OP_PUT);
            stat_->update(req->area, OP_PUT_KEY);
            PROFILER_END();
            p_new_value = &req->value;
            if (req->padding != NULL) delete req->padding;
            char *padding = new char[10];
            req->padding = padding;
            // first 8 bytes is retain for value, last 2 bytes is version
            memcpy(req->padding + 8, (char *) &(mkey.data_meta.version), 2);
            req->pad_len = 10;

            break;
        }
        case 0x02: // ADD
        {
            PROFILER_BEGIN("ADD");
            rc = storage_mgr->add(bucket, mkey, req->value, version_care, req->expire);
            TAIR_STAT.stat_put(req->area);
            stat_->update(req->area, OP_PUT);
            stat_->update(req->area, OP_PUT_KEY);
            PROFILER_END();
            p_new_value = &req->value;
            if (req->padding != NULL) delete req->padding;
            char *padding = new char[10];
            req->padding = padding;
            // first 8 bytes is retain for value, last 2 bytes is version
            memcpy(req->padding + 8, (char *) &(mkey.data_meta.version), 2);
            req->pad_len = 10;
            break;
        }
        case 0x03: // REPLACE
        {
            PROFILER_BEGIN("REPLACE");
            rc = storage_mgr->replace(bucket, mkey, req->value, version_care, req->expire);
            TAIR_STAT.stat_put(req->area);
            stat_->update(req->area, OP_PUT);
            stat_->update(req->area, OP_PUT_KEY);
            PROFILER_END();
            p_new_value = &req->value;
            if (req->padding != NULL) delete req->padding;
            char *padding = new char[10];
            req->padding = padding;
            // first 8 bytes is retain for value, last 2 bytes is version
            memcpy(req->padding + 8, (char *) &(mkey.data_meta.version), 2);
            req->pad_len = 10;
            break;
        }
        case 0x05: // INCR
        {
            PROFILER_BEGIN("INCR");
            uint64_t delta = bswap_64(*reinterpret_cast<uint64_t *>(req->value.get_data()));
            uint64_t init = bswap_64(*reinterpret_cast<uint64_t *>(req->value.get_data() + sizeof(uint64_t)));
            uint64_t result = 0;
            if (migrate_log != NULL || need_duplicate) {
                p_new_value = &new_value;
            }
            rc = storage_mgr->incr_decr(bucket, mkey, delta, init, true/* is_incr */, req->expire, result, p_new_value);
            counter_result = bswap_64(result);
            val_buf = reinterpret_cast<char *> (&counter_result);
            val_len = sizeof(counter_result);

            //! dirty trick
            //! we need a global flag to tell whether to duplicate!
            if (req->padding != NULL) delete req->padding;
            char *padding = new char[10];
            req->padding = padding;
            memcpy(req->padding, &counter_result, 8);
            memcpy(req->padding + 8, (char *) &(mkey.data_meta.version), 2);
            req->pad_len = 10;
            TAIR_STAT.stat_put(req->area); // regard as put
            stat_->update(req->area, OP_PUT);
            stat_->update(req->area, OP_PUT_KEY);
            PROFILER_END();
            break;
        }
        case 0x06: // DECR
        {
            PROFILER_BEGIN("DECR");
            uint64_t delta = bswap_64(*reinterpret_cast<uint64_t *>(req->value.get_data()));
            uint64_t init = bswap_64(*reinterpret_cast<uint64_t *>(req->value.get_data() + sizeof(uint64_t)));
            uint64_t result = 0;
            if (migrate_log != NULL || need_duplicate) {
                p_new_value = &new_value;
            }
            rc = storage_mgr->incr_decr(bucket, mkey, delta, init, false/* is_incr */, req->expire, result,
                                        p_new_value);
            counter_result = bswap_64(result);
            val_buf = reinterpret_cast<char *> (&counter_result);
            val_len = sizeof(counter_result);

            //! dirty trick
            if (req->padding != NULL) delete req->padding;
            char *padding = new char[10];
            req->padding = padding;
            memcpy(req->padding, &counter_result, 8);
            memcpy(req->padding + 8, (char *) &(mkey.data_meta.version), 2);
            req->pad_len = 10;

            TAIR_STAT.stat_put(req->area); // regard as put
            stat_->update(req->area, OP_PUT);
            stat_->update(req->area, OP_PUT_KEY);
            PROFILER_END();
            break;
        }
        case 0x0E: // APPEND
        {
            PROFILER_BEGIN("APPEND");
            if (migrate_log != NULL || need_duplicate) {
                p_new_value = &new_value;
            }
            rc = storage_mgr->append(bucket, mkey, req->value, version_care, p_new_value);
            TAIR_STAT.stat_put(req->area); // regard as put
            stat_->update(req->area, OP_PUT);
            stat_->update(req->area, OP_PUT_KEY);
            if (req->padding != NULL) delete req->padding;
            char *padding = new char[10];
            req->padding = padding;
            // first 8 bytes is retain for value, last 2 bytes is version
            memcpy(req->padding + 8, (char *) &(mkey.data_meta.version), 2);
            req->pad_len = 10;
            PROFILER_END();
            break;
        }
        case 0x0F: // PREPEND
        {
            PROFILER_BEGIN("PREPEND");
            if (migrate_log != NULL || need_duplicate) {
                p_new_value = &new_value;
            }
            rc = storage_mgr->prepend(bucket, mkey, req->value, version_care, p_new_value);
            TAIR_STAT.stat_put(req->area); // regard as put
            stat_->update(req->area, OP_PUT);
            stat_->update(req->area, OP_PUT_KEY);
            if (req->padding != NULL) delete req->padding;
            char *padding = new char[10];
            req->padding = padding;
            // first 8 bytes is retain for value, last 2 bytes is version
            memcpy(req->padding + 8, (char *) &(mkey.data_meta.version), 2);
            req->pad_len = 10;
            PROFILER_END();
            break;
        }
        case 0x1C: // TOUCH
        {
            //!TODO return the updated value
            //!NOTE! we do `touch' locally for now
            PROFILER_BEGIN("TOUCH");
            rc = storage_mgr->touch(bucket, mkey, req->expire);
            if (req->padding != NULL) delete req->padding;
            char *padding = new char[10];
            req->padding = padding;
            // first 8 bytes is retain for value, last 2 bytes is version
            memcpy(req->padding + 8, (char *) &(mkey.data_meta.version), 2);
            req->pad_len = 10;

            TAIR_STAT.stat_put(req->area); // regard as put
            stat_->update(req->area, OP_PUT);
            stat_->update(req->area, OP_PUT_KEY);
            PROFILER_END();
            break;
        }
        default: {
            log_error("unknown mc opcode: %d", req->mc_opcode);
            rc = TAIR_RETURN_NOT_SUPPORTED;
            break;
        }
    }

    if (rc == TAIR_RETURN_SUCCESS) {
        //! the order of the two `if' below truely does matter
        if (migrate_log != NULL && need_do_migrate_log(bucket) && p_new_value != NULL) {
            if (p_new_value->get_size() != 0) {
                migrate_log->log(SN_PUT, mkey, *p_new_value, bucket);
            }
        }
        if ((req->server_flag & TAIR_SERVERFLAG_DUPLICATE) == 0 && p_new_value != NULL) { //~ Double backup
            log_debug("duplicate mc_ops request to bucket %d", bucket);
            rc = duplicator->duplicate_data(req->area, &req->key, p_new_value, bucket, slaves, req->r);
        }
    }

    if (rc == TAIR_DUP_WAIT_RSP) {
        //~ wait for duplicating done, asynchronously.
        return rc;
    } else {
        //~ no need to duplicate, or shit happens. This is A single backup.
        resp = new response_mc_ops;
        if (key_buf != NULL) resp->key.set_data(key_buf, key_len);
        if (val_buf != NULL) resp->value.set_data(val_buf, val_len);
        resp->setChannelId(req->getChannelId());
        resp->config_version = heart_version;
        resp->mc_opcode = req->mc_opcode;
        resp->version = mkey.data_meta.version;
        resp->expire = mkey.data_meta.edate;
        resp->code = rc;
    }

    return rc;
}

bool tair_manager::is_localmode() {
    return localmode;
}

bool tair_manager::verify_key(data_entry &key) {
    if ((key.has_merged == true && key.get_size() >= TAIR_MAX_KEY_SIZE_WITH_AREA) ||
        (key.has_merged == false && key.get_size() >= TAIR_MAX_KEY_SIZE) || key.get_size() < 1) {
        return false;
    }
    return true;
}
}
