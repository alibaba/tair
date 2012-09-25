/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * dataserver operations abstract layer impl
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifdef WITH_KDB
#include "storage/kdb/kdb_manager.h"
#endif
#ifdef WITH_LDB
#include "storage/ldb/ldb_manager.hpp"
#endif

#include "tair_manager.hpp"
#include "migrate_manager.hpp"
#include "define.hpp"
#include "mdb_factory.hpp"
#include "fdb_manager.hpp"
#include "item_manager.hpp"
#include "duplicate_manager.hpp"
#include "dup_sync_manager.hpp"
#include "remote_sync_manager.hpp"


  namespace tair {
    tair_manager::tair_manager() : migrate_done_set(0)
    {
      status = STATUS_NOT_INITED;
      localmode = false;
      not_allow_count_negative = false;
      storage_mgr = NULL;
      table_mgr = new table_manager();
      duplicator = NULL;
      migrate_mgr = NULL;
      migrate_log = NULL;
      dump_mgr = NULL;
      remote_sync_mgr = NULL;
    }

    tair_manager::~tair_manager()
    {
      tbsys::CThreadGuard update_table_guard(&update_server_table_mutex);
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

      delete table_mgr;
      table_mgr = NULL;
    }

    bool tair_manager::initialize(tbnet::Transport *transport, tair_packet_streamer *streamer)
    {
      if (status != STATUS_NOT_INITED) {
        return true;
      }

      const char *se_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_SENGINE, NULL);
      if (se_name == NULL || (strcmp(se_name, "mdb") == 0)) {
        // init mdb
        storage_mgr = mdb_factory::create_mdb_manager();
      } else if (strcmp(se_name, "fdb") == 0){
        // init fdb
        storage_mgr = new tair::storage::fdb::fdb_manager();
      }
#ifdef WITH_KDB
      else if (strcmp(se_name, "kdb") == 0){
        // init kdb
        storage_mgr = new tair::storage::kdb::kdb_manager();
      }
#endif
#ifdef WITH_LDB
      else if (strcmp(se_name, "ldb") == 0) {
        log_info("init storage engine ldb");
        storage_mgr = new tair::storage::ldb::LdbManager();
      }
#endif
      else {
        storage_mgr = NULL;
      }

      if (storage_mgr == NULL) {
        log_error("init storage engine failed, storage engine name: %s", se_name);
        return false;
      }

      bool do_rsync = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_DO_RSYNC, 0) > 0;
      // remote synchronization init
      if (do_rsync) {
        remote_sync_mgr = new RemoteSyncManager(this);
        int ret = remote_sync_mgr->init();
        if (ret != TAIR_RETURN_SUCCESS) {
          log_error("init remote sync manager fail: %d", ret);
          delete remote_sync_mgr;
          remote_sync_mgr = NULL;
          return false;
        }
      }

      // init the storage manager for stat helper
      TAIR_STAT.set_storage_manager(storage_mgr);
      // is dataserver use localmode
      localmode = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_LOCAL_MODE, 0);
      // is allow dec to negative?
      uint32_t _allow_negative = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_COUNT_NEGATIVE, TAIR_COUNT_NEGATIVE_MODE);
      not_allow_count_negative = (0 == _allow_negative);

      // init dupicator
      int dup_sync_mode = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_DUP_SYNC, TAIR_DUP_SYNC_MODE);
      if (0 == dup_sync_mode)
      {
        duplicator = new duplicate_sender_manager(transport, streamer, table_mgr);
      }
      else
      {
        log_info("run duplicator with sync mode ");
        duplicator = new dup_sync_sender_manager(transport, streamer, this);
      }

      migrate_mgr = new migrate_manager(transport, streamer, duplicator, this, storage_mgr);

      dump_mgr = new tair::storage::dump_manager(storage_mgr);

      status = STATUS_INITED;

      // if localmode, init the table with bucket_count = 1, force set the status to STATUS_CAN_WORK
      if(localmode) {
        table_mgr->set_table_for_localmode();
        status = STATUS_CAN_WORK;
      }

      return true;
    }

    int tair_manager::prefix_puts(request_prefix_puts *request, int heart_version)
    {
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
          int ret = get_meta(request->area, *key, meta);
          if (version != 0  && ret == TAIR_RETURN_SUCCESS && version != meta.version) {
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
            rc = duplicator->duplicate_data(bucket_number, request, slaves, heart_version);
          }
        }
      }
      if (resp != NULL) {
        if (request->get_connection()->postPacket(resp) == false) {
          delete resp;
        }
      }
      return rc;
    }

    int tair_manager::prefix_removes(request_prefix_removes *request, int heart_version)
    {
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
            rc = duplicator->duplicate_data(bucket_number, request, slaves, heart_version);
          }
        }
      }
      if (resp != NULL) {
        if (!request->get_connection()->postPacket(resp)) {
          delete resp;
        }
      }
      return rc;
    }

    int tair_manager::prefix_hides(request_prefix_hides *request, int heart_version)
    {
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
            rc = duplicator->duplicate_data(bucket_number, request, slaves, heart_version);
          }
        }
      }
      if (resp != NULL) {
        if (!request->get_connection()->postPacket(resp)) {
          delete resp;
        }
      }
      return rc;
    }

    int tair_manager::prefix_incdec(request_prefix_incdec *request, int heart_version)
    {
      int rc = TAIR_RETURN_SUCCESS;
      response_prefix_incdec *resp = new response_prefix_incdec();
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
            wrapper->init_value, &ret_value, wrapper->expire, NULL, heart_version);

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
      if (ndone == request->key_count) {
        rc = TAIR_RETURN_SUCCESS;
      } else if (ndone > 0) {
        rc = TAIR_RETURN_PARTIAL_SUCCESS;
      } else {
        //rc = TAIR_RETURN_FAILED;
      }

      resp->set_code(rc);
      resp->config_version = heart_version;
      resp->setChannelId(request->getChannelId());
      if (request->server_flag & TAIR_SERVERFLAG_DUPLICATE) {
        resp->server_flag = TAIR_SERVERFLAG_DUPLICATE;
        resp->bucket_id = bucket_number;
        resp->server_id = local_server_ip::ip;
        resp->packet_id = request->packet_id;
      }
      if (rc == TAIR_RETURN_SUCCESS) {
        if ((request->server_flag & TAIR_SERVERFLAG_DUPLICATE) == 0) {
          vector<uint64_t> slaves;
          get_slaves(request->server_flag, bucket_number, slaves);
          if (!slaves.empty()) {
            delete resp;
            resp = NULL;
            log_debug("duplicate prefix inc/dec request to bucekt %d", bucket_number);
            rc = duplicator->duplicate_data(bucket_number, request, slaves, heart_version);
          }
        }
      }
      if (resp != NULL) {
        if (!request->get_connection()->postPacket(resp)) {
          delete resp;
        }
      }
      return rc;
    }

    int tair_manager::put(int area, data_entry &key, data_entry &value, int expire_time,base_packet *request,int heart_vesion)
    {
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
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
      bool version_care =  op_flag & TAIR_OPERATION_VERSION;
      PROFILER_BEGIN("put into storage");
      rc = storage_mgr->put(bucket_number, mkey, value, version_care, expire_time);
      PROFILER_END();

      if (rc == TAIR_RETURN_SUCCESS ) {
        key.data_meta = mkey.data_meta;
        // for duplicate
        key.data_meta.flag = old_flag;
        if (op_flag & TAIR_OPERATION_DUPLICATE) {
          vector<uint64_t> slaves;
          get_slaves(key.server_flag, bucket_number, slaves);
          if (slaves.empty() == false) {
            PROFILER_BEGIN("do duplicate");
            //duplicator->(area, &key, &value, bucket_number, slaves);
            if(request)
              rc=duplicator->duplicate_data(area, &key, &value, expire_time,bucket_number,
                                            slaves,(base_packet *)request,heart_vesion);
            PROFILER_END();
          }
        }

        do_remote_sync(TAIR_REMOTE_SYNC_TYPE_PUT, &mkey, &value, rc, op_flag);

        if (migrate_log != NULL && need_do_migrate_log(bucket_number)) {
          PROFILER_BEGIN("do migrate log");
          migrate_log->log(SN_PUT, mkey, value, bucket_number);
          PROFILER_END();
        }
      }
      TAIR_STAT.stat_put(area);

      return rc;
    }

    int tair_manager::direct_put(data_entry &key, data_entry &value)
    {
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
      int rc = storage_mgr->put(bucket_number, key, value, false, 0);

      TAIR_STAT.stat_put(area);

      return rc;
    }

    int tair_manager::direct_remove(data_entry &key)
    {
      if (key.get_size() >= TAIR_MAX_KEY_SIZE_WITH_AREA || key.get_size() < 1) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }
      data_entry akey = key;
      akey.decode_area();

      int bucket_number = get_bucket_number(akey);
      int rc = storage_mgr->remove(bucket_number, key, false);

      if (rc == TAIR_RETURN_DATA_NOT_EXIST) {
        // for migrate, return SUCCESS
        rc = TAIR_RETURN_SUCCESS;
      }

      TAIR_STAT.stat_remove(akey.area);

      return rc;
    }

    int tair_manager::add_count(int area, data_entry &key, int count, int init_value, int *result_value, int expire_time,base_packet * request,int heart_version)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
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
      //check if it's support add_count or it's a mdb enginer.
      int rc=storage_mgr->add_count(bucket_number,mkey,count,init_value,not_allow_count_negative,expire_time,*result_value);

      if(TAIR_RETURN_NOT_SUPPORTED!=rc)
      {
        //if add_count success,now just mdb supported add_count.
        if (rc == TAIR_RETURN_SUCCESS )
        {
          if(op_flag & TAIR_OPERATION_DUPLICATE)
          {
            //reget value.
            //todo:the storage_mgr should return data_entry when add_count;
            rc = get(area, key,old_value);
            key.data_meta.log_self();
            storage_mgr->set_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_ADDCOUNT);
            //do duplicate thing .
            rc=do_duplicate(area,key,old_value,bucket_number,request,heart_version);

          }
          //log it
          if (migrate_log != NULL && need_do_migrate_log(bucket_number)) {
            PROFILER_BEGIN("do migrate log");
            migrate_log->log(SN_PUT, key, old_value, bucket_number);
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
      rc = get(area, key, old_value);
      PROFILER_END();
      log_debug("get result: %d, flag: %d", rc, key.data_meta.flag);
      key.data_meta.log_self();
      if (rc == TAIR_RETURN_SUCCESS && storage_mgr->test_flag(key.data_meta.flag, TAIR_ITEM_FLAG_ADDCOUNT)) {
        // old value exist
        int32_t *v = (int32_t *)(old_value.get_data() + ITEM_HEAD_LENGTH);
        log_debug("old count: %d, new count: %d, init value: %d", (*v), count, init_value);
        if(not_allow_count_negative)
        {
          if(0>=(*v) && count<0)
          {
            return  TAIR_RETURN_DEC_ZERO;
          }

          if(0>((*v)+count))
          {
            *result_value = *v;
            return  TAIR_RETURN_DEC_BOUNDS;
          }
        }
        *v += count;
        *result_value = *v;
      } else if(rc == TAIR_RETURN_SUCCESS){
        //exist,but is not add_count,return error;
        log_debug("cann't override old value");
        return TAIR_RETURN_CANNOT_OVERRIDE;
      }else {
        if(not_allow_count_negative)
        {
          if(0>count && 0>=init_value )
          {
            //not allowed to add a negative number to new a key.
            return TAIR_RETURN_DEC_NOTFOUND;
          }
          if(0>(init_value + count)) return  TAIR_RETURN_DEC_BOUNDS;
        }
        // old value not exist
        char fv[INCR_DATA_SIZE];
        *result_value = init_value + count;
        SET_INCR_DATA_COUNT(fv, *result_value);
        old_value.set_data(fv, INCR_DATA_SIZE);
      }

      storage_mgr->set_flag(old_value.data_meta.flag, TAIR_ITEM_FLAG_ADDCOUNT);
      // reassign key's meta flag
      key.data_meta.flag = old_flag;
      log_debug("before put flag: %d", old_value.data_meta.flag);
      PROFILER_BEGIN("save count into storage");
      int result = put(area, key, old_value, expire_time,request,heart_version);
      PROFILER_END();
      return result;
    }

    int tair_manager::get(int area, data_entry &key, data_entry &value, bool with_stat)
    {
      if (!localmode && status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
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
      if (with_stat)
        TAIR_STAT.stat_get(area, rc);
      if (rc == TAIR_RETURN_SUCCESS) {
        rc = (value.data_meta.flag & TAIR_ITEM_FLAG_DELETED) ?
          TAIR_RETURN_HIDDEN : TAIR_RETURN_SUCCESS;
      }
      //! even if rc==TAIR_RETURN_HIDDEN, 'value' still hold the value
      return rc;
    }

    int tair_manager::get_range(int32_t area, data_entry &key_start, data_entry &key_end, int offset, int limit, int type, std::vector<data_entry*> &result, bool &has_next)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }
      if (key_start.get_size() >= TAIR_MAX_KEY_SIZE || key_start.get_size() < 1) {
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
      return rc;
    }

    int tair_manager::hide(int area, data_entry &key, base_packet *request, int heart_version)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
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
        rc = put(area, key, old_value, old_value.data_meta.edate);
        PROFILER_END();
      } else if (rc == TAIR_RETURN_HIDDEN) {
        rc = TAIR_RETURN_SUCCESS;
      }

      TAIR_STAT.stat_remove(area);
      return rc;
    }

    int tair_manager::get_hidden(int area, data_entry &key, data_entry &value)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
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
      return rc;
    }

    int tair_manager::remove(int area, data_entry &key,request_remove *request,int heart_version)
    {
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
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
      bool version_care =  op_flag & TAIR_OPERATION_VERSION;

      PROFILER_BEGIN("remove from storage engine");
      rc = storage_mgr->remove(bucket_number, mkey, version_care);
      PROFILER_END();

      if (rc == TAIR_RETURN_SUCCESS || rc == TAIR_RETURN_DATA_NOT_EXIST) {
        if (op_flag & TAIR_OPERATION_DUPLICATE) {
          vector<uint64_t> slaves;
          get_slaves(key.server_flag, bucket_number, slaves);
          if (slaves.empty() == false) {
            PROFILER_BEGIN("do duplicate");
            int rc1 = duplicator->duplicate_data(area, &key, NULL, 0, bucket_number, slaves,
                (rc == TAIR_RETURN_DATA_NOT_EXIST) ? NULL : (base_packet *)request, heart_version);
            if (TAIR_RETURN_SUCCESS == rc) rc = rc1;
            PROFILER_END();
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

      return rc;
    }

    int tair_manager::batch_put(int area, mput_record_vec* record_vec, request_mput* request, int heart_version)
    {
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

      if (TAIR_RETURN_SUCCESS == rc && migrate_log != NULL && need_do_migrate_log(bucket_number)) {
        // do migrate
        for (mput_record_vec::const_iterator it = record_vec->begin() ; it != record_vec->end(); ++it) {
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
          get_slaves(request->server_flag, bucket_number, slaves);
          if (!slaves.empty()) {
            rc = duplicator->duplicate_batch_data(bucket_number, record_vec, slaves, request, heart_version);
          }
        } else {
          log_debug("bp send return packet to duplicate source");
          response_duplicate *dresp = new response_duplicate();
          dresp->setChannelId(request->getChannelId());
          dresp->server_id = local_server_ip::ip;
          dresp->packet_id = request->packet_id;
          dresp->bucket_id = bucket_number;
          if (request->get_connection()->postPacket(dresp) == false) {
            delete dresp;
          }
        }
      }

      return rc;
    }

    int tair_manager::batch_remove(int area, const tair_dataentry_set * key_list,request_remove *request,int heart_version)
    {
      if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
      }

      int rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;

      set<data_entry*, data_entry_comparator>::iterator it;
      //check param first, one fail,all fail.
      for (it = key_list->begin(); it != key_list->end(); ++it)
      {
        uint64_t target_server_id = 0;

        data_entry *pkey = (*it);
        data_entry key=*pkey; //for same code.
        key.server_flag = request->server_flag;

        if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
          return TAIR_RETURN_ITEMSIZE_ERROR;
        }

        if (should_proxy(key, target_server_id))
        {
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
      for (it = key_list->begin(); it != key_list->end(); ++it)
      {
        data_entry *pkey = (*it);
        data_entry key=*pkey; //for same code.
        key.server_flag = request->server_flag;

        data_entry mkey = key;
        mkey.merge_area(area);

        int bucket_number = get_bucket_number(key);
        int op_flag = get_op_flag(bucket_number, key.server_flag);

        bool version_care =  op_flag & TAIR_OPERATION_VERSION;
        rc = storage_mgr->remove(bucket_number, mkey, version_care);

        if (rc == TAIR_RETURN_SUCCESS || rc == TAIR_RETURN_DATA_NOT_EXIST)
        {
          if (migrate_log != NULL && need_do_migrate_log(bucket_number)) {
            migrate_log->log(SN_REMOVE, mkey, mkey, bucket_number);
          }
          if (op_flag & TAIR_OPERATION_DUPLICATE) {
            vector<uint64_t> slaves;
            get_slaves(key.server_flag, bucket_number, slaves);
            if (slaves.empty() == false)
            {
              //for batch delete,don't wait response,let's retry it.
              rc=duplicator->direct_send(area, &key, NULL, 0,bucket_number, slaves,0);
              if(TAIR_RETURN_SUCCESS != rc)
              {
                return rc;
              }
            }
          }
        }
        else
        {
          //one fail,all fail
          return TAIR_RETURN_REMOVE_ONE_FAILED;
        }
        TAIR_STAT.stat_remove(area);
      }

      //if(_need_dup)
      return rc;
    }

    int tair_manager::clear(int area)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      return storage_mgr->clear(area);
    }

   int tair_manager::op_cmd(ServerCmdType cmd, std::vector<std::string>& params)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }
      if (cmd <= TAIR_SERVER_CMD_MIN_TYPE || cmd >= TAIR_SERVER_CMD_MAX_TYPE) {
        log_error("unknown cmd type: %d", cmd);
        return TAIR_RETURN_NOT_SUPPORTED;
      }

      int ret = TAIR_RETURN_SUCCESS;
      switch (cmd) {
      case TAIR_SERVER_CMD_PAUSE_RSYNC:
        if (remote_sync_mgr != NULL) {
          ret = remote_sync_mgr->pause(true);
        }
        break;
      case TAIR_SERVER_CMD_RESUME_RSYNC:
        if (remote_sync_mgr != NULL) {
          ret = remote_sync_mgr->pause(false);
        }
        break;
      case TAIR_SERVER_CMD_SET_CONFIG:
        ret = params.size() >= 2 ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        if (ret == TAIR_RETURN_SUCCESS && params[0] == TAIR_RSYNC_MTIME_CARE && remote_sync_mgr != NULL) {
          remote_sync_mgr->set_mtime_care(atoi(params[1].c_str()) > 0);
        }
        break;
      default:
        break;
      }

      // op cmd to storage manager
      if (ret == TAIR_RETURN_SUCCESS) {
        ret = storage_mgr->op_cmd(cmd, params);
      }
      return ret;
   }

#ifndef NOT_FIXED_ITEM_FUNC
#define  NOT_FIXED_ITEM_FUNC return TAIR_RETURN_SERVER_CAN_NOT_WORK;
#endif

   int tair_manager::add_items(int area,
                               data_entry& key,
                               data_entry& value,
                               int max_count, int expire_time/* = 0*/)
   {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (value.get_size() >= TAIR_MAX_DATA_SIZE || value.get_size() < 1) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
      }

      tbsys::CThreadGuard guard(&item_mutex[get_mutex_index(key)]);
      return json::item_manager::add_items(this,area,key,value,max_count,expire_time);
    }


    int tair_manager::get_items(int area,
        data_entry& key,
        int offset, int count, data_entry& value /*out*/,
        int type /*=ELEMENT_TYPE_INVALID*/)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
      }
      return json::item_manager::get_items(this,area,key,offset,count,value,type);
    }

    int tair_manager::get_item_count(int area, data_entry& key)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
      }
      return json::item_manager::get_item_count(this,area,key);
    }

    int tair_manager::remove_items(int area,
        data_entry& key, int offset, int count)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
      }

      //tbsys::CThreadGuard guard(&removeItemsMutex[getMutexIndex(key)]);
      tbsys::CThreadGuard guard(&item_mutex[get_mutex_index(key)]);
      return json::item_manager::remove_items(this,area,key,offset,count);
    }

    int tair_manager::get_and_remove(int area,
        data_entry& key,
        int offset, int count, data_entry& value /*out*/,
        int type /*=ELEMENT_TYPE_INVALID*/)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
      }
      //tbsys::CThreadGuard guard(&getAndRemoveItemsMutex[getMutexIndex(key)]);
      tbsys::CThreadGuard guard(&item_mutex[get_mutex_index(key)]);
      return json::item_manager::get_and_remove(this,area,key,offset,count,value,type);
    }


    int tair_manager::lock(int area, LockType lock_type, data_entry &key, base_packet *request, int heart_version)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
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

    void tair_manager::do_dump(set<dump_meta_info> dump_meta_infos)
    {
      return;// NOT_FIXED_ITEM_FUNC
      log_debug("receive dump request, size: %d", dump_meta_infos.size());
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

      for (it=dump_meta_infos.begin(); it!=dump_meta_infos.end(); it++) {
        dump_meta_info info = *it;
        tair::storage::dump_filter filter;
        filter.set_parameter(info.start_time, info.end_time, info.area, dir);
        log_debug("add dump: {startTime:%u, endTime:%u, area:%d, dir:%s }", info.start_time, info.end_time, info.area, dir.c_str());
        dump_filters.insert(filter);
      }

      dump_mgr->do_dump(dump_filters, buckets, time(NULL));

    }

    int tair_manager::get_meta(int area, data_entry &key, item_meta_info &meta)
    {
      bool has_merged = key.has_merged;
      if (!has_merged) {
        key.merge_area(area);
      }

      int rc = storage_mgr->get_meta(key, meta);
      if (!has_merged) {
        key.decode_area();
      }
      if (rc != TAIR_RETURN_NOT_SUPPORTED) {
        return rc;
      }
      //~ following would be expensive
      data_entry data;
      item_meta_info kmeta = key.data_meta;
      rc = storage_mgr->get(area, key, data);
      meta = key.data_meta;
      key.data_meta = kmeta;

      return rc;
    }

   int tair_manager::do_duplicate(int area, data_entry& key, data_entry& value,int bucket_number,base_packet *request,int heart_vesion)
   {
     int rc=0;
     vector<uint64_t> slaves;
     get_slaves(key.server_flag, bucket_number, slaves);
     if (slaves.empty() == false)
     {
       PROFILER_BEGIN("do duplicate");
       if (request)
       {
         rc = duplicator->duplicate_data(area, &key, &value,0,bucket_number,slaves,request,heart_vesion);
       }
       PROFILER_END();
     }
     return rc;
   }

    int tair_manager::do_remote_sync(TairRemoteSyncType type, common::data_entry* key, common::data_entry* value,
                                     int rc, int op_flag)
    {
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

    bool tair_manager::is_migrating()
    {
      return migrate_mgr->is_migrating();
    }

    bool tair_manager::is_local(data_entry &key)
    {
      int bucket_number = get_bucket_number(key);
      return table_mgr->is_master(bucket_number, TAIR_SERVERFLAG_CLIENT);
    }
    bool tair_manager::should_proxy(data_entry &key, uint64_t &target_server_id)
    {
      if (key.server_flag == TAIR_SERVERFLAG_PROXY || key.server_flag == TAIR_SERVERFLAG_RSYNC_PROXY || localmode)
        return false; // if this is proxy, dont proxy

      if(localmode)
        return false;

      int bucket_number = get_bucket_number(key);
      bool is_migrated = migrate_done_set.test(bucket_number);
      if (is_migrated) {
        target_server_id = table_mgr->get_migrate_target(bucket_number);
        if (target_server_id == local_server_ip::ip) // target is myself, do not proxy
          target_server_id = 0;
      }
      return is_migrated && target_server_id != 0;
    }

    bool tair_manager::need_do_migrate_log(int bucket_number)
    {
      assert (migrate_mgr != NULL);
      return migrate_mgr->is_bucket_migrating(bucket_number);
    }

    void tair_manager::set_migrate_done(int bucket_number)
    {
      assert (migrate_mgr != NULL);
      migrate_done_set.set(bucket_number, true);
    }

    void tair_manager::update_server_table(uint64_t *server_table, int server_table_size, uint32_t server_table_version, int32_t data_need_remove, vector<uint64_t> &current_state_table, uint32_t copy_count, uint32_t bucket_count)
    {
      if (localmode == true)
        return;

      tbsys::CThreadGuard update_table_guard(&update_server_table_mutex);

      log_warn("updateServerTable, size: %d", server_table_size);
      table_mgr->do_update_table(server_table, server_table_size, server_table_version, copy_count, bucket_count);
      duplicator->set_max_queue_size(10 * ((table_mgr->get_copy_count() - 1) * 3 * 3 + 1));
      storage_mgr->set_bucket_count(table_mgr->get_bucket_count());

      migrate_done_set.resize(table_mgr->get_bucket_count());
      migrate_done_set.reset();
      if (status != STATUS_CAN_WORK) {
        if (data_need_remove == TAIR_DATA_NEED_MIGRATE)
          init_migrate_log();
        table_mgr->init_migrate_done_set(migrate_done_set, current_state_table);
        status = STATUS_CAN_WORK; // set inited status to true after init buckets
      }

      vector<int> release_buckets (table_mgr->get_release_buckets());
      if (release_buckets.empty() == false && localmode == false) {
        storage_mgr->close_buckets(release_buckets);
      }

      vector<int> holding_buckets (table_mgr->get_holding_buckets());

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
      }

      vector<int> padding_buckets (table_mgr->get_padding_buckets());
      if (padding_buckets.empty() == false) {
        storage_mgr->init_buckets(padding_buckets);
      }

      // clear dump task
      dump_mgr->cancle_all();
      assert (migrate_mgr != NULL);
      migrate_mgr->do_server_list_changed();

      bucket_server_map migrates (table_mgr->get_migrates());

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
      duplicator->do_hash_table_changed();
    }

    // private methods
    uint32_t tair_manager::get_bucket_number(const data_entry &key)
    {
      uint32_t hashcode;
      int32_t diff_size = key.has_merged ? TAIR_AREA_ENCODE_SIZE : 0;
      if (key.get_prefix_size() == 0) {
        hashcode = tair::util::string_util::mur_mur_hash(key.get_data() + diff_size, key.get_size() - diff_size);
      } else {
        hashcode = tair::util::string_util::mur_mur_hash(key.get_data() + diff_size, key.get_prefix_size());
      }
      log_debug("hashcode: %u, bucket count: %d", hashcode, table_mgr->get_bucket_count());
      return hashcode % (localmode ? 1023 : table_mgr->get_bucket_count());
    }

    bool tair_manager::should_write_local(int bucket_number, int server_flag, int op_flag, int &rc)
    {

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

      PROFILER_BEGIN("migrate is done?");
      if ((server_flag == TAIR_SERVERFLAG_CLIENT || server_flag == TAIR_SERVERFLAG_PROXY ||
           server_flag == TAIR_SERVERFLAG_RSYNC || server_flag == TAIR_SERVERFLAG_RSYNC_PROXY)
          && migrate_done_set.test(bucket_number)
          && table_mgr->is_master(bucket_number, TAIR_SERVERFLAG_PROXY) == false) {
        rc = TAIR_RETURN_MIGRATE_BUSY;
        PROFILER_END();
        return false;
      }
      PROFILER_END();

      log_debug("bucket number: %d, serverFlag: %d, client const: %d", bucket_number, server_flag, TAIR_SERVERFLAG_CLIENT);
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

    void tair_manager::init_migrate_log()
    {
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
      if (!tbsys::CFileUtil::mkdirs((char *)mlog_dir)) {
        log_error("mkdir migrate log dir failed: %s", mlog_dir);
        exit(1);
      }

      const char *log_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_ULOG_MIGRATE_BASENAME, TAIR_ULOG_MIGRATE_DEFAULT_BASENAME);
      int32_t log_file_number = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_ULOG_FILENUM, TAIR_ULOG_DEFAULT_FILENUM);
      int32_t log_file_size = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_ULOG_FILESIZE, TAIR_ULOG_DEFAULT_FILESIZE);
      log_file_size *= MB_SIZE;

      migrate_log = update_log::open(mlog_dir, log_file_name, log_file_number, true, log_file_size);
      log_info("migrate log opened: %s/%s", mlog_dir, log_file_name);
      migrate_mgr->set_log(migrate_log);

    }

    void tair_manager::get_proxying_buckets(vector<uint32_t> &buckets)
    {
      if (migrate_done_set.any()) {
        for (uint32_t i=0; i<migrate_done_set.size(); ++i) {
          if (migrate_done_set.test(i))
            buckets.push_back(i);
        }
      }
    }

    void tair_manager::set_solitary()
    {
      if (status == STATUS_CAN_WORK && localmode == false) {
        status = STATUS_INITED;
        sleep(1);
        table_mgr->clear_available_server();
        duplicator->do_hash_table_changed();
        migrate_mgr->do_server_list_changed();
        log_warn("serverVersion is 1, set tairManager to solitary");
      }
    }

    bool tair_manager::is_working()
    {
      return status == STATUS_CAN_WORK || localmode;
    }

    int tair_manager::get_mutex_index(data_entry &key)
    {
      uint32_t hashcode = util::string_util::mur_mur_hash(key.get_data(), key.get_size());
      return hashcode % mutex_array_size;
    }

    void tair_manager::get_slaves(int server_flag, int bucket_number, vector<uint64_t> &slaves) {
      if (localmode || table_mgr->get_copy_count() == 1) return;

      if (server_flag == TAIR_SERVERFLAG_PROXY || server_flag == TAIR_SERVERFLAG_RSYNC_PROXY) {
        table_mgr->get_slaves(bucket_number, true).swap(slaves);
      } else {
        table_mgr->get_slaves(bucket_number, migrate_done_set.test(bucket_number)).swap(slaves);;
      }
    }

    int tair_manager::get_op_flag(int bucket_number, int server_flag)
    {
      int flag = 0;
      if (server_flag == TAIR_SERVERFLAG_CLIENT ||
          server_flag == TAIR_SERVERFLAG_PROXY) {
        flag |= TAIR_OPERATION_VERSION;
        flag |= TAIR_OPERATION_DUPLICATE;
        flag |= TAIR_OPERATION_RSYNC;
      } else if (server_flag == TAIR_SERVERFLAG_RSYNC_PROXY) { // proxied rsync data need no rsync again
        flag |= TAIR_OPERATION_VERSION;
        flag |= TAIR_OPERATION_DUPLICATE;
      } else if (server_flag == TAIR_SERVERFLAG_RSYNC) { // rsynced data need no version care or rsync again
        flag |= TAIR_OPERATION_DUPLICATE;
      }
      return flag;
    }

    int tair_manager::expire(int area, data_entry &key, int expire_time, base_packet *request, int heart_version)
    {
      if (status != STATUS_CAN_WORK) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
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

    bool tair_manager::is_localmode()
    {
      return localmode;
    }

  }
