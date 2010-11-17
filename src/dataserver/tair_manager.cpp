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
#include "tair_manager.hpp"
#include "migrate_manager.hpp"
#include "define.hpp"
#include "mdb_factory.hpp"
#include "item_manager.hpp"
#include "fdb_manager.hpp"
namespace {
   const int TAIR_OPERATION_VERSION   = 1;
   const int TAIR_OPERATION_DUPLICATE = 2;
   const int TAIR_OPERATION_REMOTE    = 4;
   const int TAIR_DUPLICATE_BUSY_RETRY_COUNT = 10;
}

namespace tair {
   tair_manager::tair_manager() : migrate_done_set(0)
   {
      status = STATUS_NOT_INITED;
      storage_mgr = NULL;
      table_mgr = new table_manager();
      duplicator = NULL;
      migrate_mgr = NULL;
      migrate_log = NULL;
      dump_mgr = NULL;
   }

   tair_manager::~tair_manager()
   {
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

   bool tair_manager::initialize(tbnet::Transport *transport, tbnet::DefaultPacketStreamer *streamer)
   {
      if (status != STATUS_NOT_INITED) {
         return true;
      }

      const char *se_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_SENGINE, NULL);
      if (se_name == NULL || (strcmp(se_name, "mdb") == 0)) {
         // init mdb
         storage_mgr = mdb_factory::create_mdb_manager(false);
      } else if (strcmp(se_name, "fdb") == 0){
         // init fdb
         storage_mgr = new tair::storage::fdb::fdb_manager();
      } else {
         return false;
      }

      if (storage_mgr == NULL) {
         log_error("init storage engine failed, storage engine name: %s", se_name);
         return false;
      }

      // init the storage manager for stat helper
      TAIR_STAT.set_storage_manager(storage_mgr);

      // init dupicator
      base_duplicator* dup;
      duplicator = new duplicate_sender_manager(transport, streamer, table_mgr);
      dup = duplicator;

      migrate_mgr = new migrate_manager(transport, streamer, dup, this, storage_mgr);

      dump_mgr = new tair::storage::dump_manager(storage_mgr);

      status = STATUS_INITED;

      return true;
   }

   int tair_manager::put(int area, data_entry &key, data_entry &value, int expire_time)
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

      // save into the storage engine
      bool version_care =  op_flag & TAIR_OPERATION_VERSION;
      PROFILER_BEGIN("put into storage");
      rc = storage_mgr->put(bucket_number, mkey, value, version_care, expire_time);
      PROFILER_END();

      if (rc == TAIR_RETURN_SUCCESS ) {
         key.data_meta = mkey.data_meta;
         if (op_flag & TAIR_OPERATION_DUPLICATE) {
            vector<uint64_t> slaves;
            get_slaves(key.server_flag, bucket_number, slaves);
            if (slaves.empty() == false) {
               PROFILER_BEGIN("do duplicate");
               duplicator->duplicate_data(area, &key, &value, bucket_number, slaves);
               PROFILER_END();
            }
         }

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
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (value.get_size() >= TAIR_MAX_DATA_SIZE || value.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      data_entry akey = key;
      akey.decode_area();

      int bucket_number = get_bucket_number(akey);
      int rc = storage_mgr->put(bucket_number, key, value, false, 0);

      TAIR_STAT.stat_put(akey.area);

      return rc;
   }

   int tair_manager::direct_remove(data_entry &key)
   {
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
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

   int tair_manager::add_count(int area, data_entry &key, int count, int init_value, int *result_value, int expire_time)
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
      PROFILER_BEGIN("get from storage");
      int rc = get(area, key, old_value);
      PROFILER_END();
      log_debug("get result: %d, flag: %d", rc, key.data_meta.flag);
      key.data_meta.log_self();
      if (rc == TAIR_RETURN_SUCCESS && IS_ADDCOUNT_TYPE(key.data_meta.flag)) {
         // old value exist
         int32_t *v = (int32_t *)(old_value.get_data() + ITEM_HEAD_LENGTH);
         log_debug("old count: %d, new count: %d, init value: %d", (*v), count, init_value);
         *v += count;
         *result_value = *v;
      } else if(rc == TAIR_RETURN_SUCCESS){
         //exist,but is not add_count,return error;
         log_debug("cann't override old value");
         return TAIR_RETURN_CANNOT_OVERRIDE;
      }else {
         // old value not exist
         char fv[6]; // 2 + sizeof(int)
         *((short *)fv) = 0x1600; // for java header
         *result_value = init_value + count;
         *((int32_t *)(fv + 2)) = *result_value;
         old_value.set_data(fv, 6);
      }

      old_value.data_meta.flag |= TAIR_ITEM_FLAG_ADDCOUNT;
      log_debug("before put flag: %d", old_value.data_meta.flag);
      PROFILER_BEGIN("save count into storage");
      int result = put(area, key, old_value, expire_time);
      PROFILER_END();
      return result;
   }

   int tair_manager::get(int area, data_entry &key, data_entry &value)
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
      log_debug("get request will server in bucket: %d", bucket_number);
      PROFILER_BEGIN("get from storage engine");
      int rc = storage_mgr->get(bucket_number, mkey, value);
      PROFILER_END();
      key.data_meta = mkey.data_meta;
      TAIR_STAT.stat_get(area, rc);
      return rc;
   }

   int tair_manager::remove(int area, data_entry &key)
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
               duplicator->duplicate_data(area, &key, NULL, bucket_number, slaves);
               PROFILER_END();
            }
         }

         if (migrate_log != NULL && need_do_migrate_log(bucket_number)) {
            PROFILER_BEGIN("do migrate log");
            migrate_log->log(SN_REMOVE, mkey, mkey, bucket_number);
            PROFILER_END();
         }

      }
      TAIR_STAT.stat_remove(area);

      return rc;
   }

   int tair_manager::clear(int area)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      return storage_mgr->clear(area);
   }

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

      key.data_meta.flag |= TAIR_ITEM_FLAG_ITEM;
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

   void tair_manager::do_dump(set<dump_meta_info> dump_meta_infos)
   {
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

   bool tair_manager::is_migrating()
   {
      return migrate_mgr->is_migrating();
   }

   bool tair_manager::should_proxy(data_entry &key, uint64_t &target_server_id)
   {
      if (key.server_flag == TAIR_SERVERFLAG_PROXY)
         return false; // if this is proxy, dont proxy

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
      tbsys::CThreadGuard update_table_guard(&update_server_table_mutex);

      log_debug("updateServerTable, size: %d", server_table_size);
      table_mgr->do_update_table(server_table, server_table_size, server_table_version, copy_count, bucket_count);
      duplicator->set_max_queue_size((table_mgr->get_copy_count() - 1) * 3);
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
      if (release_buckets.empty() == false) {
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
   uint32_t tair_manager::get_bucket_number(data_entry &key)
   {
      uint32_t hashcode = tair::util::string_util::mur_mur_hash(key.get_data(), key.get_size());
      log_debug("hashcode: %u, bucket count: %d", hashcode, table_mgr->get_bucket_count());
      return hashcode % table_mgr->get_bucket_count();
   }

   bool tair_manager::should_write_local(int bucket_number, int server_flag, int op_flag, int &rc)
   {
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
      if ((server_flag == TAIR_SERVERFLAG_CLIENT || server_flag == TAIR_SERVERFLAG_PROXY)
          && migrate_done_set.test(bucket_number)
          && table_mgr->is_master(bucket_number, TAIR_SERVERFLAG_PROXY) == false) {
         rc = TAIR_RETURN_MIGRATE_BUSY;
         PROFILER_END();
         return false;
      }
      PROFILER_END();

      log_debug("bucket number: %d, serverFlag: %d, client const: %d", bucket_number, server_flag, TAIR_SERVERFLAG_CLIENT);
      if ((server_flag == TAIR_SERVERFLAG_CLIENT || server_flag == TAIR_SERVERFLAG_PROXY)
          && table_mgr->is_master(bucket_number, server_flag) == false) {
         log_debug("request rejected...");
         rc = TAIR_RETURN_WRITE_NOT_ON_MASTER;
         return false;
      }

      if (op_flag & TAIR_OPERATION_DUPLICATE) {
         bool is_available = false;
         for (int i=0; i<TAIR_DUPLICATE_BUSY_RETRY_COUNT; ++i) {
            is_available = duplicator->is_bucket_available(bucket_number);
            if (is_available)
               break;

            usleep(1000);
         }

         if (is_available == false) {
            log_debug("bucket is not avaliable, reject request");
            rc = TAIR_RETURN_DUPLICATE_BUSY;
            return false;
         }
      }

      return true;
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
      if (status == STATUS_CAN_WORK) {
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
      return status == STATUS_CAN_WORK;
   }

   int tair_manager::get_mutex_index(data_entry &key)
   {
      uint32_t hashcode = util::string_util::mur_mur_hash(key.get_data(), key.get_size());
      return hashcode % mutex_array_size;
   }

   void tair_manager::get_slaves(int server_flag, int bucket_number, vector<uint64_t> &slaves) {
      if (table_mgr->get_copy_count() == 1) return;

      if (server_flag == TAIR_SERVERFLAG_PROXY) {
         slaves = table_mgr->get_slaves(bucket_number, true);
      } else {
         slaves = (table_mgr->get_slaves(bucket_number, migrate_done_set.test(bucket_number)));
      }
   }

   int tair_manager::get_op_flag(int bucket_number, int server_flag)
   {
      int flag = 0;
      if (server_flag == TAIR_SERVERFLAG_CLIENT ||
          server_flag == TAIR_SERVERFLAG_PROXY) {
         flag |= TAIR_OPERATION_VERSION;
         flag |= TAIR_OPERATION_DUPLICATE;
         flag |= TAIR_OPERATION_REMOTE;
      }

      return flag;
   }

}
