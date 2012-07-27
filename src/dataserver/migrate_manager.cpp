/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * migrate manager impl
 *
 * Version: $Id$
 *
 * Authors:
 *   fanggang <fanggang@taobao.com>
 *     - initial release
 *
 */
#include <tbsys.h>
#include "util.hpp"
#include "define.hpp"
#include "data_entry.hpp"
#include "migrate_manager.hpp"
#include <vector>

namespace tair {

   migrate_manager::migrate_manager(tbnet::Transport *transport, tair_packet_streamer *streamer, base_duplicator *this_duplicator, tair_manager *tair_mgr, tair::storage::storage_manager *storage_mgr)
   {
      conn_mgr = new tbnet::ConnectionManager(transport, streamer, this);
      duplicator = this_duplicator;
      this->tair_mgr = tair_mgr;
      this->storage_mgr = storage_mgr;
      is_signaled = false;
      is_running = 0;
      is_stopped = false;
      is_alive = false;
      current_locked_bucket = -1;
      current_migrating_bucket = -1;
      version = 0;
      timeout = 1000;
      vector<const char*> cs_str_list = TBSYS_CONFIG.getStringList(TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
      if (cs_str_list.size() == 0U) {
         log_warn("miss config item %s:%s", TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
         return;
      }
      int port = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
      uint64_t id;
      for (uint32_t i=0; i<cs_str_list.size(); i++) {
         id = tbsys::CNetUtil::strToAddr(cs_str_list[i], port);
         if (id == 0) continue;
         config_server_list.push_back(id);
         if (config_server_list.size() == 2U) break;
      }
      if (config_server_list.size() == 0U) {
         log_error("config invalid %s:%s", TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
      }

      this->start();
   }

   migrate_manager::~migrate_manager()
  {
      log_warn("migrate_manager destruct");
      this->stop();
      int i=0;
      while(is_running && i++<9)
      {
        sleep(1);
      }
      delete conn_mgr;
   }

   void migrate_manager::set_log(update_log *log)
   {
      this->log = log;
   }

   void migrate_manager::run(tbsys::CThread *thread, void *arg)
   {
      is_alive = true;
      while (true) {
         mutex.lock();
         if (is_signaled) {
            is_signaled = false;
            mutex.unlock();
         } else {
            mutex.unlock();
            condition.wait(0);
         }
         if (_stop){
            break;
         }
         is_running = 1;
         is_stopped = false;
         uint64_t before, after;
         before = tbsys::CTimeUtil::getTime();
         do_run();
         after = tbsys::CTimeUtil::getTime();
         uint64_t interval = after - before;
         log_debug("this migrate consume time:%llu", interval);
         {
            tbsys::CThreadGuard guard(&mutex);
            is_running = 0;
            is_stopped = false;
         }
      }
      is_alive = false;
   }

   void migrate_manager::signal()
   {
      {
         tbsys::CThreadGuard guard(&mutex);
         if (!is_alive || _stop)
            return;
         if (is_running == 1)
            is_signaled = true;
         else if (!is_signaled) {
            is_signaled = true;
            condition.signal();
         }
      }
   }

   void migrate_manager::stop()
   {
      {
         tbsys::CThreadGuard guard(&mutex);
         if (!is_alive || _stop) {
            _stop = true;
            return;
         }
         is_stopped = true;
      }
      _stop = true;
      condition.signal();
      wait();
   }

   void migrate_manager::do_run()
   {
      get_data_mutex.lock();
      bool have_task = migrate_servers.empty();
      if (!have_task) {
         migrate_servers.swap(temp_servers);
      } else {
         get_data_mutex.unlock();
         return;
      }
      get_data_mutex.unlock();
      do_migrate();

      temp_servers.clear();
      migrated_buckets.clear();
   }

   void migrate_manager::do_migrate()
   {
      bucket_server_map_it  it = temp_servers.begin();
      for(; it != temp_servers.end() && !is_stopped; it++){
         int bucket_number = it->first;
         log_error("begin migrate bucket: %d", bucket_number);
         vector<uint64_t> servers = it->second;
         if(servers.empty()){
            current_locked_bucket = bucket_number;
            usleep(5000);
            while(!(duplicator->has_bucket_duplicate_done(bucket_number))){
               usleep(5000);
            }
            finish_migrate_bucket(bucket_number);
            current_locked_bucket = -1;
            continue;
         }
         for(uint i = 0; i < servers.size(); i++){
            log_debug("----to server:%s",  tbsys::CNetUtil::addrToString(servers[i]).c_str());
         }
         do_migrate_one_bucket(bucket_number, servers);
         log_error("finish %d bucket migrate",  bucket_number);
      }
   }

   void migrate_manager::do_migrate_one_bucket(int bucket_number, vector<uint64_t> dest_servers)
   {
      lsn_type last_lsn = log->get_current_lsn();
      current_migrating_bucket = bucket_number;
      usleep(MISECONDS_WAITED_FOR_WRITE);
      bool is_sucess = migrate_data_file(bucket_number, dest_servers);
      if (!is_sucess) {
         log_debug("migrate be stopped or file error!");
         current_migrating_bucket = -1;
         return;
      }
      lsn_type current_lsn = log->get_current_lsn();

      while((current_lsn - last_lsn >= MIGRATE_LOCK_LOG_LEN)) {
         is_sucess = migrate_log(bucket_number, dest_servers, last_lsn, current_lsn);
         if (!is_sucess) {
            current_migrating_bucket = -1;
            return;
         }
         last_lsn = current_lsn;
         current_lsn = log->get_current_lsn();
      }

      current_locked_bucket = bucket_number;
      usleep(MISECONDS_WAITED_FOR_WRITE);
      current_lsn = log->get_current_lsn();
      is_sucess = migrate_log(bucket_number, dest_servers, last_lsn, current_lsn);
      if (!is_sucess) {
         current_migrating_bucket = -1;
         current_locked_bucket = -1;
         return;
      }

      last_lsn = current_lsn;
      current_lsn = log->get_current_lsn();
      while (current_lsn != last_lsn ){
         is_sucess = migrate_log(bucket_number, dest_servers, last_lsn, current_lsn);
         if (!is_sucess) break;
         last_lsn = current_lsn;
         current_lsn = log->get_current_lsn();
      }

      if (!is_sucess) {
         current_locked_bucket = -1;
         current_migrating_bucket = -1;
         return;
      } else {
         while(!(duplicator->has_bucket_duplicate_done(bucket_number))) {
            usleep(1000);
            if (is_stopped || _stop){
               current_migrating_bucket = -1;
               current_locked_bucket = -1;
               return;
            }
         }
         finish_migrate_bucket(bucket_number);
         current_migrating_bucket = -1;
         current_locked_bucket = -1;
      }
   }

   bool migrate_manager::send_packet(vector<uint64_t> dest_servers, request_mupdate *packet, int db_id)
   {
      bool flag = true;
      bool ret;
     _Reput:
      for(vector<uint64_t>::iterator it = dest_servers.begin(); it < dest_servers.end()&& !is_stopped;){
         ret = true;
         uint64_t server_id = *it;
         request_mupdate *temp_packet = new request_mupdate(*packet);
         wait_object *cwo = wait_object_mgr.create_wait_object();
         if (conn_mgr->sendPacket(server_id, temp_packet, NULL, (void*)((long)cwo->get_id())) == false) {
            log_error("Send migrate put packet to %s failure, bucket: %d",
                      tbsys::CNetUtil::addrToString(server_id).c_str(), db_id);
            delete temp_packet;
            ret = false;
         } else {
            cwo->wait_done(1, timeout);
            base_packet *tpacket = cwo->get_packet();
            if (tpacket == NULL || tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
               log_error("Send migrate put packet 2 failure, server: %s, bucket: %d",
                         tbsys::CNetUtil::addrToString(server_id).c_str(), db_id);
               ret = false;
            } else {
               response_return *rpacket = (response_return *)tpacket;
               if (rpacket->get_code() != TAIR_RETURN_SUCCESS) {
                 log_error("migrate not return success, server: %s, ret: %d", tbsys::CNetUtil::addrToString(server_id).c_str(), rpacket->get_code());
                 ::usleep(500);
                  ret = false;
               }
            }
         }
         if (ret) {
            it = dest_servers.erase(it);
         } else{
            ++it;
         }
         wait_object_mgr.destroy_wait_object(cwo);
      }
      if (is_stopped || _stop){
         flag = false;
         return flag;
      } else {
         if (!dest_servers.empty()){
            goto _Reput;
         }
      }
      return flag;
   }

   bool migrate_manager::migrate_data_file(int db_id, vector<uint64_t> dest_servers)
   {
      bool flag = true;
      bool tag = false;
      md_info info;
      info.is_migrate = true;
      info.db_id = db_id;
      storage_mgr->begin_scan(info);
      vector<item_data_info*> items;
      bool have_item = storage_mgr->get_next_items(info, items);
      request_mupdate *packet = new request_mupdate();
      packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
      uint64_t total_count = 0;

      while(true) {
         if (!items.empty()) {
            total_count += items.size();
            tag = true;
            for(vector<item_data_info *>::iterator itor = items.begin(); itor != items.end(); itor++) {
               item_data_info *item = *itor;
               data_entry key(item->m_data, item->header.keysize, false);
               key.data_meta = item->header;
               // skip embedded cache when migrating
               key.data_meta.flag = TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
               key.has_merged = true;
               if (item->header.magic == MAGIC_ITEM_META_LDB_PREFIX)
                  key.set_prefix_size(item->header.prefixsize); 
               data_entry value(item->m_data + item->header.keysize, item->header.valsize, false);
               value.data_meta = item->header;
               bool is_succuss = packet->add_put_key_data(key, value);
               log_debug("thiskey is %d", key.has_merged);
               if (!is_succuss) {
                  flag = send_packet(dest_servers, packet, db_id);
                  delete packet;
                  packet = new request_mupdate();
                  packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
                  is_succuss = packet->add_put_key_data(key, value);
                  assert(is_succuss);
               }
            }
            for(vector<item_data_info *>::iterator it = items.begin(); it < items.end(); it++) {
               free(*it);
            }
            items.clear();
         }

         if(flag == false){
           log_error("send migrate packet fail. bucket: %d", db_id);
           break;
         }
         if (have_item) {
            have_item = storage_mgr->get_next_items(info, items);
         } else {
            break;
         }
      }

      if (flag && tag) {
         flag = send_packet(dest_servers, packet, db_id);
      }
      delete packet;
      packet = NULL;
      storage_mgr->end_scan(info);
      log_warn("migrate bucket db data end. total count: %d, all_done: %d, send suc: %d", total_count, !have_item, flag);
      return flag;
   }

   bool migrate_manager::migrate_log(int db_id, vector<uint64_t> dest_servers, lsn_type start_lsn, lsn_type end_lsn)
   {
      bool flag = true, ret_flag = true, tag = false;
      log_scan_hander *handle = log->begin_scan(db_id, start_lsn, end_lsn);
      handle = log->get_next(handle);
      const log_record_entry *log_entry = handle->get_log_entry();
      uint count = 0;
      request_mupdate *packet = new request_mupdate();
      packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
      uint offset = 0;
      bool is_succuss = false;
      while(log_entry != NULL) {
         tag = true;
         data_entry key = log_entry->key;
         key.data_meta = log_entry->header;
         key.has_merged = true;
         log_debug("logis:%S", key.get_data());
         data_entry value;
         if (log_entry->operation_type == (uint8_t)SN_PUT) {
            // skip embedded cache when migrating
            key.data_meta.flag = TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
            value = log_entry->value;
            value.data_meta = log_entry->header;
            is_succuss = packet->add_put_key_data(key, value);
         }
         if(log_entry->operation_type == (uint8_t)SN_REMOVE) {
            key.set_version(log_entry->header.version);
            is_succuss = packet->add_del_key(key);
         }

         if (!is_succuss) {
            ret_flag = send_packet(dest_servers, packet, db_id);
            count += offset;
            delete packet;
            packet = new request_mupdate();
            packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
            if (log_entry->operation_type == (uint8_t)SN_PUT) {
               is_succuss = packet->add_put_key_data(key, value);
            }
            if(log_entry->operation_type == (uint8_t)SN_REMOVE) {
               is_succuss = packet->add_del_key(key);
            }
            assert(is_succuss == true);
            offset = 1;
         } else {
            offset++;
         }

         if (!ret_flag){
            delete log_entry;
            flag = false;
            break;
         }

         //delete packet;
         delete log_entry;
         log_entry = NULL;
         handle = log->get_next(handle);
         log_entry = handle->get_log_entry();
         if (count >= 100) {
            log->set_hlsn(handle->current_lsn());
            count = 0;
         }
      }

      if (flag && tag ) {
         flag = send_packet(dest_servers, packet, db_id);
      }
      delete packet;

      log->set_hlsn(handle->current_lsn());
      log->end_scan(handle);
      return flag;
   }

   void migrate_manager::finish_migrate_bucket(int bucket_number)
   {
      request_migrate_finish packet;
      packet.server_id = local_server_ip::ip;
      packet.bucket_no = bucket_number;
      packet.version = version;
      bool ret = true;
      bool flag = true;

     _ReSend:
      int i = config_server_list.size();
      wait_object *cwo = wait_object_mgr.create_wait_object();
      for(vector<uint64_t>::iterator it = config_server_list.begin(); it < config_server_list.end(); it++ ){
         uint64_t server_id = *it;
         request_migrate_finish *temp_packet = new request_migrate_finish(packet);
         if (conn_mgr->sendPacket(server_id, temp_packet, NULL, (void*)((long)cwo->get_id())) == false) {
            log_error("Send Migratefinishpacket:%d to CS %S failure.", bucket_number, tbsys::CNetUtil::addrToString(server_id).c_str());
            delete temp_packet;
            --i;
         }
      }
      if (i == 0){
         wait_object_mgr.destroy_wait_object(cwo);
         // fix bug: if we kill cfg_server before ds when we stop tair cluster, ds will be fall into endless loop here.
         if (is_stopped || _stop){
           log_warn("dataserver has receive stop signal, stop finish_migrate_bucket");
           return;
         }
         goto _ReSend;
      }

      ret = true;
      cwo->wait_done(1, timeout);
      base_packet *tpacket = cwo->get_packet();
      if (tpacket == NULL || tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
         log_error("send migrateFinishPacket to config server:  bucket: %d failed!", bucket_number);
         ret = false;
      } else {
         response_return *rpacket = (response_return *)tpacket;
         if (rpacket->get_code() == EXIT_FAILURE) {
            log_error("send migrateFinishPacket to config serve, but version is error");
            flag = false;
         }
      }

      wait_object_mgr.destroy_wait_object(cwo);

      if (is_stopped || _stop){
         return;
      }
      if(!ret){
         usleep(10000);
         goto _ReSend;
      }
      if (flag) {
         tbsys::CThreadGuard guard(&get_migrating_buckets_mutex);
         migrated_buckets.push_back(bucket_number);
         tair_mgr->set_migrate_done(bucket_number);
      }
   }

   void migrate_manager::set_migrate_server_list(bucket_server_map migrate_server_list, uint32_t version)
   {
      tbsys::CThreadGuard guard(&get_data_mutex);
      migrate_servers.swap(migrate_server_list);
      this->version = version;
      log_error("my migrate version is %d", version);
      is_running = 0;
      signal();
   }

   void migrate_manager::do_server_list_changed()
   {
      if(is_running == 1) {
         is_stopped = true;
      }
      while(is_running == 1) {
         usleep(5000);
      }
      is_running = 2;
   }

   bool migrate_manager::is_bucket_available(int bucket_number)
   {
      return current_locked_bucket != bucket_number;
   }

   bool migrate_manager::is_bucket_migrating(int bucket_number)
   {
      return current_migrating_bucket == bucket_number;
   }

   bool migrate_manager::is_migrating()
   {
      return is_running == 1;
   }

   vector<uint64_t> migrate_manager::get_migrated_buckets()
   {
      tbsys::CThreadGuard guard(&get_migrating_buckets_mutex);
      return migrated_buckets;
   }

   tbnet::IPacketHandler::HPRetCode migrate_manager::handlePacket(tbnet::Packet *packet, void *args)
   {
      if (!packet->isRegularPacket()) {
         tbnet::ControlPacket *cp = (tbnet::ControlPacket*)packet;
         log_warn("ControlPacket, cmd:%d", cp->getCommand());
      }
      int id = (int)((long)args);
      if (id) {
         if (packet->isRegularPacket()) {
            wait_object_mgr.wakeup_wait_object(id, (base_packet*)packet);
         }
         else {
            packet->free();
         }
      } else if (packet->isRegularPacket()) {
         packet->free();
      }
      return tbnet::IPacketHandler::KEEP_CHANNEL;
   }
}
