/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * dataserver operations abstract layer
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_MANAGER_H
#define TAIR_MANAGER_H

#include <tbsys.h>
#include <tbnet.h>
#include <boost/dynamic_bitset.hpp>
#include "define.hpp"
#include "dump_data_info.hpp"
#include "util.hpp"
#include "data_entry.hpp"
#include "storage_manager.hpp"
#include "dump_manager.hpp"
#include "dump_filter.hpp"
#include "table_manager.hpp"
#include "duplicate_base.hpp"
#include "update_log.hpp"
#include "stat_helper.hpp"
#include "plugin_manager.hpp"
#include "put_packet.hpp"
#include "get_packet.hpp"
#include "remove_packet.hpp"
#include "lock_packet.hpp"
#include "inc_dec_packet.hpp"
#include "packet_streamer.hpp"
#include "prefix_puts_packet.hpp"
#include "prefix_removes_packet.hpp"
#include "response_mreturn_packet.hpp"

namespace tair {
   enum {
      ELEMENT_TYPE_INT = 0, //int 32bit
      ELEMENT_TYPE_LLONG,// long long 64 bit
      ELEMENT_TYPE_DOUBLE,//double
      ELEMENT_TYPE_STRING,//string
      ELEMENT_TYPE_INVALID,
   };
   using namespace tair::common;
   class migrate_manager;
   class RemoteSyncManager;

   class tair_manager {
      enum {
         STATUS_NOT_INITED,
         STATUS_INITED,
         STATUS_CAN_WORK,
      };

      const static int mutex_array_size = 1000;

   public:
      tair_manager();
      ~tair_manager();

      bool initialize(tbnet::Transport *transport, tair_packet_streamer *streamer);

      int prefix_puts(request_prefix_puts *request, int version);
      int prefix_removes(request_prefix_removes *request, int version);
      int prefix_hides(request_prefix_hides *request, int version);
      int prefix_incdec(request_prefix_incdec *request, int version);
      int put(int area, data_entry &key, data_entry &value, int expire_time,base_packet *request=NULL,int version=0);
      //int put(int area, data_entry &key, data_entry &value, int expire_time,request_put *request,int version);
      int add_count(int area, data_entry &key, int count, int init_value, int *result_value, int expire_time,base_packet * request,int version);
      int get(int area, data_entry &key, data_entry &value, bool with_stat = true);
      int hide(int area, data_entry &key, base_packet *request = NULL, int heart_version = 0);
      int get_hidden(int area, data_entry &key, data_entry &value);
      int remove(int area, data_entry &key,request_remove *request=NULL,int version=0);
      int batch_remove(int area, const tair_dataentry_set * key_list,request_remove *request,int version);
      int batch_put(int area, mput_record_vec* record_vec, request_mput* request, int version);
      int clear(int area);

      int op_cmd(ServerCmdType cmd, std::vector<std::string>& params);
      int expire(int area, data_entry &key, int expire_time, base_packet *request = NULL, int version = 0);

      int direct_put(data_entry &key, data_entry &value);
      int direct_remove(data_entry &key);

      int get_range(int32_t area, data_entry &key_start, data_entry &key_end, int offset, int limit, int type, std::vector<data_entry*> &result, bool &has_next); 
    
      int add_items(int area,
                    data_entry& key,
                    data_entry& value,
                    int max_count, int expire_time = 0);

      int get_items(int area,
                    data_entry& key,
                    int offset, int count, data_entry& value /*out*/,
                    int type = ELEMENT_TYPE_INVALID);

      int get_item_count(int area, data_entry& key);

      int remove_items(int area, data_entry& key, int offset, int count);

      int get_and_remove(int area,
                         data_entry& key,
                         int offset, int count, data_entry& value /*out*/,
                         int type = ELEMENT_TYPE_INVALID);

     int lock(int area, LockType lock_type, data_entry& key, base_packet *request = NULL, int heart_version = 0);

      bool is_migrating();
      bool should_proxy(data_entry &key, uint64_t &targetServerId);
      bool is_local(data_entry &key);
      void set_migrate_done(int bucketNumber);
      void set_area_quota(int area,uint64_t quota) {
         storage_mgr->set_area_quota(area,quota);
      }
      void get_slaves(int server_flag, int bucket_number, vector<uint64_t> &slaves);

      void set_solitary(); // make this data server waitting for next direction
      bool is_working();

      void update_server_table(uint64_t *server_table, int server_table_size, uint32_t server_table_version, int32_t data_need_remove, vector<uint64_t> &current_state_table, uint32_t copy_count, uint32_t bucket_count);

      void get_proxying_buckets(vector<uint32_t> &buckets);

      void do_dump(set<dump_meta_info> dump_meta_infos);

      int get_meta(int area, data_entry &key, item_meta_info &meta);

      plugin::plugins_manager plugins_manager;
      tair::storage::storage_manager *get_storage_manager() { return storage_mgr; }
      RemoteSyncManager* get_remote_sync_manager() { return remote_sync_mgr; }

      bool is_localmode();
      uint32_t get_bucket_number(const data_entry &key);
      table_manager* get_table_manager() { return table_mgr; }

   private:
      tair::storage::storage_manager *get_storage_manager(data_entry &key);
      bool should_write_local(int bucket_number, int server_flag, int op_flag, int &rc);
      bool need_do_migrate_log(int bucket_number);
      int get_op_flag(int bucket_number, int server_flag);
      void init_migrate_log();
      int get_mutex_index(data_entry &key);
      int do_duplicate(int area, data_entry& key, data_entry& value,int bucket_number,base_packet *request,int heart_vesion);
      int do_remote_sync(TairRemoteSyncType type, common::data_entry* key, common::data_entry* value, int rc, int op_flag);

   private:
      int status;
      bool localmode;
      bool not_allow_count_negative;
      tbsys::CThreadMutex counter_mutex[mutex_array_size];
      tbsys::CThreadMutex item_mutex[mutex_array_size];

      tbsys::CThreadMutex update_server_table_mutex;
      tair::storage::storage_manager *storage_mgr;
      table_manager *table_mgr;
      base_duplicator *duplicator;
      migrate_manager *migrate_mgr;
      boost::dynamic_bitset<> migrate_done_set;
      update_log *migrate_log;
      tair::storage::dump_manager *dump_mgr;
      RemoteSyncManager *remote_sync_mgr;
   };
}
#endif
