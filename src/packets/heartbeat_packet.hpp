/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * heartbeat packet between dataserver and configserver
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_HEARTBEAT_PACKET_H
#define TAIR_PACKET_HEARTBEAT_PACKET_H
#include "base_packet.hpp"
#include "server_hash_table_packet.hpp"
#include "stat_info.hpp"
#include "stat_helper.hpp"
using namespace std;
namespace tair {


   class request_heartbeat : public base_packet {
   public:
      request_heartbeat()
      {
         setPCode(TAIR_REQ_HEARTBEAT_PACKET);
         server_id = 0U;
         loop_count = 0U;
         config_version = 0U;
         status = 0U;
         server_flag = 0;
         //memset(&_stat, 0, sizeof(tair_stat));
         plugins_version = 0U;
         area_capacity_version = 0U;
         pull_migrated_info =0U;
         stat_info = NULL;
         stat_info_size = 0;
         stats = NULL;
      }

      request_heartbeat(request_heartbeat &packet)
      {
         setPCode(TAIR_REQ_HEARTBEAT_PACKET);
         server_id = packet.server_id;
         loop_count = packet.loop_count;
         config_version = packet.config_version;
         status = packet.status;
         server_flag = packet.server_flag;
         //memcpy(&_stat, &(packet._stat), sizeof(tair_stat));
         plugins_version = packet.plugins_version;
         area_capacity_version = packet.area_capacity_version;
         vec_area_capacity_info = packet.vec_area_capacity_info;
         vec_bucket_no = packet.vec_bucket_no;
         pull_migrated_info = packet.pull_migrated_info;
         stat_info_size = packet.stat_info_size;
         stat_info = NULL;
         stats = NULL;
         if (stat_info_size > 0) {
            stat_info = (char *)malloc(stat_info_size);
            memcpy(stat_info, packet.stat_info, stat_info_size);
            // we prefer rebuild stats rather than copy here
         }
      }

      ~request_heartbeat()
      {
         if (stat_info != NULL) {
            ::free(stat_info);
            stat_info = NULL;
         }
         if (stats != NULL) {
            ::free(stats);
            stats = NULL;
         }
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt8(server_flag);
         output->writeInt64(server_id);
         output->writeInt32(loop_count);
         output->writeInt32(config_version);
         output->writeInt32(status);
         //output->writeBytes(&_stat, sizeof(tair_stat));
         output->writeInt8(pull_migrated_info);
         output->writeInt32(vec_bucket_no.size());
         for (size_t i = 0; i < vec_bucket_no.size(); i++) {
            output->writeInt32(vec_bucket_no[i]);
         }
         output->writeInt32(plugins_version);
         output->writeInt32(area_capacity_version);
         output->writeInt32(vec_area_capacity_info.size());
         for(uint32_t i = 0; i < vec_area_capacity_info.size(); i++) {
            output->writeInt32(vec_area_capacity_info[i].first);
            output->writeInt64(vec_area_capacity_info[i].second);
         }
         output->writeInt32(stat_info_size);
         if (stat_info_size > 0) {
            output->writeBytes(stat_info, stat_info_size);
         }

         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 12) {
            log_warn( "buffer data too few.");
            return false;
         }
         server_flag = input->readInt8();
         server_id = input->readInt64();
         loop_count = input->readInt32();
         config_version = input->readInt32();
         status = input->readInt32();
         pull_migrated_info = input->readInt8();
         size_t size = input->readInt32();
         if (size > 0) {
            vec_bucket_no.reserve(size);
            for (size_t i = 0; i < size; i++) {
               vec_bucket_no.push_back(input->readInt32());
            }
         }
         plugins_version = input->readInt32();
         area_capacity_version = input->readInt32();
         vec_area_capacity_info.clear();
         uint32_t capacityInfoSize = input->readInt32();
         for(uint32_t i = 0; i < capacityInfoSize; i++) {
            uint32_t area = input->readInt32();
            uint64_t capacity = input->readInt64();
            vec_area_capacity_info.push_back(make_pair(area, capacity));
         }

         stat_info_size = input->readInt32();
         if (stat_info_size > 0) {
            stat_info = (char *)malloc(stat_info_size);
            input->readBytes(stat_info, stat_info_size);
         }
         return true;
      }

      void set_stat_info(const char *stat_info_value, int size)
        {
         if (stat_info != NULL) {
            stat_info_size = 0;
            ::free(stat_info);
            stat_info = NULL;
         }
         if (stats != NULL) {
            ::free(stats);
            stats = NULL;
         }

         if (size > 0 && stat_info_value != NULL) {
            stat_info_size = size;
            stat_info = (char *)malloc(size);
            memcpy(stat_info, stat_info_value, size);
         }
      }

      tair_stat *get_stat()
      {
         if (stat_info_size == 0 || stat_info == NULL)
            return NULL;

         if (stats != NULL) {
            return stats;
         }

         unsigned long dest_len = STAT_TOTAL_SIZE;
         unsigned long src_len = stat_info_size;
         stats = (tair_stat *)malloc(dest_len);
         memset(stats, 0, dest_len);

         if (uncompress((Bytef*)stats, &dest_len, (const Bytef*)stat_info, src_len) != Z_OK) {
            ::free(stats);
            log_error( "uncompress stat info failed");
         } else {
            log_debug("uncompress successed");
         }
         return stats;
      }

   public:
      uint64_t server_id;
      uint32_t loop_count;
      uint32_t config_version;
      uint32_t status; // server status, {0: alive, 1: down, 2: initing, 3: synced}
      //tair_stat _stat;
      uint32_t  plugins_version;
      uint32_t  area_capacity_version;
      vector<pair<uint32_t, uint64_t> > vec_area_capacity_info;
      uint8_t  pull_migrated_info;   // 0 don't need migrated info, 1 need migrated info
      vector<uint32_t> vec_bucket_no;

      char *stat_info;
      int stat_info_size;
      tair_stat *stats;

   };

   class response_heartbeat : public base_packet, public server_hash_table_packet {
   public:

      response_heartbeat() 
      {
         setPCode(TAIR_RESP_HEARTBEAT_PACKET);
         client_version = 0U;
         server_version = 0U;
         down_slave_config_server = 0U;
         data_need_move = -1;
         plugins_flag = -1;
         plugins_version = 0U;
         area_capacity_version = 0U;
         copy_count = 0U;
         bucket_count = 0U;
      }

      ~response_heartbeat()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(client_version);
         output->writeInt32(server_version);
         output->writeInt32(bucket_count);
         output->writeInt32(copy_count);
         output->writeInt64(down_slave_config_server);
         output->writeInt32(data_need_move);
         output->writeInt32(hash_table_size);
         if (hash_table_data != NULL && hash_table_size > 0) {
            output->writeBytes(hash_table_data, hash_table_size);
         }
         output->writeBytes(group_name, 64);
         output->writeInt32(plugins_flag);
         output->writeInt32(plugins_version);
         if (plugins_flag > 0) {
            for (vector<string>::iterator it = plugins_dll_names.begin();
                 it != plugins_dll_names.end(); it++) {
               size_t len = it->length();
               if (len > TAIR_MAX_FILENAME_LEN - 1) len = TAIR_MAX_FILENAME_LEN - 1;
               output->writeInt32(len);
               output->writeBytes(it->c_str(),len);
            }
         }
         output->writeInt32(area_capacity_version);
         output->writeInt32(vec_area_capacity_info.size());
         for(uint32_t i = 0; i < vec_area_capacity_info.size(); i++) {
            output->writeInt32(vec_area_capacity_info[i].first);
            output->writeInt64(vec_area_capacity_info[i].second);
         }

         output->writeInt32(migrated_info.size());
         if (migrated_info.size() > 0) {
            assert(migrated_info.size() % (copy_count + 1) == 0);
            for (size_t i = 0; i < migrated_info.size(); ++i) {
               output->writeInt64(migrated_info[i]);
            }
         }

         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 20) {
            log_warn( "buffer data too few.");
            return false;
         }
         client_version = input->readInt32();
         server_version = input->readInt32();
         bucket_count = input->readInt32();
         copy_count = input->readInt32();
         down_slave_config_server = input->readInt64();
         data_need_move = input->readInt32();
         set_hash_table(NULL, input->readInt32());
         if (hash_table_size > input->getDataLen()) {
            log_warn( "buffer data too few.");
            return false;
         }
         if (hash_table_size > 0 && hash_table_data != NULL) {
            input->readBytes(hash_table_data, hash_table_size);
         }
         input->readBytes(group_name, 64);
         group_name[63] = '\0';
         plugins_flag = input->readInt32();
         plugins_dll_names.clear();
         plugins_version = input->readInt32();
         if (plugins_flag > 0) {
            plugins_dll_names.reserve(plugins_flag);
            for (int32_t i = 0; i < plugins_flag; i++) {
               char dll_name[TAIR_MAX_FILENAME_LEN];
               size_t len = input->readInt32();
               input->readBytes(dll_name, len);
               dll_name[len] = '\0';
               plugins_dll_names.push_back(dll_name);
            }
         }
         area_capacity_version = input->readInt32();
         vec_area_capacity_info.clear();
         uint32_t capacityInfoSize = input->readInt32();
         for(uint32_t i = 0; i < capacityInfoSize; i++) {
            uint32_t area = input->readInt32();
            uint64_t capacity = input->readInt64();
            vec_area_capacity_info.push_back(make_pair(area, capacity));
         }
         int size = input->readInt32();
         assert(migrated_info.size() % (copy_count + 1) == 0);
         for (int i = 0 ; i < size; ++i) {
            migrated_info.push_back(input->readInt64());
         }
         return true;
      }
      bool have_plugins_info()
      {
         return plugins_flag > 0;
      }
      void set_plugins_names(const std::set<std::string>& dll_names)
      {
         plugins_flag = dll_names.size();
         plugins_dll_names.reserve(plugins_flag);
         for (std::set<std::string>::const_iterator it = dll_names.begin();
              it != dll_names.end(); it++) {
            plugins_dll_names.push_back(*it);
         }
      }
      void set_area_capacity(const std::map<uint32_t, uint64_t>& capacity, uint32_t copy_count, uint32_t server_count )
      {
         uint32_t div =  server_count;
         if (div) {
            vec_area_capacity_info.reserve(capacity.size());
            for (std::map<uint32_t, uint64_t>::const_iterator it = capacity.begin();
                 it != capacity.end(); it++) {
               vec_area_capacity_info.push_back(std::make_pair(it->first, it->second / div));
            }
         }
      }
   public:
      uint32_t client_version;
      uint32_t server_version;
      uint32_t bucket_count;
      uint32_t copy_count;
      uint64_t down_slave_config_server;
      int32_t  data_need_move;
      char group_name[64];
      int32_t plugins_flag;
      uint32_t plugins_version;
      vector<string>  plugins_dll_names;
      uint32_t area_capacity_version;
      vector<pair<uint32_t, uint64_t> > vec_area_capacity_info;

      // migrated_info will contains bucket number and all serverId which is a copy of this. if bucket's count greater than 1, repeate this
      // for exmaple when copy count is 3 , this may be  bucketId1, serverid11,serverid12,serverid13, bucketId2,serverid21,serverid22,serverid23
      vector<uint64_t> migrated_info;

   private:
      response_heartbeat(const response_heartbeat&);

   };
}
#endif
