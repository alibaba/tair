/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for client to pull table from configserver
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_GET_GROUP_PACKET_H
#define TAIR_PACKET_GET_GROUP_PACKET_H
#include "base_packet.hpp"
#include "server_hash_table_packet.hpp"
namespace tair {
   class request_get_group : public base_packet {
   public:

      request_get_group()
      {
         setPCode(TAIR_REQ_GET_GROUP_PACKET);
         group_name[0] = '\0';
         config_version = 0;
      }

      request_get_group(const request_get_group &packet)
      {
         setPCode(TAIR_REQ_GET_GROUP_PACKET);
         set_group_name(packet.group_name);
         config_version = packet.config_version;
      }

      ~request_get_group()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(config_version);
         output->writeString(group_name);
         return true;
      }
      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 8) {
            log_warn("buffer data too few.");
            return false;
         }
         config_version = input->readInt32();
         char *p = &group_name[0];
         input->readString(p, 64);
         return true;
      }

      // set_group_name
      void set_group_name(const char *group_name_value) 
      {
         group_name[0] = '\0';
         if (group_name_value != NULL) {
            strncpy(group_name, group_name_value, 64);
            group_name[63] = '\0';
         }
      }

   public:
      char group_name[64];
      uint32_t config_version;
   };
   class response_get_group : public base_packet, public server_hash_table_packet {
   public:
      response_get_group()
      {
         config_version = 0;
         setPCode(TAIR_RESP_GET_GROUP_PACKET);
         bucket_count = 0;
         copy_count = 0;
      }


      ~response_get_group()
      {
      }


      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(bucket_count);
         output->writeInt32(copy_count);
         output->writeInt32(config_version);
         // config
         int size = config_map.size();

         output->writeInt32(size);
         tbsys::STR_STR_MAP::iterator it;
         for (it=config_map.begin(); it!=config_map.end(); ++it) {
            output->writeString(it->first.c_str());
            output->writeString(it->second.c_str());
         }
         // data
         output->writeInt32(hash_table_size);
         if (hash_table_size > 0 && hash_table_data != NULL) {
            output->writeBytes(hash_table_data, hash_table_size);
         }
         // avilable server
         output->writeInt32(available_server_ids.size());
         set<uint64_t>::iterator it_as = available_server_ids.begin();
         for (; it_as != available_server_ids.end(); ++it_as) {
            output->writeInt64(*it_as);
         }
         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 12) {
            log_warn("buffer data too few.");
            return false;
         }
         bucket_count = input->readInt32();
         copy_count = input->readInt32();
         config_version = input->readInt32();
         // config
         int size = input->readInt32();
         char key[64];
         char value[1024];
         char *tmp;
         for (int i=0; i<size; i++) {
            tmp = key;
            input->readString(tmp, 64);
            key[63] = '\0';
            tmp = value;
            input->readString(tmp, 1024);
            value[1023] = '\0';
            config_map[key] = value;
         }
         if (input->getDataLen() < 4+hash_table_size) {
            log_warn("buffer data too few.");
            return false;
         }
         // data
         set_hash_table(NULL, input->readInt32());
         if (hash_table_size > 0 && hash_table_data != NULL) {
            input->readBytes(hash_table_data, hash_table_size);
         }
         //avaiable server
         available_server_ids.clear();
         size = input->readInt32();
         for (int i = 0; i < size; ++i) {
            available_server_ids.insert(input->readInt64());
         }

         return true;
      }

   public:
      uint32_t  bucket_count;
      uint32_t  copy_count;
      uint32_t  config_version;
      tbsys::STR_STR_MAP config_map;
      set<uint64_t> available_server_ids;
   private:
      response_get_group(const response_get_group&);
   };
}
#endif
