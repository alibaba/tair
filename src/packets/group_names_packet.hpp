/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for query group names from configserver
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_GROUP_NAMES_PACKET
#define TAIR_PACKET_GROUP_NAMES_PACKET
#include "base_packet.hpp"
#include "ping_packet.hpp"
namespace tair {
   class request_group_names : public request_ping {
   public:
      request_group_names() : request_ping()
      {
         setPCode(TAIR_REQ_GROUP_NAMES_PACKET);
      }


      request_group_names(request_group_names &packet) : request_ping(packet)
      {
         setPCode(TAIR_REQ_GROUP_NAMES_PACKET);
      }
   };
   class response_group_names : public base_packet {
   public:

      response_group_names()
      {
         status = 0;
         setPCode(TAIR_RESP_GROUP_NAMES_PACKET);
      }
      ~response_group_names() 
      {
      }

      bool encode(tbnet::DataBuffer *output) 
      {
         output->writeInt32(status);
         output->writeInt32(group_name_list.size());
         for (uint32_t i=0; i<group_name_list.size(); i++) {
            output->writeString(group_name_list[i].c_str());
         }
         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 8) {
            log_warn( "buffer data too few.");
            return false;
         }
         status = input->readInt32();
         int count = input->readInt32();
         char group_name[64];
         char *p = &group_name[0];
         for (int i=0; i<count; i++) {
            input->readString(p, 64);
            group_name[63] = '\0';
            group_name_list.push_back(group_name);
         }
         return true;
      }
      // set_group_name
      void add_group_name(const char *group_name)
      {
         group_name_list.push_back(group_name);
      }
   public:
      int status;
      vector<string> group_name_list;
   private:
      response_group_names(const response_group_names&);
   };
}
#endif
