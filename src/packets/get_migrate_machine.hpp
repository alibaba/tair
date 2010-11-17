/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for query migrating dataservers currently
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_GET_MIGRATE_MACHINR_PACKET_H
#define TAIR_PACKET_GET_MIGRATE_MACHINR_PACKET_H
#include "base_packet.hpp"
#include "server_hash_table_packet.hpp"
namespace tair {
   /**
    * get migrating machine of group which srver_id belongs to
    */
   class request_get_migrate_machine : public base_packet {
   public:

      request_get_migrate_machine()
      {
         setPCode(TAIR_REQ_GET_MIGRATE_MACHINE_PACKET);
         server_id = 0;
      }

      ~request_get_migrate_machine()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt64(server_id);
         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 8) {
            log_warn("buffer data too few.");
            return false;
         }
         server_id = input->readInt64();
         return true;
      }

   public:
      uint64_t server_id;
   private:
      request_get_migrate_machine(const request_get_migrate_machine&);
   };
   class response_get_migrate_machine : public base_packet {
   public:
      response_get_migrate_machine()
      {
         setPCode(TAIR_RESP_GET_MIGRATE_MACHINE_PACKET);
      }

      ~response_get_migrate_machine()
      {
      }


      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(vec_ms.size());
         for (size_t i = 0; i < vec_ms.size(); ++i) {
            output->writeInt64(vec_ms[i].first);
            output->writeInt32(vec_ms[i].second);
         }
         return true;
      }


      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 4) {
            log_warn("buffer data too few.");
            return false;
         }
         vec_ms.clear();
         int size = input->readInt32();
         for (int i=0; i<size; i++) {
            uint64_t server_id = input->readInt64();
            uint32_t count = input->readInt32();
            vec_ms.push_back(make_pair(server_id,count));
         }
         return true;
      }

   public:
      vector<pair<uint64_t, uint32_t> > vec_ms;      //serverId, and bucket count
   private:
      response_get_migrate_machine(const response_get_migrate_machine&);
   };
}
#endif
