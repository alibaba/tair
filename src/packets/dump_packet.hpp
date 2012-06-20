/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * dump request packet
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_DUMP_PACKET_H
#define TAIR_PACKET_DUMP_PACKET_H
#include "base_packet.hpp"
#include "dump_data_info.hpp"
namespace tair {
   class request_dump : public base_packet {
   public:
      request_dump()
      {
         setPCode(TAIR_REQ_DUMP_PACKET);
      }

      request_dump(request_dump &packet) : base_packet(packet)
      {
         setPCode(TAIR_REQ_DUMP_PACKET);
         info_set = packet.info_set;
      }

      ~request_dump() {
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(info_set.size());
         if (info_set.size() > 0) {
            set<dump_meta_info>::iterator it;
            for (it = info_set.begin(); it != info_set.end(); it++) {
               dump_meta_info info = *it;
               output->writeInt32(info.start_time);
               output->writeInt32(info.end_time);
               output->writeInt32(info.area);
            }
         }
         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         int size = input->readInt32();
         info_set.clear();
         for (int i=0; i<size; ++i) {
            dump_meta_info info;
            info.start_time = input->readInt32();
            info.end_time = input->readInt32();
            info.area = input->readInt32();
            info_set.insert(info);
         }
         return true;
      }

      void add_dump_info(dump_meta_info info)
      {
         info_set.insert(info);
      }

   public:
      set<dump_meta_info> info_set;
   };
}
#endif
