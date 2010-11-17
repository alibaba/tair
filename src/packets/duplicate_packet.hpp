/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * duplicate packet
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_DUPLICATE_PACKET_H
#define TAIR_PACKET_DUPLICATE_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_duplicate : public base_packet {
   public:

      request_duplicate()
      {
         setPCode(TAIR_REQ_DUPLICATE_PACKET);
         server_flag = 1;
         bucket_number = -1;
         packet_id = 0;
         area = 0;
      }


      request_duplicate(const request_duplicate &packet)
      {
         setPCode(TAIR_REQ_DUPLICATE_PACKET);
         server_flag = packet.server_flag;
         key.clone(packet.key);
         data.clone(packet.data);
         bucket_number = packet.bucket_number;
         packet_id = packet.packet_id;
         area = packet.area;
      }


      ~request_duplicate()
      {
      }


      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt8(server_flag);
         output->writeInt16(area);
         key.encode(output);
         data.encode(output);
         output->writeInt32(packet_id);
         output->writeInt32(bucket_number);

         return true;
      }


      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
      {
         if (header->_dataLen < 15) {
            log_warn("buffer data too few.");
            return false;
         }
         server_flag = input->readInt8();
         area = input->readInt16();
         key.decode(input);
         data.decode(input);
         packet_id = input->readInt32();
         bucket_number = input->readInt32();
         return true;
      }

   public:
      data_entry    key;
      data_entry    data;
      uint32_t      packet_id;
      int           bucket_number;
      int           area;
   };
   class response_duplicate : public base_packet {
   public:
      response_duplicate()
      {
         setPCode(TAIR_RESP_DUPLICATE_PACKET);
         server_flag = 1;
         bucket_id = -1;
      }

      ~response_duplicate()
      {
      }


      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(packet_id);
         output->writeInt64(server_id);
         output->writeInt32(bucket_id);
         return true;
      }


      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
      {
         packet_id = input->readInt32();
         server_id = input->readInt64();
         bucket_id = input->readInt32();
         return true;
      }

   public:
      uint32_t        packet_id;
      int32_t         bucket_id;
      uint64_t        server_id;
   private:
      response_duplicate(const response_duplicate&);
   };
}
#endif
