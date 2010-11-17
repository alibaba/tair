/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for counter operations
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_INC_DEC_PACKET_H
#define TAIR_PACKET_INC_DEC_PACKET_H
#include "base_packet.hpp"
#include "ping_packet.hpp"
#include "data_entry.hpp"
namespace tair {
   class request_inc_dec : public base_packet {
   public:
      request_inc_dec()
      {
         setPCode(TAIR_REQ_INCDEC_PACKET);
         server_flag = 0;
         area = 0;
         add_count = 0;
         init_value = 0;
         expired = 0;
      }

      request_inc_dec(request_inc_dec &packet)
      {
         setPCode(TAIR_REQ_INCDEC_PACKET);
         server_flag = packet.server_flag;
         area = packet.area;
         add_count = packet.add_count;
         init_value = packet.init_value;
         expired = packet.expired;
         key.clone(packet.key);
      }

      ~request_inc_dec()
      {
      }


      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt8(server_flag);
         output->writeInt16(area);
         output->writeInt32(add_count);
         output->writeInt32(init_value);
         output->writeInt32(expired);
         key.encode(output);

         return true;
      }


      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) 
      {
         if (header->_dataLen < 13) {
            log_warn( "buffer data too few.");
            return false;
         }
         server_flag = input->readInt8();
         area = input->readInt16();
         add_count = input->readInt32();
         init_value = input->readInt32();
         expired = input->readInt32();

         key.decode(input);
         return true;
      }

   public:
      uint16_t        area;
      int32_t         add_count;
      int32_t         init_value;
      int32_t         expired;
      data_entry    key;

   };

   class response_inc_dec : public request_ping {
   public:

      response_inc_dec() : request_ping()
     {
         setPCode(TAIR_RESP_INCDEC_PACKET);
      }


      response_inc_dec(response_inc_dec &packet) : request_ping(packet)
      {
         setPCode(TAIR_RESP_INCDEC_PACKET);
      }
   private:
      response_inc_dec(const response_inc_dec&);
   };

}
#endif
