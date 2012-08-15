/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * expire packet
 *
 * Version: $Id: put_packet.hpp 1041 2012-08-07 06:27:42Z mobing $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_EXPIRE_PACKET_H
#define TAIR_PACKET_EXPIRE_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_expire : public base_packet {
   public:
      request_expire()
      {
         setPCode(TAIR_REQ_EXPIRE_PACKET);
         server_flag = 0;
         area = 0;
         expired = 0;
      }

      request_expire(request_expire &packet)
      {
         setPCode(TAIR_REQ_EXPIRE_PACKET);
         server_flag = packet.server_flag;
         area = packet.area;
         expired = packet.expired;
         key.clone(packet.key);
      }

      size_t size()
      {
        return 1 + 2 + 4 + key.encoded_size() + 16;
      }

      virtual uint16_t ns()
      {
        return area;
      }

      ~request_expire()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt8(server_flag);
         output->writeInt16(area);
         output->writeInt32(expired);
         key.encode(output);
         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 15) {
            log_warn( "buffer data too few.");
            return false;
         }
         server_flag = input->readInt8();
         area = input->readInt16();
         expired = input->readInt32();

         key.decode(input);

         return true;
      }

   public:
      uint16_t        area;
      int32_t         expired;
      data_entry      key;
   };
}
#endif
