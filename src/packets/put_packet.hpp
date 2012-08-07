/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * put packet
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_PUT_PACKET_H
#define TAIR_PACKET_PUT_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_put : public base_packet {
   public:
      request_put()
      {
         setPCode(TAIR_REQ_PUT_PACKET);
         server_flag = 0;
         area = 0;
         version = 0;
         expired = 0;
      }

      request_put(request_put &packet) 
      {
         setPCode(TAIR_REQ_PUT_PACKET);
         server_flag = packet.server_flag;
         area = packet.area;
         version = packet.version;
         expired = packet.expired;
         key.clone(packet.key);
         data.clone(packet.data);
      }

      size_t size() 
      {
        return 1 + 2 + 2 + 4 + key.encoded_size() + data.encoded_size() + 16;
      }

      virtual uint16_t ns() 
      {
        return area;
      }

      ~request_put()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt8(server_flag);
         output->writeInt16(area);
         output->writeInt16(version);
         output->writeInt32(expired);
         key.encode(output);
         data.encode_with_compress(output);
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
         version = input->readInt16();
         expired = input->readInt32();

         key.decode(input);
         data.decode(input);
         key.data_meta.version = version;

         return true;
      }

   public:
      uint16_t        area;
      uint16_t        version;
      int32_t         expired;
      data_entry    key;
      data_entry    data;
   };
}
#endif
