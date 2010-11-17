/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for remove one particular area 
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_REMOVE_AREA_PACKET_H
#define TAIR_PACKET_REMOVE_AREA_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_remove_area : public base_packet {
   public:

      request_remove_area()
      {
         setPCode(TAIR_REQ_REMOVE_AREA_PACKET);
         area = 0;
      }


      request_remove_area(request_remove_area &packet)
      {
         setPCode(TAIR_REQ_REMOVE_AREA_PACKET);
         area = packet.area;
      }


      ~request_remove_area()
      {
      }


      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(area);
         return true;
      }


      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < (int)sizeof(int)) {
            log_warn( "buffer data too few.");
            return false;
         }
         area = input->readInt32();
         return true;
      }

   public:
      int                 area;
   };
}
#endif
