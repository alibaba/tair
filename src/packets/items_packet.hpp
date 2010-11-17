/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for item operations
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_ITEMS_H
#define TAIR_PACKET_ITEMS_H
#include "base_packet.hpp"
#include "put_packet.hpp"
#include "get_packet.hpp"
namespace tair {
   class request_add_items : public request_put {
   public:
      request_add_items() : request_put()
      {
         setPCode(TAIR_REQ_ADDITEMS_PACKET);
      }
      request_add_items(request_add_items& packet) : request_put(packet)
      {
         setPCode(TAIR_REQ_ADDITEMS_PACKET);
         max_count = packet.max_count;
      }
      bool encode(tbnet::DataBuffer *output)
      {
         if( !request_put::encode(output)){
            return false;
         }
         output->writeInt32(static_cast<uint32_t>(max_count));
         return true;
      }
      bool decode(tbnet::DataBuffer *input,tbnet::PacketHeader *header)
      {
         if( !request_put::decode(input,header)){
            return false;
         }
         max_count = static_cast<int>( input->readInt32());
         return true;
      }

      int max_count;
   private:
      request_add_items& operator = (const request_add_items&) const;
   };

   class request_get_items : public request_get {
   public:
      request_get_items() : request_get(),count(0),offset(0)
      {
         setPCode(TAIR_REQ_GETITEMS_PACKET);
      }

      request_get_items(request_get_items& packet) : request_get(packet)
      {
         setPCode(TAIR_REQ_GETITEMS_PACKET);
         count = packet.count;
         offset = packet.offset;
         type = packet.type;
      }

      bool encode(tbnet::DataBuffer *output)
      {
         if( !request_get::encode(output)){
            return false;
         }
         output->writeInt32(static_cast<uint32_t>(count));
         output->writeInt32(static_cast<uint32_t>(offset));
         output->writeInt32(static_cast<uint32_t>(type));
         return true;
      }
      bool decode(tbnet::DataBuffer *input,tbnet::PacketHeader *header)
      {
         if( !request_get::decode(input,header)){
            return false;
         }
         count = static_cast<int>( input->readInt32());
         offset = static_cast<int>( input->readInt32());
         type = static_cast<int>( input->readInt32());
         return true;
      }

      int count;
      int offset;
      int type;
   };

   class response_get_items : public response_get {
   public:
      response_get_items() : response_get()
      {
         setPCode(TAIR_RESP_GETITEMS_PACKET);
      }
   };

   class request_remove_items : public request_get_items {
   public:
      request_remove_items() : request_get_items()
      {
         setPCode(TAIR_REQ_REMOVEITEMS_PACKET);
      }
      request_remove_items(request_remove_items& packet) : request_get_items(packet)
      {
         setPCode(TAIR_REQ_REMOVEITEMS_PACKET);
      }

   };

   class request_get_and_remove_items : public request_get_items {
   public:
      request_get_and_remove_items() : request_get_items()
      {
         setPCode(TAIR_REQ_GETANDREMOVEITEMS_PACKET);
      }

      request_get_and_remove_items(request_get_and_remove_items& packet) : request_get_items(packet)
      {
         setPCode(TAIR_REQ_GETANDREMOVEITEMS_PACKET);
      }

   };

   class request_get_items_count : public request_get{
   public:
      request_get_items_count() : request_get()
      {
         setPCode(TAIR_REQ_GETITEMSCOUNT_PACKET);
      }
      request_get_items_count(request_get_items_count& packet) : request_get(packet)
      {
         setPCode(TAIR_REQ_GETITEMSCOUNT_PACKET);
      }

   };

} /* tair */
#endif
