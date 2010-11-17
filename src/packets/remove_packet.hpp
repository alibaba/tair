/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * remove packet
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_REMOVE_PACKET_H
#define TAIR_PACKET_REMOVE_PACKET_H
#include "base_packet.hpp"
#include "get_packet.hpp"
namespace tair {
   class request_remove : public request_get {
   public:

      request_remove() : request_get()
      {
         setPCode(TAIR_REQ_REMOVE_PACKET);
      }


      request_remove(request_remove &packet) : request_get(packet)
      {
         setPCode(TAIR_REQ_REMOVE_PACKET);
      }


      bool encode(tbnet::DataBuffer *output)
      {
         return request_get::encode(output);
      }
   };
}
#endif
