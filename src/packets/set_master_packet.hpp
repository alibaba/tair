/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for set one configserver as the master
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_SET_MASTER_PACKET_H
#define TAIR_PACKET_SET_MASTER_PACKET_H
#include "base_packet.hpp"
#include "ping_packet.hpp"
namespace tair {
   class request_set_master : public request_ping {
   public:
      request_set_master() : request_ping()
      {
         setPCode(TAIR_REQ_SETMASTER_PACKET);
      }

   };
}
#endif
