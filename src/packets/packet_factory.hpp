/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * packet factory creates packet according to packet code
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_FACTORY_H
#define TAIR_PACKET_FACTORY_H
#include "base_packet.hpp"
namespace tair {
   class tair_packet_factory : public tbnet::IPacketFactory {
   public:
       tair_packet_factory() {}
      ~tair_packet_factory();

      tbnet::Packet *createPacket(int pcode);

      static int set_return_packet(base_packet *packet, int code,const char *msg, uint32_t version);
      static int set_return_packet(tbnet::Connection *conn,uint32_t chid,int cmd_id,int code,const char *msg,uint32_t version);
   };
}
#endif
