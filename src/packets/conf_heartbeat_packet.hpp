/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * heartbeat packet between configservers
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_CONF_HB_PACKET_H
#define TAIR_PACKET_CONF_HB_PACKET_H

#include "base_packet.hpp"
namespace tair {
   class request_conf_heartbeart : public base_packet {
   public:
      request_conf_heartbeart()
      {
         setPCode(TAIR_REQ_CONFHB_PACKET);
         server_id = 0U;
         loop_count = 0U;
      }

      request_conf_heartbeart(request_conf_heartbeart &packet)
      {
         setPCode(TAIR_REQ_CONFHB_PACKET);
         server_id = packet.server_id;
         loop_count = packet.loop_count;
      }

      ~request_conf_heartbeart() {
      }

      bool encode(tbnet::DataBuffer *output) {
         output->writeInt64(server_id);
         output->writeInt32(loop_count);

         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 12) {
            log_warn("buffer data too few");
            return false;
         }
         server_id = input->readInt64();
         loop_count = input->readInt32();

         return true;
      }
   public:
      uint64_t server_id;
      uint32_t loop_count;
   };
}
#endif
