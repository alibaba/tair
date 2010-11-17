/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * data server control packet
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_DATASERVER_CTRL_H
#define TAIR_PACKET_DATASERVER_CTRL_H
#include "base_packet.hpp"
namespace tair {
   class request_data_server_ctrl :public base_packet {
   public:
      enum {
         CTRL_UP = 1,
         CTRL_DOWN
      };
      request_data_server_ctrl()
      {
         setPCode(TAIR_REQ_DATASERVER_CTRL_PACKET);
         server_id = cmd = 0;
      }

      request_data_server_ctrl(const request_data_server_ctrl &packet) 
      {
         setPCode(TAIR_REQ_DATASERVER_CTRL_PACKET);
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt64(server_id);
         output->writeInt16(cmd);
         return true;
      }
      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 10) {
            log_warn("buffer data too few.");
            return false;
         }
         server_id = input->readInt64();
         cmd = input->readInt16();
         return true;
      }
   public:
      uint64_t server_id;
      uint16_t cmd;

   };
}
#endif
