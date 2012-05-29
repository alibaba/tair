/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: prefix_removes_packet.hpp 28 2010-09-19 05:18:09Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu.hfl <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_PREFIX_REMOVES_PACKET_HPP
#define TAIR_PACKET_PREFIX_REMOVES_PACKET_HPP

#include "base_packet.hpp"
#include "remove_packet.hpp"
namespace tair {
  class request_prefix_removes : public request_remove {
  public:
    request_prefix_removes() {
      setPCode(TAIR_REQ_PREFIX_REMOVES_PACKET);
      packet_id = 0;
    }

    request_prefix_removes(request_prefix_removes &rhs) : request_remove(rhs) {
        setPCode(TAIR_REQ_PREFIX_REMOVES_PACKET);
        packet_id = rhs.packet_id;
    }

    bool encode(tbnet::DataBuffer *output) {
      if (!request_remove::encode(output)) {
        return false;
      }
      if (server_flag != TAIR_SERVERFLAG_CLIENT) {
        output->writeInt32(packet_id);
      }
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
      if (!request_remove::decode(input, header)) {
        return false;
      }
      if (server_flag != TAIR_SERVERFLAG_CLIENT) {
        packet_id = input->readInt32();
      }
      return true;
    }

    void swap(request_prefix_removes &rhs) {
      request_remove::swap(rhs);
      std::swap(packet_id, rhs.packet_id);
    }
  public:
    uint32_t packet_id;
  };
}

#endif
