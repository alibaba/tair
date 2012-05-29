/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * invalid packet
 *
 * Version: $Id: prefix_hides_packet.hpp 28 2011-09-05 05:18:09Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */

#ifndef PREFIX_HIDES_PACKET_HPP
#define PREFIX_HIDES_PACKET_HPP
#include "hide_packet.hpp"

namespace tair {
  class request_prefix_hides : public request_hide {
  public:
    request_prefix_hides() {
        setPCode(TAIR_REQ_PREFIX_HIDES_PACKET);
    }

    request_prefix_hides(request_prefix_hides &packet) : request_hide(packet) {
      setPCode(TAIR_REQ_PREFIX_HIDES_PACKET);
    }

    bool encode(tbnet::DataBuffer *output) {
      if (!request_hide::encode(output)) {
        return false;
      }
      if (server_flag != TAIR_SERVERFLAG_CLIENT) {
        output->writeInt32(packet_id);
      }
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
      if (!request_hide::decode(input, header)) {
        return false;
      }
      if (server_flag != TAIR_SERVERFLAG_CLIENT) {
        packet_id = input->readInt32();
      }
      return true;
    }

    void swap(request_prefix_hides &rhs) {
      request_hide::swap(rhs);
      std::swap(packet_id, rhs.packet_id);
    }
  public:
    uint32_t packet_id;
  };
}

#endif
