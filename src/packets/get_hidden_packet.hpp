/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#ifndef TAIR_PACKET_FORCE_GET_PACKET_HPP
#define TAIR_PACKET_FORCE_GET_PACKET_HPP

#include "get_packet.hpp"

namespace tair {
class request_get_hidden : public request_get {
public:
    request_get_hidden() : request_get() {
        setPCode(TAIR_REQ_GET_HIDDEN_PACKET);
    }

    request_get_hidden(request_get_hidden &packet) : request_get(packet) {
        setPCode(TAIR_REQ_GET_HIDDEN_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_READ;
    }

    bool encode(DataBuffer *output) {
        if (request_get::encode(output) == false) {
            return false;
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (request_get::decode(input, header) == false) {
            return false;
        }
        return true;
    }
};
}
#endif
