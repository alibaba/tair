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

#ifndef TAIR_PACKET_REMOVE_PACKET_H
#define TAIR_PACKET_REMOVE_PACKET_H

#include "base_packet.hpp"
#include "get_packet.hpp"

namespace tair {
class request_remove : public request_get {
public:

    request_remove() : request_get() {
        setPCode(TAIR_REQ_REMOVE_PACKET);
    }

    request_remove(const request_remove &packet)
            : request_get(packet) {
        setPCode(TAIR_REQ_REMOVE_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    bool encode(DataBuffer *output) {
        return request_get::encode(output);
    }
};
}
#endif
