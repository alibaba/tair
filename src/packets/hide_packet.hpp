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

#ifndef TAIR_PACKET_HIDE_PACKET_HPP
#define TAIR_PACKET_HIDE_PACKET_HPP

#include "get_packet.hpp"

namespace tair {
class request_hide : public request_get {
public:
    request_hide() : request_get() {
        setPCode(TAIR_REQ_HIDE_PACKET);
    }

    request_hide(request_hide &packet) : request_get(packet) {
        setPCode(TAIR_REQ_HIDE_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }
};
}
#endif
