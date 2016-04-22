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

#ifndef PREFIX_GET_HIDDES_PACKET_HPP
#define PREFIX_GET_HIDDES_PACKET_HPP

#include "get_packet.hpp"

namespace tair {
class request_prefix_get_hiddens : public request_get {
public:
    request_prefix_get_hiddens() {
        setPCode(TAIR_REQ_PREFIX_GET_HIDDENS_PACKET);
    }

    request_prefix_get_hiddens(request_prefix_get_hiddens &rhs) : request_get(rhs) {
        setPCode(TAIR_REQ_PREFIX_GET_HIDDENS_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_READ;
    }
};
}

#endif
