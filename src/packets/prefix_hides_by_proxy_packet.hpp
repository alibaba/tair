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

#ifndef PREFIX_HIDES_BY_PROXY_PACKET_HPP
#define PREFIX_HIDES_BY_PROXY_PACKET_HPP

#include "hide_by_proxy_packet.hpp"

namespace tair {
class request_prefix_hides_by_proxy : public request_hide_by_proxy {
public:
    request_prefix_hides_by_proxy() {
        setPCode(TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET);
    }

    request_prefix_hides_by_proxy(request_prefix_hides_by_proxy &packet) : request_hide_by_proxy(packet) {
        setPCode(TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }
};
}

#endif
