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

#ifndef PREFIX_INVALIDS_PACKET_HPP
#define PREFIX_INVALIDS_PACKET_HPP

#include "invalid_packet.hpp"

namespace tair {
class request_prefix_invalids : public request_invalid {
public:
    request_prefix_invalids() {
        setPCode(TAIR_REQ_PREFIX_INVALIDS_PACKET);
    }

    request_prefix_invalids(const request_prefix_invalids &packet)
            : request_invalid(packet) {
        setPCode(TAIR_REQ_PREFIX_INVALIDS_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }
};
}

#endif
