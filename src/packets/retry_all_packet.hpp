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

#ifndef RETRY_ALL_PACKET_H
#define RETRY_ALL_PACKET_H

#include "base_packet.hpp"
#include "invalid_packet.hpp"

namespace tair {
class request_retry_all : public request_invalid {
public:
    request_retry_all() {
        setPCode(TAIR_REQ_RETRY_ALL_PACKET);
        is_sync = ASYNC_INVALID;
    }

    request_retry_all(const request_retry_all &retry_all)
            : request_invalid(retry_all) {
        setPCode(TAIR_REQ_RETRY_ALL_PACKET);
        is_sync = ASYNC_INVALID;
    }

    ~request_retry_all() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }
};
}
#endif
