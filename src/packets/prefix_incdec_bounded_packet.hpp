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

#ifndef TAIR_PREFIX_INCDEC_BOUNDED_PACKET_H
#define TAIR_PREFIX_INCDEC_BOUNDED_PACKET_H

#include "base_packet.hpp"
#include "data_entry.hpp"
#include "counter_wrapper.hpp"

namespace tair {
class request_prefix_incdec_bounded : public request_prefix_incdec {
public:
    request_prefix_incdec_bounded() : request_prefix_incdec() {
        setPCode(TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET);
        low_bound = 0;
        upper_bound = 0;
    }

    request_prefix_incdec_bounded(const request_prefix_incdec_bounded &rhs) : request_prefix_incdec(rhs) {
        setPCode(TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET);
        low_bound = rhs.low_bound;
        upper_bound = rhs.upper_bound;
    }

    request_prefix_incdec_bounded &operator=(const request_prefix_incdec_bounded &rhs) {
        if (this != &rhs) {
            request_prefix_incdec::operator=(rhs);
            low_bound = rhs.low_bound;
            upper_bound = rhs.upper_bound;
        }
        return *this;
    }

    ~request_prefix_incdec_bounded() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    bool encode(DataBuffer *output) {
        if (request_prefix_incdec::encode(output) == false) {
            return false;
        }
        output->writeInt32(low_bound);
        output->writeInt32(upper_bound);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  | requestprefix_incdecr:
        //  | int8   server_flag     1B    (must)
        //  | int16  area            2B    (must)
        //  | --------------------------
        //  | total                  3B
        //  |
        //  | dataentry   pkey     decode  (must)
        //  | int32  key_count       4B    (must)
        //  |
        //  | list:                            (depend on `key_count`)
        //  |   dataentry         skey     decode
        //  |   counter_wrapper   wrapper  decode
        //  |
        //  | int32     packet_id    4B    (depend on `server_flag != CLIENT`)

        //  int32   low_bound        4B    (must)
        //  int32   high_bound       4B    (must)

        if (request_prefix_incdec::decode(input, header) == false) {
            return false;
        }

        if (input->readInt32((uint32_t *) &low_bound) == false ||
            input->readInt32((uint32_t *) &upper_bound) == false) {
            log_warn("low/upper bound failed: "
                             "server_flag %x, area %d, key_count %d",
                     server_flag, area, key_count);
            return false;
        }

        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;
        return request_prefix_incdec::size() + 4 + 4;
    }

    void swap(request_prefix_incdec_bounded &rhs) {
        request_prefix_incdec::swap(rhs);
        std::swap(low_bound, rhs.low_bound);
        std::swap(upper_bound, rhs.upper_bound);
    }

    inline int32_t get_low_bound() {
        return low_bound;
    }

    inline int32_t get_upper_bound() {
        return upper_bound;
    }

protected:
    int32_t low_bound;
    int32_t upper_bound;
};

class response_prefix_incdec_bounded : public response_prefix_incdec {
public:
    response_prefix_incdec_bounded() : response_prefix_incdec() {
        setPCode(TAIR_RESP_PREFIX_INCDEC_BOUNDED_PACKET);
    }

    response_prefix_incdec_bounded(const response_prefix_incdec_bounded &rhs)
            : response_prefix_incdec(rhs) {
        setPCode(TAIR_RESP_PREFIX_INCDEC_BOUNDED_PACKET);
    }

    response_prefix_incdec_bounded &operator=(const response_prefix_incdec_bounded &rhs) {
        if (this != &rhs) {
            response_prefix_incdec::operator=(rhs);
        }
        return *this;
    }

    ~response_prefix_incdec_bounded() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }
};
}

#endif
