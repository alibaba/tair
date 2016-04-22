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

#ifndef TAIR_PACKET_INC_DEC_BOUNDED_PACKET_H
#define TAIR_PACKET_INC_DEC_BOUNDED_PACKET_H

#include "inc_dec_packet.hpp"
#include "base_packet.hpp"

namespace tair {
class request_inc_dec_bounded : public request_inc_dec {
public:
    request_inc_dec_bounded() : request_inc_dec() {
        setPCode(TAIR_REQ_INCDEC_BOUNDED_PACKET);
        low_bound = 0;
        upper_bound = 0;
    }

    request_inc_dec_bounded(request_inc_dec_bounded &packet) : request_inc_dec(packet) {
        setPCode(TAIR_REQ_INCDEC_BOUNDED_PACKET);
        low_bound = packet.low_bound;
        upper_bound = packet.upper_bound;
    }

    ~request_inc_dec_bounded() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    bool encode(DataBuffer *output) {
        if (request_inc_dec::encode(output) == false) {
            return false;
        }
        output->writeInt32(low_bound);
        output->writeInt32(upper_bound);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  [ request_inc_dec
        //  | int8   server_flag    1B      (must)
        //  | int16  area           2B      (must)
        //  | int32  add_count      4B      (must)
        //  | int32  init_value     4B      (must)
        //  | int32  expire         4B      (must)
        //  | -------------------------
        //  | total                15B
        //  |
        //  | dataentry  key         decode  (must)

        //  int32  low_bound      4B         (must)
        //  int32  high_bound     4B         (must)
        if (request_inc_dec::decode(input, header) == false) {
            return false;
        }

        if (input->readInt32((uint32_t *) &low_bound) == false ||
            input->readInt32((uint32_t *) &upper_bound) == false) {
            log_warn("key decode failed: "
                             "server_flag %x, area %d, add_count %d, init_value %d, expired %d",
                     server_flag, area, add_count, init_value, expired);
            return false;
        }

        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;
        return request_inc_dec::size() + 4 + 4;
    }

    inline int32_t get_low_bound() {
        return low_bound;
    }

    inline int32_t get_upper_bound() {
        return upper_bound;
    }

protected:
    //value belong to [low_bound, upper_bound]
    int32_t low_bound;
    int32_t upper_bound;
private:
    request_inc_dec_bounded(const request_inc_dec_bounded &);
};

class response_inc_dec_bounded : public response_inc_dec {
public:
    response_inc_dec_bounded() : response_inc_dec() {
        setPCode(TAIR_RESP_INCDEC_BOUNDED_PACKET);
    }

    response_inc_dec_bounded(response_inc_dec_bounded &packet) : response_inc_dec(packet) {
        setPCode(TAIR_RESP_INCDEC_BOUNDED_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

private:
    response_inc_dec_bounded(const response_inc_dec_bounded &);
};
}
#endif
