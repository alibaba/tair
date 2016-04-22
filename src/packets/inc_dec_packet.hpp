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

#ifndef TAIR_PACKET_INC_DEC_PACKET_H
#define TAIR_PACKET_INC_DEC_PACKET_H

#include "base_packet.hpp"
#include "ping_packet.hpp"
#include "data_entry.hpp"

namespace tair {
class request_inc_dec : public base_packet {
public:
    request_inc_dec() {
        setPCode(TAIR_REQ_INCDEC_PACKET);
        server_flag = 0;
        area = 0;
        add_count = 0;
        init_value = 0;
        expired = 0;
    }

    request_inc_dec(request_inc_dec &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_INCDEC_PACKET);
        server_flag = packet.server_flag;
        area = packet.area;
        add_count = packet.add_count;
        init_value = packet.init_value;
        expired = packet.expired;
        key.clone(packet.key);
    }

    ~request_inc_dec() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        output->writeInt32(add_count);
        output->writeInt32(init_value);
        output->writeInt32(expired);
        key.encode(output);

        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  int8   server_flag    1B        (must)
        //  int16  area           2B        (must)
        //  int32  add_count      4B        (must)
        //  int32  init_value     4B        (must)
        //  int32  expire         4B        (must)
        //  -------------------------
        //  total                15B
        if (input->readInt8(&server_flag) == false ||
            input->readInt16(&area) == false ||
            input->readInt32((uint32_t *) &add_count) == false ||
            input->readInt32((uint32_t *) &init_value) == false ||
            input->readInt32((uint32_t *) &expired) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
        }

#if TAIR_MAX_AREA_COUNT < 65536
        if (area >= TAIR_MAX_AREA_COUNT) {
            log_warn("area overflow: "
                             "server_flag %x, area %d, add_count %d, init_value %d, expired %d",
                     server_flag, area, add_count, init_value, expired);
            return false;
        }
#endif

        if (!key.decode(input)) {
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

        size_t total = 1 + 2 + 4 + 4 + 4;
        total += key.encoded_size();
        return total + 16; // header 16 bytes
    }

    uint16_t ns() const {
        return area;
    }

public:
    uint16_t area;
    int32_t add_count;
    int32_t init_value;
    int32_t expired;
    data_entry key;
    int32_t result_value;
};

class response_inc_dec : public request_ping /* what the hell */ {
public:

    response_inc_dec() : request_ping() {
        setPCode(TAIR_RESP_INCDEC_PACKET);
    }


    response_inc_dec(response_inc_dec &packet) : request_ping(packet) {
        setPCode(TAIR_RESP_INCDEC_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    virtual size_t size() const {
        return 4 + 4 + 16; // header 16 bytes
    }

    virtual void set_code(int code) {
    }

private:
    response_inc_dec(const response_inc_dec &);
};

}
#endif
