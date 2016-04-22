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

#ifndef TAIR_PACKET_EXPIRE_PACKET_H
#define TAIR_PACKET_EXPIRE_PACKET_H

#include "base_packet.hpp"

namespace tair {
class request_expire : public base_packet {
public:
    request_expire() {
        setPCode(TAIR_REQ_EXPIRE_PACKET);
        server_flag = 0;
        area = 0;
        expired = 0;
    }

    request_expire(request_expire &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_EXPIRE_PACKET);
        server_flag = packet.server_flag;
        area = packet.area;
        expired = packet.expired;
        key.clone(packet.key);
    }

    ~request_expire() {
    }

    virtual size_t size() const {
        return 1 + 2 + 4 + key.encoded_size() + 16; //simple, so no need to read getDataLen()
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    uint16_t ns() const {
        return area;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        output->writeInt32(expired);
        key.encode(output);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  int8   server_flag   1B     (must)
        //  int16  area          2B     (must)
        //  int32  expire        4B     (must)
        //  ------------------------
        //  total                7B
        if (input->readInt8((uint8_t *) &server_flag) == false ||
            input->readInt16((uint16_t *) &area) == false ||
            input->readInt32((uint32_t *) &expired) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
        }


#if TAIR_MAX_AREA_COUNT < 65536
        if (area >= TAIR_MAX_AREA_COUNT) {
            log_warn("area overflow: "
                             "server_flag %x, area %d, expired %d",
                     server_flag, area, expired);
            return false;
        }
#endif

        if (!key.decode(input)) {
            log_warn("key decode failed: "
                             "server_flag %x, area %d, expired %d",
                     server_flag, area, expired);
            return false;
        }

        return true;
    }

public:
    uint16_t area;
    int32_t expired;
    data_entry key;
};
}
#endif
