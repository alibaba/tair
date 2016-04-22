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

#ifndef TAIR_PACKET_REMOVE_AREA_PACKET_H
#define TAIR_PACKET_REMOVE_AREA_PACKET_H

#include "base_packet.hpp"

namespace tair {
class request_remove_area : public base_packet {
public:

    request_remove_area() {
        setPCode(TAIR_REQ_REMOVE_AREA_PACKET);
        area = 0;
    }

    request_remove_area(const request_remove_area &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_REMOVE_AREA_PACKET);
        area = packet.area;
    }

    ~request_remove_area() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(area);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < (int) sizeof(int)) {
            log_warn("buffer data too few.");
            return false;
        }
        area = input->readInt32();
        return true;
    }

    virtual size_t size() const {
        return 4 + 16;// header 16 bytes
    }

    uint16_t ns() const {
        return (uint16_t) area;
    }

public:
    int area;
};
}
#endif
