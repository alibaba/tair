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

#ifndef TAIR_PACKET_PING_PACKET_H
#define TAIR_PACKET_PING_PACKET_H

#include "base_packet.hpp"

namespace tair {
class request_ping : public base_packet {
public:

    request_ping() {
        setPCode(TAIR_REQ_PING_PACKET);
        config_version = 0;
        value = 0;
    }

    request_ping(const request_ping &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_PING_PACKET);
        config_version = packet.config_version;
        value = packet.value;
    }

    ~request_ping() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(config_version);
        output->writeInt32(value);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 8) {
            log_warn("buffer data too few.");
            return false;
        }
        config_version = input->readInt32();
        value = input->readInt32();
        return true;
    }

public:
    uint32_t config_version;
    int value;
};
}
#endif
