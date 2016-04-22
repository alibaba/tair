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

#ifndef TAIR_STAT_CMD_PACKET_H_
#define TAIR_STAT_CMD_PACKET_H_

#include "base_packet.hpp"
#include "stat_manager.h"

namespace tair {

struct stat_arg {
    uint32_t ip;
    uint16_t ns;
    uint16_t op;

    uint32_t in;
    uint32_t out;
    uint32_t cnt;
};

class stat_cmd_view_packet : public base_packet {
private:
    stat_arg arg;

public:
    stat_cmd_view_packet() {
        setPCode(TAIR_STAT_CMD_VIEW);
    }

    void set_arg(const stat_arg &arg) {
        this->arg = arg;
    }

    const stat_arg &get_arg() {
        return arg;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeBytes((char *) &arg, sizeof(arg));
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (size_t(header->_dataLen) < sizeof(arg)) {
            return false;
        }
        input->readBytes((char *) &arg, sizeof(arg));
        return true;
    }

    virtual size_t size() const {
        return sizeof(arg);
    }

    virtual uint16_t ns() const {
        return 0;
    }

private:
    stat_cmd_view_packet(const stat_cmd_view_packet &);
};

}
#endif
