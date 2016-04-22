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

#ifndef TAIR_PACKET_MIG_FINISH_PACKET_H
#define TAIR_PACKET_MIG_FINISH_PACKET_H

#include "base_packet.hpp"

namespace tair {
class request_migrate_finish : public base_packet {
public:

    request_migrate_finish() {
        setPCode(TAIR_REQ_MIG_FINISH_PACKET);
        server_id = 0;
        bucket_no = 0;
        version = 0;
    }

    request_migrate_finish(request_migrate_finish &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_MIG_FINISH_PACKET);
        version = packet.version;
        server_id = packet.server_id;
        bucket_no = packet.bucket_no;
    }

    ~request_migrate_finish() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(version);
        output->writeInt64(server_id);
        output->writeInt32(bucket_no);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 8) {
            log_warn("buffer data too few.");
            return false;
        }
        version = input->readInt32();
        server_id = input->readInt64();
        bucket_no = input->readInt32();
        return true;
    }

public:

    uint32_t version;
    uint64_t server_id;
    int bucket_no;
private:
};
}
#endif
