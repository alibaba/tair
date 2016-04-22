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

#ifndef TAIR_PACKET_DUMP_BUCKET_PACKET_H
#define TAIR_PACKET_DUMP_BUCKET_PACKET_H

#include "base_packet.hpp"

namespace tair {
class request_dump_bucket : public base_packet {
public:
    request_dump_bucket() {
        setPCode(TAIR_REQ_DUMP_BUCKET_PACKET);
        dbid = 0;
        path[0] = '\0';
    }

    request_dump_bucket(const request_dump_bucket &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_DUMP_BUCKET_PACKET);
        dbid = packet.dbid;
        set_path(packet.path);
    }

    ~request_dump_bucket() {
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(dbid);
        output->writeString(path);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 8) {
            log_warn("buffer data too few.");
            return false;
        }
        dbid = input->readInt32();
        char *p = &path[0];
        input->readString(p, TAIR_MAX_FILENAME_LEN - 1);
        return true;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_READ;
    }

    void set_path(const char *path_str) {
        path[0] = '\0';
        if (path_str != NULL) {
            strcpy(path, path_str);
            path[TAIR_MAX_FILENAME_LEN - 1] = '\0';
        }
    }

public:
    int dbid;
    char path[TAIR_MAX_FILENAME_LEN];
};
}
#endif
