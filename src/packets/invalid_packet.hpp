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

#ifndef TAIR_PACKET_INVAL_PACKET_HPP
#define TAIR_PACKET_INVAL_PACKET_HPP

#include "get_packet.hpp"

namespace tair {
class request_invalid : public request_get {
public:
    request_invalid() : request_get() {
        group_name[0] = '\0';
        setPCode(TAIR_REQ_INVAL_PACKET);
        is_sync = SYNC_INVALID;
    }

    request_invalid(const request_invalid &packet)
            : request_get(packet) {
        set_group_name(packet.group_name);
        setPCode(TAIR_REQ_INVAL_PACKET);
        is_sync = SYNC_INVALID;
    }

    void set_group_name(const char *groupname) {
        if (groupname != NULL) {
            strncpy(group_name, groupname, MAXLEN_GROUP_NAME);
        }
    }

    inline const char *get_group_name() {
        return group_name;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    bool encode(DataBuffer *output) {
        if (request_get::encode(output) == false) {
            return false;
        }
        output->writeString(group_name);
        output->writeInt32(is_sync);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  | request_get:
        //  | int8   server_flag   1B     (must)
        //  | int16  area          2B     (must)
        //  | int32  key_count     4B     (must)
        //  | ------------------------
        //  | total                7B
        //  |
        //  | list:                           (depend on `key_count`)
        //  | dataentry  key1   decode
        //  | dataentry  key2   decode
        //  | ...        ...    ...
        //  | dataentry  keyN   decode

        //  string   group_name {int32 + len} (must)
        //  int32    is_sync       4B         (must)
        if (request_get::decode(input, header) == false) {
            return false;
        }

        char *p = group_name;
        if (input->readString(p, MAXLEN_GROUP_NAME) == false) {
            log_warn("group name failed: "
                             "server_flag %x, area %d, key_count %d",
                     server_flag, area, key_count);
            return false;
        }
        group_name[MAXLEN_GROUP_NAME - 1] = '\0';

        if (input->readInt32(&is_sync) == false) {
            log_warn("group name failed: "
                             "server_flag %x, area %d, key_count %d, group_name %s",
                     server_flag, area, key_count, group_name);
            return false;
        }

        return true;
    }

    void set_sync(uint32_t is_sync) {
        this->is_sync = is_sync;
    }

public:
    enum {
        MAXLEN_GROUP_NAME = 64
    };
    char group_name[MAXLEN_GROUP_NAME];
    uint32_t is_sync;
};
}
#endif
