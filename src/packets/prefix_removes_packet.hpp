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

#ifndef TAIR_PACKET_PREFIX_REMOVES_PACKET_HPP
#define TAIR_PACKET_PREFIX_REMOVES_PACKET_HPP

#include "base_packet.hpp"
#include "remove_packet.hpp"

namespace tair {
class request_prefix_removes : public request_remove {
public:
    request_prefix_removes() {
        setPCode(TAIR_REQ_PREFIX_REMOVES_PACKET);
        packet_id = 0;
    }

    request_prefix_removes(const request_prefix_removes &rhs)
            : request_remove(rhs) {
        setPCode(TAIR_REQ_PREFIX_REMOVES_PACKET);
        packet_id = rhs.packet_id;
    }

    request_prefix_removes(const tair_dataentry_set &mkey_set, const int area) {
        setPCode(TAIR_REQ_PREFIX_REMOVES_PACKET);
        tair_dataentry_set::const_iterator itr = mkey_set.begin();
        while (itr != mkey_set.end()) {
            this->add_key(*itr, true);
            ++itr;
        }
        this->area = area;
        packet_id = 0;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    bool encode(DataBuffer *output) {
        if (!request_remove::encode(output)) {
            return false;
        }
        if (server_flag != TAIR_SERVERFLAG_CLIENT) {
            output->writeInt32(packet_id);
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  | reuqest_remove:
        //  |
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

        //  int32   packet_id        4B    (depend on `server_flag != CLIENT`)
        if (!request_remove::decode(input, header)) {
            return false;
        }

        if (server_flag != TAIR_SERVERFLAG_CLIENT) {
            if (input->readInt32(&packet_id) == false) {
                log_warn("packet_id decode failed: "
                                 "server_flag %x, area %d, key_count %d",
                         server_flag, area, key_count);
                return false;
            }
        }
        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = request_remove::size();
        if (server_flag != TAIR_SERVERFLAG_CLIENT)
            total += 4;
        return total; // header 16  bytes is already added in request_remove::size()
    }

    void swap(request_prefix_removes &rhs) {
        request_remove::swap(rhs);
        std::swap(packet_id, rhs.packet_id);
    }

public:
    uint32_t packet_id;
};
}

#endif
