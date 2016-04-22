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

#ifndef PREFIX_HIDES_PACKET_HPP
#define PREFIX_HIDES_PACKET_HPP

#include "hide_packet.hpp"

namespace tair {
class request_prefix_hides : public request_hide {
public:
    request_prefix_hides() {
        setPCode(TAIR_REQ_PREFIX_HIDES_PACKET);
    }

    request_prefix_hides(request_prefix_hides &packet) : request_hide(packet) {
        setPCode(TAIR_REQ_PREFIX_HIDES_PACKET);
    }

    request_prefix_hides(const tair_dataentry_set &mkey_set, const int area) {
        setPCode(TAIR_REQ_PREFIX_HIDES_PACKET);
        tair_dataentry_set::const_iterator itr = mkey_set.begin();
        while (itr != mkey_set.end()) {
            this->add_key(*itr, true);
            ++itr;
        }
        this->area = area;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    bool encode(DataBuffer *output) {
        if (!request_hide::encode(output)) {
            return false;
        }
        if (server_flag != TAIR_SERVERFLAG_CLIENT) {
            output->writeInt32(packet_id);
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (!request_hide::decode(input, header)) {
            return false;
        }
        if (server_flag != TAIR_SERVERFLAG_CLIENT) {
            packet_id = input->readInt32();
        }
        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = request_hide::size();
        if (server_flag != TAIR_SERVERFLAG_CLIENT)
            total += 4;
        return total; //header 16 bytes is added in request_hide::size()
    }

    void swap(request_prefix_hides &rhs) {
        request_hide::swap(rhs);
        std::swap(packet_id, rhs.packet_id);
    }

public:
    uint32_t packet_id;
};
}

#endif
