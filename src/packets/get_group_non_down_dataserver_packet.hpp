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

#ifndef TAIR_PACKET_GET_GROUP_NOT_DOWN_DATASERVER_PACKET_HPP_
#define TAIR_PACKET_GET_GROUP_NOT_DOWN_DATASERVER_PACKET_HPP_

#include "base_packet.hpp"

namespace tair {
class request_get_group_not_down_dataserver : public base_packet {
public:
    request_get_group_not_down_dataserver() {
        setPCode(TAIR_REQ_GET_GROUP_NON_DOWN_DATASERVER_PACKET);
        group_name[0] = '\0';
        config_version = 0;
    }

    ~request_get_group_not_down_dataserver() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(config_version);
        output->writeString(group_name);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 8) {
            log_warn("buffer data too few.");
            return false;
        }
        config_version = input->readInt32();
        char *p = &group_name[0];
        input->readString(p, 64);
        return true;
    }

    // set_group_name
    void set_group_name(const char *group_name_value) {
        group_name[0] = '\0';
        if (group_name_value != NULL) {
            strncpy(group_name, group_name_value, 64);
            group_name[63] = '\0';
        }
    }

public:
    char group_name[64];
    uint32_t config_version;
};

class response_get_group_non_down_dataserver : public base_packet {
public:
    response_get_group_non_down_dataserver() {
        config_version = 0;
        setPCode(TAIR_RESP_GET_GROUP_NON_DOWN_DATASERVER_PACKET);
    }

    ~response_get_group_non_down_dataserver() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(config_version);
        // avilable server
        output->writeInt32(non_down_server_ids.size());

        for (set<uint64_t>::iterator it_as = non_down_server_ids.begin(); it_as != non_down_server_ids.end(); ++it_as)
            output->writeInt64(*it_as);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 12) {
            log_warn("buffer data too few.");
            return false;
        }
        config_version = input->readInt32();
        //avaiable server
        non_down_server_ids.clear();
        int32_t size = input->readInt32();
        for (int32_t i = 0; i < size; ++i)
            non_down_server_ids.insert(input->readInt64());
        return true;
    }

public:
    uint32_t config_version;
    set<uint64_t> non_down_server_ids;
private:
    response_get_group_non_down_dataserver(const response_get_group_non_down_dataserver &);
};
}
#endif
