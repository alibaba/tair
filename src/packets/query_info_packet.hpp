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

#ifndef TAIR_PACKET_QUERY_INFO_PACKET_H
#define TAIR_PACKET_QUERY_INFO_PACKET_H

#include "base_packet.hpp"

namespace tair {
class request_query_info : public base_packet {
public:
    enum {
        Q_AREA_CAPACITY = 1,
        Q_MIG_INFO,
        Q_DATA_SERVER_INFO,
        Q_GROUP_INFO,
        Q_STAT_INFO,
    };

    request_query_info() {
        setPCode(TAIR_REQ_QUERY_INFO_PACKET);
        query_type = 0;
        server_id = 0;
    }

    ~request_query_info() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(query_type);
        output->writeString(group_name.c_str());
        output->writeInt64(server_id);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 12) {
            log_warn("buffer data too few.");
            return false;
        }
        query_type = input->readInt32();
        char *str = NULL;
        input->readString(str, 0);
        if (str) {
            group_name = str;
            ::free(str);
        }
        server_id = input->readInt64();
        return true;
    }

public:
    uint32_t query_type;
    string group_name;
    uint64_t server_id;
private:
    request_query_info(const request_query_info &);
};

class response_query_info : public base_packet {
public:
    response_query_info() {
        setPCode(TAIR_RESP_QUERY_INFO_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(map_k_v.size());
        for (map<string, string>::iterator it = map_k_v.begin(); it != map_k_v.end(); it++) {
            output->writeString(it->first);
            output->writeString(it->second);
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 4) {
            log_warn("buffer data too few.");
            return false;
        }
        uint32_t size = input->readInt32();
        for (uint32_t i = 0; i < size; i++) {
            char *key = NULL;
            char *value = NULL;
            string s_key;
            string s_value;
            input->readString(key, 0);
            input->readString(value, 0);
            if (key) {
                s_key = key;
                ::free(key);
            }
            if (value) {
                s_value = value;
                ::free(value);
            }
            map_k_v[s_key] = s_value;
        }
        return true;
    }

public:
    map<string, string> map_k_v;
private:
    response_query_info(const response_query_info &);
};
}
#endif
