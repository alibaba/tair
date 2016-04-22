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

#ifndef INVAL_STAT_PACKET_H
#define INVAL_STAT_PACKET_H

#include "invalid_packet.hpp"

namespace tair {
class request_inval_stat : public request_invalid {
public:
    request_inval_stat() : request_invalid() {
        setPCode(TAIR_REQ_INVAL_STAT_PACKET);
    }

    request_inval_stat(request_inval_stat &inval_stat) : request_invalid(inval_stat) {
        setPCode(TAIR_REQ_INVAL_STAT_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_READ;
    }
};

class response_inval_stat : public base_packet {
public:
    response_inval_stat() : base_packet() {
        config_version = 0;
        key = NULL;
        stat_value = NULL;
        setPCode(TAIR_RESP_INVAL_STAT_PACKET);
        uncompressed_data_size = 0;
        group_count = 0;
        code = 0;
    }

    ~response_inval_stat() {
        if (key != NULL) {
            delete key;
            key = NULL;
        }
        if (stat_value != NULL) {
            delete stat_value;
            stat_value = NULL;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(config_version);
        output->writeInt32(code);
        output->writeInt64(uncompressed_data_size);
        output->writeInt32(group_count);
        if (key != NULL && stat_value != NULL) {
            key->encode(output);
            stat_value->encode(output);
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 8) {
            log_warn("data is too few");
            return false;
        }
        config_version = input->readInt32();
        code = input->readInt32();
        uncompressed_data_size = input->readInt64();
        group_count = input->readInt32();
        key = new data_entry();
        if (key == NULL) {
            log_error("[FATAL ERROR] exception from the data_entry()]");
            return false;
        }
        stat_value = new data_entry();
        if (stat_value == NULL) {
            log_error("[FATAL ERROR] exception from the data_entry()");
            return false;
        }
        key->decode(input);
        stat_value->decode_with_decompress(input);
        return true;
    }

    inline int get_code() { return code; }

    inline void set_code(int _code) { code = _code; }

public:
    data_entry *key;
    data_entry *stat_value;
    uint32_t config_version;
    unsigned long uncompressed_data_size;
    uint32_t group_count;
private:
    int code;

    response_inval_stat(response_inval_stat &);
};
}
#endif
