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

#ifndef TAIR_PACKET_STATISTICS_PACKET_H
#define TAIR_PACKET_STATISTICS_PACKET_H

#include "base_packet.hpp"
#include "snappy.h"

namespace tair {

class request_statistics : public base_packet {
public:
    request_statistics() : flag_(0) {
        setPCode(TAIR_REQ_STATISTICS_PACKET);
        set_with_schema(false);
    }

    ~request_statistics() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(flag_);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        flag_ = input->readInt8();
        return true;
    }

    void set_with_schema(bool with_header) {
        if (with_header)
            flag_ |= 1;
        else
            flag_ &= ~1;
    }

    bool get_with_schema() {
        return (flag_ & 1) == 1;
    }

private:
    uint8_t flag_;
};

class response_statistics : public base_packet {
public:
    response_statistics() : config_version_(0), data_len_(0),
                            data_(NULL), schema_len_(0), schema_(NULL), schema_version_(0) {
        setPCode(TAIR_RESP_STATISTICS_PACKET);
    }

    ~response_statistics() {
        if (LIKELY(data_ != NULL)) {
            delete[] data_;
            data_ = NULL;
        }
        if (UNLIKELY(schema_ != NULL)) {
            delete[] schema_;
            schema_ = NULL;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(config_version_);
        output->writeInt32(data_len_);
        output->writeInt32(schema_len_);
        output->writeInt32(schema_version_);
        if (LIKELY(data_len_ != 0))
            output->writeBytes(data_, data_len_);
        if (UNLIKELY(schema_len_ != 0))
            output->writeBytes(schema_, schema_len_);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        config_version_ = input->readInt32();
        data_len_ = input->readInt32();
        schema_len_ = input->readInt32();
        schema_version_ = input->readInt32();
        if (LIKELY(data_len_ != 0)) {
            data_ = new char[data_len_];
            input->readBytes(data_, data_len_);
        }
        if (UNLIKELY(schema_len_ != 0)) {
            schema_ = new char[schema_len_];
            input->readBytes(schema_, schema_len_);
        }
        return true;
    }

    bool get_schema(char *&schema_str, int32_t &schema_str_len) const {
        if (UNLIKELY(schema_len_ != 0)) {
            schema_str_len = schema_len_;
            schema_str = new char[schema_str_len];
            memcpy(schema_str, schema_, schema_str_len);
            return true;
        }
        return false;
    }

    bool get_data(char *&data, int32_t &data_len) const {
        char *tmp_data;
        size_t tmp_data_len;
        if (0 == data_len_) {
            log_warn("there is no stat data in dataserver");
            return false;
        }

        if (!snappy::GetUncompressedLength(data_, data_len_, &tmp_data_len)) {
            log_error("snappy decompress err while GetUncompressedLength");
            return false;
        }
        tmp_data = new char[tmp_data_len];
        if (!snappy::RawUncompress(data_, data_len_, tmp_data)) {
            log_error("snappy decompress err while RawUncompress");
            delete[] tmp_data;
            return false;
        }
        data = tmp_data;
        data_len = (int32_t) tmp_data_len;
        return true;
    }

    int32_t get_version() const {
        return schema_version_;
    }

public:
    uint32_t config_version_;

    int32_t data_len_;
    char *data_;

    int32_t schema_len_;
    char *schema_;
    int32_t schema_version_;
};
}
#endif
