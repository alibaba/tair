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

#ifndef TAIR_PACKET_MCOPS_PACKET_H
#define TAIR_PACKET_MCOPS_PACKET_H

#include "base_packet.hpp"

namespace tair {

class request_mc_ops : public base_packet {
public:
    request_mc_ops() {
        setPCode(TAIR_REQ_MC_OPS_PACKET);
        server_flag = 0;
        area = 0;
        version = 0;
        expire = 0;
        mc_opcode = 0;
        padding = NULL;
        pad_len = 0;
    }

    request_mc_ops(const request_mc_ops &rhs)
            : base_packet(rhs) {
        server_flag = rhs.server_flag;
        area = rhs.area;
        version = rhs.version;
        expire = rhs.expire;
        mc_opcode = rhs.mc_opcode;
        key = rhs.key;
        value = rhs.value;
        if ((pad_len = rhs.pad_len) != 0) {
            padding = new char[pad_len];
            memcpy(padding, rhs.padding, pad_len);
        } else {
            padding = NULL;
        }
    }

    ~request_mc_ops() {
        delete padding;
        padding = NULL;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = 1 + 2 + 2 + 4 + 2;

        total += 4 + key.get_size();
        total += 4 + value.get_size();
        total += 4 + pad_len;

        return total + 16; // header 16 bytes
    }

    virtual base_packet::Type get_type() {
        // this packet is a little complex, so many
        // deferent type packet yse it for transfer format
        // so I just set it to be mix, if we turn readonly
        // on, all using this packet for transfer format will
        // be refused
        return base_packet::REQ_MIX;
    }

    uint16_t ns() const {
        return area;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        output->writeInt16(version);
        output->writeInt32(expire);
        output->writeInt16(mc_opcode);

        output->writeInt32(key.get_size());
        if (key.get_size() > 0) {
            output->writeBytes(key.get_data(), key.get_size());
        }
        output->writeInt32(value.get_size());
        if (value.get_size() > 0) {
            output->writeBytes(value.get_data(), value.get_size());
        }
        output->writeInt32(pad_len);
        if (pad_len > 0) {
            output->writeBytes(padding, pad_len);
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        server_flag = input->readInt8();
        area = input->readInt16();
        version = input->readInt16();
        expire = input->readInt32();
        mc_opcode = input->readInt16();

        bool success = true;
        do {
            char *key_buf = NULL;
            int32_t key_len = 0;
            char *val_buf = NULL;
            int32_t val_len = 0;

            key_len = input->readInt32();
            if (key_len > (1 << 10)) {
                log_warn("key_len illegal: %u", key_len);
                success = false;
                break;
            }
            if (key_len > 0) {
                key_buf = new char[key_len];
                if (!(success = input->readBytes(key_buf, key_len))) {
                    break;
                }
                key.set_alloced_data(key_buf, key_len);
            }

            val_len = input->readInt32();
            if (val_len > (1 << 20)) {
                log_warn("val_len illegal: %u", val_len);
                success = false;
                break;
            }
            if (val_len > 0) {
                val_buf = (char *) malloc(val_len);
                if (!(success = input->readBytes(val_buf, val_len))) {
                    success = false;
                    break;
                }
                value.set_alloced_data(val_buf, val_len);
            }

            pad_len = input->readInt32();
            if (pad_len > (1 << 10)) {
                log_warn("pad_len illegal: %u", pad_len);
                success = false;
                break;
            }
            if (pad_len > 0) {
                padding = new char[pad_len];
                if (!(success = input->readBytes(padding, pad_len))) {
                    break;
                }
            }
        } while (false);

        return success;
    }

public:
    uint16_t area;
    uint16_t version;
    int32_t expire;
    int16_t mc_opcode;
    data_entry key;
    data_entry value;
    char *padding;
    uint32_t pad_len;
};

class response_mc_ops : public request_mc_ops {
public:
    response_mc_ops() {
        setPCode(TAIR_RESP_MC_OPS_PACKET);
        config_version = 0;
        code = 0;
    }

    response_mc_ops(const response_mc_ops &rhs) : request_mc_ops(rhs) {
        setPCode(TAIR_RESP_MC_OPS_PACKET);
        config_version = rhs.config_version;
        code = rhs.code;
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        if (!request_mc_ops::encode(output)) {
            return false;
        }
        output->writeInt32(config_version);
        output->writeInt16(code);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (!request_mc_ops::decode(input, header)) {
            return false;
        }
        config_version = input->readInt32();
        code = input->readInt16();
        return true;
    }

    virtual size_t size() const {
        if (UNLIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = request_mc_ops::size();
        total += 4 + 2;
        return total; //header 16 byte is added in request_mc_ops.size()
    }

    virtual void set_code(int code) {
        this->code = code;
    }

    virtual bool failed() const {
        return code != TAIR_RETURN_SUCCESS;
    }

public:
    int32_t config_version;
    int16_t code;
};

}
#endif
