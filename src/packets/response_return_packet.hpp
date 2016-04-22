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

#ifndef TAIR_PACKET_RESPONSE_RETURN_PACKET_H
#define TAIR_PACKET_RESPONSE_RETURN_PACKET_H

#include "base_packet.hpp"

namespace tair {
class response_return : public base_packet {
public:
    response_return() {
        setPCode(TAIR_RESP_RETURN_PACKET);
        code = 0;
        config_version = 0;
        msg[0] = '\0';
    }

    response_return(int chid, int code, const char *msg, uint32_t version = 0) {
        this->code = code;
        config_version = version;
        if (msg != NULL) {
            snprintf(this->msg, 128, "%s", msg);
        }
        setChannelId(chid);
        setPCode(TAIR_RESP_RETURN_PACKET);
    }

    ~response_return() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(config_version);
        output->writeInt32(code);
        output->writeString(msg);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < (int) (sizeof(int) * 3)) {
            log_warn("buffer data too few.");
            return false;
        }
        config_version = input->readInt32();
        code = input->readInt32();
        char *p = &msg[0];
        input->readString(p, 128);
        return true;
    }

    virtual void set_code(int code) {
        this->code = code;
    }

    int get_code() {
        return code;
    }

    void set_message(const char *msg) {
        snprintf(this->msg, 128, "%s", msg);
    }

    char *get_message() {
        return msg;
    }

    virtual size_t size() const {
        if (UNLIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        // config_version 4 byte
        // code 4 byte
        // msg len 4 byte
        // msg strlen(len)
        size_t len = strlen(msg);
        if (len > 0) len += 1;
        return 12 + len + 16;  //header 16 bytes
    }

    virtual bool failed() const {
        return code != TAIR_RETURN_SUCCESS;
    }

public:
    int code;
    char msg[128];
    uint32_t config_version;
private:
    response_return(const response_return &);
};
}
#endif
