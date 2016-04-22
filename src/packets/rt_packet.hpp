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

#ifndef TAIR_PACKET_RT_PACKET_HPP
#define TAIR_PACKET_RT_PACKET_HPP

#include "base_packet.hpp"
#include "modules/rt_object.hpp"

namespace tair {

class request_rt : public base_packet {
public:
    request_rt() {
        setPCode(TAIR_REQ_RT_PACKET);
        opcode_ = -1;
        cmd_ = RT_GET;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *out) {
        out->writeInt32(opcode_);
        out->writeInt32(cmd_);
        return true;
    }

    bool decode(DataBuffer *in, PacketHeader *header) {
        return in->readInt32((uint32_t *) &opcode_) && in->readInt32((uint32_t *) &cmd_);
    }

public:
    int opcode_;
    enum RT_TYPE {
        RT_GET = 0,
        RT_ENABLE = 1,
        RT_DISABLE = 2,
        RT_RELOAD = 3,
        RT_RELOAD_LIGHT = 4,
    } cmd_;
};

class response_rt : public base_packet {
public:
    response_rt() {
        setPCode(TAIR_RESP_RT_PACKET);
    }

    ~response_rt() {
        for (size_t i = 0; i < rtos_.size(); ++i) {
            delete rtos_[i];
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *out) {
        out->writeInt32(code_);
        out->writeInt32((uint32_t) rtos_.size());
        if (!rtos_.empty()) {
            for (size_t i = 0; i < rtos_.size(); ++i) {
                RTObject *rto = rtos_[i];
                out->writeInt32(rto->opcode_);
                for (uint32_t j = 0; j < (uint32_t) RTObject::RT_TYPE_NUM; ++j) {
                    out->writeInt64(rto->rt_types_[j].opcnt_);
                    out->writeInt64(rto->rt_types_[j].rt_us_acc_);
                    out->writeInt64(rto->rt_types_[j].rt_us_max_);
                    for (uint32_t k = 0; k <= RTObject::RT_ORDER_MAX; ++k) {
                        out->writeInt64(rto->rt_types_[j].rt_buckets_[k]);
                    }
                }
            }
        }
        return true;
    }

    bool decode(DataBuffer *in, PacketHeader *header) {
        uint32_t size = 0;
        if (!in->readInt32((uint32_t *) &code_)) return false;
        if (!in->readInt32(&size)) return false;
        rtos_.reserve(size);
        for (uint32_t i = 0; i < size; ++i) {
            RTObject *rto = new RTObject;
            rtos_.push_back(rto);
            if (!in->readInt32((uint32_t *) &rto->opcode_)) return false;
            for (uint32_t j = 0; j < (uint32_t) RTObject::RT_TYPE_NUM; ++j) {
                if (!in->readInt64(&rto->rt_types_[j].opcnt_)) return false;
                if (!in->readInt64(&rto->rt_types_[j].rt_us_acc_)) return false;
                if (!in->readInt64(&rto->rt_types_[j].rt_us_max_)) return false;
                for (uint32_t k = 0; k <= RTObject::RT_ORDER_MAX; ++k) {
                    if (!in->readInt64(&rto->rt_types_[j].rt_buckets_[k])) return false;
                }
            }
        }
        return true;
    }

    int get_code() const {
        return code_;
    }

    void set_code(int code) {
        code_ = code;
    }

    void add_rto(RTObject *rto) {
        rtos_.push_back(rto);
    }

    void serialize(std::string &str, bool json = false) {
        char buf[1 << 10];
        const size_t size = sizeof(buf);
        str.reserve(1 << 16);
        if (!json) {
            for (size_t i = 0; i < rtos_.size(); ++i) {
                RTObject *rto = rtos_[i];
                rto->serialize(str, buf, size, base_packet::name);
            }
        } else {
        }
    }

private:
    int code_;
    vector<RTObject *> rtos_;
};


}

#endif
