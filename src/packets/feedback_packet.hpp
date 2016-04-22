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

#ifndef TAIR_PACKET_FEEDBACK_PACKET_HPP
#define TAIR_PACKET_FEEDBACK_PACKET_HPP

#include "base_packet.hpp"

namespace tair {

class response_feedback : public base_packet {
public:
    response_feedback() {
        setPCode(TAIR_RESP_FEEDBACK_PACKET);
        ns_ = 0;
        key_ = NULL;
        has_key_ = false;
        feedback_type_ = FEEDBACK_HOT_KEY;
    }

    ~response_feedback() {
        if (key_ != NULL) {
            delete key_;
            key_ = NULL;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *out) {
        out->writeInt32(feedback_type_);
        if (feedback_type_ == FEEDBACK_HOT_KEY) {
            out->writeInt16(ns_);
            out->writeInt8(has_key_);
            if (has_key_) {
                key_->encode(out);
            }
        }
        return true;
    }

    bool decode(DataBuffer *in, PacketHeader *header) {
        if (!in->readInt32(&feedback_type_)) {
            log_error("decode feedback_type_ error");
            return false;
        }
        if (feedback_type_ == FEEDBACK_HOT_KEY) {
            if (!in->readInt16(&ns_)) {
                log_error("decode ns_ error, feedback_type_: %u", feedback_type_);
                return false;
            }
            if (!in->readInt8((uint8_t *) &has_key_)) {
                log_error("decode has_key_ error, feedback_type_: %u, ns_: %u", feedback_type_, ns_);
                return false;
            }
            if (has_key_) {
                key_ = new data_entry;
                if (!key_->decode(in)) {
                    log_error("decode key error, feedback_type_: %u, ns_: %hu", feedback_type_, ns_);
                    return false;
                }
            }
        }
        return true;
    }

    void set_ns(uint16_t ns) {
        ns_ = ns;
    }

    virtual uint16_t ns() const {
        return ns_;
    }

    void set_key(const data_entry *key) {
        if (key != NULL) {
            has_key_ = true;
            key_ = new data_entry(*key);
        }
    }

    data_entry *key() const {
        return key_;
    }

public:
    enum FeedbackType {
        FEEDBACK_HOT_KEY = 0,
    };

    void set_feedback_type(enum FeedbackType type) {
        feedback_type_ = type;
    }

private:
    uint32_t feedback_type_;
    uint16_t ns_;
    bool has_key_;
    data_entry *key_;
};

}

#endif
