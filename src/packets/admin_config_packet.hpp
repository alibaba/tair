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

#ifndef TAIR_PACKET_ADMIN_CONFIG_PACKET_H_
#define TAIR_PACKET_ADMIN_CONFIG_PACKET_H_

#include "base_packet.hpp"

namespace tair {
class request_admin_config : public base_packet {
public:
    request_admin_config() : size_(0), last_version_(0), next_version_(0) {
        setPCode(TAIR_REQ_ADMIN_CONFIG_PACKET);
    }

    enum TaskOperation {
        kSet = 0, kDel = 1, kCheck = 2, kSync = 3, kGet = 4
    };

    bool encode(DataBuffer *) { return false; }

    virtual size_t size() {
        return size_;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (input->readInt64(&last_version_) == false) return false;
        if (input->readInt64(&next_version_) == false) return false;
        if (input->readInt16(&flag_) == false) return false;
        if (input->readLittleString(optstr_) == false) return false;
        if (optstr_ == "SET")
            opt_ = kSet;
        else if (optstr_ == "DEL")
            opt_ = kDel;
        else if (optstr_ == "CHECK")
            opt_ = kCheck;
        else if (optstr_ == "SYNC")
            opt_ = kSync;
        else if (optstr_ == "GET")
            opt_ = kGet;
        else
            return false;
        if (input->readLittleString(cid_) == false) return false;
        size_ = 26 + cid_.length() + optstr_.length();
        if (flag_ & kCompressed) {
            //TODO: decompress
        } else {
            int32_t size = 0;
            if (input->readInt32(&size) == false) return false;
            if (size < 0) return false;
            for (int32_t i = 0; i < size; ++i) {
                std::string key;
                if (input->readLittleString(key) == false) return false;
                std::string val;
                if (input->readLittleString(val) == false) return false;
                configs_[key] = val;
                size_ += 4 + key.length() + val.length();
            }
        }
        return true;
    }

    const std::map<std::string, std::string> &configs() const {
        return configs_;
    }

    int64_t last_version() {
        return last_version_;
    }

    int64_t next_version() {
        return next_version_;
    }

    TaskOperation opt() {
        return opt_;
    }

    const std::string &cid() const { return cid_; }

    const std::string &optstr() const { return optstr_; }

private:
    const static int32_t kCompressed = 0x01;
    size_t size_;
    TaskOperation opt_;
    int16_t flag_;
    int64_t last_version_;
    int64_t next_version_;
    std::string cid_;
    std::string optstr_;
    std::map<std::string, std::string> configs_;
};

class response_admin_config : public base_packet {
public:
    response_admin_config() : size_(0), version_(0) {
        setPCode(TAIR_RESP_ADMIN_CONFIG_PACKET);
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        int16_t flag = 0;
        output->writeInt64(version_);
        output->writeInt16(flag);
        output->writeInt32(configs_.size());
        std::map<std::string, std::string>::iterator iter = configs_.begin();
        for (; iter != configs_.end(); ++iter) {
            if (output->writeLittleString(iter->first) == false) return false;
            if (output->writeLittleString(iter->second) == false) return false;
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        return false;
    }

    virtual size_t size() {
        if (size_ == 0) {
            size_ = 8 + 2 + 4;
            std::map<std::string, std::string>::iterator iter = configs_.begin();
            for (; iter != configs_.end(); ++iter) {
                size_ += iter->first.length() + iter->second.length() + 4;
            }
        }
        return size_;
    }

    void set_version(int64_t v) {
        version_ = v;
        size_ = 0;
    }

    std::map<std::string, std::string> &configs() {
        return configs_;
        size_ = 0;
    }

private:
    size_t size_;
    int64_t version_;
    std::map<std::string, std::string> configs_;
};
}
#endif
