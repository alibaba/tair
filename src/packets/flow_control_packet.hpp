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

#ifndef TAIR_FLOW_CONTROL_PACKET_HPP
#define TAIR_FLOW_CONTROL_PACKET_HPP

#include "base_packet.hpp"
#include "flowrate.h"

namespace tair {

class flow_control_set : public base_packet {
public:
    flow_control_set() : type(tair::stat::IN),
                         lower_mb(-1),
                         upper_mb(-1),
                         ns(-1),
                         success(false) {
        setPCode(TAIR_FLOW_CONTROL_SET);
    }

    flow_control_set &operator=(const flow_control_set &rhs) {
        base_packet::operator=(rhs);
        this->type = rhs.type;
        this->lower_mb = rhs.lower_mb;
        this->upper_mb = rhs.upper_mb;
        this->ns = rhs.ns;
        this->success = rhs.success;
        return *this;
    }

    virtual size_t size() const {
        return 20 + 16;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(type);
        output->writeInt32(lower_mb);
        output->writeInt32(upper_mb);
        output->writeInt32(ns);
        output->writeInt32(success);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 20)
            return false;

        type = static_cast<tair::stat::FlowType>(
                input->readInt32());
        lower_mb = input->readInt32();
        upper_mb = input->readInt32();
        ns = input->readInt32();
        success = input->readInt32();
        return true;
    }

    tair::stat::FlowType getType() { return type; }

    int32_t getLowerMB() { return lower_mb; }

    int32_t getUpperMB() { return upper_mb; }

    int32_t getNamespace() { return ns; }

    void setType(tair::stat::FlowType type) {
        this->type = type;
    }

    void setLimit(int lower_mb, int upper_mb) {
        this->lower_mb = lower_mb;
        this->upper_mb = upper_mb;
    }

    void setNamespace(int ns) {
        this->ns = ns;
    }

    void setSuccess(bool f) {
        this->success = f;
    }

    bool isSuccess() {
        return this->success;
    }

private:
    flow_control_set(const flow_control_set &);

private:
    tair::stat::FlowType type;
    int32_t lower_mb;
    int32_t upper_mb;
    int32_t ns;
    bool success;
};

class flow_check : public base_packet {
public:
    flow_check() {
        setPCode(TAIR_FLOW_CHECK);
    }

    virtual size_t size() const {
        return 4 + 16;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(ns);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 4)
            return false;
        ns = input->readInt32();
        return true;
    }

    void setNamespace(int32_t ns) {
        this->ns = ns;
    }

    int32_t getNamespace() {
        return ns;
    }

private:
    flow_check(const flow_check &);

private:
    int32_t ns;
};

class flow_control : public base_packet {
public:
    flow_control() : user_data(NULL) {
        setPCode(TAIR_FLOW_CONTROL);
    }

    virtual size_t size() const {
        return 8 + 16; // 16bytes tbnet packet header
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(status);
        output->writeInt32(ns);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 8)
            return false;
        status = input->readInt32();
        ns = input->readInt32();
        return true;
    }

    void set_status(int s) {
        this->status = s;
    }

    void set_ns(int ns) {
        this->ns = ns;
    }

    int get_ns() {
        return ns;
    }

    int get_status() {
        return status;
    }

    void set_user_data(void *user_data) {
        this->user_data = user_data;
    }

    void *get_user_data() {
        return user_data;
    }

    void set_addr(const easy_addr_t &addr) { this->addr = addr; }

    const easy_addr_t &get_addr() { return addr; }

private:
    flow_control(const flow_control &);

private:
    int ns;
    int status;

    void *user_data;
    easy_addr_t addr;
};

}

#endif

