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

#ifndef TAIR_PACKET_DATASERVER_CTRL_H
#define TAIR_PACKET_DATASERVER_CTRL_H

#include "base_packet.hpp"

namespace tair {
class request_data_server_ctrl : public base_packet {
public:
    request_data_server_ctrl() {
        setPCode(TAIR_REQ_DATASERVER_CTRL_PACKET);
        op_cmd = DATASERVER_CTRL_OP_NR;
    }

    request_data_server_ctrl(const request_data_server_ctrl &packet) {
        setPCode(TAIR_REQ_DATASERVER_CTRL_PACKET);
        server_id_list = packet.server_id_list;
        op_cmd = packet.op_cmd;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(server_id_list.size());
        for (vector<uint64_t>::const_iterator it = server_id_list.begin(); it != server_id_list.end(); ++it)
            output->writeInt64(*it);
        uint8_t int_op_cmd = op_cmd;
        output->writeInt8(int_op_cmd);
        output->writeString(group_name);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 5) {
            log_warn("buffer data too few.");
            return false;
        }
        int32_t server_id_list_size = input->readInt32();
        for (int32_t i = 0; i < server_id_list_size; ++i) {
            uint64_t server_id = input->readInt64();
            server_id_list.push_back(server_id);
        }
        uint8_t int_op_cmd = input->readInt8();
        if (int_op_cmd >= DATASERVER_CTRL_OP_NR)
            return false;
        op_cmd = static_cast<DataserverCtrlOpType>(int_op_cmd);
        char *p = group_name;
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

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

public:
    vector<uint64_t> server_id_list;
    DataserverCtrlOpType op_cmd;
    char group_name[64];
};

class response_data_server_ctrl : public base_packet {
public:
    response_data_server_ctrl() {
        setPCode(TAIR_RESP_DATASERVER_CTRL_PACKET);
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(return_code.size());
        for (vector<DataserverCtrlReturnType>::const_iterator it = return_code.begin(); it != return_code.end(); ++it) {
            uint8_t rt = *it;
            output->writeInt8(rt);
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 4) {
            log_warn("buffer data too few.");
            return false;
        }
        int32_t return_code_num = input->readInt32();
        for (int32_t i = 0; i < return_code_num; ++i) {
            uint8_t rt = input->readInt8();
            if (rt >= DATASERVER_CTRL_RETURN_NR)
                return false;
            return_code.push_back(static_cast<DataserverCtrlReturnType>(rt));
        }
        return true;
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

public:
    vector<DataserverCtrlReturnType> return_code;
};

}
#endif
