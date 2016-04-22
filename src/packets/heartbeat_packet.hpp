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

#ifndef TAIR_PACKET_HEARTBEAT_PACKET_H
#define TAIR_PACKET_HEARTBEAT_PACKET_H

#include "base_packet.hpp"
#include "server_hash_table_packet.hpp"
#include "stat_info.hpp"
#include "stat_helper.hpp"

using namespace std;
namespace tair {

// heartbeat will carry transition status information,
// we capsuled them to this(one int type) just for heartbeat packet compatibility.
// format:
//   +--------------+-----------+---------+---------+---------------------+
//   |0             |1          |2        |3        |8                  31|
//   +--------------+-----------+---------+---------+---------------------+
//   |data_need_move|failovering|migrating| reserved| transition version  |
//   +--------------+-----------+---------+---------+---------------------+
struct cluster_transition_info {
    static const uint32_t MAX_TRANSITION_VERSION = 1U << 23;
    uint32_t transition_version;
    bool failovering;
    bool migrating;
    bool data_need_move;

    cluster_transition_info() {
        transition_version = 0;
        failovering = migrating = data_need_move = false;
    }

    cluster_transition_info(uint32_t info) { set(info); }

    uint32_t get() {
        return (transition_version << 8) |
               (data_need_move ? 1 : 0) |
               ((failovering ? 1 : 0) << 1) |
               ((migrating ? 1 : 0) << 2);
    }

    void set(uint32_t info) {
        transition_version = info >> 8;
        data_need_move = (info & 0x1) != 0;
        failovering = (info & 0x2) != 0;
        migrating = (info & 0x4) != 0;
    }

    void
    reset(uint32_t transition_version, bool data_need_move = false, bool failovering = false, bool migrating = false) {
        this->transition_version = transition_version;
        this->data_need_move = data_need_move;
        this->failovering = failovering;
        this->migrating = migrating;
    }
};

class request_heartbeat : public base_packet {
public:
    request_heartbeat() {
        setPCode(TAIR_REQ_HEARTBEAT_PACKET);
        server_id = 0U;
        transition_info = 0U;
        config_version = 0U;
        recovery_version = 0;
        server_flag = 0;
        plugins_version = 0U;
        area_capacity_version = 0U;
        pull_migrated_info = 0U;
        stat_info = NULL;
        stat_info_size = 0;
        stats = NULL;
    }

    request_heartbeat(request_heartbeat &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_HEARTBEAT_PACKET);
        server_id = packet.server_id;
        transition_info = packet.transition_info;
        config_version = packet.config_version;
        recovery_version = packet.recovery_version;
        server_flag = packet.server_flag;
        plugins_version = packet.plugins_version;
        area_capacity_version = packet.area_capacity_version;
        vec_area_capacity_info = packet.vec_area_capacity_info;
        vec_bucket_no = packet.vec_bucket_no;
        pull_migrated_info = packet.pull_migrated_info;
        stat_info_size = packet.stat_info_size;
        stat_info = NULL;
        stats = NULL;
        if (stat_info_size > 0) {
            stat_info = (char *) malloc(stat_info_size);
            memcpy(stat_info, packet.stat_info, stat_info_size);
            // we prefer rebuild stats rather than copy here
        }
    }

    ~request_heartbeat() {
        if (stat_info != NULL) {
            ::free(stat_info);
            stat_info = NULL;
        }
        if (stats != NULL) {
            ::free(stats);
            stats = NULL;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt64(server_id);
        output->writeInt32(transition_info.get());
        output->writeInt32(config_version);
        output->writeInt32(recovery_version);
        output->writeInt8(pull_migrated_info);
        output->writeInt32(vec_bucket_no.size());
        for (size_t i = 0; i < vec_bucket_no.size(); i++) {
            output->writeInt32(vec_bucket_no[i]);
        }
        output->writeInt32(plugins_version);
        output->writeInt32(area_capacity_version);
        output->writeInt32(vec_area_capacity_info.size());
        for (uint32_t i = 0; i < vec_area_capacity_info.size(); i++) {
            output->writeInt32(vec_area_capacity_info[i].first);
            output->writeInt64(vec_area_capacity_info[i].second);
        }
        output->writeInt32(stat_info_size);
        if (stat_info_size > 0) {
            output->writeBytes(stat_info, stat_info_size);
        }

        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 12) {
            log_warn("buffer data too few.");
            return false;
        }
        server_flag = input->readInt8();
        server_id = input->readInt64();
        transition_info.set(input->readInt32());
        config_version = input->readInt32();
        recovery_version = input->readInt32();
        pull_migrated_info = input->readInt8();
        size_t size = input->readInt32();
        if (size > 0) {
            vec_bucket_no.reserve(size);
            for (size_t i = 0; i < size; i++) {
                vec_bucket_no.push_back(input->readInt32());
            }
        }
        plugins_version = input->readInt32();
        area_capacity_version = input->readInt32();
        vec_area_capacity_info.clear();
        uint32_t capacityInfoSize = input->readInt32();
        for (uint32_t i = 0; i < capacityInfoSize; i++) {
            uint32_t area = input->readInt32();
            uint64_t capacity = input->readInt64();
            vec_area_capacity_info.push_back(make_pair(area, capacity));
        }

        stat_info_size = input->readInt32();
        if (stat_info_size > 0) {
            stat_info = (char *) malloc(stat_info_size);
            input->readBytes(stat_info, stat_info_size);
        }
        return true;
    }

    void set_stat_info(const char *stat_info_value, int size) {
        if (stat_info != NULL) {
            stat_info_size = 0;
            ::free(stat_info);
            stat_info = NULL;
        }
        if (stats != NULL) {
            ::free(stats);
            stats = NULL;
        }

        if (size > 0 && stat_info_value != NULL) {
            stat_info_size = size;
            stat_info = (char *) malloc(size);
            memcpy(stat_info, stat_info_value, size);
        }
    }

    tair_stat *get_stat() {
        if (stat_info_size == 0 || stat_info == NULL)
            return NULL;

        if (stats != NULL) {
            return stats;
        }

        unsigned long dest_len = STAT_TOTAL_SIZE;
        unsigned long src_len = stat_info_size;
        stats = (tair_stat *) malloc(dest_len);
        memset(stats, 0, dest_len);

        int ret = uncompress((Bytef *) stats, &dest_len, (const Bytef *) stat_info, src_len);
        if (ret != Z_OK) {
            ::free(stats);
            stats = NULL;
            log_error("uncompress stat info failed, ret: %d", ret);
        } else {
            log_debug("uncompress successed");
        }
        return stats;
    }

    void set_is_server_ready(bool is_server_ready) {
        if (LIKELY(is_server_ready))
            server_id &= ~((uint64_t) 1 << 63); // set bit not_ready_flag to 0
        else
            server_id |= ((uint64_t) 1 << 63);  // set bit not_ready_flag to 1
    }

    bool check_is_server_ready() const {
        uint64_t not_ready_flag = server_id & ((uint64_t) 1 << 63);
        if (not_ready_flag != 0)
            return false;
        return true;
    }

    void set_server_id(uint64_t a_server_id) {
        server_id = (server_id & server_id_flag_header_mask) | (a_server_id & (~server_id_flag_header_mask));
    }

    uint64_t get_server_id() const {
        return server_id & (~server_id_flag_header_mask);
    }

private:
    /*
      for compatible, really ugly, header 16bit for flag
    server_id, uint64_t
   |----------+----------+------------+-----------------|
   | ip       | port     | not used   | not_ready_flag  |
   |----------+----------+------------+-----------------|
   | 0     31 | 32    47 | 48      62 |              63 |
   |----------+----------+------------+-----------------|
   if is_ready_flag == false, set bit not_ready_flag to 1
   else set bit not_ready_flag to 0
    */
    uint64_t server_id;
    static const uint64_t server_id_flag_header_mask = 0xffff000000000000;

public:
    cluster_transition_info transition_info;
    uint32_t config_version;
    uint32_t recovery_version; // recovery version used in recover process
    uint32_t plugins_version;
    uint32_t area_capacity_version;
    vector<pair<uint32_t, uint64_t> > vec_area_capacity_info;
    uint8_t pull_migrated_info;   // 0 don't need migrated info, 1 need migrated info
    vector<uint32_t> vec_bucket_no;

    char *stat_info;
    int stat_info_size;
    tair_stat *stats;

};

class response_heartbeat : public base_packet, public server_hash_table_packet {
public:

    response_heartbeat() {
        setPCode(TAIR_RESP_HEARTBEAT_PACKET);
        client_version = 0U;
        server_version = 0U;
        down_slave_config_server = 0U;
        transition_info = 0U;
        plugins_flag = -1;
        plugins_version = 0U;
        area_capacity_version = 0U;
        copy_count = 0U;
        bucket_count = 0U;
        recovery_version = 0U;
    }

    ~response_heartbeat() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(client_version);
        output->writeInt32(server_version);
        output->writeInt32(bucket_count);
        output->writeInt32(copy_count);
        output->writeInt64(down_slave_config_server);
        output->writeInt32(transition_info.get());
        output->writeInt32(hash_table_size);
        if (hash_table_data != NULL && hash_table_size > 0) {
            output->writeBytes(hash_table_data, hash_table_size);
        }
        output->writeBytes(group_name, 64);
        output->writeInt32(plugins_flag);
        output->writeInt32(plugins_version);
        if (plugins_flag > 0) {
            for (vector<string>::iterator it = plugins_dll_names.begin();
                 it != plugins_dll_names.end(); it++) {
                size_t len = it->length();
                if (len > TAIR_MAX_FILENAME_LEN - 1) len = TAIR_MAX_FILENAME_LEN - 1;
                output->writeInt32(len);
                output->writeBytes(it->c_str(), len);
            }
        }
        output->writeInt32(area_capacity_version);
        output->writeInt32(vec_area_capacity_info.size());
        for (uint32_t i = 0; i < vec_area_capacity_info.size(); i++) {
            output->writeInt32(vec_area_capacity_info[i].first);
            output->writeInt64(vec_area_capacity_info[i].second);
        }

        output->writeInt32(migrated_info.size());
        if (migrated_info.size() > 0) {
            assert(migrated_info.size() % (copy_count + 1) == 0);
            for (size_t i = 0; i < migrated_info.size(); ++i) {
                output->writeInt64(migrated_info[i]);
            }
        }

        // if failovering encode the recovery list and recovery_version
        if (transition_info.failovering) {
            output->writeInt32(recovery_ds.size());
            if (recovery_ds.size() > 0) {
                for (size_t i = 0; i < recovery_ds.size(); ++i) {
                    output->writeInt64(recovery_ds[i]);
                }
            }
            output->writeInt32(recovery_version);
        }

        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 20) {
            log_warn("buffer data too few.");
            return false;
        }
        client_version = input->readInt32();
        server_version = input->readInt32();
        bucket_count = input->readInt32();
        copy_count = input->readInt32();
        down_slave_config_server = input->readInt64();
        transition_info.set(input->readInt32());
        set_hash_table(NULL, input->readInt32());
        if (hash_table_size > input->getDataLen()) {
            log_warn("buffer data too few.");
            return false;
        }
        if (hash_table_size > 0 && hash_table_data != NULL) {
            input->readBytes(hash_table_data, hash_table_size);
        }
        input->readBytes(group_name, 64);
        group_name[63] = '\0';
        plugins_flag = input->readInt32();
        plugins_dll_names.clear();
        plugins_version = input->readInt32();
        if (plugins_flag > 0) {
            plugins_dll_names.reserve(plugins_flag);
            for (int32_t i = 0; i < plugins_flag; i++) {
                char dll_name[TAIR_MAX_FILENAME_LEN];
                size_t len = input->readInt32();
                input->readBytes(dll_name, len);
                dll_name[len] = '\0';
                plugins_dll_names.push_back(dll_name);
            }
        }
        area_capacity_version = input->readInt32();
        vec_area_capacity_info.clear();
        uint32_t capacityInfoSize = input->readInt32();
        for (uint32_t i = 0; i < capacityInfoSize; i++) {
            uint32_t area = input->readInt32();
            uint64_t capacity = input->readInt64();
            vec_area_capacity_info.push_back(make_pair(area, capacity));
        }
        int size = input->readInt32();
        assert(migrated_info.size() % (copy_count + 1) == 0);
        for (int i = 0; i < size; ++i) {
            migrated_info.push_back(input->readInt64());
        }

        if (transition_info.failovering) {
            int size = input->readInt32();
            for (int i = 0; i < size; ++i) {
                recovery_ds.push_back(input->readInt64());
            }
            recovery_version = input->readInt32();
        }

        return true;
    }

    bool have_plugins_info() {
        return plugins_flag > 0;
    }

    void set_plugins_names(const std::set<std::string> &dll_names) {
        plugins_flag = dll_names.size();
        plugins_dll_names.reserve(plugins_flag);
        for (std::set<std::string>::const_iterator it = dll_names.begin();
             it != dll_names.end(); it++) {
            plugins_dll_names.push_back(*it);
        }
    }

    void set_area_capacity(const std::map<uint32_t, uint64_t> &capacity, uint32_t copy_count, uint32_t server_count) {
        uint32_t div = server_count;
        if (div) {
            vec_area_capacity_info.reserve(capacity.size());
            for (std::map<uint32_t, uint64_t>::const_iterator it = capacity.begin();
                 it != capacity.end(); it++) {
                vec_area_capacity_info.push_back(std::make_pair(it->first, it->second / div));
            }
        }
    }

public:
    uint32_t client_version;
    uint32_t server_version;
    uint32_t bucket_count;
    uint32_t copy_count;
    uint64_t down_slave_config_server;
    cluster_transition_info transition_info;
    char group_name[64];
    int32_t plugins_flag;
    uint32_t plugins_version;
    vector<string> plugins_dll_names;
    uint32_t area_capacity_version;
    vector<pair<uint32_t, uint64_t> > vec_area_capacity_info;

    // migrated_info will contains bucket number and all serverId which is a copy of this. if bucket's count greater than 1, repeate this
    // for exmaple when copy count is 3 , this may be  bucketId1, serverid11,serverid12,serverid13, bucketId2,serverid21,serverid22,serverid23
    vector<uint64_t> migrated_info;

    // recovery_ds contains the ds that is being recovered
    vector<uint64_t> recovery_ds;
    uint32_t recovery_version;

private:
    response_heartbeat(const response_heartbeat &);

};
}
#endif
