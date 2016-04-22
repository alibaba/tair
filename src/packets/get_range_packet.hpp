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

#ifndef TAIR_PACKET_GET_RANGE_PACKET_H
#define TAIR_PACKET_GET_RANGE_PACKET_H

#include "base_packet.hpp"

namespace tair {
class request_get_range : public base_packet {
public:
    request_get_range() {
        setPCode(TAIR_REQ_GET_RANGE_PACKET);
        cmd = 0;
        server_flag = 0;
        area = 0;
        offset = 0;
        limit = 0;
    }

    request_get_range(request_get_range &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_GET_RANGE_PACKET);
        cmd = packet.cmd;
        server_flag = packet.server_flag;
        area = packet.area;
        offset = packet.offset;
        limit = packet.limit;
        key_start = packet.key_start;
        key_end = packet.key_end;
    }

    ~request_get_range() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_READ;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt16(cmd);
        output->writeInt16(area);
        output->writeInt32(offset);
        output->writeInt32(limit);

        key_start.encode(output);
        key_end.encode(output);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  int8   server_flag    1B     (must)
        //  int16  area           2B     (must)
        //  int32  offset         4B     (must)
        //  int32  limit          4B     (must)
        //  -------------------------
        //  total                11B
        if (input->readInt8(&server_flag) == false ||
            input->readInt16(&cmd) == false ||
            input->readInt16(&area) == false ||
            input->readInt32(&offset) == false ||
            input->readInt32(&limit) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
        }

#if TAIR_MAX_AREA_COUNT < 65536
        if (area >= TAIR_MAX_AREA_COUNT) {
            log_warn("area overflow: "
                             "server_flag %x, cmd %d, area %d, offset %d, limit %d",
                     server_flag, cmd, area, offset, limit);
            return false;
        }
#endif

        if (!key_start.decode(input)) {
            log_warn("key_start decode failed: "
                             "server_flag %x, cmd %d, area %d, offset %d, limit %d",
                     server_flag, cmd, area, offset, limit);
            return false;
        }

        if (!key_end.decode(input)) {
            log_warn("key_end decode failed: "
                             "server_flag %x, cmd %d, area %d, offset %d, limit %d",
                     server_flag, cmd, area, offset, limit);
            return false;
        }

        return true;
    }

    virtual const data_entry *pick_key() const {
        //~ data_entry.size() is not const!!!!!!!!!!!!!!!!!!!!!!!!!!
        //if (key_start.size() == 0) return NULL;
        return &key_start;
    }

    virtual void dump() const {
        char ascii_key[72];
        ascii_key[0] = '\0';
        const data_entry *picked = pick_key();
        if (picked != NULL) {
            pick_key()->to_ascii(ascii_key, sizeof(ascii_key));
        }
        fprintf(stderr,
                "op: %s(%d)\n\t"
                        "area: %d, cmd: %u, offset:%u, limit: %u\n\t"
                        "key: %s\n",
                name(getPCode()), getPCode(), area, cmd, offset, limit, ascii_key);
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = 1 + 2 + 2 + 4 + 4;
        total += key_start.encoded_size() + key_end.encoded_size();

        return total + 16; //header 16 bytes
    }

    virtual uint16_t ns() const {
        return area;
    }

public:
    uint16_t cmd;
    uint16_t area;
    uint32_t offset;
    uint32_t limit;
    data_entry key_start;
    data_entry key_end;
};

class response_get_range : public base_packet {
public:

    response_get_range() {
        config_version = 0;
        setPCode(TAIR_RESP_GET_RANGE_PACKET);
        key_count = 0;
        key_data_vector = NULL;
        proxyed_key_list = NULL;
        code = 0;
        cmd = 0;
        flag = 0;
    }

    ~response_get_range() {
        if (key_data_vector != NULL) {
            for (uint32_t i = 0; i < key_data_vector->size(); i++) {
                if ((*key_data_vector)[i] != NULL)
                    delete (*key_data_vector)[i];
            }
            delete key_data_vector;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(config_version);
        output->writeInt32(code);
        output->writeInt16(cmd);
        output->writeInt32(key_count);
        output->writeInt16(flag);
        if (key_data_vector != NULL) {
            for (uint32_t i = 0; i < key_data_vector->size(); i++) {
                data_entry *data = (*key_data_vector)[i];
                if (data != NULL)
                    data->encode(output);
            }
            output->writeInt32(0);
        }
        return true;
    }

    virtual size_t size() const {
        if (UNLIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = 4 + 4 + 2 + 4 + 2;
        if (key_data_vector != NULL) {
            for (uint32_t i = 0; i < key_data_vector->size(); ++i) {
                data_entry *data = (*key_data_vector)[i];
                if (data != NULL)
                    total += data->encoded_size();
            }
            total += 4;
        }
        return total + 16; //header 16 bytes
    }

    void set_cmd(int cmd) {
        this->cmd = cmd;
    }

    virtual void set_code(int code) {
        this->code = code;
    }

    int get_code() {
        return code;
    }

    void set_hasnext(bool hasnext) {
        if (hasnext)
            flag |= FLAG_HASNEXT;
        else
            flag &= FLAG_HASNEXT_MASK;
    }

    bool get_hasnext() {
        return flag & FLAG_HASNEXT;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 8) {
            log_warn("buffer data too few.");
            return false;
        }

        config_version = input->readInt32();
        code = input->readInt32();
        cmd = input->readInt16();
        key_count = input->readInt32();
        flag = input->readInt16();

        key_data_vector = new tair_dataentry_vector();
        for (uint32_t i = 0; i < key_count; i++) {
            data_entry *key = new data_entry();
            key->decode(input);
            key_data_vector->push_back(key);
        }
        if (key_count > 0) {
            input->readInt32();
        }

        return true;
    }

    // add key and data
    void add_key_data(data_entry *key, data_entry *value) {
        if (key_data_vector == NULL) {
            key_data_vector = new tair_dataentry_vector;
        }
        key_data_vector->push_back(key);
        key_data_vector->push_back(value);
        key_count++;
    }

    void set_key_data_vector(tair_dataentry_vector *result) {
        if (key_data_vector != NULL) {
            for (uint32_t i = 0; i < key_data_vector->size(); i++) {
                if ((*key_data_vector)[i] != NULL) {
                    delete (*key_data_vector)[i];
                }
            }
            delete key_data_vector;
        }

        key_data_vector = result;
    }

    void set_key_count(int count) {
        key_count = count;
    }

    virtual bool failed() const {
        return code != TAIR_RETURN_SUCCESS;
    }

public:
    uint32_t config_version;
    uint32_t key_count;
    uint16_t cmd;
    uint16_t flag;
    tair_dataentry_vector *key_data_vector;
    tair_dataentry_set *proxyed_key_list;
private:
    int code;

    response_get_range(const response_get_range &);
};
}
#endif //TAIR_PACKET_GET_RANGE_PACKET_H

