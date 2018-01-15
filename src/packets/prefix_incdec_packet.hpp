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

#ifndef TAIR_PREFIX_INCDEC_PACKET_H
#define TAIR_PREFIX_INCDEC_PACKET_H

#include "base_packet.hpp"
#include "data_entry.hpp"
#include "counter_wrapper.hpp"

namespace tair {
class request_prefix_incdec : public base_packet {
public:
    request_prefix_incdec() {
        setPCode(TAIR_REQ_PREFIX_INCDEC_PACKET);
        server_flag = 0;
        area = 0;
        pkey = NULL;
        key_count = 0;
        packet_id = 0;
        key_counter_map = NULL;
    }

    request_prefix_incdec(const request_prefix_incdec &rhs) {
        setPCode(TAIR_REQ_PREFIX_INCDEC_PACKET);
        server_flag = rhs.server_flag;
        area = rhs.area;
        key_count = rhs.key_count;
        packet_id = rhs.packet_id;
        if (rhs.pkey != NULL) {
            pkey = new data_entry(*rhs.pkey);
        } else {
            pkey = NULL;
        }

        if (rhs.key_counter_map != NULL) {
            key_counter_map = new key_counter_map_t;
            key_counter_map_t::const_iterator it = rhs.key_counter_map->begin();
            while (it != rhs.key_counter_map->end()) {
                data_entry *key = new data_entry(*it->first);
                counter_wrapper *wrapper = new counter_wrapper(*it->second);
                key_counter_map->insert(make_pair(key, wrapper));
                ++it;
            }
        } else {
            key_counter_map = NULL;
        }
    }

    request_prefix_incdec &operator=(const request_prefix_incdec &rhs) {
        if (this != &rhs) {
            base_packet::operator=(rhs);
            this->~request_prefix_incdec();
            server_flag = rhs.server_flag;
            area = rhs.area;
            key_count = rhs.key_count;
            packet_id = rhs.packet_id;
            pkey = new data_entry(*rhs.pkey);
            key_counter_map = new key_counter_map_t;
            key_counter_map_t::const_iterator it = rhs.key_counter_map->begin();
            while (it != rhs.key_counter_map->end()) {
                data_entry *key = new data_entry(*it->first);
                counter_wrapper *wrapper = new counter_wrapper(*it->second);
                key_counter_map->insert(make_pair(key, wrapper));
                ++it;
            }
        }
        return *this;
    }

    ~request_prefix_incdec() {
        if (pkey != NULL) {
            delete pkey;
            pkey = NULL;
        }
        if (key_counter_map != NULL) {
            key_counter_map_t::iterator it = key_counter_map->begin();
            while (it != key_counter_map->end()) {
                data_entry *key = it->first;
                counter_wrapper *wrapper = it->second;
                ++it;
                delete key;
                delete wrapper;
            }
            delete key_counter_map;
            key_counter_map = NULL;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    bool encode(DataBuffer *output) {
        if (key_count == 0) {
            return false; //~ packet's empty
        }
        output->writeInt8(server_flag);
        output->writeInt16(area);
        pkey->encode(output);
        output->writeInt32(key_count);
        key_counter_map_t::const_iterator it = key_counter_map->begin();
        while (it != key_counter_map->end()) {
            it->first->encode(output);
            it->second->encode(output);
            ++it;
        }
        if (server_flag != TAIR_SERVERFLAG_CLIENT) {
            output->writeInt32(packet_id);
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  int8   server_flag     1B    (must)
        //  int16  area            2B    (must)
        //  --------------------------
        //  total                  3B
        if (input->readInt8(&server_flag) == false ||
            input->readInt16(&area) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false; //~ packet's broken
        }

#if TAIR_MAX_AREA_COUNT < 65536
        if (area >= TAIR_MAX_AREA_COUNT) {
            log_warn("area overflow, server_flag %x, area %d", server_flag, area);
            return false;
        }
#endif

        pkey = new data_entry();
        if (!pkey->decode(input)) {
            log_warn("pkey decode failed, server_flag %x, area %d", server_flag, area);
            return false;
        }

        if (input->readInt32(&key_count) == false) {
            log_warn("key_count decode failed, server_flag %x, area %d", server_flag, area);
            return false;
        }

        if (key_count > 10000) {
            log_error("prefix_incdec_packet, key_count: %u, area: %u, server_flag: 0x%x",
                      key_count, area, server_flag);
            return false;
        }
        key_counter_map = new key_counter_map_t;
        for (uint32_t i = 0; i < key_count; ++i) {
            data_entry *key = new data_entry();
            if (!key->decode(input)) {
                delete key;
                key = NULL;

                log_warn("key decode failed, "
                                 "server_flag %x, area %d, key_count %d, index %u",
                         server_flag, area, key_count, i);
                return false;
            }

            counter_wrapper *wrapper = new counter_wrapper;
            if (!wrapper->decode(input)) {
                delete key;
                key = NULL;
                delete wrapper;
                wrapper = NULL;

                log_warn("wrapper decode failed, "
                                 "server_flag %x, area %d, key_count %d, index %u",
                         server_flag, area, key_count, i);
                return false;
            }

            if (key_counter_map->insert(make_pair(key, wrapper)).second == false) {
                delete key;
                delete wrapper;
            }
        }

        if (key_count != key_counter_map->size()) {
            log_warn("duplicate key received, omitted");
            key_count = key_counter_map->size();
        }

        if (server_flag != TAIR_SERVERFLAG_CLIENT) {
            if (input->readInt32(&packet_id) == false) {
                log_warn("packet_id decode failed, "
                                 "server_flag %x, area %d, key_count %d",
                         server_flag, area, key_count);
                return false;
            }
        }

        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = 1 + 2 + 4;
        if (key_count != 0) {
            total += pkey->encoded_size();
            key_counter_map_t::const_iterator it = key_counter_map->begin();
            while (it != key_counter_map->end()) {
                total += it->first->encoded_size();
                total += it->second->encoded_size();
                ++it;
            }
        }
        if (server_flag != TAIR_SERVERFLAG_CLIENT)
            total += 4;
        return total + 16; //header 16 bytes
    }

    uint16_t ns() const {
        return area;
    }

    void add_key_counter(data_entry *key, counter_wrapper *wrapper, bool copy = false) {
        if (copy) {
            key = new data_entry(*key);
            wrapper = new counter_wrapper(*wrapper);
        }
        if (key_counter_map == NULL) {
            key_counter_map = new key_counter_map_t;
        }
        key_counter_map->insert(make_pair(key, wrapper));
    }

    void set_pkey(data_entry *pkey, bool copy = false) {
        if (copy) {
            pkey = new data_entry(*pkey);
        }
        if (this->pkey != NULL) {
            delete this->pkey;
        }
        this->pkey = pkey;
    }

    data_entry *get_pkey() const {
        return pkey;
    }

    void swap(request_prefix_incdec &rhs) {
        std::swap(area, rhs.area);
        std::swap(pkey, rhs.pkey);
        std::swap(key_count, rhs.key_count);
        std::swap(key_counter_map, rhs.key_counter_map);
        std::swap(packet_id, rhs.packet_id);
    }

public:
    typedef __gnu_cxx::hash_map<data_entry *, counter_wrapper *,
            tair::common::data_entry_hash, tair::common::data_entry_equal_to> key_counter_map_t;
    uint16_t area;
    data_entry *pkey;
    uint32_t key_count;
    key_counter_map_t *key_counter_map;
    uint32_t packet_id;
};

class response_prefix_incdec : public base_packet {
public:
    response_prefix_incdec() {
        setPCode(TAIR_RESP_PREFIX_INCDEC_PACKET);
        config_version = 0;
        code = 0;
        nsuccess = 0;
        nfailed = 0;
        server_flag = 0;
        bucket_id = 0;
        server_id = 0L;
        packet_id = 0;
        success_key_value_map = NULL;
        failed_key_code_map = NULL;
    }

    response_prefix_incdec(const response_prefix_incdec &rhs) {
        setPCode(TAIR_RESP_PREFIX_INCDEC_PACKET);
        copy(rhs);
    }

    response_prefix_incdec &operator=(const response_prefix_incdec &rhs) {
        if (this != &rhs) {
            this->~response_prefix_incdec();
            copy(rhs);
        }
        return *this;
    }

    ~response_prefix_incdec() {
        key_code_map_t::iterator it;

        if (success_key_value_map != NULL) {
            tair::common::defree(*success_key_value_map);
            delete success_key_value_map;
            success_key_value_map = NULL;
        }

        if (failed_key_code_map != NULL) {
            tair::common::defree(*failed_key_code_map);
            delete failed_key_code_map;
            failed_key_code_map = NULL;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(config_version);
        output->writeInt32(code);
        output->writeInt32(nsuccess);
        if (server_flag != TAIR_SERVERFLAG_CLIENT) {
            output->writeInt32(bucket_id);
            output->writeInt64(server_id);
            output->writeInt32(packet_id);
        }
        key_code_map_t::iterator it;
        if (nsuccess > 0) {
            assert(success_key_value_map != NULL);
            it = success_key_value_map->begin();
            while (it != success_key_value_map->end()) {
                it->first->encode(output);
                output->writeInt32(it->second);
                ++it;
            }
        }
        output->writeInt32(nfailed);
        if (nfailed > 0) {
            assert(failed_key_code_map != NULL);
            it = failed_key_code_map->begin();
            while (it != failed_key_code_map->end()) {
                it->first->encode(output);
                output->writeInt32(it->second);
                ++it;
            }
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 16) {
            return false; //~ packet's broken
        }
        config_version = input->readInt32();
        code = input->readInt32();

        nsuccess = input->readInt32();
        bucket_id = input->readInt32();
        server_id = input->readInt64();
        packet_id = input->readInt32();
        if (nsuccess > 0) {
            success_key_value_map = new key_code_map_t;
            for (uint32_t i = 0; i < nsuccess; ++i) {
                data_entry *key = new data_entry();
                key->decode(input);
                int value = input->readInt32();
                success_key_value_map->insert(make_pair(key, value));
            }
        }

        nfailed = input->readInt32();
        if (nfailed > 0) {
            failed_key_code_map = new key_code_map_t;
            for (uint32_t i = 0; i < nfailed; ++i) {
                data_entry *key = new data_entry();
                key->decode(input);
                int code = input->readInt32();
                failed_key_code_map->insert(make_pair(key, code));
            }
        }

        return true;
    }

    virtual size_t size() const {
        if (UNLIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = 4 + 4 + 4;
        if (server_flag != TAIR_SERVERFLAG_CLIENT)
            total += 4 + 8 + 4;
        key_code_map_t::iterator it;
        if (nsuccess > 0) {
            assert(success_key_value_map != NULL);
            it = success_key_value_map->begin();
            while (it != success_key_value_map->end()) {
                total += it->first->encoded_size() + 4;
                ++it;
            }
        }
        total += 4;
        if (nfailed > 0) {
            assert(failed_key_code_map != NULL);
            it = failed_key_code_map->begin();
            while (it != failed_key_code_map->end()) {
                total += it->first->encoded_size() + 4;
                ++it;
            }
        }
        return total + 16; //header 16 bytes
    }

    void add_key_value(data_entry *key, int32_t value, bool copy = false) {
        if (copy) {
            key = new data_entry(*key);
        }
        if (success_key_value_map == NULL) {
            success_key_value_map = new key_code_map_t;
        }
        success_key_value_map->insert(make_pair(key, value));
        ++nsuccess;
    }

    void add_key_code(data_entry *key, int32_t code, bool copy = false) {
        if (copy) {
            key = new data_entry(*key);
        }
        if (failed_key_code_map == NULL) {
            failed_key_code_map = new key_code_map_t;
        }
        failed_key_code_map->insert(make_pair(key, code));
        ++nfailed;
    }

    virtual void set_code(int code) {
        this->code = code;
    }

    virtual bool failed() const {
        return code != TAIR_RETURN_SUCCESS;
    }

private:
    void copy(const response_prefix_incdec &rhs) {
        config_version = rhs.config_version;
        code = rhs.code;
        nsuccess = rhs.nsuccess;
        nfailed = rhs.nfailed;
        bucket_id = rhs.bucket_id;
        server_id = rhs.server_id;
        packet_id = rhs.packet_id;

        key_code_map_t::const_iterator it;
        if (rhs.success_key_value_map != NULL) {
            success_key_value_map = new key_code_map_t;
            it = rhs.success_key_value_map->begin();
            while (it != rhs.success_key_value_map->end()) {
                data_entry *key = new data_entry(*it->first);
                success_key_value_map->insert(make_pair(key, it->second));
                ++it;
            }
        } else {
            success_key_value_map = NULL;
        }

        if (rhs.failed_key_code_map != NULL) {
            failed_key_code_map = new key_code_map_t;
            it = rhs.failed_key_code_map->begin();
            while (it != rhs.failed_key_code_map->end()) {
                data_entry *key = new data_entry(*it->first);
                failed_key_code_map->insert(make_pair(key, it->second));
                ++it;
            }
        } else {
            failed_key_code_map = NULL;
        }
    }

public:

    uint32_t config_version;
    int32_t code;
    uint32_t nsuccess;
    uint32_t nfailed;
    int32_t bucket_id;
    uint64_t server_id;
    uint32_t packet_id;
    key_code_map_t *success_key_value_map;
    key_code_map_t *failed_key_code_map;
};
}

#endif
