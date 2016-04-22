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

#ifndef TAIR_PREFIX_PUTS_PACKET_H
#define TAIR_PREFIX_PUTS_PACKET_H

#include "base_packet.hpp"

namespace tair {
class request_prefix_puts : public base_packet {
public:
    request_prefix_puts() {
        setPCode(TAIR_REQ_PREFIX_PUTS_PACKET);
        area = 0;
        server_flag = 0;
        key_count = 0;
        packet_id = 0;
        pkey = NULL;
        kvmap = NULL;
    }

    request_prefix_puts(const request_prefix_puts &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_PREFIX_PUTS_PACKET);
        area = packet.area;
        key_count = packet.key_count;
        packet_id = packet.packet_id;
        pkey = new data_entry(*packet.pkey);
        kvmap = new tair_keyvalue_map();
        tair_keyvalue_map::const_iterator it = packet.kvmap->begin();
        while (it != packet.kvmap->end()) {
            data_entry *key = new data_entry(*it->first);
            data_entry *value = new data_entry(*it->second);
            kvmap->insert(make_pair(key, value));
            ++it;
        }
    }

    ~request_prefix_puts() {
        if (pkey != NULL) {
            delete pkey;
            pkey = NULL;
        }
        if (kvmap != NULL) {
            tair_keyvalue_map::iterator it = kvmap->begin();
            while (it != kvmap->end()) {
                data_entry *key = it->first;
                data_entry *value = it->second;
                ++it;
                delete key;
                delete value;
            }
            delete kvmap;
            kvmap = NULL;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    virtual const data_entry *pick_key() const {
        if (key_count == 0) {
            return NULL;
        }
        assert(kvmap != NULL && !kvmap->empty());
        return kvmap->begin()->first;
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
                        "area: %d, key_count: %d, packet size: %lu, server_flag:%d\n\t"
                        "key: %s\n",
                name(getPCode()), getPCode(), area, key_count, size(), server_flag, ascii_key);
    }

    void swap(request_prefix_puts &rhs) {
        std::swap(server_flag, rhs.server_flag);
        std::swap(area, rhs.area);
        std::swap(pkey, rhs.pkey);
        std::swap(key_count, rhs.key_count);
        std::swap(kvmap, rhs.kvmap);
        std::swap(packet_id, rhs.packet_id);
    }

    bool encode(DataBuffer *output) {
        if (pkey == NULL || kvmap == NULL) {
            log_error("packet not complete!");
            return false;
        }
        output->writeInt8(server_flag);
        output->writeInt16(area);
        pkey->encode(output);
        output->writeInt32(key_count);
        tair_keyvalue_map::iterator itr = kvmap->begin();
        while (itr != kvmap->end()) {
            itr->first->encode(output);
            itr->second->encode(output);
            ++itr;
        }
        if (server_flag != TAIR_SERVERFLAG_CLIENT) {
            output->writeInt32(packet_id);
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  int8    server_flag      1B    (must)
        //  int16   area             2B    (must)
        //  ----------------------------
        //  total                    3B
        if (input->readInt8(&server_flag) == false ||
            input->readInt16(&area) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
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

        if (key_count > TAIR_MAX_KEY_COUNT) {
            log_warn("too large key_count, "
                             "server_flag: %x, area: %d, key_count: %d",
                     server_flag, area, key_count);
            return false;
        }

        if (key_count > 500) {
            log_warn("such large key_count, "
                             "server_flag: %x, area: %d, key_count: %d",
                     server_flag, area, key_count);
        }

        kvmap = new tair_keyvalue_map();
        for (uint32_t i = 0; i < key_count; ++i) {
            data_entry *key = new data_entry();
            data_entry *value = new data_entry();

            if (!key->decode(input)) {
                delete key;
                delete value;

                log_warn("key decode failed, "
                                 "server_flag %x, area %d, key_count %d, index %u",
                         server_flag, area, key_count, i);
                return false;
            }

            if (!value->decode(input)) {
                delete key;
                delete value;

                log_warn("value decode failed, "
                                 "server_flag %x, area %d, key_count %d, index %u",
                         server_flag, area, key_count, i);
                return false;
            }

            if (kvmap->insert(make_pair(key, value)).second == false) {
                delete key;
                delete value;
            }
        }

        if (key_count != kvmap->size()) {
            log_warn("duplicate key received, omitted");
            key_count = kvmap->size();
        }

        if (server_flag != TAIR_SERVERFLAG_CLIENT) {
            if (input->readInt32(&packet_id) == false) {
                log_warn("packet_id decode failed, "
                                 "server_flag %x, area %d, key_count %d",
                         server_flag, area, key_count);
                return false;
            }
        } else {
            packet_id = 0;
        }

        return true;
    }

    void set_pkey(char *key, int size) {
        if (pkey != NULL) {
            delete pkey;
        }
        pkey = new data_entry(key, size);
    }

    void set_pkey(data_entry *key, bool copy = false) {
        if (pkey != NULL) {
            delete pkey;
        }
        if (copy) {
            pkey = new data_entry(*key);
        } else {
            pkey = key;
        }
    }

    void add_key_value(data_entry *key, data_entry *value, bool copy = false) {
        if (kvmap == NULL) {
            kvmap = new tair_keyvalue_map();
        }
        if (copy) {
            key = new data_entry(*key);
            value = new data_entry(*value);
        }
        if (!kvmap->insert(make_pair(key, value)).second) {
            if (copy) {
                delete key;
                delete value;
            }
        } else {
            ++key_count;
        }
    }

    //@override
    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = 1 + 2 + 4;
        if (pkey != NULL)
            total += pkey->encoded_size();
        if (kvmap != NULL) {
            tair_keyvalue_map::iterator itr = kvmap->begin();
            while (itr != kvmap->end()) {
                total += itr->first->encoded_size();
                total += itr->second->encoded_size();
                ++itr;
            }
        }
        if (server_flag != TAIR_SERVERFLAG_CLIENT)
            total += 4;

        return total + 16; //header 16 bytes
    }

    //@override
    uint16_t ns() const {
        return area;
    }

public:
    uint16_t area;
    uint32_t key_count;
    uint32_t packet_id;
    data_entry *pkey;
    tair_keyvalue_map *kvmap;
private:
    request_prefix_puts &operator=(const request_prefix_puts &);
};
}

#endif
