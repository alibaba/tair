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

#ifndef TAIR_PACKETS_MUPDATE_H
#define TAIR_PACKETS_MUPDATE_H

#include "base_packet.hpp"
#include "dataserver/update_log.hpp"

namespace tair {
class operation_record {
public:
    operation_record() {
        operation_type = 0;
        key = NULL;
        value = NULL;
    }

    operation_record(operation_record &rec) {
        operation_type = rec.operation_type;
        key = new data_entry(*(rec.key));
        if (operation_type == (uint8_t) SN_PUT) {
            value = new data_entry(*(rec.value));
        } else {
            value = NULL;
        }
    }

    ~operation_record() {
        delete key;
        delete value;
        key = NULL;
        value = NULL;
    }

public:
    uint8_t operation_type;
    data_entry *key;
    data_entry *value;
};

typedef vector<operation_record *> tair_operc_vector;

class request_mupdate : public base_packet {

public:
    request_mupdate() {
        setPCode(TAIR_REQ_MUPDATE_PACKET);
        server_flag = TAIR_SERVERFLAG_MIGRATE;
        count = 0;
        len = 8;
        alloc = true;
        key_and_values = NULL;
    }

    request_mupdate(const request_mupdate &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_MUPDATE_PACKET);
        server_flag = packet.server_flag;
        count = packet.count;
        len = packet.len;
        alloc = packet.alloc;
        key_and_values = NULL;
        if (packet.key_and_values != NULL) {
            key_and_values = new tair_operc_vector();
            tair_operc_vector::iterator it;
            for (it = packet.key_and_values->begin(); it != packet.key_and_values->end(); ++it) {
                operation_record *oprec = new operation_record(**it);
                key_and_values->push_back(oprec);
            }
        }
    }

    ~request_mupdate() {
        if (key_and_values != NULL && alloc) {
            tair_operc_vector::iterator it;
            for (it = key_and_values->begin(); it != key_and_values->end(); ++it) {
                delete (*it);
            }

            //key_and_values->clear();
            delete key_and_values;
            key_and_values = NULL;
        }
    }

    virtual base_packet::Type get_type() {
        // use for migrate, so it is special
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt32(count);
        if (key_and_values != NULL) {
            tair_operc_vector::iterator it;
            for (it = key_and_values->begin(); it != key_and_values->end(); ++it) {
                operation_record *oprec = (*it);
                output->writeInt8(oprec->operation_type);
                oprec->key->encode(output);
                log_debug("encodekey is %d", oprec->key->has_merged);
                if (oprec->operation_type == 1) {
                    oprec->value->encode(output);
                }
            }
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 8) {
            log_warn("buffer data too few.");
            return false;
        }
        server_flag = input->readInt8();
        count = input->readInt32();

        if (count > 0) {
            key_and_values = new tair_operc_vector();
            for (uint i = 0; i < count; i++) {
                operation_record *oprec = new operation_record();
                oprec->operation_type = input->readInt8();
                data_entry *key = new data_entry();
                key->decode(input);
                log_debug("decodekey is %d", key->has_merged);
                oprec->key = key;
                if (oprec->operation_type == 1) {
                    data_entry *value = new data_entry();
                    value->decode(input);
                    oprec->value = value;
                } else {
                    oprec->value = NULL;
                }
                key_and_values->push_back(oprec);
            }
        }
        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16; // header 16 bytes

        size_t total = 1 + 4;
        if (key_and_values != NULL) {
            tair_operc_vector::iterator it;
            for (it = key_and_values->begin(); it != key_and_values->end(); ++it) {
                operation_record *oprec = (*it);
                total += 1; // type
                total += oprec->key->encoded_size();
                if (1 == oprec->operation_type)
                    total += oprec->value->encoded_size();
            }
        }
        return total + 16; // header 16 bytes
    }

    //add put key and data
    bool add_put_key_data(const data_entry &key, const data_entry &data) {
        uint temp = len + key.get_size() + 1 + data.get_size();
        if (temp > MAX_MUPDATE_PACKET_SIZE && count > 0) {
            return false;
        }
        if (key_and_values == NULL) {
            key_and_values = new tair_operc_vector();
        }

        operation_record *oprec = new operation_record();
        oprec->operation_type = 1;
        oprec->key = new data_entry();
        //log_debug("2addputkeyis %d",key.m_isMerged);
        oprec->key->clone(key);
        //log_debug("addputkeyis %d", oprec->key->m_isMerged);
        oprec->value = new data_entry();
        oprec->value->clone(data);
        key_and_values->push_back(oprec);
        len += key.get_size() + 1;
        len += data.get_size();
        count++;
        return true;
    }

    //add del key
    bool add_del_key(const data_entry &key) {
        uint temp = len + key.get_size() + 1;
        if (temp > MAX_MUPDATE_PACKET_SIZE) {
            return false;
        }
        if (key_and_values == NULL) {
            key_and_values = new tair_operc_vector();
        }
        operation_record *oprec = new operation_record();
        oprec->key = new data_entry();
        oprec->key->clone(key);
        //log_debug("addkeyis %d", oprec->key->m_isMerged);
        oprec->operation_type = 2;
        oprec->value = NULL;
        key_and_values->push_back(oprec);
        count++;
        len += key.get_size() + 1;
        return true;
    }

public:
    uint32_t count;
    uint32_t len;
    bool alloc;
    tair_operc_vector *key_and_values;
};
}
#endif
