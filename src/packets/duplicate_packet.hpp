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

#ifndef TAIR_PACKET_DUPLICATE_PACKET_H
#define TAIR_PACKET_DUPLICATE_PACKET_H

#include "base_packet.hpp"
#include "common/record.hpp"

namespace tair {
class request_duplicate : public base_packet {
public:

    request_duplicate() {
        setPCode(TAIR_REQ_DUPLICATE_PACKET);
        server_flag = TAIR_SERVERFLAG_DUPLICATE;
        bucket_number = -1;
        packet_id = 0;
        area = 0;
    }


    request_duplicate(const request_duplicate &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_DUPLICATE_PACKET);
        server_flag = packet.server_flag;
        key.clone(packet.key);
        data.clone(packet.data);
        bucket_number = packet.bucket_number;
        packet_id = packet.packet_id;
        area = packet.area;
    }


    ~request_duplicate() {
    }


    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        key.encode(output);
        data.encode(output);
        output->writeInt32(packet_id);
        output->writeInt32(bucket_number);

        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;
        size_t total = 1 + 2 + key.encoded_size() + data.encoded_size() + 4 + 4;
        return total + 16;// header 16 bytes
    }

    uint16_t ns() const {
        return (uint16_t) area;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        if (header->_dataLen < 15) {
            log_warn("buffer data too few.");
            return false;
        }
        server_flag = input->readInt8();
        area = input->readInt16();
        key.decode(input);
        data.decode(input);
        packet_id = input->readInt32();
        bucket_number = input->readInt32();
        return true;
    }

public:
    data_entry key;
    data_entry data;
    uint32_t packet_id;
    int bucket_number;
    int area;
};

class request_batch_duplicate : public base_packet {
public:
    request_batch_duplicate() {
        setPCode(TAIR_REQ_BATCH_DUPLICATE_PACKET);
        server_flag = TAIR_SERVERFLAG_DUPLICATE,   // 1
                bucket = -1;
        packet_id = 0;
        records = NULL;
    }

    ~request_batch_duplicate() {
        if (records != NULL) {
            std::vector<common::Record *>::iterator iter;
            for (iter = records->begin(); iter != records->end(); iter++) {
                delete (*iter);
            }
            delete records;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    virtual size_t size() const {
        if (getDataLen() != 0) return getDataLen() + 16;

        size_t total = 1 + 4 + 4 + 4 + 16;
        std::vector<common::Record *>::iterator iter;
        for (iter = records->begin(); iter != records->end(); iter++) {
            common::Record *rh = *iter;
            if (rh->operator_type_ == TAIR_REMOTE_SYNC_TYPE_PUT) {
                if (rh->type_ == KeyValue) {
                    KVRecord *record = static_cast<KVRecord *>(rh);
                    total += 2 + 2 + record->key_->encoded_size() + record->value_->encoded_size();
                } else {
                    log_error("unknown sub record type %d", rh->type_);
                    break;
                }
            } else if (rh->operator_type_ == TAIR_REMOTE_SYNC_TYPE_DELETE) {
                if (rh->type_ == KeyValue) {
                    KVRecord *record = static_cast<KVRecord *>(rh);
                    total += 2 + 2 + record->key_->encoded_size();
                } else {
                    log_error("unknown sub record type %d", rh->type_);
                    return false;
                }
            } else {
                log_error("unknown operator type %d", rh->operator_type_);
                break;
            }
        }
        return total;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt32(bucket);
        output->writeInt32(packet_id);
        output->writeInt32(records->size());
        std::vector<common::Record *>::iterator iter;
        for (iter = records->begin(); iter != records->end(); iter++) {
            common::Record *rh = *iter;
            // int16 type
            // int16 optype
            // data_entry key
            // data_entry value 'maybe'
            if (rh->operator_type_ == TAIR_REMOTE_SYNC_TYPE_PUT) {
                if (rh->type_ == KeyValue) {
                    KVRecord *record = static_cast<KVRecord *>(rh);
                    output->writeInt16(static_cast<int16_t>(record->type_));
                    output->writeInt16(static_cast<int16_t>(record->operator_type_));
                    assert(record->key_->has_merged == true);
                    record->key_->encode(output);
                    record->value_->encode(output);
                } else {
                    log_error("unknown sub record type %d", rh->type_);
                    return false;
                }
            } else if (rh->operator_type_ == TAIR_REMOTE_SYNC_TYPE_DELETE) {
                if (rh->type_ == KeyValue) {
                    KVRecord *record = static_cast<KVRecord *>(rh);
                    output->writeInt16(static_cast<int16_t>(record->type_));
                    output->writeInt16(static_cast<int16_t>(record->operator_type_));
                    assert(record->key_->has_merged == true);
                    record->key_->encode(output);
                } else {
                    log_error("unknown sub record type %d", rh->type_);
                    return false;
                }
            } else {
                log_error("unknown operator type %d", rh->operator_type_);
                return false;
            }
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        uint32_t count = 0;
        if (input->readInt8(&server_flag) == false ||
            input->readInt32((uint32_t *) &bucket) == false ||
            input->readInt32(&packet_id) == false ||
            input->readInt32(&count) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
        }

        records = new std::vector<common::Record *>();
        for (uint32_t i = 0; i < count; i++) {
            int16_t type;
            if (input->readInt16((uint16_t *) &type) == false) {
                log_warn("buffer data too few, buffer length %d, read type fail", header->_dataLen);
                return false;
            }

            if (type == KeyValue) {
                int16_t optype;
                if (input->readInt16((uint16_t *) &optype) == false) {
                    log_warn("buffer data too few, buffer length %d, read optype fail", header->_dataLen);
                    return false;
                }
                if (optype == TAIR_REMOTE_SYNC_TYPE_PUT) {
                    data_entry *key = new data_entry();
                    data_entry *value = new data_entry();
                    if (key->decode(input) == false) {
                        log_warn("buffer data too few, buffer length %d, decode key fail", header->_dataLen);
                        delete key;
                        delete value;
                        return false;
                    }
                    if (value->decode(input) == false) {
                        log_warn("buffer data too few, buffer length %d, decode value fail", header->_dataLen);
                        delete key;
                        delete value;
                        return false;
                    }
                    assert(key->has_merged == true);
                    records->push_back(new KVRecord(static_cast<TairRemoteSyncType>(optype), bucket, key, value));
                } else if (optype == TAIR_REMOTE_SYNC_TYPE_DELETE) {
                    data_entry *key = new data_entry();
                    if (key->decode(input) == false) {
                        log_warn("buffer deata too few, buffer length %d, decode key fail", header->_dataLen);
                        delete key;
                        return false;
                    }
                    assert(key->has_merged == true);
                    records->push_back(new KVRecord(static_cast<TairRemoteSyncType>(optype), bucket, key, NULL));
                }
            } else {
                log_warn("unsupport type %d", type);
                return false;
            }
        }

        return true;
    }

public:
    int32_t bucket;
    uint32_t packet_id;
    std::vector<Record *> *records;
private:
    request_batch_duplicate(const request_batch_duplicate &packet);
};

class response_duplicate : public base_packet {
public:
    response_duplicate() {
        setPCode(TAIR_RESP_DUPLICATE_PACKET);
        server_flag = 1;
        bucket_id = -1;
    }

    ~response_duplicate() {
    }

    virtual base_packet::Type get_type() {
        return base_packet::RESP_COMMON;
    }

    bool encode(DataBuffer *output) {
        output->writeInt32(packet_id);
        output->writeInt64(server_id);
        output->writeInt32(bucket_id);
        return true;
    }

    virtual size_t size() const {
        return 4 + 8 + 4 + 16; //header 16 bytes
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        packet_id = input->readInt32();
        server_id = input->readInt64();
        bucket_id = input->readInt32();
        return true;
    }

public:
    uint32_t packet_id;
    int32_t bucket_id;
    uint64_t server_id;
private:
    response_duplicate(const response_duplicate &);
};
}
#endif
