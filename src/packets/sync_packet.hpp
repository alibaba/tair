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

#ifndef __TAIR_PACKETS_SYNC_PACKET_H__
#define __TAIR_PACKETS_SYNC_PACKET_H__

#include "base_packet.hpp"
#include "common/record.hpp"
#include <vector>

namespace tair {
class request_sync : public base_packet {
public:
    request_sync() {
        setPCode(TAIR_REQ_SYNC_PACKET);
        server_flag = TAIR_SERVERFLAG_RSYNC;
        extflag = 0;
        bucket = 0;
        records = NULL;
        should_release = false;
    }

    ~request_sync() {
        // who create who destroy
        if (should_release && records != NULL) {
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

        size_t total = 1 + 2 + 4 + 4 + 16;
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
        //  int8    server_flag   1B      (must)
        //  int16   extflag       2B      (must default 0)
        //  int32   bucket        4B      (must)
        //  int32   count         4B
        //  -------------------------
        //  total                 17B
        output->writeInt8(server_flag);
        output->writeInt16(extflag);
        output->writeInt32(bucket);
        if (records == NULL) {
            output->writeInt32(0);
        } else {
            output->writeInt32(static_cast<int32_t>(records->size()));
            std::vector<common::Record *> &rh = *records;
            for (size_t i = 0; i < rh.size(); i++) {
                // int16 type
                // int16 optype
                // data_entry key
                // data_entry value 'maybe'
                if (rh[i]->operator_type_ == TAIR_REMOTE_SYNC_TYPE_PUT) {
                    if (rh[i]->type_ == KeyValue) {
                        KVRecord *record = static_cast<KVRecord *>(rh[i]);
                        output->writeInt16(static_cast<int16_t>(record->type_));
                        output->writeInt16(static_cast<int16_t>(record->operator_type_));
                        assert(record->key_->has_merged == true);
                        record->key_->encode(output);
                        record->value_->encode(output);
                    } else {
                        log_error("unknown sub record type %d", rh[i]->type_);
                        return false;
                    }
                } else if (rh[i]->operator_type_ == TAIR_REMOTE_SYNC_TYPE_DELETE) {
                    if (rh[i]->type_ == KeyValue) {
                        KVRecord *record = static_cast<KVRecord *>(rh[i]);
                        output->writeInt16(static_cast<int16_t>(record->type_));
                        output->writeInt16(static_cast<int16_t>(record->operator_type_));
                        assert(record->key_->has_merged == true);
                        record->key_->encode(output);
                    } else {
                        log_error("unknown sub record type %d", rh[i]->type_);
                        return false;
                    }
                } else {
                    log_error("unknown operator type %d", rh[i]->operator_type_);
                    return false;
                }
            }
        }
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        uint32_t count = 0;
        if (input->readInt8(&server_flag) == false ||
            input->readInt16((uint16_t *) &extflag) == false ||
            input->readInt32((uint32_t *) &bucket) == false ||
            input->readInt32(&count) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
        }

        records = new std::vector<common::Record *>();
        should_release = true;
        if (extflag == 0) {
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
                        if (is_meta_flag_sending_unit(key->data_meta.flag)) {
                            unset_meta_flag_sending_unit(key->data_meta.flag);
                            set_meta_flag_unit(key->data_meta.flag);
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
                        if (is_meta_flag_sending_unit(key->data_meta.flag)) {
                            unset_meta_flag_sending_unit(key->data_meta.flag);
                            set_meta_flag_unit(key->data_meta.flag);
                        }
                        assert(key->has_merged == true);
                        records->push_back(new KVRecord(static_cast<TairRemoteSyncType>(optype), bucket, key, NULL));
                    }
                } else {
                    log_warn("unsupport type %d", type);
                    return false;
                }
            }
        }
        return true;
    }

public:
    /* use for extensible properties for sync packet, for example compression, now just 0 */
    int16_t extflag;
    int32_t bucket;
    std::vector<common::Record *> *records; // who create who destroy
private:
    bool should_release;

    request_sync(const request_sync &packet);
};
}

#endif
