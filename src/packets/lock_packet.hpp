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

#ifndef TAIR_PACKET_LOCK_PACKET_H
#define TAIR_PACKET_LOCK_PACKET_H

#include "put_packet.hpp"

namespace tair {
class request_lock : public base_packet {
public:
    request_lock() : area(0), lock_type(LOCK_VALUE), data(NULL) {
        setPCode(TAIR_REQ_LOCK_PACKET);
    }

    request_lock(request_lock &packet) : base_packet(packet), area(0), lock_type(LOCK_VALUE), data(NULL) {
        setPCode(TAIR_REQ_LOCK_PACKET);
        area = packet.area;
        lock_type = packet.lock_type;
        key.clone(packet.key);
        if (PUT_AND_LOCK_VALUE == lock_type) {
            if (NULL == data) {
                data = new data_entry();
            }
            data->clone(*(packet.data));
        }
    }

    ~request_lock() {
        if (data != NULL) {
            delete data;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    bool encode(DataBuffer *output) {
        output->writeInt16(area);
        output->writeInt32(lock_type);
        key.encode(output);
        if (PUT_AND_LOCK_VALUE == lock_type) {
            if (NULL == data) {
                return false;
            }
            data->encode_with_compress(output);
        }
        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = 2 + 4 + key.encoded_size();
        if ((PUT_AND_LOCK_VALUE == lock_type) && (data != NULL))
            total += data->encoded_size();
        return total + 16; //header 16 btyes
    }

    uint16_t ns() const {
        return (uint16_t) area;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        // int16    area          2B    (must)
        // int32    lock_type     4B    (must)
        // --------------------------
        // total                  6B
        uint16_t area_i = 0;
        uint32_t lock_type_i = 0;
        if (input->readInt16(&area_i) == false ||
            input->readInt32(&lock_type_i) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
        }
        area = area_i;
        lock_type = static_cast<LockType>(lock_type_i);

#if TAIR_MAX_AREA_COUNT < 65536
        if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
            log_warn("area overflow: area %d, lock_type %d", area, lock_type);
            return false;
        }
#endif

        if (!key.decode(input)) {
            log_warn("key decode fail: area %d, lock_type %d", area, lock_type);
            return false;
        }

        if (PUT_AND_LOCK_VALUE == lock_type) {
            if (NULL == data) {
                data = new data_entry();
            }

            if (!data->decode(input)) {
                log_warn("data decode fail: area %d, lock_type %d", area, lock_type);
                return false;
            }
        }

        return true;
    }

public:
    int area;
    LockType lock_type;
    data_entry key;
    data_entry *data;
};

} /* tair */
#endif

