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

#ifndef TAIR_PACKET_UNIQ_REMOVE_PACKET_H
#define TAIR_PACKET_UNIQ_REMOVE_PACKET_H

#include "base_packet.hpp"

namespace tair {
class request_uniq_remove : public base_packet {
public:

    request_uniq_remove() {
        setPCode(TAIR_REQ_UNIQ_REMOVE_PACKET);
        area = 0;
        bucket = 0;
        key = NULL;
    }

    request_uniq_remove(const request_uniq_remove &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_UNIQ_REMOVE_PACKET);
        area = packet.area;
        bucket = packet.bucket;
        key = NULL;
        if (packet.key != NULL) {
            key = new data_entry();
            key->clone(*packet.key);
        }
    }

    virtual ~request_uniq_remove() {
        if (key) {
            delete key;
            key = NULL;
        }
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_SPECIAL;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        output->writeInt32(bucket);
        key->encode(output);

        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  int8   server_flag   1B     (must)
        //  int16  area          2B     (must)
        //  int32  bucket        4B     (must)
        //  ------------------------
        //  total                7B
        if (input->readInt8(&server_flag) == false ||
            input->readInt16(&area) == false ||
            input->readInt32(&bucket) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
        }

#if TAIR_MAX_AREA_COUNT < 65536
        if (area >= TAIR_MAX_AREA_COUNT) {
            log_warn("area overflow: "
                             "server_flag %x, area %d, bucket %d",
                     server_flag, area, bucket);
            return false;
        }
#endif

        if (bucket >= TAIR_MAX_BUCKET_NUMBER) {
            log_warn("bucket overflow: "
                             "server_flag %x, area %d, bucket %d",
                     server_flag, area, bucket);
            return false;
        }

        key = new data_entry();
        if (!key->decode(input)) {
            log_warn("key decode failed: "
                             "server_flag %x, area %d, bucket %d",
                     server_flag, area, bucket);
            return false;
        }

        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16; // header 16 bytes

        size_t total = 1 + 2 + 4;
        if (key != NULL) {
            total += key->encoded_size();
        }

        return total + 16; // header 16 bytes
    }

    virtual uint16_t ns() const {
        return area;
    }

public:
    uint16_t area;
    uint32_t bucket;
    data_entry *key;

};
}
#endif
