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

#ifndef TAIR_PACKET_PUT_PACKET_H
#define TAIR_PACKET_PUT_PACKET_H

#include "base_packet.hpp"

#ifdef WITH_COMPRESS
#include "compressor.hpp"
#endif
namespace tair {
class request_put : public base_packet {
public:
    request_put() {
        setPCode(TAIR_REQ_PUT_PACKET);
        server_flag = 0;
        area = 0;
        version = 0;
        expired = 0;
    }

    request_put(const request_put &packet)
            : base_packet(packet) {
        setPCode(TAIR_REQ_PUT_PACKET);
        server_flag = packet.server_flag;
        area = packet.area;
        version = packet.version;
        expired = packet.expired;
        key.clone(packet.key);
        data.clone(packet.data);
    }

    virtual const data_entry *pick_key() const {
        return &key;
    }

    virtual void dump() const {
        char ascii_key[72];
        ascii_key[0] = '\0';
        const data_entry *picked = pick_key();
        if (picked != NULL) {
            pick_key()->to_ascii(ascii_key, sizeof(ascii_key));
        }

        fprintf(stderr,
                "put:\n\t"
                        "area: %d, expire: %d, version: %d, packet size: %lu, server_flag:%d\n\t"
                        "key: %s\n",
                area, expired, version, size(), server_flag, ascii_key);
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;
        return 1 + 2 + 2 + 4 + key.encoded_size() + data.encoded_size() + 16;
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    uint16_t ns() const {
        return area;
    }

    ~request_put() {
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        output->writeInt16(version);
        output->writeInt32(expired);
        key.encode(output);
        data.encode_with_compress(output);
        return true;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  int8    server_flag   1B      (must)
        //  int16   area          2B      (must)
        //  int16   version       2B      (must)
        //  int32   expired       4B      (must)
        //  -------------------------
        //  total                 9B
        if (input->readInt8(&server_flag) == false ||
            input->readInt16(&area) == false ||
            input->readInt16(&version) == false ||
            input->readInt32((uint32_t *) &expired) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
        }

#if TAIR_MAX_AREA_COUNT < 65536
        if (area >= TAIR_MAX_AREA_COUNT) {
            log_warn("area overflow: "
                             "server_flag %x, area %d, version %d, expired %d",
                     server_flag, area, version, expired);
            return false;
        }
#endif

        if (!key.decode(input)) {
            log_warn("key decode failed: "
                             "server_flag %x, area %d, version %d, expired %d",
                     server_flag, area, version, expired);
            return false;
        }

        if (!data.decode(input)) {
            log_warn("data decode failed: "
                             "server_flag %x, area %d, version %d, expired %d",
                     server_flag, area, version, expired);
            return false;
        }

        key.data_meta.version = version;

        return true;
    }

public:
    uint16_t area;
    uint16_t version;
    int32_t expired;
    data_entry key;
    data_entry data;
};

class request_mput : public base_packet {
public:
    request_mput() {
        setPCode(TAIR_REQ_MPUT_PACKET);
        server_flag = 0;
        area = 0;
        len = 8;
        packet_id = 0;
        reset();
    }

    request_mput(const request_mput &packet)
            : base_packet(packet) {
        clone(packet, packet.alloc);
    }

    ~request_mput() {
        clear();
    }

    virtual base_packet::Type get_type() {
        return base_packet::REQ_WRITE;
    }

    void clone(const request_mput &packet, bool need_alloc) {
        if (this == &packet) {
            return;
        }
        clear();
        setPCode(TAIR_REQ_MPUT_PACKET);
        server_flag = packet.server_flag;
        area = packet.area;
        count = packet.count;
        len = packet.len;
        compressed = packet.compressed;
        packet_data_len = packet.packet_data_len;

        if (need_alloc) {
            if (compressed) {
                packet_data = new char[packet_data_len];
                memcpy(packet_data, packet.packet_data, packet_data_len);
            }

            alloc = true;
            record_vec = new mput_record_vec();
            mput_record_vec::iterator it;
            for (it = packet.record_vec->begin(); it != packet.record_vec->end(); ++it) {
                mput_record *rec = new mput_record(**it);
                record_vec->push_back(rec);
            }
        } else {
            alloc = packet.alloc;
            record_vec = packet.record_vec;
            packet_data = packet.packet_data;
        }

        packet_id = packet.packet_id;
    }

    // to avoid redudant memory copy, swap packet inner data owner
    void swap(request_mput &packet) {
        clone(packet, false);
        packet.reset();
    }

    void reset() {
        packet_data = NULL;
        packet_data_len = 0;
        compressed = false;

        alloc = false;
        record_vec = NULL;
        count = 0;
    }

    void clear() {
        if (record_vec != NULL && alloc) {
            mput_record_vec::iterator it;
            for (it = record_vec->begin(); it != record_vec->end(); ++it) {
                delete (*it);
            }

            delete record_vec;
            record_vec = NULL;
            count = 0;
        }

        if (packet_data != NULL) {
            delete packet_data;
            packet_data = NULL;
            packet_data_len = 0;
        }
    }

    bool compress() {
        // only compress once
        if (compressed) {
            return true;
        }

        bool ret = true;
#ifdef WITH_COMPRESS
        DataBuffer output;
        do_encode(&output);

        uint32_t raw_len = output.getDataLen();
        // do compress, use snappy now
        ret = tair::common::compressor::do_compress
          (&packet_data, reinterpret_cast<uint32_t*>(&packet_data_len),
           output.getData(), raw_len, tair::common::data_entry::compress_type) == 0 ? true : false;
        if (ret) {
          compressed = true;
        }
#endif
        return ret;
    }

    bool decompress() {
        if (!compressed) {
            return true;
        }
        if (NULL == packet_data || packet_data_len <= 0) {
            return false;
        }

        bool ret = false;
#ifdef WITH_COMPRESS
        // TODO: redundant memcpy now, compressor can provide uncompresslen interface and
        //       receive malloced buffer
        DataBuffer input;
        char* raw_data = NULL;
        uint32_t raw_data_len = 0;
        ret = tair::common::compressor::do_decompress
          (&raw_data, &raw_data_len, packet_data, packet_data_len, tair::common::data_entry::compress_type) == 0 ? true : false;
        if (ret) {
          input.ensureFree(raw_data_len);
          memcpy(input.getFree(), raw_data, raw_data_len);
          input.pourData(raw_data_len);
          delete raw_data;
          ret = do_decode(&input);
        }
#else
        log_error("decompress data but compiled without compress support");
#endif
        return ret;
    }

    bool do_encode(DataBuffer *output) {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        output->writeInt32(count);
        if (record_vec != NULL) {
            mput_record_vec::iterator it;
            for (it = record_vec->begin(); it != record_vec->end(); ++it) {
                mput_record *rec = (*it);
                rec->key->encode(output);
                rec->value->encode(output);
            }
        }
        output->writeInt32(packet_id);
        return true;
    }

    bool do_decode(DataBuffer *input) {
        if (input->readInt8(&server_flag) == false ||
            input->readInt16(&area) == false ||
            input->readInt32(&count) == false) {
            return false;
        }

#if TAIR_MAX_AREA_COUNT < 65536
        if (area >= TAIR_MAX_AREA_COUNT) {
            log_warn("area overflow: "
                             "server_flag %x, area %d, count %d",
                     server_flag, area, count);
            return false;
        }
#endif

        if (count > 0) {
            record_vec = new mput_record_vec();
            alloc = true;
            for (uint32_t i = 0; i < count; i++) {
                mput_record *rec = new mput_record();
                data_entry *key = new data_entry();
                value_entry *value = new value_entry();

                if (!key->decode(input)) {
                    delete rec;
                    delete key;
                    delete value;

                    log_warn("key decode failed: "
                                     "server_flag %x, area %d, count %d, index %u",
                             server_flag, area, count, i);
                    return false;
                }
                rec->key = key;

                if (!value->decode(input)) {
                    rec->key = NULL;
                    delete rec;
                    delete key;
                    delete value;

                    log_warn("value decode failed: "
                                     "server_flag %x, area %d, count %d, index %u",
                             server_flag, area, count, i);
                    return false;
                }
                rec->value = value;

                record_vec->push_back(rec);
            }

            return true;
        }

        if (input->readInt32(&packet_id) == false) {
            log_warn("packet_id decode failed: "
                             "server_flag %x, area %d, count %d",
                     server_flag, area, count);
            return false;
        }

        return true;
    }

    bool encode(DataBuffer *output) {
        output->writeInt8(compressed ? 1 : 0);
        if (compressed) {
            output->writeInt32(packet_data_len);
            output->writeBytes(packet_data, packet_data_len);
        } else {
            do_encode(output);
        }
        return true;
    }

    virtual size_t size() const {
        if (LIKELY(getDataLen() != 0))
            return getDataLen() + 16;

        size_t total = 1;
        if (compressed) {
            total += 4 + packet_data_len;
        } else {
            total += 1 + 2 + 4;
            if (record_vec != NULL) {
                mput_record_vec::iterator it;
                for (it = record_vec->begin(); it != record_vec->end(); ++it) {
                    mput_record *rec = (*it);
                    total += rec->key->encoded_size();
                    total += rec->value->encoded_size();
                }
            }
            total += 4;
        }
        return total + 16; // header 16 bytes
    }

    uint16_t ns() const {
        return area;
    }

    bool decode(DataBuffer *input, PacketHeader *header) {
        //  int8    server_flag   1B      (must)
        //  -------------------------
        //  total                 1B
        uint8_t compressed_i = 0;
        if (input->readInt8(&compressed_i) == false) {
            log_warn("buffer data too few, buffer length %d", header->_dataLen);
            return false;
        }
        compressed = (compressed_i != 0);

        if (compressed) {
            uint32_t packet_data_len_i = 0;
            if (input->readInt32(&packet_data_len_i) == false) {
                log_warn("packet_data_len decode failed, compressed %d", compressed);
                return false;
            }
            packet_data_len = packet_data_len_i;

            packet_data = new char[packet_data_len];
            if (input->readBytes(packet_data, packet_data_len) == false) {
                delete packet_data;
                packet_data = NULL;

                log_warn("packet_data_len decode failed, "
                                 "compressed %d, packet_data_len %zu",
                         compressed, packet_data_len);
                return false;
            }
        } else {
            if (do_decode(input) == false) {
                return false;
            }
        }
        return true;
    }

    bool add_put_key_data(const data_entry &key, const value_entry &data) {
        uint32_t temp = len + key.get_size() + 1 + data.get_size();
        if (temp > MAX_MPUT_PACKET_SIZE && count > 0) {
            log_info("mput packet size overflow: %u", temp);
            return false;
        }
        if (record_vec == NULL) {
            record_vec = new mput_record_vec();
            alloc = true;
        }

        mput_record *rec = new mput_record();
        rec->key = new data_entry();
        rec->key->clone(key);
        rec->value = new value_entry();
        rec->value->clone(data);
        record_vec->push_back(rec);
        len += key.get_size() + 1;
        len += data.get_size();
        count++;
        return true;
    }

public:
    uint16_t area;
    uint32_t count;
    uint32_t len;
    mput_record_vec *record_vec;
    bool alloc;
    // for compress
    bool compressed;
    char *packet_data;
    size_t packet_data_len;
    // for duplicate
    uint32_t packet_id;
};
}
#endif
