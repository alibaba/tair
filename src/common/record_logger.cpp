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

#include "record_logger.hpp"

#include "data_entry.hpp"

namespace tair {
namespace common {

int32_t RecordLogger::common_encode_record(char *&buf, int32_t type, data_entry *key, data_entry *value) {
    int32_t key_size = (key != NULL ? key->get_size() : 0);
    int32_t value_size = (value != NULL ? value->get_size() : 0);
    bool need_entry_tailer = (key != NULL && entry_tailer::need_entry_tailer(*key));
    entry_tailer tailer;
    int32_t total_size = 2 + sizeof(int32_t) * 3 + key_size + value_size;
    if (need_entry_tailer) {
        tailer.set(*key);
        total_size += sizeof(int32_t) + tailer.size();
    }
    buf = new char[total_size];
    char *pos = buf;

    tair::util::coding_util::encode_fixed32(pos, total_size);
    pos += sizeof(int32_t);
    pos[0] = static_cast<char>(type);
    pos[1] = static_cast<char>(need_entry_tailer ? 1 : 0);
    pos += 2;
    if (need_entry_tailer) {
        tair::util::coding_util::encode_fixed32(pos, tailer.size());
        pos += sizeof(int32_t);
        memcpy(pos, tailer.data(), tailer.size());
        pos += tailer.size();
    }
    tair::util::coding_util::encode_fixed32(pos, key_size);
    pos += sizeof(int32_t);
    if (key_size > 0) {
        memcpy(pos, key->get_data(), key_size);
        pos += key_size;
    }
    tair::util::coding_util::encode_fixed32(pos, value_size);
    pos += sizeof(int32_t);
    if (value_size > 0) {
        memcpy(pos, value->get_data(), value_size);
    }
    return total_size;
}

int32_t RecordLogger::common_decode_record(const char *buf, int32_t &type, data_entry *&key, data_entry *&value) {
    int total_size = 0;
    entry_tailer tailer;
    const char *pos = buf;
    if (pos != NULL) {
        total_size = tair::util::coding_util::decode_fixed32(pos);
        pos += sizeof(int32_t);
        type = pos[0];
        bool has_tailer = pos[1];
        pos += 2;
        if (has_tailer) {
            int32_t tailer_size = tair::util::coding_util::decode_fixed32(pos);
            pos += sizeof(int32_t);
            tailer.set(pos, tailer_size);
            pos += tailer_size;
        }
        int32_t key_size = tair::util::coding_util::decode_fixed32(pos);
        pos += sizeof(int32_t);
        if (key_size > 0) {
            key = new data_entry(pos, key_size);
            if (has_tailer) {
                tailer.consume_tailer(*key);
            }
            pos += key_size;
        }
        int32_t value_size = tair::util::coding_util::decode_fixed32(pos);
        pos += sizeof(int32_t);
        if (value_size > 0) {
            value = new data_entry(pos, value_size);
        }
    }
    return total_size;
}
}
}
