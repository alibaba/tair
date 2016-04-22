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

#ifndef TAIR_ITEM_DATA_INFO
#define TAIR_ITEM_DATA_INFO

#include "log.hpp"
#include "define.hpp"

namespace tair {
class DataBuffer;

typedef struct _item_meta {
    uint16_t magic;
    uint16_t checksum;
    uint16_t keysize; // key size max: 64KB
    uint16_t version;
    uint32_t prefixsize; // prefix size
    uint32_t valsize : 24; // value size
    uint8_t flag; // for extends
    uint32_t cdate; // item create time
    uint32_t mdate; // item last modified time
    uint32_t edate; // expire date

    void encode(DataBuffer *output, bool is_new = false) const;

    bool decode(DataBuffer *input);

    _item_meta &operator=(const _item_meta &rhs) {
        if (this != &rhs) {
            memcpy(this, &rhs, sizeof(*this));
        }
        return *this;
    }

    void log_self() {
        log_debug(
                "meta info of data: keysize[%d], valsize[%d], prefixsize[%d], version[%d], flag[%d], cdate[%d], mdate[%d], edate[%d]",
                keysize, valsize, prefixsize, version, flag, cdate, mdate, edate);
    }
} item_meta_info;

static const size_t ITEM_HEADER_LEN = sizeof(item_meta_info);

typedef struct _ItemData {
    item_meta_info header;
    char m_data[0];
} item_data_info;
}
#endif
