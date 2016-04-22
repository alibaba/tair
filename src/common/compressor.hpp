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

#include "tbsys.h"
#include "snappy_compressor.hpp"

#ifndef TAIR_DATA_ENTRY_COMPRESS
#define TAIR_DATA_ENTRY_COMPRESS

namespace tair {
namespace common {
class compressor {
public:
    static int do_compress(char **dest, uint32_t *dest_len, const char *src, uint32_t src_len, int type) {
        int ret = -1;

        switch (type) {
            case 0:
                ret = snappy_compressor::do_compress(dest, dest_len, src, src_len);
                break;
            default:
                log_error("invalid compress type, type is %d", type);
        }

        return ret;
    }

    static int do_decompress(char **dest, uint32_t *dest_len, const char *src, uint32_t src_len, int type) {
        int ret = -1;

        switch (type) {
            case 0:
                ret = snappy_compressor::do_decompress(dest, dest_len, src, src_len);
                break;
            default:
                log_error("invalid compress type, type is %d", type);
        }

        return ret;
    }
};
}
}
#endif
