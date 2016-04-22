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

#ifndef TAIR_key_value_pack_t_HPP
#define TAIR_key_value_pack_t_HPP

namespace tair {
namespace common {
struct key_value_pack_t {
    key_value_pack_t() {
        memset(this, 0, sizeof(*this));
    }

    key_value_pack_t(const key_value_pack_t &rhs)
            : key(new data_entry(*rhs.key)),
              value(new data_entry(*rhs.value)) {
        version = rhs.version;
        expire = rhs.expire;
    }

    key_value_pack_t &operator=(const key_value_pack_t &rhs) {
        if (this != &rhs) {
            *key = *rhs.key;
            *value = *rhs.value;
            version = rhs.version;
            expire = rhs.expire;
        }
        return *this;
    }

    data_entry *key;
    data_entry *value;
    int version;
    int expire;
};
}
}
#endif
