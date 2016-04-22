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

#ifndef DATA_ENTRY_LOCAL_CACHE_H
#define DATA_ENTRY_LOCAL_CACHE_H

#include "local_cache.hpp"
#include "data_entry.hpp"

namespace tair {

struct data_entry_hash {
    size_t operator()(const tair::common::data_entry &key) const {
        uint64_t code = key.get_hashcode();
        return static_cast<size_t>(code);
    }
};

struct data_entry_equal_to {
    bool operator()(const tair::common::data_entry &x,
                    const tair::common::data_entry &y) const {
        return x == y;
    }
};

typedef local_cache<tair::common::data_entry,
        tair::common::data_entry,
        tair::data_entry_hash,
        tair::data_entry_equal_to> data_entry_local_cache;
};


#endif
   
