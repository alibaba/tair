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

#ifndef TAIR_TIMED_COLLECTIONS_HPP
#define TAIR_TIMED_COLLECTIONS_HPP

#include <stdint.h>
#include <string>

namespace tair {
namespace common {
namespace timedcollections {

class ITimedCollections {
public:
    virtual ~ITimedCollections() {};

    virtual uint64_t incr_count(uint64_t server_id) = 0;

    virtual uint64_t decr_count(uint64_t server_id) = 0;

    virtual uint64_t get_count(uint64_t server_id) = 0;

    virtual const std::string view() = 0;

private:
    virtual void try_expire() = 0;
};

extern ITimedCollections *NewTimedCollections(uint64_t expire = 3600 /* second */, uint64_t max_wait_count = 10000);
}
}
}

#endif
