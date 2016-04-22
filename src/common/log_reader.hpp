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

#ifndef _COMMON_LOG_READER_
#define _COMMON_LOG_READER_

#include <queue>
#include "result.hpp"
#include "record.hpp"

namespace tair {
namespace common {
class ILogReader {
public:
    virtual ~ILogReader() {};

    virtual int init() = 0;

    virtual uint64_t restart() = 0;

    // new remote sync manager support it
    virtual void delete_log_file(uint64_t filenumber) = 0;

    // good way
    virtual Result<std::queue<tair::common::Record *> *> get_records() = 0;

    virtual RecordPosition get_min_position() = 0;

    virtual uint64_t get_reading_file_number() const = 0;

    virtual void update() = 0;

    virtual void set_max_bucket_count(int max_bucket_count) = 0;

    virtual void set_buffer_size(size_t sz) = 0;

    virtual size_t get_buffer_size() = 0;

    virtual ILogReader *clone() = 0;
};
}
}

#endif
