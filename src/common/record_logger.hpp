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

#ifndef TAIR_COMMON_RECORD_LOGGER_H
#define TAIR_COMMON_RECORD_LOGGER_H

#include "result.hpp"
#include "record.hpp"

#include <map>

namespace tair {
namespace common {
class data_entry;

class RecordLogger {
public:
    RecordLogger() : writer_count_(0), reader_count_(0) {}

    virtual ~RecordLogger() {}

    int get_writer_count() { return writer_count_; }

    int get_reader_count() { return reader_count_; }

    // init record logger
    virtual int init() = 0;

    // restart record logger, maybe do cleanup stuff(optional).
    virtual int restart() { return TAIR_RETURN_NOT_SUPPORTED; }

    // add one record to logger. index indicates writer index.
    virtual int add_record(int32_t index, int32_t type,
                           data_entry *key, data_entry *value) {
        return TAIR_RETURN_SUCCESS;
        // return TAIR_RETURN_NOT_SUPPORTED;
    }

    // get NEXT avaliable record from logger. index indicates reader index.
    // (return success && key == NULL) should mean no available
    // record now(read over maybe), otherwise, key and type must be returned if success,
    // while bucket_num/value is optional dependent on specified implementation.
    // bucket_num can assigned to valid value(maybe logged already) to avoid calculating again.
    virtual int get_record(int32_t index, int32_t &type, int32_t &bucket_num,
                           data_entry *&key, data_entry *&value,
                           bool &force_reget) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual Result<Record *> get_record(int instance) {
        return Result<Record *>(NULL, TAIR_RETURN_NOT_SUPPORTED);
    }

    virtual void update(int instance, int bucket, bool send_ok) {}

    virtual void delete_log_file(int instance, uint64_t filenumber) {}

    virtual RecordPosition get_min_position(int instance) {
        RecordPosition rp;
        return rp;
    }

    virtual void set_max_bucket_count(int max_bucket_count) {}

    virtual void watch() {}

    virtual void statistics() {}

    virtual std::string options() { return ""; }

    virtual void options(const std::map<std::string, int> &m) {}

protected:
    int32_t writer_count_;
    int32_t reader_count_;

public:
    static int32_t common_encode_record(char *&buf, int32_t type, data_entry *key, data_entry *value);

    static int32_t common_decode_record(const char *buf, int32_t &type, data_entry *&key, data_entry *&value);
};

}
}
#endif
