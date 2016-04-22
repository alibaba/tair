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

#ifndef TAIR_STORAGE_LDB_REMOTE_SYNC_LOGGER_H
#define TAIR_STORAGE_LDB_REMOTE_SYNC_LOGGER_H

#include <set>

#include "common/data_entry.hpp"
#include "common/record_logger.hpp"
#include "common/log_reader.hpp"

namespace leveldb {
class Slice;
namespace log {
class Reader;
}
}

namespace tair {
namespace common {
template<class T>
class Result;

class Record;
}

namespace storage {
namespace ldb {
class LdbManager;

class LdbInstance;

class LdbRemoteSyncLogReader : public tair::common::ILogReader {
public:
    explicit LdbRemoteSyncLogReader(LdbInstance *instance, bool new_way = false, bool delete_file = true);

    virtual ~LdbRemoteSyncLogReader();

    virtual int init();

    virtual uint64_t restart();

    // new remote sync manager support it
    virtual void delete_log_file(uint64_t filenumber);

    // use for old remote sync manager, at future will be deprecated
    int get_record(int32_t &type, int32_t &bucket_num, int32_t &origin_bucket_num,
                   tair::common::data_entry *&key, tair::common::data_entry *&value, bool &force_reget);

    // good way
    virtual tair::common::Result<std::queue<tair::common::Record *> *> get_records();

    virtual tair::common::RecordPosition get_min_position();

    virtual void update();

    virtual uint64_t get_reading_file_number() const { return reading_logfile_number_; }

    virtual void set_max_bucket_count(int max_bucket_count) { max_bucket_count_ = max_bucket_count; }

    virtual void set_buffer_size(size_t sz) { buffer_size_ = sz; }

    virtual size_t get_buffer_size() { return buffer_size_; }

    LdbRemoteSyncLogReader *clone();

private:
    LdbRemoteSyncLogReader(LdbInstance *instance, int max_bucket_count,
                           tair::common::RecordPosition &record, bool new_way, bool delete_file);

    bool update_last_sequence();

    int start_new_reader(uint64_t min_number, uint64_t offset);

    int init_reader(uint64_t number, uint64_t offset);

    void clear_reader(uint64_t number);

    int get_log_record();

    int parse_one_kv_record(int32_t &type, int32_t &bucket_num,
                            tair::common::data_entry *&key, tair::common::data_entry *&value,
                            bool skip_value);

    bool try_skip_one_kv_record(char &type, bool &is_from_unit);

    bool parse_one_key(int32_t &bucket_num, tair::common::data_entry *&key);

    bool parse_one_value(tair::common::data_entry *key, tair::common::data_entry *&value);

    bool parse_one_entry_tailer(tair::common::data_entry *entry);

    bool skip_one_entry();

    int recalc_dest_bucket(const data_entry *key);

private:
    LdbInstance *instance_;
    // first sequence number when startup
    uint64_t first_sequence_;
    // current reading logfile's filenumber
    uint64_t reading_logfile_number_;
    leveldb::log::Reader *reader_;
    uint64_t last_record_offset_;
    bool last_logfile_refed_;

    // one log record can contain several kv entry(record),
    // following is for temporary data buffer of last gotten log record
    leveldb::Slice *last_log_record_;
    std::string last_log_scratch_;
    uint64_t last_sequence_;

    int max_bucket_count_;
    //
    tair::common::RecordPosition min_position_;
    std::queue<tair::common::Record *> *records_head_of_log_file_;
    bool delete_file_;
    bool new_way_;

    size_t buffer_size_;
};

class LdbRemoteSyncLogger : public tair::common::RecordLogger {
public:
    explicit LdbRemoteSyncLogger(LdbManager *manager);

    virtual ~LdbRemoteSyncLogger();

    int init();

    int restart();

    // at future will be deprecated
    int add_record(int32_t index, int32_t type,
                   tair::common::data_entry *key, tair::common::data_entry *value);

    // use for old remote sync manager, at future will be deprecated
    int get_record(int32_t index, int32_t &type, int32_t &bucket_num,
                   tair::common::data_entry *&key, tair::common::data_entry *&value,
                   bool &force_reget);

    void set_max_bucket_count() {}

private:
    LdbManager *manager_;
    LdbRemoteSyncLogReader **reader_;
};
}
}
}
#endif
