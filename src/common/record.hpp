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

#ifndef COMMON_RECORD_HPP_
#define COMMON_RECORD_HPP_

#include "define.hpp"

namespace tair {
namespace common {
class data_entry;

class RecordPosition {
public:
    int32_t instance_;
    uint64_t filenumber_;
    uint64_t fileoffset_;
    uint64_t sequence_;
public:
    RecordPosition();

    RecordPosition(const RecordPosition &rp);

    bool operator<(const RecordPosition &rp) const;

    bool operator==(const RecordPosition &rp) const;

    RecordPosition &operator=(const RecordPosition &rp);

    void set_undefined();

    bool is_undefined();

    bool valid();

    std::string to_string();
};

typedef enum {
    KeyValue = 0,
    BatchKeyValue = 1
} RecordType;

class Record {
public:
    Record(RecordType type, RecordPosition &rp, TairRemoteSyncType optype,
           int32_t bucket, bool reget, data_entry *key);

    Record(RecordType type, TairRemoteSyncType optype, int32_t bucket,
           bool reget, data_entry *key);

    Record();

    virtual ~Record();

    virtual Record *clone() = 0;

    virtual size_t approximate_size() const = 0;

    inline void set_origin_bucket(int32_t bucket) { origin_bucket_ = bucket; }

    bool valid();

private:
    Record(const Record &);

public:
    data_entry *key_;
    RecordType type_;
    RecordPosition position_;
    TairRemoteSyncType operator_type_;
    int32_t bucket_;
    int32_t origin_bucket_;
    bool reget_;
};

class KVRecord : public Record {
public:
    KVRecord(TairRemoteSyncType optype, int32_t bucket, data_entry *key,
             data_entry *value);

    KVRecord(RecordPosition &rp, TairRemoteSyncType optype, int32_t bucket,
             bool reget, data_entry *key, data_entry *value);

    virtual ~KVRecord();

    virtual Record *clone();

    virtual size_t approximate_size() const;

private:
    KVRecord();

    KVRecord(const KVRecord &);

public:
    data_entry *value_;
};

class BatchBucketRecord : public Record {
public:
    BatchBucketRecord(int32_t bucket, std::vector<Record *> *value);

    virtual ~BatchBucketRecord();

    virtual Record *clone();

    virtual size_t approximate_size() const;

private:
    BatchBucketRecord();

    BatchBucketRecord(const BatchBucketRecord &);

public:
    std::vector<Record *> *value_;
};
}
}

#endif
