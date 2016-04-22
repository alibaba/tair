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

#include "record.hpp"
#include "data_entry.hpp"

#include <climits>

namespace tair {
namespace common {
/* =====================RecordPosition===================== */
RecordPosition::RecordPosition() {
    instance_ = -1;
    filenumber_ = 0;
    fileoffset_ = 0;
    sequence_ = 0;
}

RecordPosition::RecordPosition(const RecordPosition &rp) {
    instance_ = rp.instance_;
    filenumber_ = rp.filenumber_;
    fileoffset_ = rp.fileoffset_;
    sequence_ = rp.sequence_;
}

bool RecordPosition::operator<(const RecordPosition &rp) const {
    assert(instance_ == rp.instance_);
    if (filenumber_ != rp.filenumber_) return filenumber_ < rp.filenumber_;
    if (fileoffset_ != rp.fileoffset_) return fileoffset_ < rp.fileoffset_;
    return sequence_ < rp.sequence_;
}

bool RecordPosition::operator==(const RecordPosition &rp) const {
    if (instance_ != rp.instance_) return false;
    if (filenumber_ != rp.filenumber_) return false;
    if (fileoffset_ != rp.fileoffset_) return false;
    return sequence_ == rp.sequence_;
}

RecordPosition &RecordPosition::operator=(const RecordPosition &rp) {
    instance_ = rp.instance_;
    sequence_ = rp.sequence_;
    filenumber_ = rp.filenumber_;
    fileoffset_ = rp.fileoffset_;
    return *this;
}

void RecordPosition::set_undefined() {
    sequence_ = ULLONG_MAX;
    fileoffset_ = ULLONG_MAX;
}

bool RecordPosition::is_undefined() {
    return sequence_ == ULLONG_MAX && fileoffset_ == ULLONG_MAX;
}

bool RecordPosition::valid() {
    return instance_ != -1;
}

std::string RecordPosition::to_string() {
    std::stringstream ss;
    ss << "{\"Instance\" = " << instance_;
    if (sequence_ == ULLONG_MAX) {
        ss << ", \"Sequence\" = \"unknown\"";
    } else {
        ss << ", \"Sequence\" = " << sequence_;
    }
    ss << ", \"FileNumber\" = " << filenumber_;
    if (fileoffset_ == ULLONG_MAX) {
        ss << ", \"FileOffset\" = \"unknown\"}";
    } else {
        ss << ", \"FileOffset\" = " << fileoffset_ << "}";
    }
    return ss.str();
}

/* =====================End===================== */


/* ====================Record=================== */
Record::Record(RecordType type, RecordPosition &rp, TairRemoteSyncType optype,
               int32_t bucket, bool reget, data_entry *key) {
    type_ = type;
    position_ = rp;
    operator_type_ = optype;
    bucket_ = bucket;
    origin_bucket_ = bucket_;
    reget_ = reget;
    key_ = key;
}

Record::Record(RecordType type, TairRemoteSyncType optype,
               int32_t bucket, bool reget, data_entry *key) {
    type_ = type;
    operator_type_ = optype;
    bucket_ = bucket;
    origin_bucket_ = bucket;
    reget_ = reget;
    key_ = key;
}

Record::Record() {}

Record::~Record() {
//      showstack();
//      fprintf(stderr, "address this: %p,key_: %p\n", this, key_);
    delete key_;
}

bool Record::valid() {
    if (operator_type_ > TAIR_REMOTE_SYNC_TYPE_NONE &&
        operator_type_ < TAIR_REMOTE_SYNC_TYPE_MAX) {
        return true;
    }
    return false;
}
/* =====================End===================== */


/* ====================KVRecord=================== */
KVRecord::KVRecord(TairRemoteSyncType optype, int32_t bucket,
                   data_entry *key, data_entry *value)
        : Record(KeyValue, optype, bucket, false, key) {
    value_ = value;
}

KVRecord::KVRecord(RecordPosition &rp, TairRemoteSyncType optype, int32_t bucket,
                   bool reget, data_entry *key, data_entry *value)
        : Record(KeyValue, rp, optype, bucket, reget, key) {
    value_ = value;
}

KVRecord::KVRecord() {}

KVRecord::~KVRecord() {
    delete value_;
}

Record *KVRecord::clone() {
    KVRecord *record = new KVRecord();
    if (this->key_ != NULL) {
        record->key_ = new data_entry();
        record->key_->clone(*(this->key_));
    } else {
        record->key_ = NULL;
    }
    if (this->value_ != NULL) {
        record->value_ = new data_entry();
        record->value_->clone(*(this->value_));
    } else {
        record->value_ = NULL;
    }
    record->type_ = this->type_;
    record->position_ = this->position_;
    record->operator_type_ = this->operator_type_;
    record->bucket_ = this->bucket_;
    record->origin_bucket_ = this->origin_bucket_;
    record->reget_ = this->reget_;
    return record;
}

size_t KVRecord::approximate_size() const {
    size_t size = 0;
    if (this->key_ != NULL) {
        size += this->key_->get_size();
    }
    if (this->value_ != NULL) {
        size += this->value_->get_size();
    }
    return size;
}

/* =====================End===================== */

/* ===================BatchBucketRecord=============== */
BatchBucketRecord::BatchBucketRecord(int32_t bucket, std::vector<Record *> *value)
        : Record(BatchKeyValue, TAIR_REMOTE_SYNC_TYPE_BATCH, bucket, false, NULL) {
    value_ = value;
    if (value_ != NULL && value_->empty() == false) {
        position_ = value_->front()->position_;
    }
}

BatchBucketRecord::BatchBucketRecord() {}

BatchBucketRecord::~BatchBucketRecord() {
    if (value_ != NULL) {
        for (size_t i = 0; i < value_->size(); i++) {
            delete (*value_)[i];
        }
        delete value_;
    }
}

Record *BatchBucketRecord::clone() {
    BatchBucketRecord *record = new BatchBucketRecord();
    if (this->key_ != NULL) {
        record->key_ = new data_entry();
        record->key_->clone(*(this->key_));
    } else {
        record->key_ = NULL;
    }
    if (this->value_ != NULL) {
        record->value_ = new std::vector<common::Record *>();
        std::vector<common::Record *>::iterator iter;
        for (iter = this->value_->begin();
             iter != this->value_->end(); iter++) {
            record->value_->push_back((*iter)->clone());
        }
    } else {
        record->value_ = NULL;
    }
    record->type_ = this->type_;
    record->position_ = this->position_;
    record->operator_type_ = this->operator_type_;
    record->bucket_ = this->bucket_;
    record->reget_ = this->reget_;
    return record;
}

size_t BatchBucketRecord::approximate_size() const {
    size_t size = 0;
    if (this->key_ != NULL) {
        size += this->key_->get_size();
    }
    if (this->value_ != NULL) {
        std::vector<common::Record *>::iterator iter;
        for (iter = this->value_->begin();
             iter != this->value_->end(); iter++) {
            size += (*iter)->approximate_size();
        }
    }
    return size;
}

/* =======================End======================== */
}
}

#if 0
void test_kvrecord_approximate_size() {
  tair::common::KVRecord* kvrecord = new tair::common::KVRecord((TairRemoteSyncType)0, 0, NULL, NULL);
  kvrecord->key_ = new tair::common::data_entry("abc");
  assert(kvrecord->approximate_size() == 3);
  kvrecord->value_ = new tair::common::data_entry("abc");
  assert(kvrecord->approximate_size() == 6);
  delete kvrecord;
}

void test_batch_bucket_record_approximate_size() {
  tair::common::BatchBucketRecord* record = new tair::common::BatchBucketRecord(0, NULL);
  record->key_ = new tair::common::data_entry("abc");
  assert(record->approximate_size() == 3);
  std::vector<tair::common::Record*>* v = new std::vector<tair::common::Record*>();
  for (size_t i = 0; i < 10; i++) {
    tair::common::KVRecord* kvrecord = new tair::common::KVRecord((TairRemoteSyncType)0, 0, NULL, NULL);
    kvrecord->key_ = new tair::common::data_entry("abc");
    kvrecord->value_ = new tair::common::data_entry("abc");
    v->push_back(kvrecord);
  }
  record->value_ = v;
  assert(record->approximate_size() == 3 + 6 * 10);
  delete record;
}

int main(int argc, char** argv) {
  test_kvrecord_approximate_size();
  test_batch_bucket_record_approximate_size();
}
#endif

