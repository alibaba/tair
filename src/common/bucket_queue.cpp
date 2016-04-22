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

#include "bucket_queue.hpp"
#include "define.hpp"

#include <time.h>

namespace tair {
namespace common {

bool BucketQueue::CompareByPosition::operator()(BucketQueue *q1, BucketQueue *q2) {
    return q2->position_ < q1->position_;
}

bool BucketQueue::CompareByEndTime::operator()(BucketQueue *q1, BucketQueue *q2) {
    return q1->end_time_ > q2->end_time_;
}

BucketQueue::BucketQueue(int32_t bucket) : info_(), options_() {
    bucket_ = bucket;
    end_time_ = 0;
    buffered_record_ = NULL;
}

BucketQueue::~BucketQueue() {
    while (!que_.empty()) {
        Record *record = que_.front();
        que_.pop();
        delete record;
    }
    delete buffered_record_;
}

void BucketQueue::watch() {
    info_.watch();
}

Result<Record *> BucketQueue::front() {
    size_t record_size_limit = options_.record_size_limit();
    size_t record_count_limit = options_.record_count_limit();

    if (buffered_record_ != NULL) {
        return Result<Record *>(buffered_record_, TAIR_RETURN_SUCCESS);
    } else if (que_.empty()) {
        return Result<Record *>(NULL, TAIR_RETURN_DATA_NOT_EXIST);
    }

    uint64_t count = 0;
    uint64_t size = 0;
    uint64_t pc = 0;
    uint64_t dc = 0;

    int32_t origin_bucket = bucket_;
    std::vector<Record *> *q = new std::vector<Record *>();
    for (size_t i = 0; i < que_.size() && i < record_count_limit; i++) {
        Record *t = que_.front();
        origin_bucket = t->origin_bucket_;
        q->push_back(t);
        que_.pop();

        count++;
        size += t->approximate_size();
        if (t->operator_type_ == TAIR_REMOTE_SYNC_TYPE_PUT) pc++;
        else if (t->operator_type_ == TAIR_REMOTE_SYNC_TYPE_DELETE) dc++;

        if (size > record_size_limit) break;
    }

    info_.count(count, size, pc, dc);

    buffered_record_ = new BatchBucketRecord(bucket_, q);
    buffered_record_->set_origin_bucket(origin_bucket);
    // buffered_record_->position_ = q->front()->position_;

    return Result<Record *>(buffered_record_, TAIR_RETURN_SUCCESS);
}

void BucketQueue::push(Record *record) {
    if (buffered_record_ == NULL && que_.empty()) {
        position_ = record->position_;
    }
    que_.push(record);
}

void BucketQueue::update(bool send_ok) {
    assert(buffered_record_ != NULL);
    if (send_ok) {
        end_time_ = 0;
        delete buffered_record_;
        buffered_record_ = NULL;
        if (!que_.empty()) {
            position_ = que_.front()->position_;
        }
    } else {
        end_time_ = time(NULL) + options_.fail_wait_time();
    }
}

const RecordPosition BucketQueue::get_position() {
    return position_;
}

int32_t BucketQueue::get_bucket() {
    return bucket_;
}

bool BucketQueue::can_retry() {
    if ((buffered_record_ || !que_.empty()) &&
        end_time_ <= time(NULL))
        return true;
    return false;
}

bool BucketQueue::empty() {
    return buffered_record_ == NULL && que_.empty();
}

bool BucketQueue::full() {
    if (buffered_record_ == NULL) {
        return que_.size() >= options_.queue_max_size();
    }
    return que_.size() + buffered_record_->value_->size() >= options_.queue_max_size();
}

size_t BucketQueue::size() {
    if (buffered_record_ == NULL) return que_.size();
    return que_.size() + buffered_record_->value_->size();
}
}
}


////////////////////////////////////////////////////

#if 0
void test_info_method() {
  time_t t = time(NULL);
  tair::common::BucketQueue::Info info;
  assert(info.total_fetch_count() == 0);
  assert(info.total_fetch_size() == 0);
  assert(info.watching_fetch_count() == 0);
  assert(info.watching_fetch_size() == 0);
  assert(info.watch_begin() == t);

  for (int i = 0; i < 10; i++) {
    info.count(i + 10, i + 100, 0, 0);
  }
  assert(info.total_fetch_count() == 10 * 10 + (0 + 9) * 10 / 2);
  assert(info.total_fetch_size() == 100 * 10 + (0 + 9) * 10 / 2);
  assert(info.watching_fetch_count() == 10 * 10 + (0 + 9) * 10 / 2);
  assert(info.watching_fetch_size() == 100 * 10 + (0 + 9) * 10 / 2);
  assert(info.watch_begin() == t);

  sleep(1);

  time_t w = time(NULL);
  info.watch();
  assert(info.total_fetch_count() == 10 * 10 + (0 + 9) * 10 / 2);
  assert(info.total_fetch_size() == 100 * 10 + (0 + 9) * 10 / 2);
  assert(info.watching_fetch_count() == 0);
  assert(info.watching_fetch_size() == 0);
  assert(info.watch_begin() == w);


  for (int i = 0; i < 10; i++) {
    info.count(i + 10, i + 100, 0, 0);
  }
  assert(info.total_fetch_count() == 2 * (10 * 10 + (0 + 9) * 10 / 2));
  assert(info.total_fetch_size() == 2 * (100 * 10 + (0 + 9) * 10 / 2));
  assert(info.watching_fetch_count() == 10 * 10 + (0 + 9) * 10 / 2);
  assert(info.watching_fetch_size() == 100 * 10 + (0 + 9) * 10 / 2);
  assert(info.watch_begin() == w);
}

void test_options() {
  tair::common::BucketQueue::Options options;

  assert(options.record_size_limit() == 1024 * 1024 * 1024);
  assert(options.record_count_limit() == 10);
  assert(options.fail_wait_time() == 2);
  assert(options.queue_max_size() == 256);

  options.set_record_size_limit(100);
  assert(options.record_size_limit() == 100);
  options.set_record_count_limit(4);
  assert(options.record_count_limit() == 4);
  options.set_fail_wait_time(3);
  assert(options.fail_wait_time() == 3);
  options.set_queue_max_size(1000);
  assert(options.queue_max_size() == 1000);

  options.set_record_size_limit(0);
  options.set_record_count_limit(0);
  options.set_fail_wait_time(0);
  options.set_queue_max_size(0);

  assert(options.record_size_limit() == 1024 * 1024 * 1024);
  assert(options.record_count_limit() == 10);
  assert(options.fail_wait_time() == 0);
  assert(options.queue_max_size() == 256);

  options.set_record_size_limit(-1);
  options.set_record_count_limit(-1);
  options.set_fail_wait_time(-1);
  options.set_queue_max_size(-1);

  assert(options.record_size_limit() == 1024 * 1024 * 1024);
  assert(options.record_count_limit() == 10);
  assert(options.fail_wait_time() == 2);
  assert(options.queue_max_size() == 256);
}

int main(int argc, char** argv) {
  test_info_method();
  test_options();

  return 0;
}
#endif
