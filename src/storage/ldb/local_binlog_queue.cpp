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

#include "local_binlog_queue.hpp"
#include "ldb_instance.hpp"
#include "ldb_manager.hpp"
#include "common/log_reader.hpp"
#include "ldb_remote_sync_logger.hpp"

#ifdef __MIXIN_DEBUG__
// for test
#include "common/mixin_test_helper.hpp"
#endif

namespace tair {
namespace storage {
namespace ldb {
// ======================== Local Binlog Queue Of Each Instance =======================
InstBinlogQueue::InstBinlogQueue(LdbInstance *instance) {
    logger_ = new LdbRemoteSyncLogReader(instance, true, false);
    block_logger_ = NULL;
    last_delete_log_file_ = 0;
    first_sequence_ = 0;
    index_ = instance->index();
}

#ifdef __MIXIN_DEBUG__
InstBinlogQueue::InstBinlogQueue(tair::common::MixinTest::Mixin* mixin) {
  tair::common::MixinTest::Value value = mixin->Get("logger");
  logger_               = static_cast<ILogReader*>(value.data_.ptrval_);
  assert(logger_ != NULL);
  block_logger_         = NULL;
  last_delete_log_file_ = 0;
  first_sequence_       = 0;
  value = mixin->Get("index");
  index_                = value.data_.intval_;
}
#endif

InstBinlogQueue::~InstBinlogQueue() {
    for (std::map<int32_t, tair::common::BucketQueue *>::iterator it = bucket_ques_.begin();
         it != bucket_ques_.end(); it++) {
        delete it->second;
    }
    delete logger_;
    delete block_logger_;
}

int InstBinlogQueue::init() {
    return logger_->init();
}

void InstBinlogQueue::set_max_bucket_count(int max_bucket_count) {
    logger_->set_max_bucket_count(max_bucket_count);
    if (block_logger_) {
        logger_->set_max_bucket_count(max_bucket_count);
    }
}

int InstBinlogQueue::restart() {
    first_sequence_ = logger_->restart();
    return TAIR_RETURN_SUCCESS;
}

void InstBinlogQueue::watch() {
    std::map<int32_t, tair::common::BucketQueue *>::iterator iter;
    for (iter = bucket_ques_.begin(); iter != bucket_ques_.end(); iter++) {
        tair::common::BucketQueue *b = iter->second;
        if (b != NULL) b->watch();
    }
}

void InstBinlogQueue::statistics() {
    std::map<int32_t, tair::common::BucketQueue *>::iterator iter;
    std::ostringstream os;
    os << std::endl;

    os << "============= Instance " << index_ << " ===============\n";
    for (iter = bucket_ques_.begin(); iter != bucket_ques_.end(); iter++) {
        tair::common::BucketQueue *b = iter->second;
        if (b != NULL) {
            os << iter->first << " --> " << b->info() << std::endl;
        }
    }

    os << "========================================\n";

    log_error("%s", os.str().c_str());
}

void InstBinlogQueue::options(const tair::common::BucketQueue::Options &options) {
    std::map<int32_t, tair::common::BucketQueue *>::iterator iter;
    for (iter = bucket_ques_.begin(); iter != bucket_ques_.end(); iter++) {
        tair::common::BucketQueue *b = iter->second;
        if (b != NULL) b->options(options);
    }
    if (logger_ != NULL) {
        logger_->set_buffer_size(options.log_buffer_size());
    }
    if (block_logger_ != NULL) {
        block_logger_->set_buffer_size(options.log_buffer_size());
    }
}

void InstBinlogQueue::read_block_binlog(tair::common::ILogReader *logger, size_t limit) {
    tair::common::Result<std::queue<tair::common::Record *> *> result = logger->get_records();
    if (result.code_ == TAIR_RETURN_SUCCESS) {
        while (!result.value_->empty() && (limit--)) {
            tair::common::Record *record = result.value_->front();

            tair::common::BucketQueue *queue = find_bucket_queue(record->bucket_);
            // if queue == null, mean this bucket queue does not exists, but bucket hold
            // by block binlog, must exists, so..
            if (queue == NULL) {
                // skip this record
                log_warn("read block binlog queue == NULL");
                // it will pop record from queue
                logger->update();
                delete record;
            } else {
                // queue's bucket == result.value_->bucket_
                if (full_queue_buckets_.find(record->bucket_) != full_queue_buckets_.end()) {
                    if (!queue->full()) {
                        queue->push(record);
                        logger->update();
                        if (queue->size() == 1) {
                            que_.push(queue);
                        }
                    } else {
                        // do noting for the reocrd, keep it and which after it stay at logger
                        break;
                    }
                } else {
                    // not in full set, block logger should skip it
                    logger->update();
                    delete record;
                }
            }
        }
    }
}

void InstBinlogQueue::read_normal_binlog(tair::common::ILogReader *logger, size_t limit) {
    tair::common::Result<std::queue<tair::common::Record *> *> result = logger->get_records();
    if (result.code_ == TAIR_RETURN_SUCCESS) {
        while (!result.value_->empty()) {
            tair::common::Record *record = result.value_->front();

            tair::common::BucketQueue *queue = find_bucket_queue(record->bucket_);
            if (queue == NULL) {
                queue = new tair::common::BucketQueue(record->bucket_);
                insert_bucket_queue(queue);
            }

            // queue's bucket == result.value_->bucket_
            if (full_queue_buckets_.find(record->bucket_) == full_queue_buckets_.end()) {
                queue->push(record);
                logger->update();

                if (queue->full()) {
                    full_queue_buckets_.insert(record->bucket_);
                    if (block_logger_ == NULL) {
                        block_logger_ = logger_->clone();
                    }
                    // do noting for record remain in temp logger buffer
                    // keep them in it
                    break;
                } else if (queue->size() == 1) {
                    // two way can go here:
                    // 1. new bucket queue, we consider that this bucket never send fail before, so put into normal queue
                    // 2. not new bucket queue, but it has no record in queue, it not in BlockQueue
                    // so queue->size() == 1, must not in BlockQueue or NormalQueue, so put it into NormalQueue
                    que_.push(queue);
                }
            } else {
                // queue in full, read block logger will process it, here should
                // skip it
                logger->update();
                delete record;
            }
        }
    }
}

tair::common::Result<tair::common::Record *> InstBinlogQueue::get_record() {
    if (block_logger_ != NULL && need_read_binlog(Block)) {
        read_block_binlog(block_logger_, InstBinlogQueue::DefaultLoopLimit);
        // try to merge two queue
        if (logger_->get_min_position() < block_logger_->get_min_position()) {
            delete block_logger_;
            block_logger_ = NULL;
            full_queue_buckets_.clear();
        }
    }

    if (need_read_binlog(Normal)) {
        read_normal_binlog(logger_, InstBinlogQueue::DefaultLoopLimit);
    }

    if (!block_que_.empty() && block_que_.top()->can_retry()) {
        tair::common::BucketQueue *queue = block_que_.top();
        block_que_.pop();
        tair::common::Result<tair::common::Record *> result = queue->front();
        return result;
    }

    if (!que_.empty()) {
        tair::common::BucketQueue *queue = que_.top();
        que_.pop();
        tair::common::Result<tair::common::Record *> result = queue->front();
        return result;
    }

    return tair::common::Result<tair::common::Record *>(NULL, TAIR_RETURN_DATA_NOT_EXIST);
}

void InstBinlogQueue::update(int bucket, bool send_ok) {
    // bucket queue must not in block_que_ or que_
    tair::common::BucketQueue *queue = find_bucket_queue(bucket);
    assert(queue != NULL);
    queue->update(send_ok);
    if (!send_ok) {
        block_que_.push(queue);
    } else if (!queue->empty()) {
        que_.push(queue);
    }
}

void InstBinlogQueue::delete_log_file(uint64_t filenumber) {
    if (filenumber > last_delete_log_file_) {
        log_warn("instance %d, filenum %"
        PRI64_PREFIX
        "u last delete log file %"
        PRI64_PREFIX
        "u",
                index_, filenumber, last_delete_log_file_);
        // filenumber must greater than 0
        // delete all log number less than filenumber
        logger_->delete_log_file(filenumber - 1);
        last_delete_log_file_ = filenumber;
    }
}

tair::common::RecordPosition InstBinlogQueue::get_min_position() {
    // TODO optimize
    log_info("================= Get Minimum Position ======================");
    tair::common::RecordPosition min_position;
    std::map<int32_t, tair::common::BucketQueue *>::iterator it;
    for (it = bucket_ques_.begin(); it != bucket_ques_.end(); it++) {
        tair::common::RecordPosition ps = it->second->get_position();
        if (ps.valid() == true && it->second->size() > 0) {
            if (min_position.valid() == false || ps < min_position) {
                min_position = ps;
                log_info("instance %d: selecting bucket queue %d(size %zu)'s min position %s",
                         index_, it->first, it->second->size(), ps.to_string().c_str());
            }
        }
    }
    tair::common::RecordPosition position = logger_->get_min_position();
    if (position.valid() == true) {
        uint64_t filenumber = logger_->get_reading_file_number();
        log_info("instance %d: logger reading file number %"
        PRI64_PREFIX
        "u", index_, filenumber);
        if (min_position.valid() == false || position < min_position) {
            min_position = position;
            log_info("instance %d: from logger's min position %s", index_, position.to_string().c_str());
        }
    } else {
        uint64_t filenumber = logger_->get_reading_file_number();
        log_info("NEVER HAPPEN instance %d: logger reading file number %"
        PRI64_PREFIX
        "u", index_, filenumber);
    }

    if (block_logger_ != NULL) {
        position = block_logger_->get_min_position();
        if (position.valid() == true) {
            uint64_t filenumber = block_logger_->get_reading_file_number();
            log_info("instance %d: block logger reading file number %"
            PRI64_PREFIX
            "u", index_, filenumber);
            if (min_position.valid() == false || position < min_position) {
                min_position = position;
                log_info("instance %d: from block logger's min position %s", index_, position.to_string().c_str());
            }
        } else {
            uint64_t filenumber = block_logger_->get_reading_file_number();
            log_info("NEVER HAPPEN instance %d: logger reading file number %"
            PRI64_PREFIX
            "u", index_, filenumber);
        }
    }

    log_info("instance %d: last select min position %s", index_, min_position.to_string().c_str());
    log_info("============== Get Minimum Position Done ====================");
    return min_position;
}

tair::common::BucketQueue *InstBinlogQueue::find_bucket_queue(int bucket) {
    std::map<int32_t, tair::common::BucketQueue *>::iterator iter = bucket_ques_.find(bucket);
    if (iter == bucket_ques_.end()) return NULL;
    return iter->second;
}

void InstBinlogQueue::insert_bucket_queue(tair::common::BucketQueue *queue) {
    bucket_ques_.insert(std::make_pair<int32_t, tair::common::BucketQueue *>(queue->get_bucket(), queue));
}

bool InstBinlogQueue::need_read_binlog(LoggerType type) {
    // TODO some speed-limit strategy
//        if (type == Normal) {
//          return que_.empty() && (block_que_.empty() || block_que_.top()->can_retry() == false);
//        }
    return true;
}

// ============================= Local Binlog Queue =====================================
LocalBinlogQueue::LocalBinlogQueue(LdbManager *manager) : options_() {
    do_watch_ = false;
    do_statistics_ = false;
    do_update_options_ = false;
    // we use leveldb's bin-log here where key/value has already been
    // added in function logic, so need no writer
    writer_count_ = 0;
    // several reader, each ldb instance has one reader(leveldb log reader)
    reader_count_ = manager->db_count_;
    for (int32_t i = 0; i < reader_count_; ++i) {
        ques_.push_back(new InstBinlogQueue(manager->ldb_instance_[i]));
    }
}

#ifdef __MIXIN_DEBUG__
LocalBinlogQueue::LocalBinlogQueue(tair::common::MixinTest::Mixin* mixin) : options_() {
  do_watch_          = false;
  do_statistics_     = false;
  do_update_options_ = false;
  writer_count_ = 0;
  tair::common::MixinTest::Value value = mixin->Get("reader_count");
  assert(value.type_ == tair::common::MixinTest::Integer);
  reader_count_ = value.data_.intval_;
  char buff[32];
  for (int32_t i = 0; i < reader_count_; i++) {
    snprintf(buff, 32, "ldbinstance#%d", i);
    tair::common::MixinTest::Value value = mixin->Get(buff);
    assert(value.type_ == tair::common::MixinTest::Pointer);
    InstBinlogQueue* iq = static_cast<InstBinlogQueue*>(value.data_.ptrval_);
    assert(iq != NULL);
    ques_.push_back(iq);
  }
}
#endif

LocalBinlogQueue::~LocalBinlogQueue() {
    for (size_t i = 0; i < ques_.size(); i++) {
        delete ques_[i];
    }
    ques_.clear();
}

int LocalBinlogQueue::init() {
    int ret = TAIR_RETURN_SUCCESS;
    for (size_t i = 0; i < ques_.size(); ++i) {
        if ((ret = ques_[i]->init()) != TAIR_RETURN_SUCCESS) {
            log_error("init reader %zu fail, ret: %d", i, ret);
            break;
        }
        log_error("%zu inst queue init ok", i);
    }
    return ret;
}

int LocalBinlogQueue::restart() {
    int ret = TAIR_RETURN_SUCCESS;
    for (size_t i = 0; i < ques_.size(); ++i) {
        if ((ret = ques_[i]->restart()) != TAIR_RETURN_SUCCESS) {
            log_error("restart reader %zu fail, ret: %d", i, ret);
            break;
        }
    }
    return ret;
}

tair::common::Result<tair::common::Record *> LocalBinlogQueue::get_record(int instance) {
    assert(instance >= 0 && (size_t) instance < ques_.size());

    if (do_watch_) {
        do_watch_ = false;
        _watch();
    }
    if (do_statistics_) {
        do_statistics_ = false;
        _statistics();
    }
    if (do_update_options_) {
        do_update_options_ = false;
        _options();
    }

    return ques_[instance]->get_record();
}

void LocalBinlogQueue::update(int instance, int bucket, bool send_ok) {
    assert(instance >= 0 && (size_t) instance < ques_.size());
    return ques_[instance]->update(bucket, send_ok);
}

void LocalBinlogQueue::delete_log_file(int instance, uint64_t filenumber) {
    assert(instance >= 0 && (size_t) instance < ques_.size());
    ques_[instance]->delete_log_file(filenumber);
}

tair::common::RecordPosition LocalBinlogQueue::get_min_position(int instance) {
    assert(instance >= 0 && (size_t) instance < ques_.size());
    return ques_[instance]->get_min_position();
}

void LocalBinlogQueue::set_max_bucket_count(int max_bucket_count) {
    for (size_t i = 0; i < ques_.size(); i++) {
        ques_[i]->set_max_bucket_count(max_bucket_count);
    }
}

void LocalBinlogQueue::_watch() {
    for (size_t i = 0; i < ques_.size(); i++) {
        ques_[i]->watch();
    }
}

void LocalBinlogQueue::watch() {
    do_watch_ = true;
}

void LocalBinlogQueue::_statistics() {
    for (size_t i = 0; i < ques_.size(); i++) {
        ques_[i]->statistics();
    }
}

void LocalBinlogQueue::statistics() {
    do_statistics_ = true;
}

void LocalBinlogQueue::_options() {
    for (size_t i = 0; i < ques_.size(); i++) {
        ques_[i]->options(options_);
    }
}

std::string LocalBinlogQueue::options() {
    std::ostringstream os;
    os << options_;
    return os.str();
}

void LocalBinlogQueue::options(const std::map<std::string, int> &m) {
    options_.update(m);
    do_update_options_ = true;
}
}
}
}
