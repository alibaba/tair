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

#ifndef LOCAL_BINLOG_QUEUE_HPP_
#define LOCAL_BINLOG_QUEUE_HPP_

#include "common/define.hpp"
#include "common/result.hpp"
#include "common/record.hpp"
#include "common/bucket_queue.hpp"
#include "common/record_logger.hpp"

#include <map>
#include <set>
#include <vector>

namespace tair {
namespace common {
namespace MixinTest {
class Mixin;
}
class ILogReader;
}

namespace storage {
namespace ldb {

class LdbInstance;

class LdbManager;

class InstBinlogQueue {
private:
    typedef enum {
        Block,
        Normal
    } LoggerType;

private:
    typedef std::priority_queue<tair::common::BucketQueue *,
            std::vector<tair::common::BucketQueue *>,
            tair::common::BucketQueue::CompareByPosition> NormalPriorityQeueue;
    typedef std::priority_queue<tair::common::BucketQueue *,
            std::vector<tair::common::BucketQueue *>,
            tair::common::BucketQueue::CompareByEndTime> BlockPriorityQueue;
public:

    InstBinlogQueue(LdbInstance *instance);

#ifdef __MIXIN_DEBUG__
    InstBinlogQueue(tair::common::MixinTest::Mixin* mixin);
#endif

    ~InstBinlogQueue();

    int init();

    int restart();

    void watch();

    void statistics();

    void options(const tair::common::BucketQueue::Options &options);

    tair::common::Result<tair::common::Record *> get_record();

    void update(int bucket, bool send_ok);

    void delete_log_file(uint64_t filenumber);

    tair::common::RecordPosition get_min_position();

    void set_max_bucket_count(int max_bucket_count);

private:

    tair::common::BucketQueue *find_bucket_queue(int bucket);

    void insert_bucket_queue(tair::common::BucketQueue *queue);

    bool need_read_binlog(LoggerType type);

    void read_normal_binlog(tair::common::ILogReader *logger, size_t limit);

    void read_block_binlog(tair::common::ILogReader *logger, size_t limit);

private:
    static const size_t DefaultLoopLimit = 1024; // usually larger than logger's temp buffer
private:
    tair::common::RecordPosition position_;
    NormalPriorityQeueue que_;
    BlockPriorityQueue block_que_;
    uint64_t first_sequence_;
    uint64_t last_delete_log_file_;
    int32_t index_;
    // total buckets = full queue buckets + not full queue buckets
    std::set<int32_t> full_queue_buckets_;
    tair::common::ILogReader *logger_;
    tair::common::ILogReader *block_logger_;
    std::map<int32_t, tair::common::BucketQueue *> bucket_ques_;
};

class LocalBinlogQueue : public tair::common::RecordLogger {
public:
    LocalBinlogQueue(LdbManager *manager);

#ifdef __MIXIN_DEBUG__
    LocalBinlogQueue(tair::common::MixinTest::Mixin* mixin);
#endif

    ~LocalBinlogQueue();

    int init();

    int restart();

    void watch();

    void statistics();

    std::string options();

    void options(const std::map<std::string, int> &m);

    tair::common::Result<tair::common::Record *> get_record(int instance);

    void update(int instance, int bucket, bool send_ok);

    void delete_log_file(int instance, uint64_t filenumber);

    tair::common::RecordPosition get_min_position(int instance);

    void set_max_bucket_count(int max_bucket_count);

private:
    void _watch();

    void _statistics();

    void _options();

private:
    bool do_watch_;
    bool do_statistics_;
    bool do_update_options_;
    tair::common::BucketQueue::Options options_;
    std::vector<InstBinlogQueue *> ques_;
};
}
}
}

#endif
