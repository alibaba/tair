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

#ifndef BUCKET_QUEUE_H_
#define BUCKET_QUEUE_H_

#include <map>
#include <queue>

#include "record.hpp"
#include "result.hpp"

namespace tair {
namespace common {

class BucketQueue {
public:
    class Info {
    public:
        Info() {
            total_fetch_count_ = 0;
            total_fetch_size_ = 0;
            total_fetch_packet_count_ = 0;
            total_fetch_put_count_ = 0;
            total_fetch_del_count_ = 0;
            watching_fetch_count_ = 0;
            watching_fetch_size_ = 0;
            watching_fetch_packet_count_ = 0;
            watching_fetch_put_count_ = 0;
            watching_fetch_del_count_ = 0;
            watch_begin_ = time(NULL);
        }

        void watch() {
            watching_fetch_count_ = 0;
            watching_fetch_size_ = 0;
            watching_fetch_packet_count_ = 0;
            watching_fetch_put_count_ = 0;
            watching_fetch_del_count_ = 0;
            watch_begin_ = time(NULL);
        }

        void count(uint64_t count, uint64_t size, uint64_t pc, uint64_t dc) {
            total_fetch_count_ += count;
            total_fetch_size_ += size;
            total_fetch_packet_count_++;
            total_fetch_put_count_ += pc;
            total_fetch_del_count_ += dc;
            watching_fetch_count_ += count;
            watching_fetch_size_ += size;
            watching_fetch_packet_count_++;
            watching_fetch_put_count_ += pc;
            watching_fetch_del_count_ += dc;
        }

        inline uint64_t total_fetch_count() { return total_fetch_count_; }

        inline uint64_t total_fetch_size() { return total_fetch_size_; }

        inline uint64_t total_fetch_packet_count() { return total_fetch_packet_count_; }

        inline uint64_t total_fetch_put_count() { return total_fetch_put_count_; }

        inline uint64_t total_fetch_del_count() { return total_fetch_del_count_; }

        inline uint64_t watching_fetch_count() { return watching_fetch_count_; }

        inline uint64_t watching_fetch_size() { return watching_fetch_size_; }

        inline uint64_t watching_fetch_packet_count() { return watching_fetch_packet_count_; }

        inline uint64_t watching_fetch_put_count() { return watching_fetch_put_count_; }

        inline uint64_t watching_fetch_del_count() { return watching_fetch_del_count_; }

        inline time_t watch_begin() { return watch_begin_; }

        friend std::ostream &operator<<(std::ostream &os, const Info &i) {
            os << "{ TCt: " << i.total_fetch_count_;
            os << ", TSz: " << i.total_fetch_size_;
            os << ", TPt: " << i.total_fetch_packet_count_;
            os << ", TPC: " << i.total_fetch_put_count_;
            os << ", TDC: " << i.total_fetch_del_count_;
            os << ", WCt: " << i.watching_fetch_count_;
            os << ", WSz: " << i.watching_fetch_size_;
            os << ", WPt: " << i.watching_fetch_packet_count_;
            os << ", WPC: " << i.watching_fetch_put_count_;
            os << ", WDC: " << i.watching_fetch_del_count_;
            os << ", WBeginTime: " << i.watch_begin_;
            os << " }";
            return os;
        }

    private:
        uint64_t total_fetch_count_;
        uint64_t total_fetch_size_;
        uint64_t total_fetch_packet_count_;
        uint64_t total_fetch_put_count_;
        uint64_t total_fetch_del_count_;
        uint64_t watching_fetch_count_;
        uint64_t watching_fetch_size_;
        uint64_t watching_fetch_packet_count_;
        uint64_t watching_fetch_put_count_;
        uint64_t watching_fetch_del_count_;
        time_t watch_begin_;
    private:
        Info(const Info &info);
    };

public:
    class Options {
    public:
        Options() {
            record_size_limit_ = SIZE_LIMIT;
            record_count_limit_ = COUNT_LIMIT;
            fail_wait_time_ = WAIT_TIME;
            queue_max_size_ = QUEUE_MAX_SIZE;
            log_buffer_size_ = LOG_BUFFER_SIZE;
        }

        Options(const Options &options) {
            record_size_limit_ = options.record_size_limit_;
            record_count_limit_ = options.record_count_limit_;
            fail_wait_time_ = options.fail_wait_time_;
            queue_max_size_ = options.queue_max_size_;
            log_buffer_size_ = options.log_buffer_size_;
        }

        void update(const std::map<std::string, int> &m) {
            std::map<std::string, int>::const_iterator iter;
            iter = m.find("RecordSizeLimit");
            if (iter != m.end()) {
                set_record_size_limit(iter->second);
            }
            iter = m.find("RecordCountLimit");
            if (iter != m.end()) {
                set_record_count_limit(iter->second);
            }
            iter = m.find("FailWaitTime");
            if (iter != m.end()) {
                set_fail_wait_time(iter->second);
            }
            iter = m.find("QueueMaxSize");
            if (iter != m.end()) {
                set_queue_max_size(iter->second);
            }
            iter = m.find("LogBufferSize");
            if (iter != m.end()) {
                set_log_buffer_size(iter->second);
            }
        }

        inline size_t record_size_limit() const { return record_size_limit_; }

        inline size_t record_count_limit() const { return record_count_limit_; }

        inline size_t fail_wait_time() const { return fail_wait_time_; }

        inline size_t queue_max_size() const { return queue_max_size_; }

        inline size_t log_buffer_size() const { return log_buffer_size_; }

        Options &operator=(const Options &options) {
            record_size_limit_ = options.record_size_limit_;
            record_count_limit_ = options.record_count_limit_;
            fail_wait_time_ = options.fail_wait_time_;
            queue_max_size_ = options.queue_max_size_;
            log_buffer_size_ = options.log_buffer_size_;
            return *this;
        }

        friend std::ostream &operator<<(std::ostream &os, const Options &o) {
            os << "{ RecordSizeLimit: " << o.record_size_limit_;
            os << ", RecordCountLimit: " << o.record_count_limit_;
            os << ", FailWaitTime: " << o.fail_wait_time_;
            os << ", QueueMaxSize: " << o.queue_max_size_;
            os << ", LogBufferSize: " << o.log_buffer_size_;
            os << " }";
            return os;
        }

    public:
        const static int WAIT_TIME = 2; /* s */
        const static size_t QUEUE_MAX_SIZE = 256;
        const static size_t COUNT_LIMIT = 10;
        const static size_t SIZE_LIMIT = 1024 * 1024;
        const static size_t LOG_BUFFER_SIZE = 10;
    private:
        inline void set_record_size_limit(int l) { record_size_limit_ = (l <= 0 ? SIZE_LIMIT : l); }

        inline void set_record_count_limit(int l) { record_count_limit_ = (l <= 0 ? COUNT_LIMIT : l); }

        inline void set_fail_wait_time(int t) { fail_wait_time_ = (t < 0 ? WAIT_TIME : t); }

        inline void set_queue_max_size(int s) { queue_max_size_ = (s <= 0 ? QUEUE_MAX_SIZE : s); }

        inline void set_log_buffer_size(int sz) { log_buffer_size_ = (sz <= 0 ? LOG_BUFFER_SIZE : sz); }

    private:
        size_t record_size_limit_;
        size_t record_count_limit_;
        size_t fail_wait_time_;
        size_t queue_max_size_;
        size_t log_buffer_size_;
    };

public:
    struct CompareByPosition {
        bool operator()(BucketQueue *q1, BucketQueue *q2);
    };

    struct CompareByEndTime {
        bool operator()(BucketQueue *q1, BucketQueue *q2);
    };

public:
    BucketQueue(int bucket);

    ~BucketQueue();

    Result<Record *> front();

    void push(Record *record);

    void update(bool send_ok);

    const RecordPosition get_position();

    int32_t get_bucket();

    bool can_retry();

    bool empty();

    bool full();

    size_t size();

    void watch();

    const Info &info() const { return info_; }

    void options(const Options &options) { options_ = options; }

private:
    int32_t bucket_;
    time_t end_time_;  /* use for retry */
    RecordPosition position_;
    BatchBucketRecord *buffered_record_;
    std::queue<Record *> que_;
    BucketQueue::Info info_;
    BucketQueue::Options options_;
};
}
}

#endif
