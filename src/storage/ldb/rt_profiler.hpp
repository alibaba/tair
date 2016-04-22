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

#ifndef TAIR_STORAGE_RT_PROFILER_HPP
#define TAIR_STORAGE_RT_PROFILER_HPP

namespace tair {
namespace storage {

enum ProbeSeqNum {
    PSN_LDB_MANAGER_PUT,
    PSN_DO_PUT,
    PSN_LEVELDB_PUT,
    PSN_LEVELDB_WRITEBATCH_PUT,
    PSN_LEVELDB_WRITE,
    PSN_LEVELDB_CV_WAIT,
    PSN_LEVELDB_LOG_ADD_RECORD,
    PSN_LEVELDB_INSERT_MMT,
    PSN_LEVELDB_MAKE_ROOM_FOR_WRITE,
    PSN_LEVELDB_MAKEROOM_SLEEP,
    PSN_LEVELDB_MAKEROOM_BGWAIT_IMM,
    PSN_LEVELDB_MAKEROOM_BGWAIT_LEVEL0,

    PSN_LDB_MANAGER_GET,
    PSN_DO_GET,
    PSN_LEVELDB_GET,
    PSN_MMT_GET,
    PSN_MMT_ITER_SEEK,
    PSN_SST_GET,

    PSN_CACHE_GET,
    PSN_CACHE_PUT,
    PSN_CACHE_DEL,

    PSN_LDB_MANAGER_RANGE,

    PSN_DB_MUTEX_LOCK,
    PSN_LEVELDB_MUTEX_LOCK,

    PSN_MAX
};

class RTProfiler {
public:
    RTProfiler();

    ~RTProfiler();

    void reset();

    void dump();

    void do_switch();

private:
    class RTObject;

    inline bool active();

    inline RTObject *rto(uint32_t seq);

    inline const char *probe_name(uint32_t seq);

private:
    class RTObject {
    public:
        inline void rt(size_t us);

        void dump(FILE *file);

        enum {
            RT_ORDER_MAX = 16,
        };
        size_t opcnt_;
        size_t rt_us_acc_;
        size_t rt_us_max_;
        size_t rt_buckets_[RT_ORDER_MAX + 1];
    };

private:
    friend class RTProbe;

    RTObject rtos_[PSN_MAX];
    bool active_;
    const char *dump_file_;
};

extern RTProfiler g_rt_profiler;

bool RTProfiler::active() {
    return active_;
}

RTProfiler::RTObject *RTProfiler::rto(uint32_t seq) {
    return &rtos_[seq];
}

void RTProfiler::RTObject::rt(size_t us) {
    (void) __sync_fetch_and_add(&opcnt_, 1);
    (void) __sync_fetch_and_add(&rt_us_acc_, us);
    size_t rt_us_max;
    do {
        rt_us_max = rt_us_max_;
    } while (us > rt_us_max &&
             !__sync_bool_compare_and_swap(&rt_us_max_, rt_us_max, us));
    size_t bucket = 0;
    while (us > 0) {
        ++bucket;
        us >>= 1;
    }
    bucket = bucket > RT_ORDER_MAX ? RT_ORDER_MAX : bucket;
    __sync_fetch_and_add(&rt_buckets_[bucket], 1);
}


class RTProbe {
public:
    RTProbe(enum ProbeSeqNum seq) {
        if (g_rt_profiler.active()) {
            rto_ = g_rt_profiler.rto(seq);
            enter_time_ = us_now();
        } else {
            enter_time_ = 0;
            rto_ = NULL;
        }
    }

    RTProbe() {
    }

    ~RTProbe() {
        if (rto_ != NULL) {
            size_t leave_time = us_now();
            rto_->rt(leave_time - enter_time_);
        }
    }

    void enter(enum ProbeSeqNum seq) {
        if (g_rt_profiler.active()) {
            rto_ = g_rt_profiler.rto(seq);
            enter_time_ = us_now();
        } else {
            rto_ = NULL;
            enter_time_ = 0;
        }
    }

    void leave() {
        if (rto_ != NULL) {
            size_t leave_time = us_now();
            rto_->rt(leave_time - enter_time_);
        }
        rto_ = NULL;
    }

private:
    size_t us_now() {
        struct timespec t;
        clock_gettime(CLOCK_MONOTONIC, &t);
        return t.tv_sec * 1000000 + t.tv_nsec / 1000;
    }

private:
    RTProfiler::RTObject *rto_;
    size_t enter_time_;
};

}
}

#endif
