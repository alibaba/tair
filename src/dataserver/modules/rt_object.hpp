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

#ifndef TAIR_RT_OBJECT_HPP
#define TAIR_RT_OBJECT_HPP

#include <string.h>
#include "seqlock.hpp"

/*
 * Bucket     RT
 * 0          < 1us
 * 1          < 2us
 * 2          < 4us
 *      ...
 * 15         < 32ms
 * 16         > 32ms
 */
namespace tair {

class RTCollector;

class RTObject {
public:
    RTObject() {
        opcode_ = -1;
        memset(rt_types_, 0, sizeof(rt_types_));
        memset(rt_types_dynamic_, 0, sizeof(rt_types_dynamic_));
    }

    explicit RTObject(int opcode) {
        opcode_ = opcode;
        memset(rt_types_, 0, sizeof(rt_types_));
        memset(rt_types_dynamic_, 0, sizeof(rt_types_dynamic_));
    }

    RTObject(const RTObject &rto) {
        rto.clone_to(*this);
    }

    const RTObject &operator=(const RTObject &rto) {
        if (this != &rto) {
            rto.clone_to(*this);
        }
        return *this;
    }

    /*
     * Register one rt, update related fields.
     * Inconsistency between these fields is not necessary,
     * assuring individual's atomicity is enough.
     */

    enum RTType {
        RT_SUCCESS = 0,
        RT_FAILED = 1,

        RT_TYPE_NUM
    };

    void rt(int us, RTType type);

    /*
     * Serialize this rto into buffer,
     * Return size of written bytes.
     */
    size_t serialize(char *buf, size_t size);

    /**
     * Reset all related fields to zero
     * !NOTE! This method is not thread-safe.
     * If thread safety were needed, replace `seqcount_t' with `seqlock_t'
     */
    void reset() {
        seqcount_.write_begin();
        memcpy(rt_types_, rt_types_dynamic_, sizeof(rt_types_dynamic_));
        seqcount_.write_end();
        memset(rt_types_dynamic_, 0, sizeof(rt_types_dynamic_));
    }

    void serialize(std::string &str, char *buf, size_t size, const char *(*name)(int)) {
        for (uint32_t j = 0; j < (uint32_t) RTObject::RT_TYPE_NUM; ++j) {
            if (rt_types_[j].opcnt_ == 0) continue;
            snprintf(buf, size,
                     "op: %s(%d), type: %u, n: %lu, mean: %lu, max: %lu, dist: {",
                     name(opcode_), opcode_, j, rt_types_[j].opcnt_,
                     rt_types_[j].rt_us_acc_ / rt_types_[j].opcnt_,
                     rt_types_[j].rt_us_max_);
            str += buf;

            uint32_t k = 0;
            for (; k < RTObject::RT_ORDER_MAX; ++k) {
                if (rt_types_[j].rt_buckets_[k] == 0) continue;
                snprintf(buf, size,
                         "\n\t<%u: %lu(%.2lf%%), ",
                         (1 << k), rt_types_[j].rt_buckets_[k],
                         100. * rt_types_[j].rt_buckets_[k] / rt_types_[j].opcnt_);
                str += buf;
            }

            snprintf(buf, size,
                     "\n\t>%u: %lu(%.2lf%%)\n}\n",
                     1 << (k - 1), rt_types_[j].rt_buckets_[k],
                     100. * rt_types_[j].rt_buckets_[k] / rt_types_[j].opcnt_);
            str += buf;
        }
    }

private:
    void clone_to(RTObject &rto) const {
        uint32_t seq;
        do {
            seq = seqcount_.read_begin();
            rto.opcode_ = opcode_;
            memcpy(rto.rt_types_, rt_types_, sizeof(rt_types_));
        } while (seqcount_.read_retry(seq));
    }

private:
    friend class RTCollector;

    friend class response_rt;

    enum {
        RT_ORDER_MAX = 16,
    };  //~ scatter rt by 2^order us, where 0<=order<=RT_ORDER_MAX
    int opcode_;          //~ corresponding opcode
    mutable seqcount_t seqcount_;

    struct {
        size_t opcnt_;         //~ number of ops for the present
        size_t rt_us_acc_;     //~ accumulated rt
        size_t rt_us_max_;     //~ max rt
        size_t rt_buckets_[RT_ORDER_MAX + 1];
    } rt_types_[RT_TYPE_NUM], rt_types_dynamic_[RT_TYPE_NUM];
};

}

#endif
