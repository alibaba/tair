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

#include <stdlib.h>
#include <stdio.h>
#include "log.hpp"
#include "rt_object.hpp"

namespace tair {

void RTObject::rt(int us, RTType type) {
    (void) __sync_fetch_and_add(&rt_types_dynamic_[type].opcnt_, 1);
    (void) __sync_fetch_and_add(&rt_types_dynamic_[type].rt_us_acc_, us);
    do {
        int rt_us_max = rt_types_dynamic_[type].rt_us_max_;
        if (us > rt_us_max) {
            if (__sync_bool_compare_and_swap(&rt_types_dynamic_[type].rt_us_max_,
                                             rt_us_max, us)) {
                break;
            }
        } else {
            break;
        }
    } while (true);

    size_t bucket = 0;
    while (us > 0) {
        ++bucket;
        us >>= 1;
    }
    bucket = bucket > RT_ORDER_MAX ? RT_ORDER_MAX : bucket;
    __sync_fetch_and_add(&rt_types_dynamic_[type].rt_buckets_[bucket], 1);
}

size_t RTObject::serialize(char *buf, size_t size) {
    char *p = buf;
    if (p == NULL) {
        size = 1024;
        p = new char[size];
        p[0] = 0;
    }
    size_t len = 0;
    for (uint32_t i = 0; i < (uint32_t) RT_TYPE_NUM; ++i) {
        if (rt_types_[i].opcnt_ == 0) continue;
        size_t rt_us_avg = rt_types_[i].rt_us_acc_ / rt_types_[i].opcnt_;
        len += snprintf(p + len, size - len, "opcode: %d\n\t", opcode_);
        len += snprintf(p + len, size - len,
                        "avg rt: %lu(us), max rt: %lu(us)\n\t",
                        rt_us_avg, rt_types_[i].rt_us_max_);
        len += snprintf(p + len, size - len,
                        "%8s  %-10s\n\t", "BUCKET", "RT");
        for (size_t i = 0; i <= RT_ORDER_MAX && len < size; ++i) {
            if (rt_types_[i].rt_buckets_[i] != 0) {
                len += snprintf(p + len, size - len,
                                "%8lu  %-10lu\n\t",
                                i, rt_types_[i].rt_buckets_[i]);
            }
        }
        len += snprintf(p + len, size - len, "\n");
    }
    if (buf == NULL) {
        fprintf(stderr, "%s", p);
        delete[] p;
    }
    return len;
}

}
