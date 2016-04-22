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

#ifndef TAIR_SEQLOCK_HPP
#define TAIR_SEQLOCK_HPP

#if defined(__i386__) || defined(__x86_64__)

#include <pthread.h>

namespace tair {

class seqlock_t {
public:
    seqlock_t(int pshared = PTHREAD_PROCESS_PRIVATE) {
        pthread_spin_init(&lock_, pshared);
        seq_ = 0;
    }

    uint32_t read_begin() {
        return seq_;
    }

    bool read_retry(uint32_t seq) {
        return (seq & 1) | (seq_ ^ seq);
    }

    void write_begin() {
        pthread_spin_lock(&lock_);
        ++seq_;
        asm volatile("":: :"memory");
    }

    void write_end() {
        asm volatile("":: :"memory");
        ++seq_;
        pthread_spin_lock(&lock_);
    }

private:
    pthread_spinlock_t lock_;
    uint32_t seq_;
};

class seqcount_t {
public:
    seqcount_t() {
        seq_ = 0;
    }

    uint32_t read_begin() {
        return seq_;
    }

    bool read_retry(uint32_t seq) {
        return (seq & 1) | (seq_ ^ seq);
    }

    void write_begin() {
        ++seq_;
        asm volatile("":: :"memory");
    }

    void write_end() {
        asm volatile("":: :"memory");
        ++seq_;
    }

private:
    uint32_t seq_;
};

}
#else // defined(__i386__) || defined(__x86_64__)
#error "tair::seqlock_t does not support this architecture!"
#endif // defined(__i386__) || defined(__x86_64__)

#endif
