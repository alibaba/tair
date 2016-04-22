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

#ifndef TAIR_RWLOCK_HPP
#define TAIR_RWLOCK_HPP

#if defined(__i386__) || defined(__x86_64__)
namespace tair {

class rwlock_t {
public:
    rwlock_t() {
        lock = RW_LOCK_BIAS;
    }

    void read_lock() {
        asm volatile(
        "1:"
                " lock; decl (%0)\n\t"
                " jns 3f\n\t"
                " lock; incl (%0)\n\t"
                "2:\n\t"
                " pause\n\t"
                " cmpl $1, (%0)\n\t"
                " js 2b\n\t"
                " jmp 1b\n\t"
                "3:\n\t"
        :
        :"r"(&this->lock)
        :"memory"
        );
    }

    bool read_trylock() {
        bool ret = false;
        asm volatile (
        " lock; decl (%1)\n\t"
                " setns %0\n\t"
                " jns 1f\n\t"
                " lock; incl (%1)\n\t"
                "1:\n\t"
        : "=r"(ret)
        : "r"(&this->lock)
        : "memory"
        );
        return ret;
    }

    void read_unlock() {
        asm volatile ( "lock; incl (%0)\n\t" : : "r"(&this->lock) : "memory");
    }

    void write_lock() {
        asm volatile (
        "1:\n\t"
                " lock; subl %1, (%0)\n\t"
                " jns 3f\n\t"
                " lock; addl %1, (%0)\n\t"
                "2:\n\t"
                " pause\n\t"
                " cmpl %1, (%0)\n\t"
                " js 2b\n\t"
                " jmp 1b\n\t"
                "3:\n\t"
        :
        : "r"(&this->lock), "i"(RW_LOCK_BIAS)
        : "memory"
        );
    }

    bool write_trylock() {
        bool ret = false;
        asm volatile (
        " lock; subl %2, (%1)\n\t"
                " setns %0\n\t"
                " jns 1f\n\t"
                " lock; addl %2, (%1)\n\t"
                "1:\n\t"
        : "=r"(ret)
        : "r"(&this->lock), "i"(RW_LOCK_BIAS)
        : "memory"
        );
        return ret;
    }

    void write_unlock() {
        asm volatile (
        "lock; addl %1, %0\n\t"
        : "+m"(this->lock)
        : "i"(RW_LOCK_BIAS)
        : "memory"
        );
    }

private:
    static const int RW_LOCK_BIAS = 0x100000;
    volatile int lock;
};

class read_guard_t {
public:
    read_guard_t(rwlock_t *lock) : lock_(lock) {
        lock_->read_lock();
    }

    ~read_guard_t() {
        lock_->read_unlock();
    }

private:
    rwlock_t *lock_;
};

class write_guard_t {
public:
    write_guard_t(rwlock_t *lock) : lock_(lock) {
        lock_->write_lock();
    }

    ~write_guard_t() {
        lock_->write_unlock();
    }

private:
    rwlock_t *lock_;
};

}
#else // defined(__i386__) || defined(__x86_64__)
#error "tair::rwlock_t cannot compile in this architecture!"
#endif // defined(__i386__) || defined(__x86_64__)

#endif
