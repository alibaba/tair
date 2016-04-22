#ifndef EASY_LOCK_ATOMIC_H_
#define EASY_LOCK_ATOMIC_H_

#include <easy_define.h>
#include <stdint.h>
#include <sched.h>

/**
 * 原子操作
 */

EASY_CPP_START

#define EASY_SMP_LOCK               "lock;"
#define easy_atomic_set(v,i)        ((v) = (i))
typedef volatile int32_t            easy_atomic32_t;

// 32bit
static __inline__ void easy_atomic32_add(easy_atomic32_t *v, int i)
{
    __asm__ __volatile__(
        EASY_SMP_LOCK "addl %1,%0"
        : "=m" ((*v)) : "r" (i), "m" ((*v)));
}
static __inline__ int32_t easy_atomic32_add_return(easy_atomic32_t *value, int32_t diff)
{
    int32_t                 old = diff;
    __asm__ volatile (
        EASY_SMP_LOCK "xaddl %0, %1"
        :"+r" (diff), "+m" (*value) : : "memory");
    return diff + old;
}
static __inline__ void easy_atomic32_inc(easy_atomic32_t *v)
{
    __asm__ __volatile__(EASY_SMP_LOCK "incl %0" : "=m" (*v) :"m" (*v));
}
static __inline__ void easy_atomic32_dec(easy_atomic32_t *v)
{
    __asm__ __volatile__(EASY_SMP_LOCK "decl %0" : "=m" (*v) :"m" (*v));
}

// 64bit
#if __WORDSIZE == 64
typedef volatile int64_t easy_atomic_t;
static __inline__ void easy_atomic_add(easy_atomic_t *v, int64_t i)
{
    __asm__ __volatile__(
        EASY_SMP_LOCK "addq %1,%0"
        : "=m" ((*v)) : "r" (i), "m" ((*v)));
}
static __inline__ int64_t easy_atomic_add_return(easy_atomic_t *value, int64_t i)
{
    int64_t                 __i = i;
    __asm__ __volatile__(
        EASY_SMP_LOCK "xaddq %0, %1;"
        :"=r"(i)
        :"m"(*value), "0"(i));
    return i + __i;
}
static __inline__ int64_t easy_atomic_cmp_set(easy_atomic_t *lock, int64_t old, int64_t set)
{
    uint8_t                 res;
    __asm__ volatile (
        EASY_SMP_LOCK "cmpxchgq %3, %1; sete %0"
        : "=a" (res) : "m" (*lock), "a" (old), "r" (set) : "cc", "memory");
    return res;
}
static __inline__ void easy_atomic_inc(easy_atomic_t *v)
{
    __asm__ __volatile__(EASY_SMP_LOCK "incq %0" : "=m" (*v) :"m" (*v));
}
static __inline__ void easy_atomic_dec(easy_atomic_t *v)
{
    __asm__ __volatile__(EASY_SMP_LOCK "decq %0" : "=m" (*v) :"m" (*v));
}
#else
typedef volatile int32_t easy_atomic_t;
#define easy_atomic_add(v,i) easy_atomic32_add(v,i)
#define easy_atomic_add_return(v,diff) easy_atomic32_add_return(v,diff)
#define easy_atomic_inc(v) easy_atomic32_inc(v)
#define easy_atomic_dec(v) easy_atomic32_dec(v)
static __inline__ int32_t easy_atomic_cmp_set(easy_atomic_t *lock, int32_t old, int32_t set)
{
    uint8_t                 res;
    __asm__ volatile (
        EASY_SMP_LOCK "cmpxchgl %3, %1; sete %0"
        : "=a" (res) : "m" (*lock), "a" (old), "r" (set) : "cc", "memory");
    return res;
}
#endif

#define easy_trylock(lock)  (*(lock) == 0 && easy_atomic_cmp_set(lock, 0, 1))
#define easy_unlock(lock)   {__asm__ ("" ::: "memory"); *(lock) = 0;}
#define easy_spin_unlock easy_unlock

static __inline__ void easy_spin_lock(easy_atomic_t *lock)
{
    int                     i, n;

    for ( ; ; ) {
        if (*lock == 0 && easy_atomic_cmp_set(lock, 0, 1)) {
            return;
        }

        for (n = 1; n < 1024; n <<= 1) {

            for (i = 0; i < n; i++) {
                __asm__ (".byte 0xf3, 0x90");
            }

            if (*lock == 0 && easy_atomic_cmp_set(lock, 0, 1)) {
                return;
            }
        }

        sched_yield();
    }
}

static __inline__ void easy_clear_bit(unsigned long nr, volatile void *addr)
{
    int8_t                  *m = ((int8_t *) addr) + (nr >> 3);
    *m &= (int8_t)(~(1 << (nr & 7)));
}
static __inline__ void easy_set_bit(unsigned long nr, volatile void *addr)
{
    int8_t                  *m = ((int8_t *) addr) + (nr >> 3);
    *m |= (int8_t)(1 << (nr & 7));
}

typedef struct easy_spinrwlock_t {
    easy_atomic_t           ref_cnt;
    easy_atomic_t           wait_write;
} easy_spinrwlock_t;
#define EASY_SPINRWLOCK_INITIALIZER {0, 0}
static __inline__ int easy_spinrwlock_rdlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        int                     cond = 1;

        while (cond) {
            int                     loop = 1;

            do {
                easy_atomic_t           oldv = lock->ref_cnt;

                if (0 <= oldv
                        && 0 == lock->wait_write) {
                    easy_atomic_t           newv = oldv + 1;

                    if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                        cond = 0;
                        break;
                    }
                }

                asm("pause");
                loop <<= 1;
            } while (loop < 1024);

            sched_yield();
        }
    }

    return ret;
}
static __inline__ int easy_spinrwlock_wrlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        int                     cond = 1;
        easy_atomic_inc(&lock->wait_write);

        while (cond) {
            int                     loop = 1;

            do {
                easy_atomic_t           oldv = lock->ref_cnt;

                if (0 == oldv) {
                    easy_atomic_t           newv = -1;

                    if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                        cond = 0;
                        break;
                    }
                }

                asm("pause");
                loop <<= 1;
            } while (loop < 1024);

            sched_yield();
        }

        easy_atomic_dec(&lock->wait_write);
    }

    return ret;
}
static __inline__ int easy_spinrwlock_try_rdlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        ret = EASY_AGAIN;
        easy_atomic_t           oldv = lock->ref_cnt;

        if (0 <= oldv
                && 0 == lock->wait_write) {
            easy_atomic_t           newv = oldv + 1;

            if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                ret = EASY_OK;
            }
        }
    }

    return ret;
}
static __inline__ int easy_spinrwlock_try_wrlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        ret = EASY_AGAIN;
        easy_atomic_t           oldv = lock->ref_cnt;

        if (0 == oldv) {
            easy_atomic_t           newv = -1;

            if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                ret = EASY_OK;
            }
        }
    }

    return ret;
}
static __inline__ int easy_spinrwlock_unlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        while (1) {
            easy_atomic_t           oldv = lock->ref_cnt;

            if (-1 == oldv) {
                easy_atomic_t           newv = 0;

                if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                    break;
                }
            } else if (0 < oldv) {
                easy_atomic_t           newv = oldv - 1;

                if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                    break;
                }
            } else {
                ret = EASY_ERROR;
                break;
            }
        }
    }

    return ret;
}

EASY_CPP_END

#endif
