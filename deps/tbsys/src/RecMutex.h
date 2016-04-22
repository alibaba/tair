/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong
 *
 */

#ifndef TBSYS_RMUTEX_H
#define TBSYS_RMUTEX_H
#include "Lock.h"
#include "ThreadException.h"

namespace tbutil
{
class Cond;
/** 
 * @brief RecMutex 实现的是递归互斥体
 * 非递归互斥体的使用一样，在使用递归互斥体时，你必须遵守一些简
 * 单的规则:
 * 1.除非发出调用的线程持有锁，否则不要针对某个互斥体调用unlock
 * 2.要让互斥体能够被其他线程获取，你调用unlock 的次数必须和你调用
 * lock 的次数相同（在递归互斥体的内部实现中，有一个初始化成零的
 * 计数器。每次调用lock，计数器就会加一，每次调用unlock，计数
 * 器就会减一； 当计数器回到零时，另外的线程就可以获取互斥体了）
 */
class RecMutex
{
public:

    typedef LockT<RecMutex> Lock;
    typedef TryLockT<RecMutex> TryLock;

    RecMutex();
    ~RecMutex();

    /** 
     * @brief lock 函数尝试获取互斥体。如果互斥体已被另一个线程锁住，它就
     * 会挂起发出调用的线程，直到互斥体变得可用为止。如果互斥体可用、
     * 或者已经被发出调用的线程锁住，这个调用就会锁住互斥体，并立即返回
     */
    void lock() const;

    /** 
     * @brief tryLock函数的功能与lock类似，但如果互斥体已被另一个线程锁住,
     * 它不会阻塞调用者，而会返回false。否则返回值是true
     * @return 
     */
    bool tryLock() const;

    /** 
     * @brief unlock 函数解除互斥体的加锁
     */
    void unlock() const;

    bool willUnlock() const;

private:

    // noncopyable
    RecMutex(const RecMutex&);
    RecMutex& operator=(const RecMutex&);

    struct LockState
    {
        pthread_mutex_t* mutex;
        int count;
    };

    void unlock(LockState&) const;
    void lock(LockState&) const;

    friend class Cond;

    mutable pthread_mutex_t _mutex;

    mutable int _count;
};
}//end namespace 
#endif
