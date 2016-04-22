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

#ifndef TBSYS_MUTEX_H
#define TBSYS_MUTEX_H 

#include <pthread.h>
#include "Lock.h"
#include "ThreadException.h"

namespace tbutil
{
/** 
* @brief 互斥体,被实现为简单的数据结构
* Mutex非递归锁,使用时需要注意以下几点:
* 1.不要在同一线程中第二次调用lock
* 2.除非发出调用的线程持有某个互斥体，否则不要针对该互斥体调用unlock                                                          
*/
class Mutex
{
public:

    typedef LockT<Mutex> Lock;
    typedef TryLockT<Mutex> TryLock;

    Mutex();
    ~Mutex();

    /** 
     * @brief lock 函数尝试获取互斥体。如果互斥体已经锁住，它就会挂起发出
     * 调用的线程（calling thread），直到互斥体变得可用为止。一旦发出调
     * 用的线程获得了互斥体，调用就会立即返回
     */
    void lock() const;

    /** 
     * @brief tryLock 函数尝试获取互斥体。如果互斥体可用，互斥体就会锁
     * 住，而调用就会返回true。如果其他线程锁住了互斥体，调用返回false
     * 
     * @return 
     */
    bool tryLock() const;

    /** 
     * @brief unlock 函数解除互斥体的加锁
     */
    void unlock() const;

    /** 
     * @brief 是否已经加锁标记
     * 
     * @return 
     */
    bool willUnlock() const;

private:

    Mutex(const Mutex&);
    Mutex& operator=(const Mutex&);

    struct LockState
    {
        pthread_mutex_t* mutex;
    };

    void unlock(LockState&) const;
    void lock(LockState&) const;
    mutable pthread_mutex_t _mutex;

    friend class Cond;
};
}//end namespace
#endif
