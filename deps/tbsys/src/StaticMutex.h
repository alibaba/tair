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

#ifndef TBSYS_STATIC_MUTEX_H
#define TBSYS_STATIC_MUTEX_H
#include "Lock.h"
#include "ThreadException.h"
namespace tbutil
{
class Cond;
/** 
 * @brief 互斥体,被实现为简单的数据结构
 * 所以其实例可以静态声明，并在编译过程中初始化 
 * StaticMutex是非递归锁,使用时需要注意以下几点:
 * 1.不要在同一线程中第二次调用lock
 * 2.除非发出调用的线程持有某个互斥体，否则不要针对该互斥体调用unlock
 */
class StaticMutex
{
public:

    typedef LockT<StaticMutex> Lock;
    typedef TryLockT<StaticMutex> TryLock;

    /** 
     * @brief lock 函数尝试获取互斥体。如果互斥体已经锁住，它就会挂起发出
     * 调用的线程（calling thread），直到互斥体变得可用为止。一旦发出调
     * 用的线程获得了互斥体，调用就会立即返回
     */
    void lock() const;

    /** 
     * @brief tryLock 函数尝试获取互斥体。如果互斥体可用，互斥体就会锁
     住，而调用就会返回true。如果其他线程锁住了互斥体，调用返回false
     * 
     * @return 
     */
    bool tryLock() const;

    /** 
     * @brief unlock 函数解除互斥体的加锁
     */
    void unlock() const;


    mutable pthread_mutex_t _mutex;

    friend class Cond;
private:
    struct LockState
    {
        pthread_mutex_t* mutex;
    };

    void unlock(LockState&) const;
    void lock(LockState&) const;
};

#define TNET_STATIC_MUTEX_INITIALIZER { PTHREAD_MUTEX_INITIALIZER }


extern StaticMutex globalMutex;

inline void StaticMutex::lock() const
{
    const int rt = pthread_mutex_lock(&_mutex);
    if( 0 != rt)
    {
        if(rt == EDEADLK)
        {
#ifdef _NO_EXCEPTION
            if ( rt != 0 )
            {
                assert(!"ThreadLockedException");
                TBSYS_LOG(ERROR,"%s","ThreadLockedException");
            }
#else
            throw ThreadLockedException(__FILE__, __LINE__);
#endif
        }
        else
        {
#ifdef _NO_EXCEPTION
            if ( rt != 0 )
            { 
                assert(!"ThreadSyscallException");
                TBSYS_LOG(ERROR,"%s","ThreadSyscallException");
            }
#else
            throw ThreadSyscallException(__FILE__, __LINE__, rt);
#endif
        }
    }
}

inline bool StaticMutex::tryLock() const
{
    const int rc = pthread_mutex_trylock(&_mutex);
    if(rc != 0 && rc != EBUSY)
    {
        if(rc == EDEADLK)
        {
#ifdef _NO_EXCEPTION
           if ( rc != 0 )
           {
               assert(!"ThreadLockedException");
               TBSYS_LOG(ERROR,"%s","ThreadLockedException");
           }
#else
            throw ThreadLockedException(__FILE__, __LINE__);
#endif
        }
        else
        {
#ifdef _NO_EXCEPTION
           if ( rc != 0 )
           {
               assert(!"ThreadSyscallException");
               TBSYS_LOG(ERROR,"%s","ThreadSyscallException");
           }
#else
            throw ThreadSyscallException(__FILE__, __LINE__,rc);
#endif
        }
    }
    return (rc == 0);
}

inline void StaticMutex::unlock() const
{
    const int rc = pthread_mutex_unlock(&_mutex);
#ifdef _NO_EXCEPTION
    if ( rc != 0 )
    {
        assert(!"ThreadSyscallException");
        TBSYS_LOG(ERROR,"%s","ThreadSyscallException");
    }
#else
    if(rc != 0)
    {
        throw ThreadSyscallException(__FILE__, __LINE__, rc);
    }
#endif
}

inline void
StaticMutex::unlock(LockState& state) const
{
    state.mutex = &_mutex;
}

inline void
StaticMutex::lock(LockState&) const
{
}

}// end namespace tbutil
#endif
