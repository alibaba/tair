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

#ifndef TBSYS_LOCK_H
#define TBSYS_LOCK_H

#include "ThreadException.h"

namespace tbutil 
{
/**
 * @brief LockT是简单的模板类，由构造器和析构器构成
 * 构造器针对它的参数调用lock,析构器调用unlock,
 * 通过实例化类型为Lock的局部变量,可以完全解决死锁问题
 */
template <typename T>
class LockT
{
public:
    
    LockT(const T& mutex) :
        _mutex(mutex)
    {
        _mutex.lock();
        _acquired = true;
    }

    ~LockT()
    {
        if (_acquired)
        {
            _mutex.unlock();
        }
    }
    
    void acquire() const
    {
        if (_acquired)
        {
#ifdef _NO_EXCEPTION
           assert(!"ThreadLockedException");
#else
           throw ThreadLockedException(__FILE__, __LINE__);
#endif
        }
        _mutex.lock();
        _acquired = true;
    }


    bool tryAcquire() const
    {
        if (_acquired)
        {
#ifdef _NO_EXCEPTION
            assert(!"ThreadLockedException");
#else
            throw ThreadLockedException(__FILE__, __LINE__);
#endif
        }
        _acquired = _mutex.tryLock();
        return _acquired;
    }

    void release() const
    {
        if (!_acquired)
        {
#ifdef _NO_EXCEPTION
            assert(!"ThreadLockedException");
#else
            throw ThreadLockedException(__FILE__, __LINE__);
#endif
        }
        _mutex.unlock();
        _acquired = false;
    }

    bool acquired() const
    {
        return _acquired;
    }
   
protected:
    
    LockT(const T& mutex, bool) :
        _mutex(mutex)
    {
        _acquired = _mutex.tryLock();
    }

private:
    
    LockT(const LockT&);
    LockT& operator=(const LockT&);

    const T& _mutex;
    mutable bool _acquired;

    friend class Cond;
};

/** 
 * @brief TryLockT是简单的模板类，由构造器和析构器构成
 * 构造器针对它的参数调用lock,析构器调用unlock,
 * 通过实例化类型为TryLock的局部变量,可以完全解决死锁问题
 */
template <typename T>
class TryLockT : public LockT<T>
{
public:

    TryLockT(const T& mutex) :
        LockT<T>(mutex, true)
    {}
};
}  

#endif
