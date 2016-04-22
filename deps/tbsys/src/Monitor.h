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

#ifndef TBSYS_MONITOR_H
#define TBSYS_MONITOR_H
#include <iostream>
#include "Lock.h"
#include "Cond.h"

//using namespace std;

namespace tbutil
{
/** 
* @brief 监控器是一个模板类，它需要Mutex或都RecMutex作模板参数,监控器是一种
* 用于保护临界区的同步机制,和互斥体一样，在临界区内只能一个线程在活动
* 监控器允许你在临界区内挂起线程,这样另外一个线程就能进入临界区，第二个线程就
* 能退出临界区,或者在临界区内挂起自己,无论哪种情况原来的线程都会被唤醒，继续
* 执行原来的任务
*/
template <class T>
class Monitor
{
public:

    typedef LockT<Monitor<T> > Lock;
    typedef TryLockT<Monitor<T> > TryLock;

    Monitor();
    ~Monitor();

    /** 
     * @brief 这个函数尝试锁住监控器。如果监控器已被另外的线程锁住，发出
     * 调用的线程就会挂起，直到监控器可用为止。在调用返回时，监控器已被它锁住
     */
    void lock() const;
    /** 
     * @brief 这个函数解除监控器的加锁。如果有另外的线程在等待进入监控器
     * (也就是阻塞在lock 调用中)，其中一个线程会被唤醒，并锁住监控器
     * 
     * @return 
     */
    void unlock() const;
    /** 
     * @brief 这个函数尝试锁住监控器。如果监控器可用，调用就锁住监控器，
     * 返回true。如果监控器已被另外的线程锁住，调用返回false 
     */
    bool tryLock() const;

    /** 
     * @brief 这个函数挂起发出调用的线程，同时释放监控器上的锁。其他线程可能调用notify
     * 或notifyAll 来唤醒在wait 调用中挂起的线程.当wait 调用返回时，监控器重被锁住,
     * 而挂起的线程会恢复执行 
     * @return 
     */
    bool wait() const;
    /** 
     * @brief 这个函数挂起调用它的线程，直到指定的时间流逝。如果有另外的
     * 线程调用notify 或notifyAll，在发生超时之前唤醒挂起的线程，
     * 这个调用返回true，监控器重被锁住，挂起的线程恢复执行。而如果
     * 发生超时，函数返回false
     * @param Time
     * 
     * @return 
     */
    bool timedWait(const Time&) const;
    /** 
     * @brief 这个函数唤醒目前在wait 调用中挂起的一个线程。如果在调用
     * notify 时没有这样的线程，通知就会丢失(也就是说，如果没有线程
     * 能被唤醒，对notify 的调用不会被记住)
     */
    void notify();
    /** 
     * @brief 这个函数唤醒目前在wait 调用中挂起的所有线程。和notify 一
     * 样，如果这时没有挂起的线程，对notifyAll 的调用就会丢失 
     */
    void notifyAll();

private:

    Monitor(const Monitor&);
    Monitor& operator=(const Monitor&);

    void notifyImpl(int) const;

    mutable Cond _cond;
    T _mutex;
    mutable int _nnotify;
};

template <class T> 
Monitor<T>::Monitor() :
    _nnotify(0)
{
}

template <class T> 
Monitor<T>::~Monitor()
{
}

template <class T> inline void
Monitor<T>::lock() const
{
    _mutex.lock();
    if(_mutex.willUnlock())
    {
        _nnotify = 0;
    }
}

template <class T> inline void
Monitor<T>::unlock() const
{
    if(_mutex.willUnlock())
    {
        notifyImpl(_nnotify);
    }
    _mutex.unlock();
}

template <class T> inline bool
Monitor<T>::tryLock() const
{
    bool result = _mutex.tryLock();
    if(result && _mutex.willUnlock())
    {
        _nnotify = 0;
    }
    return result;
}

template <class T> inline bool 
Monitor<T>::wait() const
{
    notifyImpl(_nnotify);
#ifdef _NO_EXCEPTION
    const bool bRet = _cond.waitImpl(_mutex);
    _nnotify = 0;
    return bRet;
#else
    try
    {
        _cond.waitImpl(_mutex);
    }
    catch(...)
    {
        _nnotify = 0;
        throw;
    }

    _nnotify = 0;
#endif
    return true;
}

template <class T> inline bool
Monitor<T>::timedWait(const Time& timeout) const
{
    notifyImpl(_nnotify);
#ifdef _NO_EXCEPTION
    const bool rc = _cond.timedWaitImpl(_mutex, timeout);
    _nnotify = 0;
    return rc;
#else
    try
    {
        _cond.timedWaitImpl(_mutex, timeout);
    }
    catch(...)
    {
        _nnotify = 0;
        throw;
    }
    _nnotify = 0;
#endif
    return true;
}

template <class T> inline void
Monitor<T>::notify()
{
    if(_nnotify != -1)
    {
        ++_nnotify;
    }
}

template <class T> inline void
Monitor<T>::notifyAll()
{
    _nnotify = -1;
}


template <class T> inline void
Monitor<T>::notifyImpl(int nnotify) const
{
    if(nnotify != 0)
    {
        if(nnotify == -1)
        {
            _cond.broadcast();
            return;
        }
        else
        {
            while(nnotify > 0)
            {
                _cond.signal();
                --nnotify;
            }
        }
    }
}
}//end namespace
#endif
