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

#ifndef TBSYS_RW_LOCK_H
#define TBSYS_RW_LOCK_H

#include <pthread.h>
#include "tblockguard.h"

namespace tbsys
{
    enum ELockMode
    {
        NO_PRIORITY,
        WRITE_PRIORITY,
        READ_PRIORITY
    };

    /** 
     * @brief linux线程 读写锁中的读锁的封装
     */
    class CRLock
    {
    public:
        CRLock(pthread_rwlock_t* lock) : _rlock(lock) {}
        ~CRLock() {}
        
        /** 
         * @brief 加锁
         * 
         * @return 
         */
        int lock() const;
        /** 
         * @brief 尝试加锁
         * 
         * @return 
         */
        int tryLock() const;
        /** 
         * @brief 解锁
         * 
         * @return 
         */
        int unlock() const;
        
    private:
        mutable pthread_rwlock_t* _rlock;
    };

    /** 
     * @brief linux 线程读写锁中的写锁的封装
     */
    class CWLock
    {
    public:
        CWLock(pthread_rwlock_t* lock) : _wlock(lock) {}
        ~CWLock() {}
        
        int lock() const;
        int tryLock() const;
        int unlock() const;
        
    private:
        mutable pthread_rwlock_t* _wlock;
    };    

    class CRWLock 
    {
    public:
        CRWLock(ELockMode lockMode = NO_PRIORITY);
        ~CRWLock();

        CRLock* rlock() const {return _rlock;}
        CWLock* wlock() const {return _wlock;} 

    private:
        CRLock* _rlock;
        CWLock* _wlock;
        pthread_rwlock_t _rwlock;
    };

    /** 
     * @brief 对linux线程锁中的读写锁封装
     */
    class CRWSimpleLock
    {
    public:
        CRWSimpleLock(ELockMode lockMode = NO_PRIORITY);
        ~CRWSimpleLock();
        
        int rdlock();
        int wrlock();
        int tryrdlock();
        int trywrlock();
        int unlock();
        
    private:    
        pthread_rwlock_t _rwlock;
    };

    /** 
     * @brief linux 线程锁中读锁的助手类
     */
    class CRLockGuard
    {
    public:
        CRLockGuard(const CRWLock& rwlock, bool block = true) : _guard((*rwlock.rlock()), block) {}
        ~CRLockGuard(){}

        bool acquired()
        {
            return _guard.acquired();
        }

    private:
        CLockGuard<CRLock> _guard;
    };
    
    /** 
     * @brief linux 线程锁中写锁的助手类
     */
    class CWLockGuard
    {
    public:
        CWLockGuard(const CRWLock& rwlock, bool block = true) : _guard((*rwlock.wlock()), block) {}
        ~CWLockGuard(){}

        bool acquired()
        {
            return _guard.acquired();
        }

    private:
        CLockGuard<CWLock> _guard;
    };
}

#endif
