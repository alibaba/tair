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

#include "tbrwlock.h"

using namespace tbsys;

int CRLock::lock() const
{
    return pthread_rwlock_rdlock(_rlock);
}

int CRLock::tryLock() const
{
    return pthread_rwlock_tryrdlock(_rlock);
}

int CRLock::unlock() const
{
    return pthread_rwlock_unlock(_rlock);
}

int CWLock::lock() const
{
    return pthread_rwlock_wrlock(_wlock);
}

int CWLock::tryLock() const
{
    return pthread_rwlock_trywrlock(_wlock);
}

int CWLock::unlock() const
{
    return pthread_rwlock_unlock(_wlock);
}

//////////////////////////////////////////////////////////////////////////////////////
CRWLock::CRWLock(ELockMode lockMode)
{
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    if (lockMode == READ_PRIORITY)
    {
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_READER_NP);
    }
    else if (lockMode == WRITE_PRIORITY)
    {
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    }
    pthread_rwlock_init(&_rwlock, &attr);
    
    _rlock = new CRLock(&_rwlock);
    _wlock = new CWLock(&_rwlock);
}

CRWLock::~CRWLock()
{
    pthread_rwlock_destroy(&_rwlock);
    delete _rlock;
    delete _wlock;
}

//////////////////////////////////////////////////////////////////////////////////////
CRWSimpleLock::CRWSimpleLock(ELockMode lockMode) 
{
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    if (lockMode == READ_PRIORITY)
    {
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_READER_NP);
    }
    else if (lockMode == WRITE_PRIORITY)
    {
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    }
    pthread_rwlock_init(&_rwlock, &attr);
}

CRWSimpleLock::~CRWSimpleLock()
{
    pthread_rwlock_destroy(&_rwlock);
}

int CRWSimpleLock::rdlock()
{
    return pthread_rwlock_rdlock(&_rwlock);
}

int CRWSimpleLock::wrlock()
{
     return pthread_rwlock_wrlock(&_rwlock);
}

int CRWSimpleLock::tryrdlock()
{
    return pthread_rwlock_tryrdlock(&_rwlock);
}

int CRWSimpleLock::trywrlock()
{
    return pthread_rwlock_trywrlock(&_rwlock);
}

int CRWSimpleLock::unlock()
{
    return pthread_rwlock_unlock(&_rwlock);
}
