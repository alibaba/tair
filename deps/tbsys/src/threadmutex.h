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

#ifndef TBSYS_MUTEX_H_
#define TBSYS_MUTEX_H_

#include <assert.h>

namespace tbsys {

/*
 * author cjxrobot
 *
 * Linux线程锁
 */

/** 
* @brief linux线程锁互斥锁简单封装 
*/
class CThreadMutex {

public:
    /*
     * 构造函数
     */
    CThreadMutex() {
        //assert(pthread_mutex_init(&_mutex, NULL) == 0);
        const int iRet = pthread_mutex_init(&_mutex, NULL);
        (void) iRet;
        assert( iRet == 0 );
    }

    /*
     * 析造函数
     */
    ~CThreadMutex() {
        pthread_mutex_destroy(&_mutex);
    }

    /**
     * 加锁
     */

    void lock () {
        pthread_mutex_lock(&_mutex);
    }

    /**
     * trylock加锁
     */

    int trylock () {
        return pthread_mutex_trylock(&_mutex);
    }    

    /**
     * 解锁
     */
    void unlock() {
        pthread_mutex_unlock(&_mutex);
    }

protected:

    pthread_mutex_t _mutex;
};

/** 
 * @brief 线程的Guard
 */
class CThreadGuard
{
public:
    CThreadGuard(CThreadMutex *mutex)
    {
      _mutex = NULL;
        if (mutex) {
            _mutex = mutex;
            _mutex->lock();
        }
    }
    ~CThreadGuard()
    {
        if (_mutex) {
            _mutex->unlock();
        }
    }
private:
    CThreadMutex *_mutex;
};

}

#endif /*MUTEX_H_*/
