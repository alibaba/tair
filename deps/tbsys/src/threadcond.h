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

#ifndef TBSYS_COND_H_
#define TBSYS_COND_H_

namespace tbsys {

/** 
 * @brief  Linux线程条件变量 
 */
class CThreadCond : public CThreadMutex {

public:

    /**
     * 构造函数
     */
    CThreadCond() {
        pthread_cond_init(&_cond, NULL);
    }

    /**
     * 析造函数
     */
    ~CThreadCond() {
        pthread_cond_destroy(&_cond);
    }

    /**
     * 等待信号
     *
     * @param  milliseconds  等待超时间(单位:ms), 0 = 永久等待，
     */
    bool wait(int milliseconds = 0) {
        bool ret = true;

        if (milliseconds == 0) { // 永久等待
            pthread_cond_wait(&_cond, &_mutex);
        } else {

            struct timeval curtime;

            struct timespec abstime;
            gettimeofday(&curtime, NULL);

            int64_t us = (static_cast<int64_t>(curtime.tv_sec) *
                          static_cast<int64_t>(1000000) +
                          static_cast<int64_t>(curtime.tv_usec) +
                          static_cast<int64_t>(milliseconds) *
                          static_cast<int64_t>(1000));

            abstime.tv_sec = static_cast<int>(us / static_cast<int64_t>(1000000));
            abstime.tv_nsec = static_cast<int>(us % static_cast<int64_t>(1000000)) * 1000;
            ret = (pthread_cond_timedwait(&_cond, &_mutex, &abstime) == 0);
        }

        return ret;
    }

    /**
     * 唤醒一个
     */
    void signal() {
        pthread_cond_signal(&_cond);
    }

    /**
     * 唤醒所有
     */
    void broadcast() {
        pthread_cond_broadcast(&_cond);
    }

private:
    pthread_cond_t _cond;
};

}

#endif /*COND_H_*/
