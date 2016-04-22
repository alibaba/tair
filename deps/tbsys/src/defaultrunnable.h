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

#ifndef TBSYS_DEFAULT_RUNNABLE_H_
#define TBSYS_DEFAULT_RUNNABLE_H_

namespace tbsys {

/** 
 * @brief 线程执行具体的业务的封装类,同时它维护了一个线程数据，也可以将此类
 * 看成一个线程池类
 */
class CDefaultRunnable : public Runnable {

public:
    /**
     * 构造
     */
    CDefaultRunnable(int threadCount = 1);
    
    /**
     * 析构
     */
    virtual ~CDefaultRunnable();

    /**
     * 设置线程数
     */
    void setThreadCount(int threadCount);
    
    /** 
     * create %_threadCount threads
     * @return started thread count;
     */
    int start();

    /**
     * stop
     */
    void stop();

    /**
     * wait
     */
    void wait();

protected:    
    CThread *_thread;
    int _threadCount;
    bool _stop;
};

}

#endif /*RUNNABLE_H_*/
