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

#ifndef TBSYS_THREAD_H_
#define TBSYS_THREAD_H_

#include <linux/unistd.h>

namespace tbsys {

/** 
 * @brief 对linux线程简单封装 
 */
class CThread {

public:
    /**
     * 构造函数
     */
    CThread() {
        tid = 0;
        pid = 0;
    }

    /**
     * 起一个线程，开始运行
     */
    bool start(Runnable *r, void *a) {
        runnable = r;
        args = a;
        return 0 == pthread_create(&tid, NULL, CThread::hook, this);
    }

    /**
     * 等待线程退出
     */
    void join() {
        if (tid) {
            pthread_join(tid, NULL);
            tid = 0;
            pid = 0;
        }
    }

    /**
     * 得到Runnable对象
     * 
     * @return Runnable
     */
    Runnable *getRunnable() {
        return runnable;
    }

    /**
     * 得到回调参数
     * 
     * @return args
     */
    void *getArgs() {
        return args;
    }
    
    /***
     * 得到线程的进程ID
     */
    int getpid() {
        return pid;
    }

    /**
     * 线程的回调函数
     * 
     */

    static void *hook(void *arg) {
        CThread *thread = (CThread*) arg;
        thread->pid = gettid();

        if (thread->getRunnable()) {
            thread->getRunnable()->run(thread, thread->getArgs());
        }

        return (void*) NULL;
    }
    
private:    
    /**
     * 得到tid号
     */
    #ifdef _syscall0
    static _syscall0(pid_t,gettid)
    #else
    static pid_t gettid() { return static_cast<pid_t>(syscall(__NR_gettid));}
    #endif

private:
    pthread_t tid;      // pthread_self() id
    int pid;            // 线程的进程ID
    Runnable *runnable;
    void *args;
};

}

#endif /*THREAD_H_*/
