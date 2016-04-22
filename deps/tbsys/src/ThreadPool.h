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

#ifndef TBSYS_THREAD_POOL_H 
#define TBSYS_THREAD_POOL_H 
#include <list>
#include <deque>
#include <vector>
#include "Shared.h"
#include "Mutex.h"
#include "RecMutex.h"
#include "Cond.h"
#include "Monitor.h"
#include "TbThread.h"
#include "EventHandler.h"

#define DEFAUTL_STACK_SIZE 4*1024*1024   //16M
#define DEFALUT_LIST_SIZE_MAX 40960 


namespace tbutil 
{
/** 
* @brief ThreadPool 是一个线程池类,它实现一个领导者/跟随者模型 
*/
class ThreadPool : public Monitor<Mutex>
{
public:

    ThreadPool(int size = 1 , int sizeMax=1,int sizeWarn=1,
                              int listSizeMax= DEFALUT_LIST_SIZE_MAX,
                              int stackSize=DEFAUTL_STACK_SIZE);
    virtual ~ThreadPool();

    void destroy();

    /** 
     * @brief 增加一个任务
     * 
     * @param workItem: 新增加的任务
     * 
     * @return 
     */
    int  execute(ThreadPoolWorkItem* workItem);
    /** 
     * @brief 选择新的领导者
     * 
     * @param thid
     */
    inline void promoteFollower(pthread_t thid);

    /** 
     * @brief 等待所有线程退出
     */
    void joinWithAllThreads();

    /** 
     * @brief 是否达到线程队列的最大限制
     * 
     * @return 
     */
    bool isMaxCapacity() const;

private:

    bool run(pthread_t thid); // Returns true if a follower should be promoted.

    bool _destroyed;

    std::list<ThreadPoolWorkItem*> _workItems;
    int _listSize;
    int _procSize;
    const int _listSizeMax;
    Monitor<Mutex> _monitor; 
    class EventHandlerThread : public tbutil::Thread
    {
    public:
        
        EventHandlerThread(const ThreadPool*);
        virtual void run();

    private:

        ThreadPool* _pool;
    };
    friend class EventHandlerThread;

    const int _size; // Number of threads that are pre-created.
    const int _sizeMax; // Maximum number of threads.
    const int _sizeWarn; // If _inUse reaches _sizeWarn, a "low on threads" warning will be printed.
    const size_t _stackSize;

    std::vector<tbutil::ThreadPtr> _threads; // All threads, running or not.
    int _running; // Number of running threads.
    int _inUse; // Number of threads that are currently in use.
    double _load; // Current load in number of threads.

    bool _promote;
    volatile int _waitingNumber;
};
}
#endif
