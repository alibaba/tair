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

#include "tbsys.h"

namespace tbsys {

    /* 构造函数 */
    CFileQueueThread::CFileQueueThread(CFileQueue *queue, int threadCount, IQueueHandler *handler, void *args) : 
        CDefaultRunnable(threadCount)
    {
        assert(queue != NULL);
        _queue = queue;
        _handler = handler;
        _args = args;
    }
    
    /* 析构函数 */
    CFileQueueThread::~CFileQueueThread(void)
    {
        stop();
    }

    /* 写入数据 */
    int CFileQueueThread::writeData(void *data, int len)
    {
        if (data == NULL || len <= 0) {
            return EXIT_FAILURE;
        }
        _mutex.lock();
        _queue->push(data, len);
        _mutex.signal();
        _mutex.unlock();
        return EXIT_SUCCESS;
    }
    
    /* 停止 */
    void CFileQueueThread::stop()
    {
        _mutex.lock();
        _stop = true;
        _mutex.broadcast();
        _mutex.unlock();
    }
    
    /* 运行入口函数 */
    void CFileQueueThread::run(CThread *thread, void *args)
    {
        int threadIndex = (int)((long)(args));
        int total_count = 0;
        int64_t end_time = 0;
        int64_t start_time = CTimeUtil::getTime();
        bool dosuc = true;
        _mutex.lock();
        while(!_stop) {
            while(_stop == 0 && _queue->isEmpty()) {
                _mutex.wait();
            }
            if (_stop) {
                break;
            }
            queue_item *item = _queue->pop(threadIndex);
            int cnt = ++total_count;
            if (total_count >= 1000) {
                total_count = 0;
                end_time = CTimeUtil::getTime();
            }
            _mutex.unlock();
            if (item != NULL) {
                if (_handler != NULL) {
                    _handler->handleQueue(&item->data[0], item->len, threadIndex, _args);
                }
                dosuc = true;
                free(item);
            } else {
                dosuc = false;
            }
            if (end_time > start_time) {
                TBSYS_LOG(INFO, "task execute speed: %d task/s", (int)((int64_t)1000000*cnt/(end_time-start_time)));
                start_time = end_time;
            }
            _mutex.lock();
            if (dosuc) _queue->finish(threadIndex);
        }
        _mutex.unlock();
    }
}

//////////////////END
