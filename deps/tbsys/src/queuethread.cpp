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
    CQueueThread::CQueueThread(int threadCount, IQueueHandler *handler, void *args) 
        : CDefaultRunnable(threadCount)
    {
        _handler = handler;
        _args = args;
    }
    
    /* 析构函数 */
    CQueueThread::~CQueueThread(void)
    {
        stop();
    }

    /* 写入数据 */
    int CQueueThread::writeData(void *data, int len)
    {
        if (data == NULL || len <= 0) {
            return EXIT_FAILURE;
        }
        data_pair *item = new data_pair();
        item->data = (char*) malloc(len);
        assert(item->data != NULL);
        memcpy(item->data, data, len);
        item->len = len;        
        _mutex.lock();
        _queue.push(item);
        _mutex.signal();
        _mutex.unlock();
        return EXIT_SUCCESS;
    }
    
    /* 停止 */
    void CQueueThread::stop()
    {
        _mutex.lock();
        _stop = true;
        _mutex.broadcast();
        _mutex.unlock();
    }
    
    /* 运行入口函数 */
    void CQueueThread::run(CThread *thread, void *args)
    {
        int threadIndex = (int)((long)(args));
        _mutex.lock();
        while(!_stop) {
            while(_stop == 0 && _queue.empty()) {
                _mutex.wait();
            }
            if (_stop) {
                break;
            }
            data_pair *item = _queue.front();
            _queue.pop();
            _mutex.unlock();
            if (item != NULL) {
                if (_handler) {
                    _handler->handleQueue(item->data, item->len, threadIndex, _args);
                }
                if (item->data) {
                    free(item->data);
                }
                free(item);
            }
            _mutex.lock();
        }
        _mutex.unlock();
    }
}

//////////////////END
