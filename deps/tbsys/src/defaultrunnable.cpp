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

/**
 * 构造
 */
CDefaultRunnable::CDefaultRunnable(int threadCount) {
    _stop = false;
    _threadCount = threadCount;
    _thread = NULL;
}
/*
 * 析构
 */
CDefaultRunnable::~CDefaultRunnable() {
    if (_thread) {
        delete[] _thread;
        _thread = NULL;
    }
}

/**
 * 设置线程数
 */
void CDefaultRunnable::setThreadCount(int threadCount)
{
    if (_thread != NULL) {
        TBSYS_LOG(ERROR, "已经在运行了不能设置线程数");
        return;
    }
    _threadCount = threadCount;
}

// start
int CDefaultRunnable::start() {
    if (_thread != NULL || _threadCount < 1) {
        TBSYS_LOG(ERROR, "start failure, _thread: %p, threadCount: %d", _thread, _threadCount);
        return 0;
    }
    _thread = new CThread[_threadCount];
    if (NULL == _thread)
    {
        TBSYS_LOG(ERROR, "create _thread object failed, threadCount: %d", _threadCount);
        return 0;
    }
    int i = 0;
    for (; i<_threadCount; i++)
    {
        if (!_thread[i].start(this, (void*)((long)i)))
        {
          return i;
        }
    }
    return i;
}

// stop
void CDefaultRunnable::stop() {
    _stop = true;
}

// wait
void CDefaultRunnable::wait() {
    if (_thread != NULL)
    {
        for (int i=0; i<_threadCount; i++)
        {
            _thread[i].join();
        }
    }
}

}

////////END
