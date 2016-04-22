/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#ifndef TAIR_THREAD_GUARD_HPP_
#define TAIR_THREAD_GUARD_HPP_

#include "tbsys.h"

namespace tair {
namespace common {
class thread_guard {
public:
    thread_guard(tbsys::CThreadMutex *mutex) {
        _mutex = NULL;
        _flag = true;
        if (mutex) {
            _mutex = mutex;
            _mutex->lock();
        }
    }

    thread_guard(tbsys::CThreadMutex *mutex, bool flag) {
        _mutex = NULL;
        _flag = flag;
        if (flag && mutex) {
            _mutex = mutex;
            _mutex->lock();
        }
    }

    ~thread_guard() {
        if (_flag && _mutex) {
            _mutex->unlock();
        }
    }

private:
    tbsys::CThreadMutex *_mutex;
    bool _flag;
};
}
}

#endif
