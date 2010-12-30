/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * lock array support
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *
 */
#include "locker.hpp"

namespace tair {
  namespace storage {
    namespace kdb {

      locker::locker(int bucket_number)
      {
        this->bucket_number = bucket_number;
        b_locks = NULL;
        init();
      }

      locker::~locker()
      {

        for(int i = 0; i < bucket_number; i++)
        {
          pthread_rwlock_destroy(b_locks + i);
        }

        if(b_locks != NULL && bucket_number > 0)
        {
          delete[]b_locks;
        }
      }

      void locker::init()
      {
        b_locks = new pthread_rwlock_t[bucket_number];
        for(int i = 0; i < bucket_number; i++) {
          if(pthread_rwlock_init((pthread_rwlock_t *) b_locks + i, NULL) != 0) {
            log_error("init lockers failed [%d]", i);
            // destory inited locks
            for(int j = i; j >= 0; j--) {
              pthread_rwlock_destroy(b_locks + j);
            }
            exit(1);
          }
        }
      }

      bool locker::lock(int index, bool is_write)
      {
        if(is_write) {
          return pthread_rwlock_wrlock(b_locks + index) == 0;
        }
        else {
          return pthread_rwlock_rdlock(b_locks + index) == 0;
        }
      }

      bool locker::unlock(int index)
      {
        return pthread_rwlock_unlock(b_locks + index) == 0;
      }

    }

  }
}
