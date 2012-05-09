/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * prefix locker
 *
 * Version: $Id: request_processor.hpp 737 2012-04-17 08:58:19Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */

#ifndef TAIR_PREFIX_LOCKER_HPP
#define TAIR_PREFIX_LOCKER_HPP
#include <pthread.h>

#include "util.hpp"
#include "data_entry.hpp"

namespace tair {
  class prefix_locker {
  public:
    prefix_locker() {
      for (size_t i = 0; i < LOCKER_NUM; ++i) {
        pthread_mutex_init(lockers + i, NULL);
      }
    }

    ~prefix_locker() {
      for (size_t i = 0; i < LOCKER_NUM; ++i) {
        pthread_mutex_destroy(lockers + i);
      }
    }

    void lock(const data_entry &key) {
      if (key.get_prefix_size() == 0) {
        return ;
      }

      uint32_t hash = util::string_util::mur_mur_hash(key.get_data(), key.get_prefix_size());
      size_t index = hash % LOCKER_NUM;
      pthread_mutex_lock(lockers + index);
    }

    void unlock(const data_entry &key) {
      if (key.get_prefix_size() == 0) {
        return ;
      }

      uint32_t hash = util::string_util::mur_mur_hash(key.get_data(), key.get_prefix_size());
      size_t index = hash % LOCKER_NUM;
      pthread_mutex_unlock(lockers + index);
    }

  private:
    prefix_locker(const prefix_locker&);
    prefix_locker& operator=(const prefix_locker&);

  private:
    enum { LOCKER_NUM = 1023 };
    pthread_mutex_t lockers[LOCKER_NUM];
  };
}
#endif
