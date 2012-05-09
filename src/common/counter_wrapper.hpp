/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id: counter_wrapper.hpp 392 2012-04-01 02:02:41Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu.hfl <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_COUNTER_WRAPPER_HPP
#define TAIR_COUNTER_WRAPPER_HPP
#include "tbnet.h"

namespace tair {
  namespace common {
    struct counter_wrapper {
      counter_wrapper() {
        memset(this, 0, sizeof(*this));
      }

      counter_wrapper(const counter_wrapper &rhs) {
        memcpy(this, &rhs, sizeof(*this));
      }

      counter_wrapper& operator=(const counter_wrapper &rhs) {
        if (this != &rhs) {
          memcpy(this, &rhs, sizeof(*this));
        }
        return *this;
      }

      bool encode(tbnet::DataBuffer *output) const {
        output->writeInt32(count);
        output->writeInt32(init_value);
        output->writeInt32(expire);
        return true;
      }

      bool decode(tbnet::DataBuffer *input) {
        count = input->readInt32();
        init_value = input->readInt32();
        expire = input->readInt32();
        return true;
      }

      int32_t       count;
      int32_t       init_value;
      int32_t       expire;
    };
  }
}
#endif
