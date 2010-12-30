/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * fdb_bucket hold one bucket for fdb storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_FDB_BUCKET_H
#define TAIR_FDB_BUCKET_H

#include "data_entry.hpp"
#include "fdb_item_manager.hpp"
#include "locker.hpp"

namespace tair {
  namespace storage {
    namespace fdb  {
      using namespace tair::common;
  
      class locker;
      class fdb_bucket {
      public:
        fdb_bucket();
        ~fdb_bucket();

        bool start(int bucket_number);
        void stop();

        int put(data_entry & key, data_entry & value, bool version_care,
                uint32_t expire_time, bool exclusive = true);

        int get(data_entry & key, data_entry & value, bool exclusive = true);

        int remove(data_entry & key, bool version_care, bool exclusive =
                   true);

        void backup();

        void get_stat(tair_stat * stat);

      private:
          bool is_item_expired(fdb_item & item);
        int pad_size(int data_size);

      private:
          fdb_item_manager * item_manager;
          locker *lockers;
      };
    }
  }
}
#endif
