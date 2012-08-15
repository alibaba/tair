/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * storage engine interface impl
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef FDB_MANAGER_H
#define FDB_MANAGER_H

#include <ext/hash_map>
#include <vector>

#include <tbsys.h>
#include "storage_manager.hpp"
#include "fdb_bucket.hpp"
#include "define.hpp"
#include "data_entry.hpp"
#include "mdb_factory.hpp"
#include "data_file_reader.hpp"
#include "stat_info.hpp"

namespace tair {
  namespace storage {
    namespace fdb {

      using namespace __gnu_cxx;
      typedef hash_map <int, fdb_bucket *> fdb_buckets_map;

      class fdb_manager:public tair::storage::storage_manager {
      public:
        fdb_manager();
        ~fdb_manager();

        int put(int bucket_number, data_entry & key, data_entry & value,
                bool version_care, int expire_time);

        int get(int bucket_number, data_entry & key, data_entry & value, bool stat = true);

        int remove(int bucket_number, data_entry & key, bool version_care);

        int clear(int area);

        bool init_buckets(const vector <int>&buckets);

        void close_buckets(const vector <int>&buckets);
        bool get_next_items(md_info & info, vector <item_data_info *>&list);
        void set_area_quota(int area, uint64_t quota)
        {
        }
        void set_area_quota(std::map<int, uint64_t> &quota_map)
        {
        }

        void begin_scan(md_info & info);
        void end_scan(md_info & info);

        void get_stats(tair_stat * stat);

      private:
          fdb_bucket * get_bucket(int bucket_number);


      private:
          fdb_buckets_map * buckets_map;
        storage_manager *memory_cache;
        data_reader *mreader;
        data_reader *dreader;
          tbsys::CThreadMutex stat_lock;
      };
    }
  }
}
#endif
