/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * kyotocabinet storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_KDB_MANAGER_H
#define TAIR_STORAGE_KDB_MANAGER_H

#include "kdb_bucket.h"

#include <ext/hash_map>
#include <vector>

#include <tbsys.h>

#include "common/data_entry.hpp"
#include "common/stat_info.hpp"
#include "storage/storage_manager.hpp"

namespace tair {
  namespace storage {
    namespace kdb {

      typedef __gnu_cxx::hash_map <int, kdb_bucket* > kdb_buckets_map;

      class kdb_manager : public tair::storage::storage_manager {
        public:
          kdb_manager();
          ~kdb_manager();

          int put(int bucket_number, data_entry & key, data_entry & value,
              bool version_care, int expire_time);
          int get(int bucket_number, data_entry & key, data_entry & value);
          int remove(int bucket_number, data_entry & key, bool version_care);
          int clear(int area);

          bool init_buckets(const std::vector <int>&buckets);
          void close_buckets(const std::vector <int>&buckets);

          void begin_scan(md_info & info);
          void end_scan(md_info & info);
          bool get_next_items(md_info & info, std::vector <item_data_info *>&list);

          void set_area_quota(int area, uint64_t quota);
          void set_area_quota(std::map<int, uint64_t> &quota_map);

          void get_stats(tair_stat * stat);

        private:
          kdb_bucket* get_bucket(int bucket_number);

        private:
          kdb_buckets_map* buckets_map;
          tbsys::CThreadMutex lock;

          kdb_bucket* scan_kdb;
      };
    }
  }
}

#endif
