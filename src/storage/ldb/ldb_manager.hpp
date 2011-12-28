/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_LDB_MANAGER_H
#define TAIR_STORAGE_LDB_MANAGER_H

#include "storage/storage_manager.hpp"
#include "common/data_entry.hpp"
#include "common/stat_info.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {

      class LdbInstance;
      // manager hold buckets. single or multi leveldb instance based on config for test
      // it works, maybe make it common manager level.
      class LdbManager : public tair::storage::storage_manager
      {
      public:
        LdbManager();
        ~LdbManager();

        int put(int bucket_number, data_entry& key, data_entry& value,
                bool version_care, int expire_time);
        int get(int bucket_number, data_entry& key, data_entry& value);
        int remove(int bucket_number, data_entry& key, bool version_care);
        int clear(int area);

        bool init_buckets(const std::vector <int>& buckets);
        void close_buckets(const std::vector <int>& buckets);

        void begin_scan(md_info& info);
        void end_scan(md_info& info);
        bool get_next_items(md_info& info, std::vector <item_data_info *>& list);

        void set_area_quota(int area, uint64_t quota);
        void set_area_quota(std::map<int, uint64_t>& quota_map);

        void get_stats(tair_stat* stat);

      private:
        LdbInstance* get_db_instance(int bucket_number);

      private:
        LdbInstance** ldb_instance_;
        int32_t db_count_;
        storage_manager* cache_;
        LdbInstance* scan_ldb_;
        tbsys::CThreadMutex lock_;
      };
    }
  }
}

#endif
