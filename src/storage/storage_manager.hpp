/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * storage engine interface
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_STORAGE_MANAGER_H
#define TAIR_STORAGE_MANAGER_H

#include <stdint.h>
#include <map>
#include "util.hpp"
#include "data_entry.hpp"
#include "define.hpp"
#include "stat_info.hpp"

namespace tair {
  typedef struct _migrate_dump_index
  {
    uint32_t hash_index;
    uint32_t db_id;
    bool is_migrate;
  } md_info;
  namespace storage
  {
    using namespace tair::util;
    using namespace tair::common;
    class storage_manager  {
    public:
      storage_manager():bucket_count(0)
      {
      }

      virtual ~ storage_manager()
      {
      }

      virtual int put(int bucket_number, data_entry & key, data_entry & value,
                      bool version_care, int expire_time) = 0;

      virtual int get(int bucket_number, data_entry & key,
                      data_entry & value) = 0;

      virtual int remove(int bucket_number, data_entry & key,
                         bool version_care) = 0;

      virtual int clear(int area) = 0;

      virtual bool init_buckets(const std::vector<int> &buckets) = 0;

      virtual void close_buckets(const std::vector<int> &buckets) = 0;
      virtual bool get_next_items(md_info & info,
                                  std::vector<item_data_info *> &list) = 0;
      virtual void begin_scan(md_info & info) = 0;
      virtual void end_scan(md_info & info) = 0;

      virtual void get_stats(tair_stat * stat) = 0;

      virtual void set_area_quota(int area, uint64_t quota) = 0;
      virtual void set_area_quota(std::map<int, uint64_t> &quota_map) = 0;

      void set_bucket_count(uint32_t bucket_count)
      {
        if(this->bucket_count != 0)
          return;                //can not rest bucket count
        this->bucket_count = bucket_count;
        return;
      }
    protected:
        uint32_t bucket_count;

    };
  }
}

#endif
