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
const int ITEM_HEAD_LENGTH = 2;

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
      virtual int add_count(int bucket_num,data_entry &key, int count, int init_value,
                bool allow_negative,int expire_time,int &result_value)
      {
          return TAIR_RETURN_NOT_SUPPORTED;
      }

      virtual int get_range(int bucket_number,data_entry & key_start,data_entry & key_end, int offset, int limit, int type, std::vector<data_entry*> &result, bool &has_next){return TAIR_RETURN_NOT_SUPPORTED;}

      virtual int clear(int area) = 0;

      virtual bool init_buckets(const std::vector<int> &buckets) = 0;

      virtual void close_buckets(const std::vector<int> &buckets) = 0;
      virtual bool get_next_items(md_info & info,
                                  std::vector<item_data_info *> &list) = 0;
      virtual void begin_scan(md_info & info) = 0;
      virtual void end_scan(md_info & info) = 0;

      virtual void get_stats(tair_stat * stat) = 0;

      virtual int get_meta(data_entry &key, item_meta_info &meta) {
        return TAIR_RETURN_NOT_SUPPORTED;
      }

      virtual void set_area_quota(int area, uint64_t quota) = 0;
      virtual void set_area_quota(std::map<int, uint64_t> &quota_map) = 0;

      // Different storage engine reserve different amount of bits for value flag,
      // we don't want to depend on the shortest bucket, so storage engine should
      // implement its flag operation(may use some trick etc.) based on own bit resource.
      // If one engine allocs too few bits to hold all flags, it is just deserved that this engine
      // can not support all storage feature.

      // use bit operation default
      virtual bool test_flag(uint8_t meta, int flag)
      {
        return (meta & flag) != 0;
      }
      virtual void set_flag(uint8_t& meta, int flag)
      {
        meta |= flag;
      }
      virtual void clear_flag(uint8_t& meta, int flag)
      {
        meta &= ~flag;
      }

      virtual int op_cmd(ServerCmdType cmd, std::vector<std::string>& params) { return TAIR_RETURN_NOT_SUPPORTED; }

      virtual void set_bucket_count(uint32_t bucket_count)
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
