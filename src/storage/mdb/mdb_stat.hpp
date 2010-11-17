/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#ifndef MDB_STAT_H
#define MDB_STAT_H
#include "mdb_define.hpp"
namespace tair {

#define TAIR_DIFF_VALUE(a,b) ((a>=b)?(a-b):(a))

#pragma pack(1)
  class tair_mdb_stat {
  public:
    tair_mdb_stat()
    {
      memset(this, 0, sizeof(tair_mdb_stat));
    }
    void addStat(tair_mdb_stat & stat)
    {
      get_count += stat.get_count;
      put_count += stat.put_count;
      evict_count += stat.evict_count;
      remove_count += stat.remove_count;
      hit_count += stat.hit_count;
      request_count += stat.request_count;
      data_size += stat.data_size;
      use_size += stat.use_size;
      item_count += stat.item_count;
      curr_load += stat.curr_load;
      read_bytes += stat.read_bytes;
      write_bytes += stat.write_bytes;
    }

  public:
      uint64_t get_count;
    uint64_t put_count;
    uint64_t evict_count;
    uint64_t remove_count;
    uint64_t hit_count;
    uint64_t request_count;
    uint64_t read_bytes;
    uint64_t write_bytes;

    uint64_t data_size;
    uint64_t use_size;
    uint32_t item_count;

    uint32_t curr_load;
    uint32_t startup_time;
  };

  class mdb_area_stat
  {
  public:
    uint64_t quota;
    uint64_t data_size;
    uint64_t space_usage;

    uint64_t item_count;
    uint64_t hit_count;
    uint64_t get_count;
    uint64_t put_count;
    uint64_t remove_count;
    uint64_t evict_count;
  };

// slab
  class tair_slab_stat
  {
  public:
    uint32_t slabId;                // slabId;
    uint32_t size;                //mdb_item size
    uint32_t perslab;                // mdb_item count per page
    uint32_t page_size;                // page count per slab
    uint32_t item_count;        // mdb_item count in slab
    uint64_t evict_count;
  };

// Hash
  class tair_hash_stat
  {
  public:
    uint32_t bucket_size;
    int expanding;
    uint32_t old_bucket_size;
    uint32_t expand_bucket;
    uint32_t item_count;
    int link_depth[TAIR_SLAB_HASH_MAXDEPTH];
  };
#pragma pack()

  class tair_stat_ratio
  {
  public:
    tair_stat_ratio()
    {
      memset(this, 0, sizeof(tair_stat_ratio));
    }

    void update(tair_mdb_stat * stat_info)
    {
      uint64_t now = tbsys::CTimeUtil::getTime();
      if(last_update_time > 0) {
        uint64_t interval = (now - last_update_time);
        interval /= 1000000;
        if(interval == 0)
          interval = 1;

        uint64_t gcdiff = TAIR_DIFF_VALUE(stat_info->get_count, get_count);
        uint64_t pcdiff = TAIR_DIFF_VALUE(stat_info->put_count, put_count);
        uint64_t rcdiff =
          TAIR_DIFF_VALUE(stat_info->remove_count, remove_count);
        uint64_t trcdiff =
          TAIR_DIFF_VALUE(stat_info->request_count, request_count);
        uint64_t hcdiff = TAIR_DIFF_VALUE(stat_info->hit_count, hit_count);
        uint64_t rbdiff = TAIR_DIFF_VALUE(stat_info->read_bytes, read_bytes);
        uint64_t wbdiff =
          TAIR_DIFF_VALUE(stat_info->write_bytes, write_bytes);

        get_count_ratio = 100 * gcdiff / interval;
        put_count_ratio = 100 * pcdiff / interval;
        remove_count_ratio = 100 * rcdiff / interval;
        read_bytes_ratio = 100 * rbdiff / interval;
        write_bytes_ratio = 100 * wbdiff / interval;
        if(gcdiff > 0) {
          hit_count_ratio = 100 * hcdiff / gcdiff;
        }
        else if(get_count > 0) {
          hit_count_ratio = 100 * hit_count / get_count;
        }
        else {
          hit_count_ratio = 0;
        }
        if(put_count > 0) {
          evict_count_ratio = 100 * evict_count / put_count;
        }
        else {
          evict_count_ratio = 0;
        }
        request_count_ratio = 100 * trcdiff / interval;
      }

      get_count = stat_info->get_count;
      put_count = stat_info->put_count;
      evict_count = stat_info->evict_count;
      remove_count = stat_info->remove_count;
      hit_count = stat_info->hit_count;
      request_count = stat_info->request_count;
      read_bytes = stat_info->read_bytes;
      write_bytes = stat_info->write_bytes;

      last_update_time = now;
    }

  public:
    uint64_t get_count;
    uint64_t put_count;
    uint64_t evict_count;
    uint64_t remove_count;
    uint64_t hit_count;
    uint64_t request_count;
    uint64_t read_bytes;
    uint64_t write_bytes;

    int get_count_ratio;
    int put_count_ratio;
    int evict_count_ratio;
    int remove_count_ratio;
    int hit_count_ratio;
    int request_count_ratio;
    int read_bytes_ratio;
    int write_bytes_ratio;

    uint64_t last_update_time;
  };

}

#endif
