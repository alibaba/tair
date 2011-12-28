/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * cache stat
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */
#ifndef TAIR_STORAGE_LDB_CACHE_STAT_H
#define TAIR_STORAGE_LDB_CACHE_STAT_H

#include <tbtimeutil.h>
#include "common/define.hpp"
#include "common/file_op.hpp"

#include "storage/mdb/mdb_stat.hpp"
#include <climits>

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      typedef tair::mdb_area_stat cache_stat;
      class tair::common::FileOperation;

      // cache stat dump filter.
      // only check time here
      struct StatDumpFilter
      {
        bool ok(uint32_t when) const
        {
          return when >= start_time_ && when <= end_time_;
        }

        StatDumpFilter() : start_time_(0), end_time_(~0U) {}
        uint32_t start_time_;
        uint32_t end_time_;
      };

      class CacheStat
      {
      public:
        CacheStat();
        ~CacheStat();
        static const int32_t RESERVE_SIZE = 64;
        static const int64_t MIN_FILE_SIZE = 20 * 1 << 20;
        static const int32_t BUFFER_STAT_COUNT = 128;
        static const int32_t CACHE_STAT_SIZE = sizeof(cache_stat);
        static const int32_t STAT_BUFFER_SIZE = CACHE_STAT_SIZE * BUFFER_STAT_COUNT;

        bool start(const char* dir, int64_t max_file_size = MIN_FILE_SIZE);
        void stop();
        void destroy();
        int save(cache_stat* stat, int32_t stat_count);

        // for dump util
        int load_file(const char* file_name);
        int dump(const StatDumpFilter* dump_filter = NULL);

      private:
        int init_file(const char* file_name);
        int save_file();
        void rotate();
        // for dump util
        void dump_stat(cache_stat* stats, int32_t count,
                       cache_stat* last_stats, cache_stat* total_stat,
                       const StatDumpFilter* filter, bool& should_dump);
        void print_and_add_total_stat(cache_stat* stat, cache_stat* last_stats, cache_stat* total_stat);
        void print_total_stat(const cache_stat* stat);
        void print_sentinel(const cache_stat* stat);
        void print_stat(int32_t area, const cache_stat* stat);
        void print_head();

        inline bool check_init() { return file_ != NULL; }

        inline bool stat_valid(const cache_stat* stat)
        {
          return stat->data_size > 0;
        }
        inline bool sentinel_valid(const cache_stat* stat)
        {
          return stat->space_usage > 0; // not check data_size
        }
        // sentinel tair_stat here, data_size is 0, space_usage as time
        inline void set_sentinel_stat(cache_stat* stat)
        {
          stat->data_size = 0;
          stat->space_usage = time(NULL);
        }
        inline bool is_sentinel(const cache_stat* stat)
        {
          return stat->data_size == 0;
        }
        inline void set_area(cache_stat* stat, int32_t area)
        {
          stat->data_size |= (static_cast<uint64_t>(area)) << 48;
        }
        inline int32_t get_area_and_clear(cache_stat* stat)
        {
          int32_t area = static_cast<uint32_t>(stat->data_size >> 48);
          stat->data_size &= ~(static_cast<uint64_t>(0xffff) << 48);
          return area;
        }
        inline uint32_t get_time(const cache_stat* stat)
        {
          return stat->space_usage;
        }
        inline std::string get_time_str(const cache_stat* stat)
        {
          char buf[128];
          struct tm *tm = localtime(reinterpret_cast<const time_t*>(&stat->space_usage));
          snprintf(buf, sizeof(buf),
                   "%04d-%02d-%02d %02d:%02d:%02d",
                   tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour,
                   tm->tm_min, tm->tm_sec);
          return std::string(buf);
        }

      private:
        tair::common::FileOperation* file_;
        bool is_load_;
        int64_t file_offset_;
        int64_t max_file_size_;
        // tair_stat and time
        char buf_[STAT_BUFFER_SIZE];
        char* buf_pos_;
        int32_t remain_buf_stat_count_;
      };
    }
  }
}
#endif
