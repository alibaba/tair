/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * tair manager interface
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_ICACHE_MANAGER_H
#define TAIR_ICACHE_MANAGER_H
#include "data_entry.hpp"
#ifndef PROFILER_START
#define PROFILER_START(s)
#define PROFILER_STOP()
#define PROFILER_BEGIN(s)
#define PROFILER_END()
#define PROFILER_DUMP()
#define PROFILER_SET_THRESHOLD(sh)
#define PROFILER_SET_STATUS(st)
#endif

namespace tair {
   class base_tair_manager {
   public:
      base_tair_manager() {}
      virtual ~base_tair_manager() {}
      virtual int put(int area, data_entry &key, data_entry &data, uint16_t vesion, int expired) = 0;
      virtual bool add_count(int area, data_entry &key, int count, int initValue, int *ret_value) = 0;
      virtual bool get(int area, data_entry &key, data_entry &data) = 0;
      virtual bool remove(int area, data_entry &key) = 0;
      virtual bool clear(int area, int flag) = 0;
      virtual bool dump(int area, char *filename) = 0;
      virtual bool dump_bucket(int dbid, char *path) = 0;
      virtual int get_stat_info(int type, int area, char *data, int size) = 0;
      virtual void do_timer() = 0;
      virtual void set_int_flag(int area, data_entry &key, int flag, bool on) = 0;
      virtual int get_status() = 0;
      virtual void set_status(int newStatus) = 0;
      virtual bool is_persistent() = 0;
      virtual bool sync_db(int method, int dbid, uint64_t target) = 0;

   };
}

#endif
///////////////////////END
