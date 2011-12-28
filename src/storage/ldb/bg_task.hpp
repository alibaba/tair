/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb storage compact manager
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_LDB_BG_TASK_H
#define TAIR_STORAGE_LDB_BG_TASK_H

#include "Timer.h"

namespace leveldb
{
  class DB;
}

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      class LdbInstance;
      class LdbCompact;

      class LdbCompactTask : public tbutil::TimerTask
      {
      public:
        static const int COMPACT_PAUSE_TIME = 43200; // 12h

        LdbCompactTask();
        virtual ~LdbCompactTask();
        virtual void runTimerTask();

        bool init(LdbInstance* db);

      private:
        bool is_compact_time();
        bool need_compact();
        bool should_compact();
        void do_compact();

        void compact_for_gc();
        void compact_gc(GcType gc_type, bool& all_done);
        void compact_for_expired();

        bool get_time_range(const char* str, int32_t& min, int32_t& max);
        bool is_in_range(int32_t min, int32_t max);

      private:
        LdbInstance* db_;
        int32_t min_time_;
        int32_t max_time_;
        tbsys::CThreadMutex lock_;
        bool is_compacting_;
      };
      typedef tbutil::Handle<LdbCompactTask> LdbCompactTaskPtr;

      class BgTask
      {
      public:
        BgTask();
        ~BgTask();

        bool start(LdbInstance* db);
        void stop();

      private:
        bool init_compact_task(LdbInstance* db);
        void stop_compact_task();

      private:
        tbutil::TimerPtr timer_;
        LdbCompactTaskPtr compact_task_;
      };
    }
  }
}
#endif
