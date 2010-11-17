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
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef TAIR_DUM_MANAGER_H
#define TAIR_DUM_MANAGER_H
#include "tbsys.h"
#include "dump_filter.hpp"
#include "storage_manager.hpp"
#include <set>
namespace tair {
  namespace storage {
    class dump_thread:public tbsys::CDefaultRunnable {
    public:
      dump_thread(const std::set<dump_filter> &dump_filters,
                  const std::set<uint32_t> &bucket, uint32_t now,
                  storage_manager * storage_mgr);
      void run(tbsys::CThread * thread, void *arg);
      void cancle();
      bool is_alive() const;
    private:
        bool cancled;
      bool running;
        std::vector<dump_filter> dump_filters;
        std::set<uint32_t> buckets;
      storage_manager *storage_mgr;

    };
    class dump_manager
    {
    public:
      explicit dump_manager(storage_manager *);
      void do_dump(const std::set<dump_filter> &dump_filters,
                   const set<uint32_t> &bucket, uint32_t now);
      void cancle_all();
      bool is_all_stoped();
       ~dump_manager();
    private:
        dump_manager(const dump_manager &);
        dump_manager & operator =(const dump_manager &);
      void release_reference();
    private:
        std::set<dump_thread *>set_thread;
        tbsys::CThreadMutex mutex;
      storage_manager *storage_mgr;
    };
  }
}
#endif
