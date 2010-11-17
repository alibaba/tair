/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * statistics header file
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_STAT_HELPER_H
#define TAIR_STAT_HELPER_H

#include <zlib.h>

#include "define.hpp"
#include "log.hpp"
#include "mmap_file.hpp"
#include "stat_info.hpp"
#include "storage_manager.hpp"

namespace tair {
   const static int STAT_LENGTH = TAIR_MAX_AREA_COUNT;
   const static int STAT_TOTAL_SIZE = STAT_LENGTH * sizeof(tair_stat);
#define TAIR_STAT tair::stat_helper::stat_helper_instance
   class stat_helper : public tbsys::CDefaultRunnable {
   public:
      stat_helper();
      ~stat_helper();

      void run(tbsys::CThread *thread, void *arg);

      tair_stat *get_curr_stats();
      char *get_and_reset();
      tair_stat *get_stats();
      int get_size();

      void stat_get(int area, int rc);
      void stat_put(int area);
      void stat_evict(int area);
      void stat_remove(int area);

      void reset();

      void set_storage_manager(tair::storage::storage_manager *storage_mgr) {
         this->storage_mgr = storage_mgr;
      }

   private:
      void init();
      void do_compress();

   public:
      static stat_helper stat_helper_instance;
   private:
      tair_stat *stat;
      tair_stat *curr_stat;
      uint64_t last_send_time;
      char *compressed_data;
      int data_size;
      bool sent;
      tair::storage::storage_manager *storage_mgr;

   };
}

#endif
