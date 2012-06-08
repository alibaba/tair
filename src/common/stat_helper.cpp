/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * statistics impl
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "stat_helper.hpp"
namespace tair {
   stat_helper stat_helper::stat_helper_instance;

   stat_helper::stat_helper()
   {
      stat = NULL;
      curr_stat = NULL;
      last_send_time = tbsys::CTimeUtil::getTime();
      compressed_data = NULL;
      data_size = 0;
      sent = false;
      storage_mgr = NULL;
      init();
   }
   stat_helper::~stat_helper()
    {
      _stop = true;
      wait();
      if (compressed_data != NULL) {
         data_size = 0;
         free(compressed_data);
         compressed_data = NULL;
      }
   }

   void stat_helper::run(tbsys::CThread *thread, void *arg)
   {
      while (_stop == false) {
         reset();
         TAIR_SLEEP(_stop, 10);
      }
   }

   void stat_helper::init()
   {
      log_warn("total_size is %d", STAT_TOTAL_SIZE);
      stat = (tair_stat *)malloc(STAT_TOTAL_SIZE);
      assert (stat != NULL);
      log_warn("total_size is %d", STAT_TOTAL_SIZE);

      memset(stat, 0, STAT_TOTAL_SIZE);

   }

   void stat_helper::stat_get(int area, int rc)
   {
      if (area >= TAIR_MAX_AREA_COUNT || area < 0)
         area = 0;

      stat[area].inc_get_count();

      if (rc == TAIR_RETURN_SUCCESS) {
         stat[area].inc_hit_count();
      }
   }

   void stat_helper::stat_put(int area)
   {
      if (area >= TAIR_MAX_AREA_COUNT || area < 0)
         area = 0;

      stat[area].inc_put_count();
   }

   void stat_helper::stat_evict(int area)
   {
      if (area >= TAIR_MAX_AREA_COUNT || area < 0)
         area = 0;

      stat[area].inc_evict_count();
   }

   void stat_helper::stat_remove(int area)
   {
      if (area >= TAIR_MAX_AREA_COUNT || area < 0)
         area = 0;

      stat[area].inc_remove_count();
   }

   // not threadsafe, make sure there is only
   // one thread calling this
   void stat_helper::reset()
   {
      if (compressed_data != NULL && sent == false) return;

      tair_stat *new_stat = (tair_stat *) malloc(STAT_TOTAL_SIZE);
      assert (new_stat != NULL);
      memset(new_stat, 0, STAT_TOTAL_SIZE);

      if(curr_stat != NULL)
         free(curr_stat);

      curr_stat = stat;
      stat = new_stat;

      // fill persistent info
      if (storage_mgr != NULL) {
         storage_mgr->get_stats(curr_stat);
      }

      uint64_t now = tbsys::CTimeUtil::getTime();
      uint64_t interval = now - last_send_time;
      last_send_time = now;
      interval /= 1000000; // conver to second
      if (interval == 0) interval = 1;

      log_debug("start calculate ratio, interval: %d", interval);
      for (int i=0; i<STAT_LENGTH; i++) {
         tair_stat *cs = curr_stat + i;
         cs->set_get_count(cs->get_count() / interval);
         cs->set_put_count(cs->put_count() / interval);
         cs->set_evict_count(cs->evict_count() / interval);
         cs->set_remove_count(cs->remove_count() / interval);
         cs->set_hit_count(cs->hit_count() / interval);
      }

      // compress
      do_compress();
      sent = false;
   }

   void stat_helper::do_compress()
   {
      unsigned long dest_len = compressBound(STAT_TOTAL_SIZE);
      unsigned char *dest = (unsigned char*) malloc(dest_len);

      if (compressed_data != NULL) {
         data_size = 0;
         free(compressed_data);
         compressed_data = NULL;
      }

      int ret = compress(dest, &dest_len, (unsigned char*)curr_stat, STAT_TOTAL_SIZE);
      if (ret == Z_OK) {
         compressed_data = (char *)malloc(dest_len);
         memcpy(compressed_data, dest, dest_len);
         data_size = dest_len;
         log_debug("compress stats done (%d=>%d)", STAT_TOTAL_SIZE, dest_len);
      } else {
         log_error("compress stats error: %d", ret);
      }

      free(dest);
   }

   tair_stat *stat_helper::get_curr_stats()
   {
      return curr_stat;
   }

   char *stat_helper::get_and_reset()
   {
      if (sent == true)
         return NULL;
      sent = true;
      return compressed_data;
   }

   tair_stat *stat_helper::get_stats()
   {
      return stat;
   }

   int stat_helper::get_size()
   {
      return data_size;
   }
}
