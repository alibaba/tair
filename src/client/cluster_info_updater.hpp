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
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_CLIENT_CLUSTER_INFO_UPDATER_H
#define TAIR_CLIENT_CLUSTER_INFO_UPDATER_H

#include <tbsys.h>

#include "cluster_handler_manager.hpp"

namespace tair
{
  static const int32_t DEFAULT_CLUSTER_UPDATE_INTERVAL_MS = 30000;  // 30s
  static const int32_t FAIL_UPDATE_CLUSTER_INFO_INTERVAL_MS = 1000; // 1s
  static const int32_t URGENT_UPDATE_CLUSTER_INFO_INTERVAL_MS = 2000; // 2s

  class cluster_info_updater : public tbsys::CDefaultRunnable
  {
  public:
    cluster_info_updater();
    ~cluster_info_updater();

    void run(tbsys::CThread* thread, void* arg);

    void init(cluster_handler_manager* manager,
              const char* master_cs_addr, const char* slave_cs_addr, const char* group_name);
    void stop();

    // just signal update later
    int signal_update();
    // force do real update
    int force_update();

    cluster_handler* get_master_cluster_handler()
    {
      return &master_handler_;
    }
    inline void set_interval(int32_t interval_ms)
    {
      if (interval_ms > 0)
      {
        interval_ms_ = interval_ms;
      }
    }

    std::string debug_string()
    {
      char tmp_buf[64];
      snprintf(tmp_buf, sizeof(tmp_buf), "[ updater version: %u, last update time: ", master_handler_.get_version());
      std::string result(tmp_buf);
      tbsys::CTimeUtil::timeToStr(last_update_time_, tmp_buf);
      result.append(tmp_buf);
      result.append(", now: ");
      tbsys::CTimeUtil::timeToStr(time(NULL), tmp_buf);
      result.append(tmp_buf);
      result.append(" ]");
      return result;
    }

  private:
    int update_cluster_info(bool force, bool& urgent);
    int retrieve_cluster_info(bool force, CLUSTER_INFO_LIST& cluster_infos, uint32_t& old_version, uint32_t& new_version);

  private:
    bool inited_;
    int32_t interval_ms_;
    time_t last_update_time_;
    tbsys::CThreadCond cond_;
    cluster_handler master_handler_;
    cluster_handler_manager* manager_;
  };
}

#endif
