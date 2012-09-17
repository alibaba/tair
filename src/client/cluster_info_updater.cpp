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

#include "cluster_info_updater.hpp"

namespace tair
{
  cluster_info_updater::cluster_info_updater() :
    inited_(false), interval_ms_(DEFAULT_CLUSTER_UPDATE_INTERVAL_MS), last_update_time_(0),
    master_handler_(true), manager_(NULL) // master cluster handler can be a mock
  {
  }

  cluster_info_updater::~cluster_info_updater()
  {
    stop();
    wait();
  }

  void cluster_info_updater::init(cluster_handler_manager* manager,
                                  const char* master_cs_addr, const char* slave_cs_addr, const char* group_name)
  {
    if (NULL != manager && NULL != master_cs_addr && NULL != group_name)
    {
      master_handler_.init(master_cs_addr, slave_cs_addr, group_name);
      this->manager_ = manager;
    }
  }

  void cluster_info_updater::stop()
  {
    _stop = true;
    interval_ms_ = 0;
    cond_.signal();
  }

  int cluster_info_updater::signal_update()
  {
    cond_.signal();
    log_debug("signal update");
    return TAIR_RETURN_SUCCESS;
  }

  int cluster_info_updater::force_update()
  {
    bool urgent = false;
    log_debug("force update");
    return update_cluster_info(true, urgent);
  }

  void cluster_info_updater::run(tbsys::CThread*, void*)
  {
    int ret;
    bool urgent = false;
    int32_t interval = interval_ms_;
    while (!_stop)
    {
      ret = update_cluster_info(false, urgent);
      if (ret != TAIR_RETURN_SUCCESS)
      {
        interval = FAIL_UPDATE_CLUSTER_INFO_INTERVAL_MS;
        log_error("update cluster info fail, ret: %d. retry after %d(ms).", ret, interval);
      }
      else if (urgent)
      {
        interval = URGENT_UPDATE_CLUSTER_INFO_INTERVAL_MS;
        log_warn("urgent condition, maybe all cluster are dead. retry after %d(ms)", interval);
      }
      cond_.wait(interval);
      interval = interval_ms_;
    }
  }

  int cluster_info_updater::update_cluster_info(bool force, bool& urgent)
  {
    if (!inited_ && !(inited_ = master_handler_.start()))
    {
      return TAIR_RETURN_FAILED;
    }

    CLUSTER_INFO_LIST cluster_infos;
    uint32_t old_version = 0, new_version = 0;
    int ret = retrieve_cluster_info(force, cluster_infos, old_version, new_version);
    // force and version changed, do update.
    // cluster may be restarted and version may be reseted,
    // so new version can be less than old version.
    // TODO: If cluster is restarted and new_version happends to change to old_version unluckily,
    //       we will miss the update.
    if (TAIR_RETURN_SUCCESS == ret && (force || old_version != new_version))
    {
      urgent = manager_->update(cluster_infos);
      last_update_time_ = time(NULL);
    }
    else
    {
      log_debug("do not update 'cause ret: %d, force: %d, version change: %d <> %d", ret, force, old_version, new_version);
    }

    return ret;
  }

  int cluster_info_updater::retrieve_cluster_info(bool force, CLUSTER_INFO_LIST& cluster_infos,
                                                  uint32_t& old_version, uint32_t& new_version)
  {
    CONFIG_MAP config_map;
    old_version = master_handler_.get_version();
    // don't need update server table
    int ret = master_handler_.retrieve_server_config(false, config_map, new_version);
    if (ret != TAIR_RETURN_SUCCESS)
    {
      return ret;
    }

    if (!force && new_version == old_version)
    {
      return ret;
    }

    std::vector<std::string> clusters;
    parse_config(config_map, TAIR_MULTI_GROUPS, TAIR_CONFIG_VALUE_DELIMITERS, clusters);
    if (clusters.empty())
    {
      log_error("no cluster info config");
      ret = TAIR_RETURN_FAILED;
    }
    else
    {
      for (std::vector<std::string>::const_iterator it = clusters.begin(); it != clusters.end(); ++it)
      {
        // we have only one cs config now
        cluster_infos.push_back(cluster_info(master_handler_.get_cluster_info().master_cs_addr_,
                                             master_handler_.get_cluster_info().slave_cs_addr_,
                                             (*it)));
      }
    }
    return ret;
  }

}
