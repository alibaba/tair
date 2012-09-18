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

#include "common/define.hpp"
#include "common/log.hpp"

#include "cluster_info_updater.hpp"
#include "cluster_handler_manager.hpp"
#include "tair_multi_cluster_client_impl.hpp"

namespace tair
{
// Effect op(read) over one cluster handler that is picked based on key
#define ONE_CLUSTER_HANDLER_OP(ret, key, op)                            \
  do {                                                                  \
    bucket_shard_cluster_handler_manager_delegate delegate(handler_mgr_); \
    cluster_handler* handler = delegate.pick_handler(key);              \
    ret = (NULL == handler) ? TAIR_RETURN_SERVER_CAN_NOT_WORK : handler->get_client()->op; \
  } while (0)

// Effect op(write) over all cluster handler synchronously,
// ignore update when operating, so we use one delegate all the time.
// Break once one fail. There is no ANY data consistency guaranteed.
// TODO.
#define ALL_CLUSTER_HANDLER_OP(ret, key, op)                            \
  do {                                                                  \
    bucket_shard_cluster_handler_manager_delegate hold_delegate(handler_mgr_); \
    int32_t handler_count = hold_delegate.get_delegate()->get_handler_count(); \
    for (int32_t i = 0; i < handler_count; ++i)                         \
    {                                                                   \
      bucket_shard_cluster_handler_manager_delegate delegate(hold_delegate); \
      cluster_handler* handler = delegate.pick_handler(i, key);         \
      ret = (NULL == handler) ? TAIR_RETURN_SERVER_CAN_NOT_WORK : handler->get_client()->op; \
      if (ret != TAIR_RETURN_SUCCESS)                                   \
      {                                                                 \
        break;                                                          \
      }                                                                 \
    }                                                                   \
    if (ret != TAIR_RETURN_SUCCESS)                                     \
    {                                                                   \
      ret = TAIR_RETURN_PARTIAL_SUCCESS;                                \
    }                                                                   \
  } while (0)


  using namespace tair::common;

  tair_multi_cluster_client_impl::tair_multi_cluster_client_impl()
  {
    updater_ = new cluster_info_updater();
    handler_mgr_ = new bucket_shard_cluster_handler_manager(updater_, MAP_SHARDING_TYPE); // default map sharding
  }

  tair_multi_cluster_client_impl::~tair_multi_cluster_client_impl()
  {
    delete updater_;
    delete handler_mgr_;
  }

  bool tair_multi_cluster_client_impl::startup(const char* master_addr, const char* slave_addr, const char* group_name)
  {
    if (NULL == master_addr || group_name == NULL)
    {
      log_error("invalid master address/group_name");
      return false;
    }

    updater_->init(handler_mgr_, master_addr, slave_addr, group_name);
    int ret = updater_->force_update();
    if (ret != TAIR_RETURN_SUCCESS)
    {
      log_error("update cluster info fail. ret: %d", ret);
      return false;
    }

    if (handler_mgr_->get_handler_count() <= 0)
    {
      log_error("no alive cluster servicing now");
      return false;
    }

    updater_->start();

    return true;
  }

  int tair_multi_cluster_client_impl::get(int area, const data_entry& key, data_entry*& data)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ONE_CLUSTER_HANDLER_OP(ret, key, get(area, key, data));
    return ret;
  }

  int tair_multi_cluster_client_impl::put(int area, const data_entry& key, const data_entry& data,
                                          int expire, int version, bool fill_cache, TAIRCALLBACKFUNC pfunc, void* arg)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, key, put(area, key, data, expire, version, fill_cache, pfunc, arg));
    return ret;
  }

  int tair_multi_cluster_client_impl::remove(int area, const data_entry& key, TAIRCALLBACKFUNC pfunc, void* arg)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, key, remove(area, key, pfunc, arg));
    return ret;
  }

  int tair_multi_cluster_client_impl::incr(int area, const data_entry& key, int count, int* ret_count,
                                          int init_value, int expire)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, key, add_count(area, key, count, ret_count, init_value, expire));
    return ret;
  }

  int tair_multi_cluster_client_impl::decr(int area, const data_entry& key, int count, int* ret_count,
                                          int init_value, int expire)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, key, add_count(area, key, (-count), ret_count, init_value, expire));
    return ret;
  }

  int tair_multi_cluster_client_impl::add_count(int area, const data_entry& key, int count,
                                                int* ret_count, int init_value, int expire_time)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, key, add_count(area, key, count, ret_count, init_value, expire_time));
    return ret;
  }

  int tair_multi_cluster_client_impl::mget(int area, const vector<data_entry *> &keys, tair_keyvalue_map& data)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    if (keys.empty() || keys[0] == NULL)
    {
      ret = TAIR_RETURN_INVALID_ARGUMENT;
    }
    else
    {
      ONE_CLUSTER_HANDLER_OP(ret, *keys[0], mget(area, keys, data));
    }
    return ret;
  }

  void tair_multi_cluster_client_impl::set_timeout(int32_t timeout_ms)
  {
    handler_mgr_->set_timeout(timeout_ms);
  }

  void tair_multi_cluster_client_impl::set_update_interval(int32_t interval_ms)
  {
    updater_->set_interval(interval_ms);
  }

  uint32_t tair_multi_cluster_client_impl::get_bucket_count() const
  {
    return updater_->get_master_cluster_handler()->get_client()->get_bucket_count();
  }

  uint32_t tair_multi_cluster_client_impl::get_copy_count() const
  {
    return updater_->get_master_cluster_handler()->get_client()->get_copy_count();
  }

  void tair_multi_cluster_client_impl::close()
  {
    updater_->stop();
    handler_mgr_->close();
  }

}
