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
#ifndef TAIR_CLIENT_MULTI_CLUSTER_CLIENT_IMPL_H
#define TAIR_CLIENT_MULTI_CLUSTER_CLIENT_IMPL_H

#include "i_tair_client_impl.hpp"

namespace tair
{
  class cluster_info_updater;
  class cluster_handler_manager;

  using tair::common::data_entry;
  class tair_multi_cluster_client_impl : public i_tair_client_impl
  {
  public:
    tair_multi_cluster_client_impl();
    ~tair_multi_cluster_client_impl();

    bool startup(const char* master_addr, const char* slave_addr, const char* group_name);
    void close();

    int get(int area, const data_entry& key, data_entry*& data);
    int put(int area, const data_entry& key, const data_entry& data, int expire, int version, bool fill_cache = true,
            TAIRCALLBACKFUNC pfunc = NULL, void* arg = NULL);
    int remove(int area, const data_entry& key, TAIRCALLBACKFUNC pfunc = NULL, void* arg = NULL);
    int incr(int area, const data_entry& key, int count, int* ret_count, int init_value = 0, int expire = 0);
    int decr(int area, const data_entry& key, int count, int* ret_count, int init_value = 0, int expire = 0);
    int add_count(int area, const data_entry& key, int count, int* ret_count,
                  int init_value = 0, int expire_time = 0);
    int mget(int area, const vector<data_entry *> &keys, tair_keyvalue_map& data);
    int mput(int area, const tair_client_kv_map& kvs, int& fail_request, bool compress)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    int mdelete(int area, const vector<data_entry *> &keys)
    { return TAIR_RETURN_NOT_SUPPORTED; }

    void set_timeout(int32_t timeout_ms);
    void set_update_interval(int32_t interval_ms);
    uint32_t get_bucket_count() const;
    uint32_t get_copy_count() const;

  private:
    cluster_info_updater* updater_;
    cluster_handler_manager* handler_mgr_;
  };
}
#endif
