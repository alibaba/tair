/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * tair client api interface
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef I_TAIR_CLIENT_IMPL_H
#define I_TAIR_CLIENT_IMPL_H

#include <map>
#include <vector>
#include <string>

#include "define.hpp"
#include "data_entry.hpp"

namespace tair
{
  using tair::common::data_entry;
  using tair::common::key_code_map_t;
  using tair::common::tair_dataentry_set;
  typedef void (*TAIRCALLBACKFUNC)(int retcode, void* parg);

  class i_tair_client_impl
  {
  public:
    i_tair_client_impl();
    virtual ~i_tair_client_impl();

    virtual bool startup(const char* master_addr, const char* slave_addr, const char* group_name) = 0;
    virtual bool directup(const char *server_addr) { return false; }
    virtual void close() = 0;
    virtual int get(int area, const data_entry& key, data_entry*& data) = 0;
    virtual int put(int area, const data_entry& key, const data_entry& data,
                    int expire, int version, bool fill_cache = true, TAIRCALLBACKFUNC pfunc = NULL,void* arg = NULL) = 0;
    virtual int remove(int area, const data_entry& key, TAIRCALLBACKFUNC pfunc = NULL,void* arg = NULL) = 0;
    virtual int add_count(int area, const data_entry& key, int count, int* ret_count,
                          int init_value = 0, int expire_time = 0) = 0;
    virtual int set_count(int area, const data_entry& key, int count, int expire, int version)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int mget(int area, const std::vector<data_entry *> &keys, tair::common::tair_keyvalue_map& data) = 0;
    virtual int mput(int area, const tair::common::tair_client_kv_map& kvs, int& fail_request, bool compress) = 0;
    virtual int mdelete(int area, const std::vector<data_entry *> &keys)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int lock(int area, const data_entry& key, LockType type)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int expire(int area, const data_entry& key, int expire)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int invalidate(int area, const data_entry &key, const char *groupname)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int invalidate(int area, const data_entry &key)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int hide(int area, const data_entry &key)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int get_hidden(int area, const data_entry &key, data_entry *&value)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int prefix_get(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int prefix_put(int area, const data_entry &pkey, const data_entry &skey,
                           const data_entry &value, int expire, int version)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int prefix_hide(int area, const data_entry &pkey, const data_entry &skey)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int prefix_hides(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int prefix_get_hidden(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int prefix_remove(int area, const data_entry &pkey, const data_entry &skey)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int prefix_removes(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int get_range(int area, const data_entry &pkey, const data_entry &start_key, const data_entry &end_key, 
                          int offset, int limit, std::vector<data_entry *>  &values,short type=CMD_RANGE_ALL)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int hides(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int removes(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map)
    { return TAIR_RETURN_NOT_SUPPORTED; }

    virtual void set_timeout(int32_t timeout_ms) = 0;
    virtual void set_randread(bool rand_flag) { return; }
    virtual uint32_t get_bucket_count() const = 0;
    virtual uint32_t get_copy_count() const = 0;
    virtual uint32_t get_config_version() const { return 0; }

    virtual void query_from_configserver(uint32_t query_type, const std::string group_name,
                                 std::map<std::string, std::string>&, uint64_t server_id = 0)
    { return; }
    virtual void get_server_with_key(const data_entry& key,std::vector<std::string>& servers) { return; }
    virtual int64_t ping(uint64_t server_id) { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int op_cmd_to_cs(ServerCmdType cmd, std::vector<std::string>* params, std::vector<std::string>* ret_values)
    { return TAIR_RETURN_NOT_SUPPORTED; }
    virtual int op_cmd_to_ds(ServerCmdType cmd, std::vector<std::string>* params, const char* dest_server_addr = NULL)
    { return TAIR_RETURN_NOT_SUPPORTED; }

    const char* get_error_msg(int ret)
    {
      std::map<int,std::string>::const_iterator it = i_tair_client_impl::errmsg_.find(ret);
      return it != i_tair_client_impl::errmsg_.end() ? it->second.c_str() : "unknow";
    }

  public:
    static const std::map<int, std::string> errmsg_;
  };
}

#endif
