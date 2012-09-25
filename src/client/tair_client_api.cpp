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
 *   MaoQi <maoqi@taobao.com>
 *
 */
#include "tair_client_api.hpp"
#include "i_tair_client_impl.hpp"
#include "tair_client_api_impl.hpp"
#include "cluster_handler_manager.hpp"
#include "tair_multi_cluster_client_impl.hpp"

namespace tair {

  static i_tair_client_impl* new_tair_client(const char *master_addr,const char *slave_addr,const char *group_name);
  static TairClusterType get_cluster_type_by_config(tbsys::STR_STR_MAP& config_map);

  /*-----------------------------------------------------------------------------
   *  tair_client_api
   *-----------------------------------------------------------------------------*/

  tair_client_api::tair_client_api() : timeout_ms(2000), impl(NULL)
  {
    memset(cache_impl, 0, sizeof(cache_impl));
  }

  tair_client_api::~tair_client_api()
  {
    if (impl != NULL)
    {
      delete impl;
    }
    for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
      if (cache_impl[i] != NULL)
        delete cache_impl[i];
    }
  }

  void tair_client_api::setup_cache(int area, size_t capacity)
  {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT)
      return ;
    if (cache_impl[area] == NULL)
      cache_impl[area] = new data_entry_local_cache(capacity);
  }

  bool tair_client_api::startup(const char *master_addr,const char *slave_addr,const char *group_name)
  {
    bool ret = false;
    if (impl != NULL) {
      ret = true;
      log_info("tair client inited");
    } else {
      impl = new_tair_client(master_addr, slave_addr, group_name);
      if (NULL == impl) {
        log_error("init tair client fail.");
      } else {
        impl->set_timeout(timeout_ms);
        ret = impl->startup(master_addr,slave_addr,group_name);
        if (!ret) {
          delete impl;
          impl = NULL;
        }
      }
    }
    return ret;
  }

  bool tair_client_api::directup(const char *server_addr)
  {
    bool ret = false;
    if (impl != NULL) {
      ret = true;
      log_info("tair client inited.");
    } else {
      impl = new tair_client_impl();
      impl->set_timeout(timeout_ms);
      ret = impl->directup(server_addr);
      if (!ret) {
        delete impl;
        impl = NULL;
      }
    }
    return ret;
  }

  void tair_client_api::close()
  {
    if (impl != NULL)
    {
      impl->close();
    }
  }

  int tair_client_api::put(int area,
      const data_entry &key,
      const data_entry &data,
      int expire,
      int version,
      bool fill_cache)
  {
    int ret = impl == NULL ? TAIR_RETURN_NOT_INIT : impl->put(area,key,data,expire,version, fill_cache);
    if (TAIR_RETURN_SUCCESS == ret) {
      cache_type *cache = cache_impl[area];
      if (cache != NULL) {
        cache->put(key, data);
      }
    }
    return ret;
  }

  int tair_client_api::mput(int area, const tair_client_kv_map& kvs,
                            int& fail_request, bool compress)
  {
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->mput(area, kvs, fail_request, compress);
  }

  int tair_client_api::get(int area,
      const data_entry &key,
      data_entry*& data)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    cache_type *cache = cache_impl[area];
    if (cache != NULL) {
      // find first
      data = new data_entry();
      cache_type::result result = cache->get(key, *data);
      if (cache_type::HIT == result) {
        return TAIR_RETURN_SUCCESS;
      } else {
        delete data;
        data = NULL;
      }
    }
    int ret = impl == NULL ? TAIR_RETURN_NOT_INIT : impl->get(area,key,data);
    if (cache != NULL) {
      // update cache
      if (TAIR_RETURN_SUCCESS == ret) {
        cache->put(key, *data);
      } else if (TAIR_RETURN_DATA_EXPIRED == ret ||
            TAIR_RETURN_DATA_NOT_EXIST == ret) {
        cache->remove(key);
      }
    }
    return ret;
  }

  int tair_client_api::mget(int area,
      const vector<data_entry *> &keys,
      tair_keyvalue_map& data)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    cache_type *cache = cache_impl[area];
    if (cache != NULL) {
      vector<data_entry *> cached_keys;
      vector<data_entry *> not_hit_keys;

      for (size_t i = 0; i < keys.size(); ++i) {
        data_entry *value = new data_entry();
        cache_type::result res = cache->get(*keys[i], *value);
        if (res == cache_type::HIT) {
          data.insert(std::make_pair(keys[i], value));
          cached_keys.push_back(keys[i]);
        } else {
          not_hit_keys.push_back(keys[i]);
          delete value;
        }
      }
      if (not_hit_keys.empty())
        return TAIR_RETURN_SUCCESS;

      tair_keyvalue_map temp_data;
      int ret = impl == NULL ? TAIR_RETURN_NOT_INIT : impl->mget(area, not_hit_keys, temp_data);
      // update local cache
      tair_keyvalue_map::iterator iter = temp_data.begin();
      for(; iter != temp_data.end(); ++iter) {
        cache->put(*(iter->first), *(iter->second));
      }
      data.insert(temp_data.begin(), temp_data.end());
      return ret;
    }
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->mget(area,keys,data);
  }

  int tair_client_api::remove(int area,
      const data_entry &key)
  {
    int ret = impl == NULL ? TAIR_RETURN_NOT_INIT : impl->remove(area,key);
    if (TAIR_RETURN_SUCCESS == ret ||
        TAIR_RETURN_DATA_NOT_EXIST == ret) {
      cache_type *cache = cache_impl[area];
      if (cache != NULL) {
        cache->remove(key);
      }
    }
    return ret;
  }

   int tair_client_api::invalidate( int area, const data_entry &key, const char *groupname)
   {
       return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->invalidate(area, key, groupname);
   }

   int tair_client_api::invalidate(int area, const data_entry &key)
   {
       return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->invalidate(area, key);
   }

   int tair_client_api::hide(int area, const data_entry &key)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->hide(area, key);
   }

   int tair_client_api::get_hidden(int area, const data_entry &key, data_entry *&value)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->get_hidden(area, key, value);
   }

   int tair_client_api::prefix_get(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->prefix_get(area, pkey, skey, value);
   }

   int tair_client_api::prefix_put(int area, const data_entry &pkey, const data_entry &skey,
       const data_entry &value, int expire, int version)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->prefix_put(area, pkey, skey, value, expire, version);
   }

   int tair_client_api::prefix_hide(int area, const data_entry &pkey, const data_entry &skey)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->prefix_hide(area, pkey, skey);
   }

   int tair_client_api::prefix_hides(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->prefix_hides(area, pkey, skey_set, key_code_map);
   }

   int tair_client_api::prefix_get_hidden(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->prefix_get_hidden(area, pkey, skey, value);
   }

   int tair_client_api::prefix_remove(int area, const data_entry &pkey, const data_entry &skey)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->prefix_remove(area, pkey, skey);
   }

   int tair_client_api::prefix_removes(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->prefix_removes(area, pkey, skey_set, key_code_map);
   }

   int tair_client_api::get_range(int area, const data_entry &pkey, const data_entry &start_key, const data_entry &end_key, 
                        int offset, int limit, vector<data_entry *> &values, short type)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->get_range(area, pkey, start_key, end_key, offset, limit, values, type);
   }

   int tair_client_api::removes(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->removes(area, mkey_set, key_code_map);
   }

   int tair_client_api::hides(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map)
   {
     return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->hides(area, mkey_set, key_code_map);
   }

  void tair_client_api::invalid_local_cache(int area, const vector<data_entry *> &keys)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return ;
    }

    cache_type *cache = cache_impl[area];
    if (cache == NULL)
      return;
    for (size_t i = 0; i < keys.size(); ++i)
      cache->remove(*(keys[i]));
  }

  int tair_client_api::mdelete(int area,
      const vector<data_entry *> &keys)
  {
    int ret = impl == NULL ? TAIR_RETURN_NOT_INIT : impl->mdelete(area,keys);
    if (TAIR_RETURN_SUCCESS == ret ||
        TAIR_RETURN_PARTIAL_SUCCESS == ret) {
      invalid_local_cache(area, keys);
    }
    return ret;
  }

  int tair_client_api::minvalid(int area,
      vector<data_entry *> &keys)
  {
    return mdelete(area, keys);
  }


  int tair_client_api::incr(int area,
      const data_entry& key,
      int count,
      int *ret_count,
      int init_value/* = 0*/,
      int expire /*= 0*/)
  {
    if(area < 0 || count < 0 || expire < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->add_count(area,key,count,ret_count,init_value,expire);

  }
  int tair_client_api::decr(int area,
      const data_entry& key,
      int count,
      int *ret_count,
      int init_value/* = 0*/,
      int expire /*= 0*/)
  {
    if(area < 0 || count < 0 || expire < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->add_count(area,key,-count,ret_count,init_value,expire);

  }



  int tair_client_api::add_count(int area,
      const data_entry &key,
      int count,
      int *ret_count,
      int init_value /*= 0*/)
  {

    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->add_count(area,key,count,ret_count,init_value);

  }

  int tair_client_api::set_count(int area, const data_entry& key, int count, int expire, int version)
  {
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->set_count(area, key, count, expire, version);
  }

  int tair_client_api::lock(int area, const data_entry& key)
  {
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->lock(area, key, LOCK_VALUE);
  }

  int tair_client_api::unlock(int area, const data_entry& key)
  {
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->lock(area, key, UNLOCK_VALUE);
  }

  int tair_client_api::expire(int area,
      const data_entry& key,
      int expire)
  {
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->expire(area, key, expire);
  }
  //int tair_client_api::removeArea(int area)
  //{
  //      return impl->removeArea(area);
  //}
  //
  //int tair_client_api::dumpArea(std::set<DumpMetaInfo>& info)
  //{
  //      return impl->dumpArea(info);
  //}


#if 0
#undef ADD_ITEMS
#undef GET_ITEMS_FUNC

#define ADD_ITEMS(type)                                                 \
  int tair_client_api::add_items(int area,const data_entry &key,type data,int version,int expired,int max_count)

#define GET_ITEMS_FUNC(FuncName,type)                                   \
  int tair_client_api::FuncName(int area, const data_entry &key, int offset, int count, type result)

#define ADD_ITEMS_FUNC_IMPL(type,element_type)                          \
  ADD_ITEMS(type)                                                      \
  {                                                                    \
    return impl->add_items(area,key,data.begin(),data.end(),version,expired,max_count,element_type); \
  }

  ADD_ITEMS_FUNC_IMPL(vector<int>&,ELEMENT_TYPE_INT);
  ADD_ITEMS_FUNC_IMPL(vector<int64_t>&,ELEMENT_TYPE_LLONG);
  ADD_ITEMS_FUNC_IMPL(vector<double>&,ELEMENT_TYPE_DOUBLE);
  ADD_ITEMS_FUNC_IMPL(vector<std::string>&,ELEMENT_TYPE_STRING);


#define GET_ITEMS_FUNC_IMPL(FuncName,type,element_type)                 \
  GET_ITEMS_FUNC(FuncName,type)                                        \
  {                                                                    \
    return impl->FuncName(area,key,offset,count,result,element_type); \
  }

#define GET_ITEMS_FUNC_IMPL_INT32(FuncName,type)                        \
  GET_ITEMS_FUNC(FuncName,type)                                        \
  {                                                                    \
    vector<int64_t> tmp_result;                                       \
    int ret = impl->FuncName(area,key,offset,count,tmp_result,ELEMENT_TYPE_INT); \
    if(ret < 0){                                                      \
      return ret;                                                    \
    }                                                                 \
    copy(tmp_result.begin(),tmp_result.end(),std::back_inserter(result)); \
    return ret;                                                       \
  }

  GET_ITEMS_FUNC_IMPL_INT32(get_items,std::vector<int>&);
  GET_ITEMS_FUNC_IMPL(get_items,std::vector<int64_t>&,ELEMENT_TYPE_LLONG)
    GET_ITEMS_FUNC_IMPL(get_items,std::vector<double>&,ELEMENT_TYPE_DOUBLE)
    GET_ITEMS_FUNC_IMPL(get_items,std::vector<std::string>&,ELEMENT_TYPE_STRING)

    GET_ITEMS_FUNC_IMPL_INT32(get_and_remove,std::vector<int>&);
  GET_ITEMS_FUNC_IMPL(get_and_remove,std::vector<int64_t>&,ELEMENT_TYPE_LLONG)
    GET_ITEMS_FUNC_IMPL(get_and_remove,std::vector<double>&,ELEMENT_TYPE_DOUBLE)
    GET_ITEMS_FUNC_IMPL(get_and_remove,std::vector<std::string>&,ELEMENT_TYPE_STRING)


    int tair_client_api::remove_items(int area,
        const data_entry &value,
        int offset,
        int count)
    {
      return impl->remove_items(area,value,offset,count);
    }

  int tair_client_api::get_items_count(int area,const data_entry& key)
  {
    return impl->get_items_count(area,key);
  }
#endif

  void tair_client_api::set_log_level(const char* level)
  {
    TBSYS_LOGGER.setLogLevel(level);
  }

  void tair_client_api::set_log_file(const char* log_file)
  {
    TBSYS_LOGGER.setFileName(log_file);
  }

  void tair_client_api::set_timeout(int timeout)
  {
    timeout_ms = timeout;
    if (impl != NULL)
    {
      impl->set_timeout(timeout);
    }
  }

  void tair_client_api::set_randread(bool rand_flag)
  {
    if (impl != NULL)
    {    
      impl->set_randread(rand_flag);
    }
  }

#ifdef WITH_COMPRESS
  void tair_client_api::set_compress_type(TAIR_COMPRESS_TYPE type)
  {
    if ((type >= 0) && (type < TAIR_COMPRESS_TYPE_NUM)) {
      tair::common::data_entry::compress_type = type;
    }
  }

  void tair_client_api::set_compress_threshold(int threshold)
  {
      tair::common::data_entry::compress_threshold = threshold;
  }
#endif

  const char *tair_client_api::get_error_msg(int ret)
  {
    return impl == NULL ? NULL : impl->get_error_msg(ret);
  }

  uint32_t tair_client_api::get_bucket_count() const
  {
    return impl == NULL ? 0 : impl->get_bucket_count();
  }
  uint32_t tair_client_api::get_copy_count() const
  {
    return impl == NULL ? 0 : impl->get_copy_count();
  }
  void tair_client_api::get_server_with_key(const data_entry& key,std::vector<std::string>& servers) const
  {
    if (impl != NULL)
    {
      impl->get_server_with_key(key,servers);
    }
  }

  void tair_client_api::query_from_configserver(uint32_t query_type, const std::string &groupname, std::map<std::string, std::string> &out, uint64_t serverId)
  {
    if (impl != NULL)
    {
      impl->query_from_configserver(query_type, groupname, out, serverId);
    }
  }
  uint32_t tair_client_api::get_config_version() const
  {
    return impl == NULL ? 0 : impl->get_config_version();
  }
  int64_t tair_client_api::ping(uint64_t server_id)
  {
      return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->ping(server_id);
  }

  int tair_client_api::get_group_status(vector<string> &group, vector<string> &status)
  {
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->op_cmd_to_cs(TAIR_SERVER_CMD_GET_GROUP_STATUS, &group, &status);
  }
  int tair_client_api::get_tmp_down_server(vector<string> &group, vector<string> &down_servers)
  {
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->op_cmd_to_cs(TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER, &group, &down_servers);
  }
  int tair_client_api::set_group_status(const char *group, const char *status)
  {
    int ret = TAIR_RETURN_FAILED;
    if (impl != NULL && group != NULL && status != NULL)
    {
      std::vector<std::string> params;
      params.push_back(group);
      params.push_back(status);
      ret = impl->op_cmd_to_cs(TAIR_SERVER_CMD_SET_GROUP_STATUS, &params, NULL);
    }
    return ret;
  }
  int tair_client_api::reset_server(const char* group, std::vector<std::string>* dss)
  {
    int ret = TAIR_RETURN_FAILED;
    if (impl != NULL && group != NULL)
    {
      std::vector<std::string> params;
      params.push_back(group);
      if (dss != NULL)
      {
        for (vector<string>::iterator it = dss->begin(); it != dss->end(); ++it)
        {
          params.push_back(*it);
        }
      }
      ret = impl->op_cmd_to_cs(TAIR_SERVER_CMD_RESET_DS, &params, NULL);
    }
    return ret;
  }
  int tair_client_api::flush_mmt(const char* ds_addr)
  {
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->op_cmd_to_ds(TAIR_SERVER_CMD_FLUSH_MMT, NULL, ds_addr);
  }
  int tair_client_api::reset_db(const char* ds_addr)
  {
    return impl == NULL ? TAIR_RETURN_NOT_INIT : impl->op_cmd_to_ds(TAIR_SERVER_CMD_RESET_DB, NULL, ds_addr);
  }

  i_tair_client_impl* new_tair_client(const char *master_addr,
                                      const char *slave_addr,
                                      const char *group_name)
  {
    i_tair_client_impl* ret_client_impl = NULL;

    tair_client_impl client;
    client.set_force_service(true);
    if (!client.startup(master_addr, slave_addr, group_name))
    {
      log_error("startup to get cluster type fail.");
      return NULL;
    }
    tbsys::STR_STR_MAP config_map;
    uint32_t version = 0;
    int ret = client.retrieve_server_config(false, config_map, version);
    if (ret != TAIR_RETURN_SUCCESS)
    {
      log_error("retrieve_server_config fail, ret: %d", ret);
      return NULL;
    }

    TairClusterType type = get_cluster_type_by_config(config_map);
    switch (type)
    {
    case TAIR_CLUSTER_TYPE_SINGLE_CLUSTER:
    {
      log_warn("Tair cluster is SINGLE CLUSTER TYPE, init tair_client_impl");
      ret_client_impl = new tair_client_impl();
      break;
    }
    case TAIR_CLUSTER_TYPE_MULTI_CLUSTER:
    {
      log_warn("Tair cluster is MULTI CLUSTER TYPE, init tair_multi_cluster_client_impl");
      ret_client_impl = new tair_multi_cluster_client_impl();
      break;
    }
    default :
    {
      log_error("unsupported cluster type, can NOT init tair client. type: %d", type);
      break;
    }
    }
    return ret_client_impl;
  }

  TairClusterType get_cluster_type_by_config(tbsys::STR_STR_MAP& config_map)
  {
    // we consider cluster type by TAIR_MULTI_GROUPS now.
    // Just consider SINGLE_CLUSTER and MULTI_CLUSTER now.
    static const char* cluster_type_config = TAIR_MULTI_GROUPS;

    if (config_map.find(cluster_type_config) != config_map.end())
    {
      return TAIR_CLUSTER_TYPE_MULTI_CLUSTER;
    }
    else
    {
      return TAIR_CLUSTER_TYPE_SINGLE_CLUSTER;
    }
  }

} /* tair */
