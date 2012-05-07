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
#include "tair_client_api_impl.hpp"
#include <vector>
#include <iterator>
namespace tair {

  /*-----------------------------------------------------------------------------
   *  tair_client_api
   *-----------------------------------------------------------------------------*/

  tair_client_api::tair_client_api()
  {
    impl = new tair_client_impl();
    memset(cache_impl, 0, sizeof(cache_impl));
  }

  tair_client_api::~tair_client_api()
  {
    delete impl;
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
    return impl->startup(master_addr,slave_addr,group_name);
  }

  bool tair_client_api::directup(const char *server_addr)
  {
    return impl->directup(server_addr);
  }

  void tair_client_api::close()
  {
    impl->close();
  }

  int tair_client_api::put(int area,
      const data_entry &key,
      const data_entry &data,
      int expire,
      int version)
  {
    int ret = impl->put(area,key,data,expire,version);
    if (TAIR_RETURN_SUCCESS == ret) {
      cache_type *cache = cache_impl[area];
      if (cache != NULL) {
        cache->put(key, data);
      }
    }
    return ret;
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
    int ret = impl->get(area,key,data);
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
      int ret = impl->mget(area, not_hit_keys, temp_data);
      // update local cache
      tair_keyvalue_map::iterator iter = temp_data.begin();
      for(; iter != temp_data.end(); ++iter) {
        cache->put(*(iter->first), *(iter->second));
      }
      data.insert(temp_data.begin(), temp_data.end());
      return ret;
    }
    return impl->mget(area,keys,data);
  }

  int tair_client_api::remove(int area,
      const data_entry &key)
  {
    int ret = impl->remove(area,key);
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
       return impl->invalidate(area, key, groupname);
   }

   int tair_client_api::invalidate(int area, const data_entry &key)
   {
       return impl->invalidate(area, key);
   }

   int tair_client_api::hide(int area, const data_entry &key)
   {
     return impl->hide(area, key);
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
    int ret = impl->mdelete(area,keys);
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

    return impl->add_count(area,key,count,ret_count,init_value,expire);

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

    return impl->add_count(area,key,-count,ret_count,init_value,expire);

  }



  int tair_client_api::add_count(int area,
      const data_entry &key,
      int count,
      int *ret_count,
      int init_value /*= 0*/)
  {

    return impl->add_count(area,key,count,ret_count,init_value);

  }

  int tair_client_api::set_count(int area, const data_entry& key, int count, int expire, int version)
  {
    return impl->set_count(area, key, count, expire, version);
  }

  int tair_client_api::lock(int area, const data_entry& key)
  {
    return impl->lock(area, key, LOCK_VALUE);
  }

  int tair_client_api::unlock(int area, const data_entry& key)
  {
    return impl->lock(area, key, UNLOCK_VALUE);
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

  void tair_client_api::set_timeout(int timeout)
  {
    impl->set_timeout(timeout);
  }

  void tair_client_api::set_randread(bool rand_flag)
  {
    impl->set_randread(rand_flag);
  }

  const char *tair_client_api::get_error_msg(int ret)
  {
    return impl->get_error_msg(ret);
  }

  uint32_t tair_client_api::get_bucket_count() const
  {
    return impl->get_bucket_count();
  }
  uint32_t tair_client_api::get_copy_count() const
  {
    return impl->get_copy_count();
  }
  void tair_client_api::get_server_with_key(const data_entry& key,std::vector<std::string>& servers) const
  {
    return impl->get_server_with_key(key,servers);
  }
  bool tair_client_api::get_group_name_list(uint64_t id1, uint64_t id2, std::vector<std::string> &groupnames) const
  {
    return impl->get_group_name_list(id1, id2, groupnames);
  }
  void tair_client_api::query_from_configserver(uint32_t query_type, const std::string &groupname, std::map<std::string, std::string> &out, uint64_t serverId)
  {
    impl->query_from_configserver(query_type, groupname, out, serverId);
  }
  uint32_t tair_client_api::get_config_version() const
  {
    return impl->get_config_version();
  }
  int64_t tair_client_api::ping(uint64_t server_id)
  {
      return impl->ping(server_id);
  }
} /* tair */
