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
#include <iterator>
namespace tair {

  /*-----------------------------------------------------------------------------
   *  tair_client_api
   *-----------------------------------------------------------------------------*/

  tair_client_api::tair_client_api()
  {
    impl = new tair_client_impl();
  }

  tair_client_api::~tair_client_api()
  {
    delete impl;
  }

  bool tair_client_api::startup(const char *master_addr,const char *slave_addr,const char *group_name)
  {
    return impl->startup(master_addr,slave_addr,group_name);
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
    return impl->put(area,key,data,expire,version);
  }

  int tair_client_api::get(int area,
      const data_entry &key,
      data_entry*& data)
  {
    return impl->get(area,key,data);
  }

  int tair_client_api::mget(int area,
      vector<data_entry *> &keys,
      tair_keyvalue_map& data)
  {
    return impl->mget(area,keys,data);
  }

  int tair_client_api::remove(int area,
      const data_entry &key)
  {
    return impl->remove(area,key);
  }

  int tair_client_api::mdelete(int area,
      vector<data_entry *> &keys)
  {
    return impl->mdelete(area,keys);
  }

  int tair_client_api::minvalid(int area,
      vector<data_entry *> &keys)
  {
    return impl->mdelete(area,keys);
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
} /* tair */
