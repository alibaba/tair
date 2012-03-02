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
#include <string>
#include <map>
#include <vector>
#include <ext/hash_map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>

#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "tair_client_api.hpp"
#include "wait_object.hpp"
#include "tair_client_api_impl.hpp"
#include "put_packet.hpp"
#include "remove_area_packet.hpp"
#include "remove_packet.hpp"
#include "inc_dec_packet.hpp"
#include "dump_packet.hpp"
#include "get_packet.hpp"
#include "get_group_packet.hpp"
#include "query_info_packet.hpp"
#include "get_migrate_machine.hpp"
#include "data_server_ctrl_packet.hpp"


namespace tair {


  using namespace std;
  using namespace __gnu_cxx;
  using namespace tair::json;
  using namespace tair::common;


  /*-----------------------------------------------------------------------------
   *  tair_client_impl
   *-----------------------------------------------------------------------------*/

  tair_client_impl::tair_client_impl():inited(false),is_stop(false),packet_factory(0),streamer(0),
                                       transport(0),connmgr(0),timeout(2000),config_version(0),
                                       new_config_version(0),send_fail_count(0),this_wait_object_manager(0),
                                       bucket_count(0),copy_count(0),rand_read_flag(false)
  {
    atomic_set(&read_seq, 0);
  }

  tair_client_impl::~tair_client_impl()
  {
    close();
  }

  bool tair_client_impl::startup(const char *master_addr,const char *slave_addr,const char *group_name)
  {
    if(master_addr == NULL || group_name == NULL){
      return false;
    }

    if(inited){
      return true;
    }
    is_stop = false;

    if( !initialize() ){
      return false;
    }

    uint64_t master_cfgsvr = tbsys::CNetUtil::strToAddr(master_addr,TAIR_CONFIG_SERVER_DEFAULT_PORT);
    uint64_t slave_cfgsvr = slave_addr  == NULL ? 0 : tbsys::CNetUtil::strToAddr(slave_addr,TAIR_CONFIG_SERVER_DEFAULT_PORT);
    return startup(master_cfgsvr,slave_cfgsvr,group_name);
  }

  bool tair_client_impl::startup(uint64_t master_cfgsvr, uint64_t slave_cfgsvr, const char *group_name)
  {

    if(master_cfgsvr == 0 || group_name == NULL){
      return false;
    }

    TBSYS_LOG(DEBUG,"master_cfg:%s,group:%s \n", tbsys::CNetUtil::addrToString(master_cfgsvr).c_str(),group_name);

    if (master_cfgsvr) {
      config_server_list.push_back(master_cfgsvr);
    }
    if (slave_cfgsvr) {
      config_server_list.push_back(slave_cfgsvr);
    }

    this->group_name = group_name;

    start_tbnet();

    if (!retrieve_server_addr()){
      TBSYS_LOG(ERROR,"retrieve_server_addr falied.\n");
      close();
      return false;
    }
    inited = true;
    return true;
  }

  bool tair_client_impl::initialize()
  {

    this_wait_object_manager = new wait_object_manager();
    if(this_wait_object_manager == 0)

      return false;

    packet_factory = new tair_packet_factory();
    if( packet_factory == 0)
      goto FAIL_1;

    streamer = new tbnet::DefaultPacketStreamer();
    if(streamer == 0)
      goto FAIL_2;
    streamer->setPacketFactory(packet_factory);
    transport = new tbnet::Transport();
    if(transport == 0)
      goto FAIL_3;

    connmgr = new tbnet::ConnectionManager(transport, streamer, this);
    if(connmgr == 0)
      goto FAIL_4;
    return true;

FAIL_4:
    delete transport;
    transport = 0;
FAIL_3:
    delete streamer;
    streamer = 0;
FAIL_2:
    delete packet_factory;
    packet_factory = 0;
FAIL_1:
    delete this_wait_object_manager;
    this_wait_object_manager = 0;
    return false;
  }

  // startup, server
  bool tair_client_impl::startup(uint64_t dataserver)
  {
    my_server_list.push_back(dataserver);
    bucket_count = 1;
    copy_count = 1;
    if(!initialize()){
      return false;
    }
    start_tbnet();
    inited = true;
    return tbnet::ConnectionManager::isAlive(dataserver);
  }

  void tair_client_impl::close()
  {
    if (is_stop || !inited)
      return;
    is_stop = true;
    stop_tbnet();
    wait_tbnet();
    thread.join();
    if (connmgr)
      delete connmgr;
    if (transport)
      delete transport;
    if (streamer)
      delete streamer;
    if (packet_factory)
      delete packet_factory;
    if (this_wait_object_manager)
      delete this_wait_object_manager;
    reset();
  }

  int tair_client_impl::put(int area,
      const data_entry &key,
      const data_entry &data,
      int expired, int version )
  {

    if( !(key_entry_check(key)) || (!data_entry_check(data))){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if(key.get_size() + data.get_size() > (TAIR_MAX_DATA_SIZE + TAIR_MAX_KEY_SIZE)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if(area < 0 || area >= TAIR_MAX_AREA_COUNT || version < 0 || expired < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }

    TBSYS_LOG(DEBUG,"put to server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());


    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_put *packet = new request_put();
    packet->area = area;
    packet->key = key;
    packet->data = data;
    packet->expired = expired;
    packet->version = version;
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_return *resp = 0;


    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }

    if(tpacket == 0 || tpacket->getPCode() != TAIR_RESP_RETURN_PACKET){
      goto FAIL;
    }

    resp = (response_return*)tpacket;
    new_config_version = resp->config_version;
    ret = resp->get_code();
    if (ret != TAIR_RETURN_SUCCESS){

      if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
        //update server table immediately
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }

      goto FAIL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);

    return 0;

FAIL:

    this_wait_object_manager->destroy_wait_object(cwo);
    TBSYS_LOG(INFO, "put failure: %s ",get_error_msg(ret));

    return ret;
  }



  int tair_client_impl::get(int area, const data_entry &key, data_entry* &data )
  {

    if( !key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"get from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());
    wait_object *cwo = 0;

    base_packet *tpacket = 0;
    response_get *resp  = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    int send_success = 0;

    int s_size = server_list.size();
    int index = 0;
    if (rand_read_flag && s_size > 1) {
      index = atomic_add_return(1, &read_seq) % s_size;
    }
    int loop_count = 0;
    while (loop_count < s_size) {
      request_get *packet = new request_get();
      packet->area = area;

      packet->add_key(key.get_data(), key.get_size());
      cwo = this_wait_object_manager->create_wait_object();

      if (send_request(server_list[index],packet,cwo->get_id()) < 0) {
        this_wait_object_manager->destroy_wait_object(cwo);

        //need delete packet
        delete packet;
      } else {
        if ((ret = get_response(cwo,1,tpacket)) < 0) {
          this_wait_object_manager->destroy_wait_object(cwo);
        } else {
          ++send_success;
          break;
        }
      }
      ++loop_count;
      ++index;
      if (rand_read_flag && s_size > 1) {
        index %= s_size;
      }
    }

    if (send_success < 1) {
      TBSYS_LOG(ERROR,"all requests are failed");
      return ret;
    }
    if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_PACKET) {
      goto FAIL;
    }
    resp = (response_get*)tpacket;
    ret = resp->get_code();
    if (ret != TAIR_RETURN_SUCCESS) {
      goto FAIL;
    }
    if (resp->data) {
      data = resp->data;
      resp->data = 0;
      ret = TAIR_RETURN_SUCCESS;
    } else {
        ret = TAIR_RETURN_PROXYED_ERROR;
        TBSYS_LOG(ERROR, "proxyed get error: %d.", ret);
        goto FAIL;
    }
    TBSYS_LOG(DEBUG,"end get:ret:%d",ret);

    new_config_version = resp->config_version;
    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
FAIL:
    if (tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
      response_return *r = (response_return *)tpacket;
      new_config_version = resp->config_version;
      ret = r->get_code();
    }
    if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
      new_config_version = resp->config_version;
      send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);


    TBSYS_LOG(INFO, "get failure: %s:%s",
        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));

    return ret;
  }

  int tair_client_impl::init_request_map(int area, vector<data_entry *>& keys, request_get_map &request_gets, int server_select)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT || server_select < 0 )
    {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;

    request_get_map::iterator rq_iter;

    vector<data_entry *>::iterator key_iter;
    for (key_iter = keys.begin(); key_iter != keys.end(); ++key_iter)
    {
      if ( !key_entry_check(**key_iter))
      {
        ret = TAIR_RETURN_ITEMSIZE_ERROR;
        break;
      }

      vector<uint64_t> server_list;
      if ( !get_server_id( **key_iter, server_list))
      {
        TBSYS_LOG(DEBUG, "can not find serverId, return false");
        ret = -1;
        break;
      }
      uint32_t ser_idx = server_select;
      if (server_list.size() <= ser_idx)
      {
        TBSYS_LOG(DEBUG, "select:%d not in server_list,size %d",server_select, server_list.size());
        ser_idx = 0;
      }
      request_get *packet = NULL;
      rq_iter = request_gets.find(server_list[ser_idx]);
      if ( rq_iter != request_gets.end() )
      {
        packet = rq_iter->second;
      } else {
        packet = new request_get();
        request_gets[server_list[ser_idx]] = packet;
      }

      packet->area = area;
      packet->add_key((*key_iter)->get_data(), (*key_iter)->get_size());
      TBSYS_LOG(DEBUG,"get from server:%s",tbsys::CNetUtil::addrToString(server_list[ser_idx]).c_str());
    }

    if (ret < 0)
    {
      rq_iter = request_gets.begin();
      while (rq_iter != request_gets.end())
      {
        request_get* req = rq_iter->second;
        ++rq_iter;
        delete req;
      }
      request_gets.clear();
    }
    return ret;
  }

  int tair_client_impl::mget_impl(int area, vector<data_entry*> &keys, tair_keyvalue_map &data, int server_select)
  {
    request_get_map request_gets;
    int ret = TAIR_RETURN_SUCCESS;
    if ((ret = init_request_map(area, keys, request_gets, server_select)) < 0)
    {
      return ret;
    }

    wait_object* cwo = this_wait_object_manager->create_wait_object();
    request_get_map::iterator rq_iter = request_gets.begin();
    while (rq_iter != request_gets.end())
    {
      if (send_request(rq_iter->first, rq_iter->second, cwo->get_id()) < 0)
      {
        delete rq_iter->second;
        request_gets.erase(rq_iter++);
      }
      else
      {
        ++rq_iter;
      }
    }

    vector<response_get*> resps;
    ret = TAIR_RETURN_SEND_FAILED;

    vector<base_packet*> tpk;
    if ((ret = get_response(cwo, request_gets.size(), tpk)) < 1)
    {
      this_wait_object_manager->destroy_wait_object(cwo);
      TBSYS_LOG(ERROR,"all requests are failed");
      return ret;
    }

    vector<base_packet*>::iterator bp_iter = tpk.begin();
    for (; bp_iter != tpk.end(); ++bp_iter)
    {
      response_get * tpacket = dynamic_cast<response_get *> (*bp_iter);
      if (tpacket == NULL || tpacket->getPCode() != TAIR_RESP_GET_PACKET)
      {
        if (tpacket != NULL && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET)
        {
          response_return *r = (response_return *)tpacket;
          ret = r->get_code();
        }
        if (tpacket != NULL && ret == TAIR_RETURN_SERVER_CAN_NOT_WORK)
        {
          new_config_version = tpacket->config_version;
          send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        continue;
      }
      resps.push_back(tpacket);
      //TBSYS_LOG(DEBUG, "response from server key_count: %d", tpacket->key_count);
    }

    vector<response_get *>::iterator rp_iter = resps.begin();
    for (; rp_iter != resps.end(); ++ rp_iter)
    {
      ret = (*rp_iter)->get_code();
      if ((ret != TAIR_RETURN_SUCCESS) && (ret != TAIR_RETURN_PARTIAL_SUCCESS))
      {
        TBSYS_LOG(WARN, "get response code: %d error", ret);
        continue;
      }

      if ((*rp_iter)->key_count == 1)
      {
        data_entry* key   = new data_entry(*((*rp_iter)->key));
        data_entry* value = new data_entry(*((*rp_iter)->data));
        data.insert(tair_keyvalue_map::value_type(key, value));
      }
      else
      {
        tair_keyvalue_map* kv_map = (*rp_iter)->key_data_map;
        assert(kv_map != NULL);
        tair_keyvalue_map::iterator it = kv_map->begin();
        while (it != kv_map->end())
        {
          data_entry * key   = new data_entry(*(it->first));
          data_entry * value = new data_entry(*(it->second));
          data.insert(tair_keyvalue_map::value_type(key, value));
          ++it;
        }
      }
      new_config_version = (*rp_iter)->config_version;
    }

    ret = TAIR_RETURN_SUCCESS;
    if (keys.size() > data.size())
    {
      ret = TAIR_RETURN_PARTIAL_SUCCESS;
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
  }

  int tair_client_impl::mget(int area, vector<data_entry*> &keys, tair_keyvalue_map &data)
  {
    int ret = mget_impl(area, keys, data, 0);

    //if (ret == TAIR_RETURN_PARTIAL_SUCCESS)
    //{
    //  vector<data_entry *> diff_keys;

    //  tair_keyvalue_map::iterator iter;
    //  vector<data_entry *>::iterator key_iter;
    //  for (key_iter = keys.begin(); key_iter != keys.end(); ++key_iter)
    //  {
    //    iter = data.find(*key_iter);//why can not find??
    //    if ( iter == data.end())
    //    {
    //      diff_keys.push_back(*key_iter);
    //    }
    //  }

    //  tair_keyvalue_map diff_data;
    //  ret = mget_impl(area, diff_keys, diff_data, 1);
    //  if (ret == TAIR_RETURN_SUCCESS || ret == TAIR_RETURN_PARTIAL_SUCCESS)
    //  {
    //    iter = diff_data.begin();
    //    while (iter != diff_data.end())
    //    {
    //      data.insert(tair_keyvalue_map::value_type(iter->first, iter->second));
    //      ++iter;
    //    }
    //    diff_data.clear();
    //  }
    //  if (keys.size() > data.size())
    //  {
    //    ret = TAIR_RETURN_PARTIAL_SUCCESS;
    //  }
    //}

    return ret;
  }


  int tair_client_impl::remove(int area, const data_entry &key)
  {

    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if( !key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }


    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_remove *packet = new request_remove();
    packet->area = area;
    packet->add_key(key.get_data(), key.get_size());

    base_packet *tpacket = 0;
    response_return *resp  = 0;

    int ret = TAIR_RETURN_SEND_FAILED;
    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }
    if ( tpacket->getPCode() != TAIR_RESP_RETURN_PACKET ) {
      goto FAIL;
    }

    resp = (response_return*)tpacket;
    new_config_version = resp->config_version;
    if ( (ret = resp->get_code()) < 0) {
      if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }
      goto FAIL;
    }


    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
FAIL:

    this_wait_object_manager->destroy_wait_object(cwo);

    TBSYS_LOG(INFO, "remove failure: %s:%s",
        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));
    return ret;
  }

  int tair_client_impl::init_request_map(int area, vector<data_entry *>& keys, request_remove_map &request_removes)
  {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT)
    {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;
    request_remove_map::iterator rq_iter;

    vector<data_entry *>::iterator key_iter;
    for (key_iter = keys.begin(); key_iter != keys.end(); ++key_iter)
    {
      if (!key_entry_check(**key_iter))
      {
        ret = TAIR_RETURN_ITEMSIZE_ERROR;
        break;
      }

      vector<uint64_t> server_list;
      if (!get_server_id(**key_iter, server_list))
      {
        TBSYS_LOG(ERROR, "can not find serverId, return false");
        ret = -1;
        break;
      }

      request_remove* packet = NULL;
      rq_iter = request_removes.find(server_list[0]);
      if (rq_iter != request_removes.end())
      {
        packet = rq_iter->second;
      }
      else
      {
        packet = new request_remove();
        request_removes[server_list[0]] = packet;
      }

      packet->area = area;
      packet->add_key((*key_iter)->get_data(), (*key_iter)->get_size());
      //TBSYS_LOG(DEBUG,"remove from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());
    }

    if (ret < 0)
    {
      rq_iter = request_removes.begin();
      while (rq_iter != request_removes.end())
      {
        request_remove * req =  rq_iter->second;
        ++rq_iter;
        delete req;
      }
      request_removes.clear();
    }
    return ret;
  }

  int tair_client_impl::mdelete(int area, vector<data_entry*> &keys)
  {
    request_remove_map request_removes;
    int ret = TAIR_RETURN_SUCCESS;
    if ((ret = init_request_map(area, keys, request_removes)) < 0)
    {
      return ret;
    }
    wait_object* cwo = this_wait_object_manager->create_wait_object();
    request_remove_map::iterator rq_iter = request_removes.begin();

    while (rq_iter != request_removes.end())
    {
      if (send_request(rq_iter->first, rq_iter->second, cwo->get_id()) < 0)
      {
        delete rq_iter->second;
        request_removes.erase(rq_iter++);
      }
      else
      {
        ++rq_iter;
      }
    }

    ret = TAIR_RETURN_SEND_FAILED;
    vector<base_packet*> tpk;
    if ((ret = get_response(cwo, request_removes.size(), tpk)) < 1)
    {
      this_wait_object_manager->destroy_wait_object(cwo);
      TBSYS_LOG(ERROR, "all requests are failed, ret: %d", ret);
      return ret;
    }

    uint32_t send_success = 0;
    vector<base_packet *>::iterator bp_iter = tpk.begin();
    for (; bp_iter != tpk.end(); ++bp_iter)
    {
      response_return* tpacket = dynamic_cast<response_return*> (*bp_iter);
      if (tpacket->getPCode() != TAIR_RESP_RETURN_PACKET)
      {
        TBSYS_LOG(ERROR, "mdelete return pcode: %d", tpacket->getPCode());
        continue;
      }
      new_config_version = tpacket->config_version;
      if ((ret = tpacket->get_code()) < 0)
      {
        if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK)
        {
          send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        continue;
      }
      ++send_success;

      //TBSYS_LOG(DEBUG,"response from server key_count:%d",tpacket->key_count);
    }

    TBSYS_LOG(DEBUG,"mdelete keys size: %d, send success: %u, return packet size: %u", keys.size(), send_success, tpk.size());
    if (keys.size() != send_success)
    {
      ret = TAIR_RETURN_PARTIAL_SUCCESS;
    }
    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
  }

  // add count

  int tair_client_impl::add_count(int area,

      const data_entry &key,
      int count, int *ret_count,
      int init_value /*=0*/,int  expire_time  /*=0*/)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT || expire_time < 0 ){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if( !key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }


    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_inc_dec *packet = new request_inc_dec();
    packet->area = area;
    packet->key = key;
    packet->add_count = count;
    packet->init_value = init_value;
    packet->expired = expire_time;

    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_inc_dec *resp = 0;


    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      TBSYS_LOG(DEBUG,"get response failed.");
      goto FAIL;
    }

    if (tpacket->getPCode() != TAIR_RESP_INCDEC_PACKET) {
      goto FAIL;
    }
    resp = (response_inc_dec*)tpacket;
    *ret_count = resp->value;
    ret = 0;
    new_config_version = resp->config_version;


    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;

FAIL:
    if (tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
      response_return *r = (response_return*)tpacket;
      ret = r->get_code();
      new_config_version = r->config_version;
      if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){//just in case
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    TBSYS_LOG(INFO, "add count failure: %s", get_error_msg(ret));

    return ret;
  }

  int tair_client_impl::remove_area(int area)
  {

    if( UNLIKELY(area < -4  || area >= TAIR_MAX_AREA_COUNT)){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //1.send request to all server
    map<uint64_t,request_remove_area *> request_list;
    map<uint64_t,request_remove_area *>::iterator it;
    for (uint32_t i=0; i<my_server_list.size(); i++) {
      uint64_t server_id = my_server_list[i];
      if (server_id == 0) {
        continue;
      }
      it = request_list.find(server_id);
      if (it == request_list.end()) {
        request_remove_area *packet = new request_remove_area();
        packet->area = area;
        request_list[server_id] = packet;
      }
    }
    if(request_list.empty()){
      return TAIR_RETURN_SUCCESS;
    }
    //2. send request

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    int send_count = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    for (it=request_list.begin(); it!=request_list.end(); ++it) {
      uint64_t server_id = it->first;
      request_remove_area *packet = it->second;
      TBSYS_LOG(INFO, "request_remove_area=>%s", tbsys::CNetUtil::addrToString(server_id).c_str());


      if( (ret = send_request(server_id,packet,cwo->get_id())) < 0){

        delete packet;
      }else {
        ++send_count;
      }
    }
    base_packet *tpacket = 0;
    if(send_count > 0){
      if( (ret = get_response(cwo,send_count,tpacket)) < 0){
        //TODO log
      }
    }


    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
  }

  int tair_client_impl::dump_area(set<dump_meta_info>& info)
  {
    //1.send request to all server
    map<uint64_t,request_dump *> request_list;
    map<uint64_t,request_dump *>::iterator it;
    for (uint32_t i=0; i<my_server_list.size(); ++i) {
      uint64_t serverId = my_server_list[i];
      if (serverId == 0) {
        continue;
      }
      it = request_list.find(serverId);
      if (it == request_list.end()) {
        request_dump *packet = new request_dump();
        packet->info_set = info;
        request_list[serverId] = packet;
      }
    }
    if(request_list.empty()){
      return TAIR_RETURN_SUCCESS;
    }
    //2. send request

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    int sendCount = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    for (it=request_list.begin(); it!=request_list.end(); ++it) {
      uint64_t serverId = it->first;
      request_dump *packet = it->second;
      TBSYS_LOG(INFO, "dump_area=>%s", tbsys::CNetUtil::addrToString(serverId).c_str());

      if( (ret = send_request(serverId,packet,cwo->get_id())) < 0){

        delete packet;
      }else {
        ++sendCount;
      }
    }
    base_packet *tpacket = 0;
    if(sendCount > 0){
      if( (ret = get_response(cwo,sendCount,tpacket)) < 0){
        //TODO log
      }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;

  }


  int tair_client_impl::remove_items(int area,
      const data_entry &key,
      int offset,
      int count)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if( !key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }


    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_remove_items *packet = new request_remove_items();
    packet->area = area;
    packet->add_key(key.get_data(), key.get_size());
    packet->offset = offset;
    packet->count = count;

    base_packet *tpacket = 0;
    response_return *resp  = 0;

    int ret = TAIR_RETURN_SEND_FAILED;

    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }
    if ( tpacket->getPCode() != TAIR_RESP_RETURN_PACKET ) {
      goto FAIL;
    }
    resp = (response_return*)tpacket;
    new_config_version = resp->config_version;

    ret = resp->get_code();
    if (ret >= 0) {
      ret = 0;
    } else {
      TBSYS_LOG(INFO, "remove to failure: %s(%d)", resp->get_message(), ret);
      if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK ||
          ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
        //update server table immediately
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }
      goto FAIL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
FAIL:
    this_wait_object_manager->destroy_wait_object(cwo);

    TBSYS_LOG(INFO, "remove failure: %s:%s",
        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));
    return ret;

  }

  int tair_client_impl::get_items_count(int area,const data_entry& key)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if( !key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }


    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_get_items_count *packet = new request_get_items_count();
    packet->area = area;
    packet->add_key(key.get_data(), key.get_size());

    base_packet *tpacket = 0;
    response_return *resp  = 0;

    int ret = TAIR_RETURN_SEND_FAILED;

    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){
      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }
    if ( tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
      goto FAIL;
    }

    resp = (response_return *)tpacket;
    new_config_version = resp->config_version;
    ret = resp->get_code();
    if(ret < 0){
      if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }
      goto FAIL;
    }

    TBSYS_LOG(DEBUG,"end get_items_count:ret:%d",ret);
    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
FAIL:

    this_wait_object_manager->destroy_wait_object(cwo);
    TBSYS_LOG(INFO, "get_items_count failure: %s:%s",

        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));
    return ret;
  }

  void tair_client_impl::set_timeout(int this_timeout)
  {
    assert(timeout >= 0);
    timeout = this_timeout;
  }

  void tair_client_impl::set_randread(bool rand_flag)
  {
    rand_read_flag = rand_flag;
  }
  /*-----------------------------------------------------------------------------
   *  tair_client_impl::ErrMsg
   *-----------------------------------------------------------------------------*/


  const std::map<int,string> tair_client_impl::m_errmsg = tair_client_impl::init_errmsg();

  std::map<int,string> tair_client_impl::init_errmsg()
  {
    std::map<int,string> temp;
    temp[TAIR_RETURN_SUCCESS]                 = "success";
    temp[TAIR_RETURN_FAILED]          = "general failed";
    temp[TAIR_RETURN_DATA_NOT_EXIST]  = "data not exists";
    temp[TAIR_RETURN_VERSION_ERROR]           = "version error";
    temp[TAIR_RETURN_TYPE_NOT_MATCH]  = "data type not match";
    temp[TAIR_RETURN_ITEM_EMPTY]              = "item is empty";
    temp[TAIR_RETURN_SERIALIZE_ERROR] = "serialize failed";
    temp[TAIR_RETURN_OUT_OF_RANGE]            = "item's index out of range";
    temp[TAIR_RETURN_ITEMSIZE_ERROR]  = "key or vlaue too large";
    temp[TAIR_RETURN_SEND_FAILED]             = "send packet error";
    temp[TAIR_RETURN_TIMEOUT]         = "timeout";
    temp[TAIR_RETURN_SERVER_CAN_NOT_WORK]   = "server can not work";
    temp[TAIR_RETURN_WRITE_NOT_ON_MASTER]   = "write not on master";
    temp[TAIR_RETURN_DUPLICATE_BUSY]          = "duplicate busy";
    temp[TAIR_RETURN_MIGRATE_BUSY]    = "migrate busy";
    temp[TAIR_RETURN_PARTIAL_SUCCESS] = "partial success";
    temp[TAIR_RETURN_DATA_EXPIRED]            = "expired";
    temp[TAIR_RETURN_PLUGIN_ERROR]            = "plugin error";
    temp[TAIR_RETURN_PROXYED]                 = "porxied";
    temp[TAIR_RETURN_INVALID_ARGUMENT]        = "invalid argument";
    temp[TAIR_RETURN_CANNOT_OVERRIDE] = "cann't override old value.please check and remove it first.";
    return temp;
  }

  const char *tair_client_impl::get_error_msg(int ret)
  {
    std::map<int,string>::const_iterator it = tair_client_impl::m_errmsg.find(ret);
    return it != tair_client_impl::m_errmsg.end() ? it->second.c_str() : "unknow";
  }

  void tair_client_impl::get_server_with_key(const data_entry& key,std::vector<std::string>& servers)
  {
    if( !key_entry_check(key)){
      return;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find server_id, return false");
      return;
    }

    for(vector<uint64_t>::iterator it= server_list.begin(); it != server_list.end(); ++it){
      servers.push_back(tbsys::CNetUtil::addrToString(*it));
    }
    return;
  }

  void tair_client_impl::get_servers(std::set<uint64_t> &servers)
  {
    set<uint64_t> tmp(my_server_list.begin(), my_server_list.end());
    servers.swap(tmp);
  }


  // @override IPacketHandler
  tbnet::IPacketHandler::HPRetCode tair_client_impl::handlePacket(tbnet::Packet *packet, void *args)
  {
    if (!packet->isRegularPacket()) {
      tbnet::ControlPacket *cp = (tbnet::ControlPacket*)packet;
      TBSYS_LOG(WARN, "ControlPacket, cmd:%d", cp->getCommand());
      ++send_fail_count;
      if (cp->getCommand() == tbnet::ControlPacket::CMD_DISCONN_PACKET) {
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }
    }

    int id = static_cast<int> ((reinterpret_cast<long>(args)));
    if (id) {
      if (packet->isRegularPacket()) {

        this_wait_object_manager->wakeup_wait_object(id, (base_packet*)packet);

      } else {
        this_wait_object_manager->wakeup_wait_object(id, NULL);

      }
    } else if (packet->isRegularPacket()) {
      if (packet->getPCode() == TAIR_RESP_GET_GROUP_PACKET && args == NULL) {
        response_get_group *rggp = (response_get_group*)packet;
        new_config_version = rggp->config_version;
        if (config_version != new_config_version) {
          uint64_t *server_list = rggp->get_server_list(bucket_count,copy_count);
          //assert(rggp->server_list_count == bucket_count * copy_count);
          for (uint32_t i=0; server_list != NULL && i<(uint32_t)rggp->server_list_count
              && i<my_server_list.size(); i++) {
            TBSYS_LOG(DEBUG, "update server table: [%d] => [%s]", i, tbsys::CNetUtil::addrToString(server_list[i]).c_str());
            my_server_list[i] = server_list[i];
          }
          TBSYS_LOG(INFO, "config_version: %u => %u", config_version, rggp->config_version);
          config_version = new_config_version;
        }
      }
      delete packet;
    }

    return tbnet::IPacketHandler::KEEP_CHANNEL;
  }

  //@override Runnable
  void tair_client_impl::run(tbsys::CThread *thread, void *arg)
  {
    if (config_server_list.size() == 0U) {
      return;
    }

    int config_server_index = 0;
    uint32_t old_config_version = 0;
    // int loopCount = 0;

    while (!is_stop) {
      //++loopCount;
      TAIR_SLEEP(is_stop, 1);
      if (is_stop) break;

      if(config_version < TAIR_CONFIG_MIN_VERSION){
        //ought to update
      } else if (config_version == new_config_version && send_fail_count < UPDATE_SERVER_TABLE_INTERVAL) {
        continue;
      }

      //    if (new_config_version == old_config_version && (loopCount % 30) != 0) {
      //        continue;
      //    }
      old_config_version = new_config_version;

      config_server_index ++;
      config_server_index %= config_server_list.size();
      uint64_t serverId = config_server_list[config_server_index];

      TBSYS_LOG(WARN, "send request to configserver(%s), config_version: %u, new_config_version: %d",
          tbsys::CNetUtil::addrToString(serverId).c_str(), config_version, new_config_version);

      request_get_group *packet = new request_get_group();
      packet->set_group_name(group_name.c_str());
      if (connmgr->sendPacket(serverId, packet, NULL, NULL) == false) {
        TBSYS_LOG(ERROR, "send request_get_group to %s failure.",
            tbsys::CNetUtil::addrToString(serverId).c_str());
        delete packet;
      }
      send_fail_count = 0;
    }
  }


  void tair_client_impl::force_change_dataserver_status(uint64_t server_id, int cmd)
  {
    for (size_t i = 0; i < config_server_list.size(); ++i) {
      uint64_t conf_serverId = config_server_list[i];
      request_data_server_ctrl *packet = new request_data_server_ctrl();
      packet->server_id = server_id;
      packet->cmd = cmd;
      if (connmgr->sendPacket(conf_serverId, packet, NULL, NULL) == false) {
        TBSYS_LOG(ERROR, "send request_data_server_ctrl to %s failure.",
            tbsys::CNetUtil::addrToString(server_id).c_str());
        delete packet;
      }
    }
  }

  void tair_client_impl::get_migrate_status(uint64_t server_id,vector<pair<uint64_t,uint32_t> >& result)
  {

    if(config_server_list.size() == 0){
      TBSYS_LOG(WARN,"config server list is empty");
      return;
    }

    request_get_migrate_machine *packet = new request_get_migrate_machine();
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_get_migrate_machine *resp = 0;

    packet->server_id = server_id;


    wait_object *cwo = this_wait_object_manager->create_wait_object();


    if( (ret = send_request(config_server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }

    if(tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_MIGRATE_MACHINE_PACKET){
      goto FAIL;
    }

    resp = (response_get_migrate_machine *)tpacket;
    log_debug("resp->_vec_ms = %d", resp->vec_ms.size());
    result = resp->vec_ms;

    this_wait_object_manager->destroy_wait_object(cwo);

    return;
FAIL:

    TBSYS_LOG(DEBUG,"get_migrate_status failed:%s",get_error_msg(ret));
    this_wait_object_manager->destroy_wait_object(cwo);

  }
  void tair_client_impl::query_from_configserver(uint32_t query_type, const string group_name, map<string, string>& out, uint64_t serverId)
  {

    if(config_server_list.size() == 0){
      TBSYS_LOG(WARN,"config server list is empty");
      return;
    }


    request_query_info *packet = new request_query_info();
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_query_info *resp = 0;

    packet->query_type = query_type;
    packet->group_name = group_name;
    packet->server_id  = serverId;


    wait_object *cwo = this_wait_object_manager->create_wait_object();



    if( (ret = send_request(config_server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }

    if(tpacket == 0 || tpacket->getPCode() != TAIR_RESP_QUERY_INFO_PACKET){
      goto FAIL;
    }

    resp = (response_query_info*)tpacket;
    out = resp->map_k_v;

    this_wait_object_manager->destroy_wait_object(cwo);

    return;
FAIL:

    TBSYS_LOG(DEBUG,"query from config server failed:%s",get_error_msg(ret));
    this_wait_object_manager->destroy_wait_object(cwo);

  }

  int64_t tair_client_impl::ping(uint64_t server_id)
  {
    if (!inited) {
      if (!startup(server_id))
        return 0;
    }

    request_ping *req = new request_ping();
    req->value = 0;
    base_packet *bp = NULL;
    wait_object *cwo = this_wait_object_manager->create_wait_object();

    struct timeval tm_beg;
    struct timeval tm_end;
    gettimeofday(&tm_beg, NULL);
    bool ret = true;
    do {
      if (send_request(server_id, req, cwo->get_id()) != 0) {
        log_error("send ping packet failed.");
        ret = false;
        delete req;
        break;
      }

      if (get_response(cwo, 1, bp) != 0) {
        log_error("get ping packet timeout.");
        ret = false;
        break;
      }
      if (bp == NULL || bp->getPCode() != TAIR_RESP_RETURN_PACKET) {
        log_error("got bad packet.");
        ret = false;
        break;
      }
    } while (false);
    gettimeofday(&tm_end, NULL);

    this_wait_object_manager->destroy_wait_object(cwo);
    return static_cast<int64_t>((tm_end.tv_sec-tm_beg.tv_sec) * 1000000
        + (tm_end.tv_usec - tm_beg.tv_usec));
  }

  bool tair_client_impl::retrieve_server_addr()
  {
    if (config_server_list.size() == 0U || group_name.empty()) {
      TBSYS_LOG(WARN, "config server list is empty, or groupname is NULL, return false");
      return false;
    }
    wait_object *cwo = 0;
    int send_success = 0;

    response_get_group *rggp = 0;
    base_packet *tpacket = NULL;

    for (uint32_t i=0; i<config_server_list.size(); i++) {
      uint64_t server_id = config_server_list[i];
      request_get_group *packet = new request_get_group();
      packet->set_group_name(group_name.c_str());


      cwo = this_wait_object_manager->create_wait_object();
      if (connmgr->sendPacket(server_id, packet, NULL, (void*)((long)cwo->get_id())) == false) {
        TBSYS_LOG(ERROR, "Send RequestGetGroupPacket to %s failure.",
            tbsys::CNetUtil::addrToString(server_id).c_str());
        this_wait_object_manager->destroy_wait_object(cwo);

        delete packet;
        continue;
      }
      cwo->wait_done(1,timeout);
      tpacket = cwo->get_packet();
      if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_GROUP_PACKET){
        TBSYS_LOG(ERROR,"get group packet failed,retry");
        tpacket = 0;

        this_wait_object_manager->destroy_wait_object(cwo);

        cwo = 0;
        continue;
      }else{
        ++send_success;
        break;
      }
    }

    if (send_success <= 0 ){
      //delete packet;
      TBSYS_LOG(ERROR,"cann't connect");
      return false;
    }
    uint64_t *server_list = NULL ;
    uint32_t server_list_count = 0;

    rggp = dynamic_cast<response_get_group*>(tpacket);
    if (rggp->config_version <= 0){
      TBSYS_LOG(ERROR, "group doesn't exist: %s", group_name.c_str());
      goto OUT;
    }
    bucket_count = rggp->bucket_count;
    copy_count = rggp->copy_count;

    if(bucket_count <= 0 || copy_count <= 0){
      TBSYS_LOG(ERROR, "bucket or copy count doesn't correct");
      goto OUT;
    }
    server_list = rggp->get_server_list(bucket_count,copy_count);
    if(rggp->server_list_count <= 0 || server_list == 0){
      TBSYS_LOG(WARN, "server table is empty");
      goto OUT;
    }

    server_list_count = (uint32_t)(rggp->server_list_count);
    assert(server_list_count == bucket_count * copy_count);

    for (uint32_t i=0; server_list != 0 && i< server_list_count; ++i) {
      TBSYS_LOG(DEBUG, "server table: [%d] => [%s]", i, tbsys::CNetUtil::addrToString(server_list[i]).c_str());
      my_server_list.push_back(server_list[i]);
    }

    new_config_version = config_version = rggp->config_version; //set the same value on the first time.
    this_wait_object_manager->destroy_wait_object(cwo);
    thread.start(this, NULL);
    return true;
OUT:
    this_wait_object_manager->destroy_wait_object(cwo);

    return false;
  }

  void tair_client_impl::start_tbnet()
  {
    connmgr->setDefaultQueueTimeout(0, timeout);
    connmgr->setDefaultQueueLimit(0, 1000);
    transport->start();
  }


  void tair_client_impl::stop_tbnet()
  {
    transport->stop();
  }

  void tair_client_impl::wait_tbnet()
  {
    transport->wait();
  }

  void tair_client_impl::reset() //reset enviroment
  {
    timeout = 2000;
    config_version = 0;
    new_config_version = 0;
    send_fail_count = 0;
    bucket_count = 0;
    copy_count = 0;
    my_server_list.clear();
    config_server_list.clear();
    group_name = "";

    packet_factory = 0;
    streamer = 0;
    transport = 0;
    connmgr = 0;
    this_wait_object_manager = 0;
    inited = false;
  }

  bool tair_client_impl::get_server_id(const data_entry &key,vector<uint64_t>& server)
  {
    uint32_t hash = util::string_util::mur_mur_hash(key.get_data(), key.get_size());
    server.clear();

    if (my_server_list.size() > 0U) {
      hash %= bucket_count;
      for(uint32_t i=0;i<copy_count && i < my_server_list.size(); ++ i){
        uint64_t server_id = my_server_list[hash + i * bucket_count];
        if(server_id != 0){
          server.push_back(server_id);
        }
      }
    }
    return server.size() > 0 ? true : false;
  }

  int tair_client_impl::send_request(uint64_t server_id,base_packet *packet,int waitId)
  {
    assert(server_id != 0 && packet != 0 && waitId >= 0);
    if (connmgr->sendPacket(server_id, packet, NULL, (void*)((long)waitId)) == false) {
      TBSYS_LOG(ERROR, "Send RequestGetPacket to %s failure.",
          tbsys::CNetUtil::addrToString(server_id).c_str());
      send_fail_count ++;
      return TAIR_RETURN_SEND_FAILED;
    }
    return 0;
  }
#if 0
  int tair_client_impl::send_request(vector<uint64_t>& server,base_packet *packet,int waitId)
  {
    assert(!server.empty() && packet != 0 && waitId >= 0);

    int send_success = 0;
    for(uint32_t i=0;i<server.size();++i){
      uint64_t serverId = server[i];
      if (conn_manager->sendPacket(serverId, packet, NULL, (void*)((long)waitId)) == false) {
        TBSYS_LOG(ERROR, "Send RequestGetPacket to %s failure.",
            tbsys::CNetUtil::addrToString(serverId).c_str());
        ++m_sendFailCount;
        continue;
      }
      ++send_success;
      break;
    }
    return send_success > 0 ? TAIR_RETURN_SUCCESS : TAIR_RETURN_SEND_FAILED;
  }
#endif


  int tair_client_impl::get_response(wait_object *cwo,int wait_count,base_packet*& tpacket)

  {
    assert(cwo != 0 && wait_count >= 0);
    cwo->wait_done(wait_count, timeout);
    base_packet *packet = cwo->get_packet();
    if(packet == 0){
      return TAIR_RETURN_TIMEOUT;
    }
    tpacket = packet;
    return 0;
  }

  int tair_client_impl::get_response(wait_object *cwo, int wait_count, vector<base_packet*>& tpacket)
  {
    assert(cwo != 0 && wait_count >= 0);
    cwo->wait_done(wait_count, timeout);
    int r_num = cwo->get_packet_count();
    int push_num = (r_num < wait_count) ? r_num: wait_count;

    for (int idx = 0 ; idx < push_num ; ++ idx)
    {
      base_packet *packet = cwo->get_packet(idx);
      if(packet == NULL){
        return TAIR_RETURN_TIMEOUT;
      }
      tpacket.push_back(packet);
    }
    return push_num;
  }

  bool tair_client_impl::key_entry_check(const data_entry& key)
  {
    if( key.get_size() == 0 || key.get_data() == 0 ){
      return false;
    }
    if( (key.get_size() == 1) && ( *(key.get_data()) == '\0')){
      return false;
    }
    if( key.get_size() >= TAIR_MAX_KEY_SIZE ){
      return false;
    }
    return true;
  }


  bool tair_client_impl::data_entry_check(const data_entry& data)
  {
    if( data.get_size() == 0 || data.get_data() == 0 ){
      return false;
    }
    //      if( (data.get_size() == 1) && ( *(data.get_data()) == '\0')){
    //              return false;
    //      }
    if( data.get_size() >= TAIR_MAX_DATA_SIZE ){
      return false;
    }
    return true;
  }

}
