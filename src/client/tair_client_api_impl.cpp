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
#include "get_range_packet.hpp"
#include "get_group_packet.hpp"
#include "query_info_packet.hpp"
#include "get_migrate_machine.hpp"
#include "data_server_ctrl_packet.hpp"
#include "scoped_wrlock.hpp"
#include "base_packet.hpp"
#include "flow_control_packet.hpp"
#include "flow_view.hpp"
#include "flowrate.h"
#include "op_cmd_packet.hpp"
#include "expire_packet.hpp"

namespace tair {


  using namespace std;
  using namespace __gnu_cxx;
  using namespace tair::json;
  using namespace tair::common;


  /*-----------------------------------------------------------------------------
   *  tair_client_impl
   *-----------------------------------------------------------------------------*/

  tair_client_impl::tair_client_impl():inited(false),is_stop(false),direct(false),data_server(0),packet_factory(0),streamer(0),
  transport(0),connmgr(0),timeout(2000),config_version(0),
  new_config_version(0),send_fail_count(0),this_wait_object_manager(0),
  bucket_count(0),copy_count(0),rand_read_flag(false)
  {
    atomic_set(&read_seq, 0);
    pthread_rwlock_init(&m_init_mutex, NULL);
  }

  tair_client_impl::~tair_client_impl()
  {
    close();
    pthread_rwlock_destroy(&m_init_mutex);
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

  bool tair_client_impl::directup(const char *server_addr)
  {
    if(server_addr == NULL){
      return false;
    }

    if(inited){
      return true;
    }
    is_stop = false;

    if( !initialize() ){
      return false;
    }

    int default_port = 5191;
    uint64_t data_server = tbsys::CNetUtil::strToAddr(server_addr, default_port);

    return directup(data_server);
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

    if(inited) return true;
    CScopedRwLock __scoped_lock(&m_init_mutex,true);
    if(inited) return true;

    start_tbnet();

    if (!retrieve_server_addr()){
      TBSYS_LOG(ERROR,"retrieve_server_addr falied.\n");
      close();
      return false;
    }
    inited = true;
    return true;
  }

  bool tair_client_impl::directup(uint64_t data_server)
  {
    if(data_server == 0){
      return false;
    }

    my_server_list.push_back(data_server);

    if(inited) return true;
    CScopedRwLock __scoped_lock(&m_init_mutex,true);
    if(inited) return true;

    bucket_count = 1;
    copy_count = 1;

    start_tbnet();

    inited = true;
    this->data_server = data_server;
    this->direct = true;

    return tbnet::ConnectionManager::isAlive(data_server);
  }


  bool tair_client_impl::initialize()
  {

    this_wait_object_manager = new wait_object_manager(invoke_callback,this);
    if(this_wait_object_manager == 0)

      return false;

    packet_factory = new tair_packet_factory();
    if( packet_factory == 0)
      goto FAIL_1;

    streamer = new tair_packet_streamer();
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
    response_thread.join();
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
      int expired, int version, bool fill_cache,
      TAIRCALLBACKFUNC pfunc,void * parg )
  {
    if( !(key_entry_check(key)) || (!data_entry_check(data))){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if(key.get_size() + data.get_size() > (TAIR_MAX_DATA_SIZE + TAIR_MAX_KEY_SIZE)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if(area < 0 || area >= TAIR_MAX_AREA_COUNT || version < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }

    TBSYS_LOG(DEBUG,"put to server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    wait_object *cwo = this_wait_object_manager->create_wait_object(TAIR_REQ_PUT_PACKET,pfunc,parg,expired);

    request_put *packet = new request_put();
    packet->area = area;
    packet->key = key;
    // set fill cache flag
    packet->key.data_meta.flag = fill_cache ? TAIR_CLIENT_PUT_PUT_CACHE_FLAG : TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
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

    //if it's a async call,just return ok and wait callback.
    if(pfunc) return 0;

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

  int tair_client_impl::set_count(int area, const data_entry& key, int count,
      int expire, int version, TAIRCALLBACKFUNC pfunc,void * parg)
  {
    // This is an ugly trick, 'cause Tair's key/value has two bits flag to indicate
    // type in Java client implementation but count type's flag is added in server,
    // we don't want to add a new packet for this set_count() function, so we add two
    // bits type flag here and use put() to make following incr()/decr() happy.
    // We ignore compress type here as same as what CPP client always does.
#ifdef WITH_COMPRESS
    char buf[INCR_DATA_SIZE - 2];
    buf[0] = (count & 0xFF);
    buf[1] = ((count >> 8) & 0xFF);
    buf[2] = ((count >> 16) & 0xFF);
    buf[3] = ((count >> 24) & 0xFF);
    data_entry value;
    value.set_data(buf, INCR_DATA_SIZE - 2, false);
#else
    char buf[INCR_DATA_SIZE];
    SET_INCR_DATA_COUNT(buf, count);
    data_entry value;
    value.set_data(buf, INCR_DATA_SIZE, false);
#endif
    // force set count type flag
    value.data_meta.flag = TAIR_ITEM_FLAG_ADDCOUNT;
    return put(area, key, value, expire, version, true, pfunc, parg);
  }

  int tair_client_impl::lock(int area, const data_entry &key, LockType type,
      TAIRCALLBACKFUNC pfunc,void * arg)
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

    wait_object *cwo = this_wait_object_manager->create_wait_object(TAIR_REQ_LOCK_PACKET,pfunc,arg);
    request_lock *packet = new request_lock();
    packet->area = area;
    packet->key.data_meta = key.data_meta;
    // not copy
    packet->key.set_data(key.get_data(), key.get_size(), false);
    packet->lock_type = type;
    base_packet *tpacket = 0;
    response_return *resp  = 0;

    int ret = TAIR_RETURN_SEND_FAILED;
    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }
    //if async callback,return it.
    if(pfunc) return 0;

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

    TBSYS_LOG(INFO, "lock(%d) failure: %s:%s",
        type, tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));
    return ret;
  }

  int tair_client_impl::expire(int area, const data_entry &key, int expired)
  {
    if (!key_entry_check(key)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (key.get_size() > TAIR_MAX_KEY_SIZE) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if( area < 0 || area >= TAIR_MAX_AREA_COUNT || expired < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_expire *packet = new request_expire();
    packet->area = area;
    packet->key = key;
    packet->expired = expired;
    base_packet *tpacket = 0;
    response_return *resp  = 0;

    int ret = TAIR_RETURN_SEND_FAILED;
    if( (ret = send_request(server_list[0], packet, cwo->get_id())) < 0){
      delete packet;
      goto FAIL;
    }

    if ((ret = get_response(cwo, 1, tpacket)) < 0){
      TBSYS_LOG(ERROR, "go failure from here");
      goto FAIL;
    }

    if (tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
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
    return ret;
FAIL:
    this_wait_object_manager->destroy_wait_object(cwo);
    TBSYS_LOG(INFO, "expire failure: %s:%s",
        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));
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

      packet->add_key(key.get_data(), key.get_size(), key.get_prefix_size());
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
    if (TAIR_RETURN_SHOULD_PROXY == ret) {
      new_config_version = resp->config_version;
    }

    this_wait_object_manager->destroy_wait_object(cwo);


    TBSYS_LOG(INFO, "get failure: %s:%s",
        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));

    return ret;
  }

  int tair_client_impl::init_request_map(int area, const vector<data_entry *>& keys, request_get_map &request_gets, int server_select)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT || server_select < 0 )
    {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;

    request_get_map::iterator rq_iter;

    vector<data_entry *>::const_iterator key_iter;
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

  int tair_client_impl::mget_impl(int area, const vector<data_entry*> &keys, tair_keyvalue_map &data, int server_select)
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
    }

    vector<response_get *>::iterator rp_iter = resps.begin();
    bool kv_map_null = false;
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
      else if ((*rp_iter)->key_count > 1)
      {
        tair_keyvalue_map* kv_map = (*rp_iter)->key_data_map;
        if (kv_map == NULL)
        {
          TBSYS_LOG(ERROR, "mget response's key data map is null");
          //why? break;
          kv_map_null = true;
          break;
        }
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

    if (kv_map_null)
    {
      //return fail
      ret = TAIR_RETURN_FAILED;
      //free
      tair_keyvalue_map::iterator mit = data.begin();
      for ( ; mit != data.end(); ++mit)
      {
        delete mit->first;
        delete mit->second;
      }
      data.clear();
    }
    else
    {
      ret = TAIR_RETURN_SUCCESS;
      if (keys.size() > data.size())
      {
        ret = TAIR_RETURN_PARTIAL_SUCCESS;
      }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
  }

  int tair_client_impl::mget(int area, const vector<data_entry*> &keys, tair_keyvalue_map &data)
  {
    return mget_impl(area, keys, data, 0);
  }

  int tair_client_impl::remove(int area, const data_entry &key,TAIRCALLBACKFUNC pfunc,void * arg)
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


    wait_object *cwo = this_wait_object_manager->create_wait_object(TAIR_REQ_REMOVE_PACKET,pfunc,arg);
    request_remove *packet = new request_remove();
    packet->area = area;
    packet->add_key(key.get_data(), key.get_size(), key.get_prefix_size());

    base_packet *tpacket = 0;
    response_return *resp  = 0;

    int ret = TAIR_RETURN_SEND_FAILED;
    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }
    //if async callback,return it.
    if(pfunc) return 0;

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
    return ret;
  }

  int tair_client_impl::invalidate(int area, const data_entry &key, const char *groupname)
  {
    if (groupname == NULL) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (!key_entry_check(key)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (invalid_server_list.size() == 0) {
      TBSYS_LOG(ERROR, "invalidate server list is empty.");
      return TAIR_RETURN_FAILED;
    }
    static hash_map<uint64_t, int, __gnu_cxx::hash<int> > fail_count_map;


    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_invalid *req = new request_invalid();
    req->set_group_name(groupname);
    req->area = area;
    req->add_key(key.get_data(), key.get_size());

    int ret_code = TAIR_RETURN_SUCCESS;
    do {
      const int inval_size = invalid_server_list.size();
      uint64_t server_id = invalid_server_list[rand() % inval_size];
      base_packet * tpacket = NULL;
      //~ send request.
      log_debug("send request_invalid to %s.", tbsys::CNetUtil::addrToString(server_id).c_str());
      if (connmgr->sendPacket(server_id, req, NULL, (void*)(long)cwo->get_id()) == false) {
        log_error("send request_invalid to %s failed.", tbsys::CNetUtil::addrToString(server_id).c_str());
        int fail_count = ++fail_count_map[server_id];
        if (fail_count >= 10) {
          //! remove invalid server, if 10 consecutive timeout encountered.
          log_error("invalid server %s regarded to be down.",
              tbsys::CNetUtil::addrToString(server_id).c_str());
          shrink_invalidate_server(server_id);
        }
        send_fail_count++;
        ret_code = TAIR_RETURN_SEND_FAILED;
        delete req;
        break;
      }
      //~ get response
      if ((ret_code = get_response(cwo, 1, tpacket)) != 0) {
        log_warn("get invalid response timeout.");
        int fail_count = ++fail_count_map[server_id];
        if (fail_count >= 10) {
          //! remove invalid server, if 10 consecutive timeout encountered.
          log_error("invalid server %s regarged to be down.",
              tbsys::CNetUtil::addrToString(server_id).c_str());
          shrink_invalidate_server(server_id);
        }
      }
      //~ got response
      if (tpacket != NULL) {
        response_return *resp = dynamic_cast<response_return*>(tpacket);
        if (resp != NULL) {
          if ((ret_code = resp->get_code()) != TAIR_RETURN_SUCCESS) {
            TBSYS_LOG(ERROR, "invalidate failure, ret_code: %", ret_code);
          } else {
            fail_count_map[server_id] = 0;
          }
        } else {
          TBSYS_LOG(ERROR, "packet cannot be casted to response_return");
        }
      } else {
        log_error("tpacket is null");
        ret_code = TAIR_RETURN_FAILED;
      }

    } while (false);

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret_code;
  }

  int tair_client_impl::invalidate(int area, const data_entry &key)
  {
    return invalidate(area, key, group_name.c_str());
  }

  int tair_client_impl::hide(int area, const data_entry &key)
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
    request_hide *packet = new request_hide();
    packet->area = area;
    packet->add_key(key.get_data(), key.get_size(), key.get_prefix_size()); //~ prefix_hide may call this method

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

  int tair_client_impl::get_hidden(int area, const data_entry &key, data_entry *&value)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(key)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    request_get_hidden *req = new request_get_hidden();
    req->add_key(key.get_data(), key.get_size(), key.get_prefix_size());
    req->area = area;
    response_get *resp = NULL;

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
      TBSYS_LOG(WARN, "no dataserver available");
      return TAIR_RETURN_FAILED;
    }
    TBSYS_LOG(DEBUG, "get from server: %s", tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    int ret = do_request(req, resp, cwo, server_list[0]);
    if (resp != NULL) {
      new_config_version = resp->config_version;
      value = resp->data;
      if (value == NULL) {
        ret = TAIR_RETURN_PROXYED;
      }
      resp->data = NULL;
    }
    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
  }

  int tair_client_impl::prefix_get(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if(!key_entry_check(pkey) || !key_entry_check(skey)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);

    return get(area, mkey, value);
  }

  int tair_client_impl::prefix_put(int area, const data_entry &pkey, const data_entry &skey,
      const data_entry &value, int expire, int version)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if(!key_entry_check(pkey) || !key_entry_check(skey) || !data_entry_check(value)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);

    return put(area, mkey, value, expire, version);
  }

  int tair_client_impl::prefix_hide(int area, const data_entry &pkey, const data_entry &skey)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);

    return hide(area, mkey);
  }

  /**
   * hide with multiple merged key
   * should only be called by invalid server
   */
  int tair_client_impl::hides(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (mkey_set.empty()) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    request_prefix_hides *req = new request_prefix_hides();
    req->area = area;
    response_mreturn *resp = NULL;
    int ret = TAIR_RETURN_SUCCESS;

    do {
      tair_dataentry_set::const_iterator itr = mkey_set.begin();
      data_entry *mkey = NULL;
      while (itr != mkey_set.end()) {
        mkey = *itr;
        req->add_key(mkey, true);
        ++itr;
      }

      vector<uint64_t> server_list;
      if (!get_server_id(*mkey, server_list)) {
        TBSYS_LOG(WARN, "no dataserver available");
        delete req;
        ret = TAIR_RETURN_FAILED;
        break;
      }

      wait_object *cwo = this_wait_object_manager->create_wait_object();
      ret = do_request(req, resp, cwo, server_list[0]);
      if (resp != NULL && ret != TAIR_RETURN_SUCCESS && resp->key_code_map != NULL) {
        key_code_map.clear();
        resp->key_code_map->swap(key_code_map);
      }
      this_wait_object_manager->destroy_wait_object(cwo);
    } while (false);
    return ret;
  }

  int tair_client_impl::prefix_hides(int area, const data_entry &pkey,
      const tair_dataentry_set &skey_set, key_code_map_t &key_code_map)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    request_prefix_hides *req = new request_prefix_hides();
    req->area = area;
    response_mreturn *resp = NULL;
    int ret = TAIR_RETURN_SUCCESS;

    do {
      tair_dataentry_set::const_iterator itr = skey_set.begin();
      data_entry *mkey = NULL;
      while (itr != skey_set.end()) {
        data_entry *skey = *itr;
        mkey = new data_entry();
        merge_key(pkey, *skey, *mkey);
        if (!key_entry_check(*mkey)) {
          delete mkey;
          ret = TAIR_RETURN_ITEMSIZE_ERROR;
          break;
        }
        req->add_key(mkey);
        ++itr;
      }
      if (ret != TAIR_RETURN_SUCCESS) {
        delete req;
        break;
      }

      vector<uint64_t> server_list;
      if (!get_server_id(*mkey, server_list)) {
        TBSYS_LOG(DEBUG, "no dataserver available");
        delete req;
        ret = TAIR_RETURN_FAILED;
        break;
      }

      wait_object *cwo = this_wait_object_manager->create_wait_object();
      ret = do_request(req, resp, cwo, server_list[0]);
      if (resp != NULL && ret != TAIR_RETURN_SUCCESS && resp->key_code_map != NULL) {
        key_code_map.clear();
        resp->key_code_map->swap(key_code_map);
      }
      this_wait_object_manager->destroy_wait_object(cwo);
    } while (false);
    return ret;
  }

  int tair_client_impl::prefix_get_hidden(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);

    return get_hidden(area, mkey, value);
  }

  int tair_client_impl::prefix_remove(int area, const data_entry &pkey, const data_entry &skey)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);
    return remove(area, mkey);
  }

  int tair_client_impl::removes(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (mkey_set.empty()) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    request_prefix_removes *req = new request_prefix_removes();
    req->area = area;
    response_mreturn *resp = NULL;
    int ret = TAIR_RETURN_SUCCESS;

    do {
      tair_dataentry_set::const_iterator itr = mkey_set.begin();
      data_entry *mkey = NULL;
      while (itr != mkey_set.end()) {
        mkey = *itr;
        req->add_key(mkey, true);
        ++itr;
      }

      vector<uint64_t> server_list;
      if (!get_server_id(*mkey, server_list)) {
        TBSYS_LOG(WARN, "no dataserver available");
        delete req;
        ret = TAIR_RETURN_FAILED;
        break;
      }

      wait_object *cwo = this_wait_object_manager->create_wait_object();
      ret = do_request(req, resp, cwo, server_list[0]);
      if (resp == NULL) {
        this_wait_object_manager->destroy_wait_object(cwo);
        break;
      }
      if (ret != TAIR_RETURN_SUCCESS && resp->key_code_map != NULL) {
        key_code_map.clear();
        resp->key_code_map->swap(key_code_map);
      }
      this_wait_object_manager->destroy_wait_object(cwo);
    } while (false);
    return ret;
  }

  int tair_client_impl::get_range(int area, const data_entry &pkey, const data_entry &start_key, const data_entry &end_key, 
      int offset, int limit, vector<data_entry *> &values,short type)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (limit < 0 || offset < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
       
    data_entry merge_skey, merge_ekey;
    merge_key(pkey, start_key, merge_skey);
    merge_key(pkey, end_key, merge_ekey);

    if (!key_entry_check(merge_skey) || !key_entry_check(merge_ekey)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    vector<uint64_t> server_list;
    if (!get_server_id(merge_skey, server_list)) {
      TBSYS_LOG(WARN, "no dataserver available");
      return TAIR_RETURN_FAILED;
    }
    if (limit == 0)
      limit = RANGE_DEFAULT_LIMIT;

    request_get_range *packet = new request_get_range();
    packet->area = area;
    packet->cmd = type;
    packet->offset = offset;
    packet->limit = limit;
    packet->key_start.set_data(merge_skey.get_data(), merge_skey.get_size());
    packet->key_start.set_prefix_size(merge_skey.get_prefix_size());
    packet->key_end.set_data(merge_ekey.get_data(), merge_ekey.get_size());
    packet->key_end.set_prefix_size(merge_ekey.get_prefix_size());

    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_get_range *resp = 0;

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    if((ret = send_request(server_list[0],packet,cwo->get_id())) < 0){
      delete packet;
      this_wait_object_manager->destroy_wait_object(cwo);
      TBSYS_LOG(ERROR, "get_range failure: %s %d",get_error_msg(ret), ret);
      return ret;
    }

    if((ret = get_response(cwo,1,tpacket)) < 0){
      this_wait_object_manager->destroy_wait_object(cwo);
      TBSYS_LOG(ERROR, "get_range get_response failure: %s %d ",get_error_msg(ret), ret);
      return ret;
    }

    if(tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_RANGE_PACKET){
      TBSYS_LOG(ERROR, "get_range response packet error. pcode: %d", tpacket->getPCode());
      ret = TAIR_RETURN_FAILED;
    }
    else {
      resp = (response_get_range*)tpacket;
      new_config_version = resp->config_version;
      ret = resp->get_code();
      if (ret != TAIR_RETURN_SUCCESS){
        if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
          //update server table immediately
          send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
      }
      else {
        for(size_t i = 0; i < resp->key_data_vector->size(); i++) { 
          data_entry *data = new data_entry(*((*resp->key_data_vector)[i])); 
          values.push_back(data); 
        }
        if (resp->get_hasnext()){
          ret = TAIR_HAS_MORE_DATA;
        }
      }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
  }

  int tair_client_impl::prefix_removes(int area, const data_entry &pkey,
      const tair_dataentry_set &skey_set, key_code_map_t &key_code_map)
  {
    //~ duplicate logic from prefix_hides
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT) {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    request_prefix_removes *req = new request_prefix_removes();
    req->area = area;
    response_mreturn *resp = NULL;
    int ret = TAIR_RETURN_SUCCESS;

    do {
      tair_dataentry_set::const_iterator itr = skey_set.begin();
      data_entry *mkey = NULL;
      while (itr != skey_set.end()) {
        data_entry *skey = *itr;
        mkey = new data_entry();
        merge_key(pkey, *skey, *mkey);
        if (!key_entry_check(*mkey)) {
          delete mkey;
          ret = TAIR_RETURN_ITEMSIZE_ERROR;
          break;
        }
        req->add_key(mkey);
        ++itr;
      }
      if (ret != TAIR_RETURN_SUCCESS) {
        delete req;
        break;
      }

      vector<uint64_t> server_list;
      if (!get_server_id(*mkey, server_list)) {
        TBSYS_LOG(DEBUG, "no dataserver available");
        delete req;
        ret = TAIR_RETURN_FAILED;
        break;
      }

      wait_object *cwo = this_wait_object_manager->create_wait_object();
      ret = do_request(req, resp, cwo, server_list[0]);
      if (resp == NULL) {
        this_wait_object_manager->destroy_wait_object(cwo);
        break;
      }
      if (ret != TAIR_RETURN_SUCCESS && resp->key_code_map != NULL) {
        key_code_map.clear();
        resp->key_code_map->swap(key_code_map);
      }
      this_wait_object_manager->destroy_wait_object(cwo);
    } while (false);
    return ret;
  }

  int tair_client_impl::init_request_map(int area, const vector<data_entry *>& keys, request_remove_map &request_removes)
  {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT)
    {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;
    request_remove_map::iterator rq_iter;

    vector<data_entry *>::const_iterator key_iter;
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

  int tair_client_impl::mdelete(int area, const vector<data_entry*> &keys)
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
      }

      TBSYS_LOG(DEBUG,"mdelete keys size: %d, send success: %u, return packet size: %u",
              keys.size(), send_success, tpk.size());
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

  int tair_client_impl::op_cmd_to_cs(ServerCmdType cmd, std::vector<std::string>* params,
                                     std::vector<std::string>* ret_values)
  {
    if (config_server_list.empty() || config_server_list[0] == 0L) {
      log_error("no configserver available");
      return TAIR_RETURN_FAILED;
    }

    request_op_cmd *req = new request_op_cmd();
    req->cmd = cmd;
    if (params != NULL) {
      req->params = *params;
    }

    response_op_cmd *resp = NULL;
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    uint64_t master = config_server_list[0];

    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = NULL;
    do {
      if ((ret = send_request(master, req, cwo->get_id())) != TAIR_RETURN_SUCCESS) {
        delete req;
        break;
      }
      if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        break;
      }
      resp = dynamic_cast<response_op_cmd*>(tpacket);
      if (resp == NULL) {
        ret = TAIR_RETURN_FAILED;
        break;
      }
      ret = resp->code;
      if (ret_values != NULL) {
        *ret_values = resp->infos;
      }
    } while (false);

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
  }

  int tair_client_impl::op_cmd_to_ds(ServerCmdType cmd, std::vector<std::string>* params, const char* dest_server_addr)
  {
    std::map<uint64_t, request_op_cmd*> request_map;
    std::map<uint64_t, request_op_cmd*>::iterator it;

    if (dest_server_addr != NULL) {       // specify server_id
      uint64_t dest_server_id = tbsys::CNetUtil::strToAddr(dest_server_addr, 0);
      if (dest_server_id == 0) {
        log_error("invalid dest_server_addr: %s", dest_server_addr);
      } else {
        request_op_cmd *packet = new request_op_cmd();
        packet->cmd = cmd;
        if (params != NULL) {
          packet->params = *params;
        }
        request_map[dest_server_id] = packet;
      }
    } else {                    // send request to all server
      for (uint32_t i=0; i<my_server_list.size(); i++) {
        uint64_t server_id = my_server_list[i];
        if (server_id == 0) {
          continue;
        }
        it = request_map.find(server_id);
        if (it == request_map.end()) {
          request_op_cmd *packet = new request_op_cmd();
          packet->cmd = cmd;
          if (params != NULL) {
            packet->params = *params;
          }
          request_map[server_id] = packet;
        }
      }
    }

    if (request_map.empty()) {
      log_error("no request");
      return TAIR_RETURN_FAILED;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    int send_count = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    it = request_map.begin();
    while (it != request_map.end()) {
      uint64_t server_id = it->first;
      request_op_cmd *packet = it->second;
      TBSYS_LOG(INFO, "request_op_cmd %d=>%s", cmd, tbsys::CNetUtil::addrToString(server_id).c_str());

      if ((ret = send_request(server_id,packet,cwo->get_id())) != TAIR_RETURN_SUCCESS) {
        log_error("send request_op_cmd request fail: %s", tbsys::CNetUtil::addrToString(server_id).c_str());
        request_map.erase(it++);
        delete packet;
      } else {
        ++send_count;
        it++;
      }
    }

    vector<base_packet*> tpk;
    if ((ret = get_response(cwo, send_count, tpk)) < 1) {
      this_wait_object_manager->destroy_wait_object(cwo);
      TBSYS_LOG(ERROR, "all requests are failed, ret: %d", ret);
      return ret == 0 ? TAIR_RETURN_FAILED : ret;
    }

    int fail_request = 0;
    vector<base_packet*>::iterator bp_iter = tpk.begin();
    for (; bp_iter != tpk.end(); ++bp_iter)
    {
      if ((*bp_iter)->getPCode() == TAIR_RESP_RETURN_PACKET)
      {
        response_return* tpacket = dynamic_cast<response_return*>(*bp_iter);
        if (tpacket != NULL)
        {
          ret = tpacket->get_code();
          if (TAIR_RETURN_SUCCESS != ret)
          {
            if (TAIR_RETURN_SERVER_CAN_NOT_WORK == ret)
            {
              new_config_version = tpacket->config_version;
              send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
            log_error("get response fail: ret: %d", ret);
            ++fail_request;
          }
        }
      }
      else
      {
        log_error("not get response packet: %d", (*bp_iter)->getPCode());
        ++fail_request;
      }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return fail_request > 0 ? TAIR_RETURN_PARTIAL_SUCCESS : TAIR_RETURN_SUCCESS;
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
    temp[TAIR_RETURN_ITEMSIZE_ERROR]  = "key or value too large";
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
    temp[TAIR_RETURN_DEC_BOUNDS] = "can't dec to negative when allow_count_negative=0";
    temp[TAIR_RETURN_DEC_ZERO] = "can't dec zero number when allow_count_negative=0";
    temp[TAIR_RETURN_DEC_NOTFOUND] = "dec but not found when allow_count_negative=0";
    temp[TAIR_RETURN_LOCK_EXIST]                 = "lock exist";
    temp[TAIR_RETURN_LOCK_NOT_EXIST]                 = "lock not exist";
    temp[TAIR_RETURN_HIDDEN] = "item is hidden";
    temp[TAIR_RETURN_SHOULD_PROXY] = "the key should be proxy";
    //temp[TAIR_RETURN_NO_INVALID_SERVER]       = "invlaid but not found invalid server";
    return temp;
  }

  const char *tair_client_impl::get_error_msg(int ret)
  {
    std::map<int,string>::const_iterator it = tair_client_impl::m_errmsg.find(ret);
    return it != tair_client_impl::m_errmsg.end() ? it->second.c_str() : "unknown";
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

  bool tair_client_impl::get_group_name_list(uint64_t id1, uint64_t id2, std::vector<std::string> &groupnames)
  {
    if (id1 == 0 && id2 == 0) {
      return false;
    }
    if (initialize() == false) {
      return false;
    }
    start_tbnet();
    inited = true;

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_group_names *req = new request_group_names();
    bool ret = false;
    do {
      if (id1 != 0) {
        if (connmgr->sendPacket(id1, req, NULL, (void*)(long)cwo->get_id()) == false) {
          TBSYS_LOG(ERROR, "send request_group_names packet to %s failed.",
              tbsys::CNetUtil::addrToString(id1).c_str());
        } else {
          ret = true;
        }
      }
      if (!ret && id2 != 0) {
        if (connmgr->sendPacket(id2, req, NULL, (void*)(long)cwo->get_id()) == false) {
          TBSYS_LOG(ERROR, "send request_group_names packet to %s failed.",
              tbsys::CNetUtil::addrToString(id2).c_str());
        } else {
          ret = true;
        }
      }
      if (!ret) {
        delete req;
        break;
      }
      base_packet *tpacket = NULL;
      response_group_names * resp = NULL;
      if (get_response(cwo, 1, tpacket) == TAIR_RETURN_TIMEOUT) {
        TBSYS_LOG(ERROR, "get reponse_group_names packet from timeout.");
        ret = false;
        break;
      }
      if (tpacket != NULL && tpacket->getPCode() == TAIR_RESP_GROUP_NAMES_PACKET) {
        resp = dynamic_cast<response_group_names*>(tpacket);
        if (resp != NULL) {
          groupnames = resp->group_name_list;
          ret = true;
        }
      } else {
        ret = false;
        break;
      }
    } while (false);

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;

  }

  void tair_client_impl::get_servers(std::set<uint64_t> &servers)
  {
    set<uint64_t> tmp(my_server_list.begin(), my_server_list.end());
    servers.swap(tmp);
  }

  int tair_client_impl::get_flow(uint64_t addr, int ns, tair::stat::Flowrate &rate)
  {
    int ret = TAIR_RETURN_SEND_FAILED;

    flow_view_request *request = new flow_view_request();
    request->setNamespace(ns);
    flow_view_response *response = NULL;

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    do {
      ret = send_request(addr, request, cwo->get_id());
      if (ret != 0) {
        delete request;
        break;
      }
      base_packet *temp = NULL;
      ret = get_response(cwo, 1, temp);
      if (ret < 0 || temp == 0) {
        break;
      }
      if(temp->getPCode() != TAIR_RESP_FLOW_VIEW) {
        ret = TAIR_RETURN_FAILED;
        break;
      }

      response = dynamic_cast<flow_view_response *>(temp);
      rate = response->getFlowrate();
    } while (false);
    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
  }

  int tair_client_impl::set_flow_limit_bound(uint64_t addr, int &ns, int &lower, int &upper, tair::stat::FlowType &type)
  {
    int ret = TAIR_RETURN_SEND_FAILED;

    flow_control_set *request = new flow_control_set();
    request->setNamespace(ns);
    request->setLimit(lower, upper);
    request->setType(type);
    flow_control_set *response = NULL;

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    do {
      ret = send_request(addr, request, cwo->get_id());
      if(ret != 0) {
        delete request;
        break;
      }
      base_packet *temp = NULL;
      ret = get_response(cwo, 1, temp);
      if (ret < 0 || temp == 0) {
        break;
      }
      if(temp->getPCode() != TAIR_FLOW_CONTROL_SET) {
        ret = TAIR_RETURN_FAILED;
        break;
      }

      response = dynamic_cast<flow_control_set *>(temp);
      ns = response->getNamespace();
      lower = response->getLower();
      upper = response->getUpper();
      type = response->getType();
      if (response->isSuccess() == false)
        ret = TAIR_RETURN_FAILED;
    } while (false);
    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
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
          for (uint32_t i=0; server_list != NULL && i<(uint32_t)rggp->server_list_count
              && i<my_server_list.size(); i++) {
            TBSYS_LOG(DEBUG, "update server table: [%d] => [%s]", i, tbsys::CNetUtil::addrToString(server_list[i]).c_str());
            my_server_list[i] = server_list[i];
          }
          parse_invalidate_server(rggp);
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
    int type= static_cast<int> ((reinterpret_cast<long>(arg)));
    if(1==type)
    {
      //start the send thread.
      return do_queue_response();
    }

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
    int heart_type=0;
    int response_type=1;

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

    if (server_list_count != bucket_count * copy_count)
    {
      TBSYS_LOG(ERROR, "server table is wrong, server_list_count: %u, bucket_count: %u, copy_count: %u",
          server_list_count, bucket_count, copy_count);
      goto OUT;
    }

    for (uint32_t i=0; server_list != 0 && i< server_list_count; ++i) {
      TBSYS_LOG(DEBUG, "server table: [%d] => [%s]", i, tbsys::CNetUtil::addrToString(server_list[i]).c_str());
      my_server_list.push_back(server_list[i]);
    }

    new_config_version = config_version = rggp->config_version; //set the same value on the first time.

    //~ parse invalid servers
    parse_invalidate_server(rggp);

    this_wait_object_manager->destroy_wait_object(cwo);

    thread.start(this, (void *)heart_type);
    response_thread.start(this, (void *)response_type);

    return true;
OUT:
    this_wait_object_manager->destroy_wait_object(cwo);

    return false;
  }

  void tair_client_impl::parse_invalidate_server(response_get_group *resp)
  {
    tbsys::STR_STR_MAP::iterator it;
    it = resp->config_map.find("invalidate_server");
    if (it == resp->config_map.end()) {
      TBSYS_LOG(WARN, "no invalid server info found.");
      return ;
    }

    char *str = strdup(it->second.c_str());
    vector<char*> iplist;
    tbsys::CStringUtil::split(str, ";,", iplist);

    std::vector<uint64_t> tmp;
    tmp.reserve(TAIR_MAX_INVALSVR_CNT);

    for (size_t i = 0; i < TAIR_MAX_INVALSVR_CNT && i < iplist.size(); ++i) {
      uint64_t id = tbsys::CNetUtil::strToAddr(iplist[i], TAIR_INVAL_SERVER_DEFAULT_PORT);
      if (id != 0 && tbnet::ConnectionManager::isAlive(id)) {
        log_warn("got invalid server %s.", iplist[i]);
        tmp.push_back(id);
      }
    }
    free(str);
    //~ duplicate tmp, if neccessary.
    size_t size = tmp.size();
    if (size > 0) {
      size_t loop = TAIR_MAX_INVALSVR_CNT / size;
      for (size_t i = 1; i < loop; ++i) {
        for (size_t j = 0; j < size; ++j) {
          tmp.push_back(tmp[j]);
        }
      }
    }
    //~ replace invalid_server_list with tmp.
    invalid_server_list.swap(tmp);
  }

  void tair_client_impl::shrink_invalidate_server(uint64_t server_id)
  {
    //~ remove server_id from invalid_server_list.
    std::vector<uint64_t> tmp;
    tmp.reserve(TAIR_MAX_INVALSVR_CNT);
    for (size_t i = 0; i < invalid_server_list.size(); ++i) {
      if (invalid_server_list[i] != server_id) {
        tmp.push_back(invalid_server_list[i]);
      }
    }
    //~ duplicate
    size_t size = tmp.size();
    if (size > 0) {
      size_t loop = TAIR_MAX_INVALSVR_CNT / size;
      for (size_t i = 1; i < loop; ++i) {
        for (size_t j = 0; j < size; ++j) {
          tmp.push_back(tmp[j]);
        }
      }
    }
    invalid_server_list.swap(tmp);
  }

   uint32_t tair_client_impl::get_config_version() const
   {
     return config_version;
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
    if (this->direct) {
      server.push_back(this->data_server);
      return true;
    }

    uint32_t hash;
    int prefix_size = key.get_prefix_size();
    if (prefix_size == 0) {
      hash = util::string_util::mur_mur_hash(key.get_data(), key.get_size());
    } else {
      hash = util::string_util::mur_mur_hash(key.get_data(), prefix_size);
    }
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
    int ret = TAIR_RETURN_SUCCESS;
    if (server_id == 0 || packet == NULL)
    {
      TBSYS_LOG(ERROR, "param invalid. server id: %"PRI64_PREFIX"u", server_id);
      ret = TAIR_RETURN_FAILED;
    }
    else
    {
      if (connmgr->sendPacket(server_id, packet, NULL, (void*)((long)waitId)) == false)
      {
        TBSYS_LOG(ERROR, "Send RequestGetPacket to %s failure.",
            tbsys::CNetUtil::addrToString(server_id).c_str());
        send_fail_count ++;
        ret = TAIR_RETURN_SEND_FAILED;
      }
    }
    return ret;
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


  int tair_client_impl::get_response(wait_object *cwo, int wait_count, base_packet*& tpacket)

  {
    int ret = TAIR_RETURN_SUCCESS;
    if (cwo == NULL)
    {
      TBSYS_LOG(ERROR, "param invalid, cwo is null, wait count: %d", wait_count);
      ret = TAIR_RETURN_FAILED;
    }
    else
    {
      cwo->wait_done(wait_count, timeout);
      base_packet *packet = cwo->get_packet();
      if(packet == 0)
      {
        ret = TAIR_RETURN_TIMEOUT;
      }
      tpacket = packet;
    }
    return ret;
  }

  int tair_client_impl::get_response(wait_object *cwo, int wait_count, vector<base_packet*>& tpacket)
  {
    if (cwo == NULL)
    {
      TBSYS_LOG(ERROR, "param invalid, cwo is null, wait count: %d", wait_count);
      return TAIR_RETURN_FAILED;
    }
    cwo->wait_done(wait_count, timeout);
    int r_num = cwo->get_packet_count();
    int push_num = (r_num < wait_count) ? r_num: wait_count;

    for (int idx = 0 ; idx < push_num ; ++ idx)
    {
      base_packet *packet = cwo->get_packet(idx);
      if(packet == NULL)
      {
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
    if( data.get_size() >= TAIR_MAX_DATA_SIZE ){
      return false;
    }
    return true;
  }

  int tair_client_impl::invoke_callback(void * phandler,wait_object * obj)
  {
      tair_client_impl *pclient= (tair_client_impl  *) phandler;
      return pclient->push_waitobject(obj);
  }
  int tair_client_impl::push_waitobject(wait_object * obj)
  {
    //push it to queue,let another thread to send it.
    //todo check queue length;
    log_debug("push_waitobject to queue,id=%d\n",obj->get_id());
    m_return_object_queue.put(obj);
    return 0;
  }

  int tair_client_impl::handle_response_obj(wait_object * cwo)
  {
    log_debug("do_async_response id=%d\n",cwo->get_id());
    base_packet *tpacket = cwo->get_packet();
    if(tpacket == 0)
    {
      //tell the caller is failed with timeout.
      return cwo->do_async_response(TAIR_RETURN_TIMEOUT);
    }
    //now check the response code.
    int _cmd= tpacket->getPCode() ;
    int ret;
    response_return *resp =  NULL;
    switch (_cmd)
    {
      case TAIR_RESP_RETURN_PACKET:
          resp =  (response_return*)tpacket;
          new_config_version = resp->config_version;
          ret = resp->get_code();
          if (ret != TAIR_RETURN_SUCCESS)
          {
            if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER)
            {
              //update server table immediately
              send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
          }
          return cwo->do_async_response(ret);
        break;
      default:
        break;
    }
    delete tpacket;
    return 0;
  }

  void tair_client_impl::do_queue_response()
  {
    log_debug("start thread do_queue_response");
    while (!is_stop)
    {
      //TAIR_SLEEP(is_stop, 1);
      //if (is_stop) break;
      try
      {
        wait_object * cwo=m_return_object_queue.get(1000); //1s
        if(!cwo) continue;
        handle_response_obj(cwo);
        delete cwo;
      }
      catch(...)
      {
        log_warn("unknow error! get timeoutqueue error!");
      }
    }
  }
}
