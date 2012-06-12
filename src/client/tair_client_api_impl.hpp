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
#ifndef __TAIR_CLIENT_IMPL_H
#define __TAIR_CLIENT_IMPL_H

#include <string>
#include <vector>
#include <map>
#include <ext/hash_map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>

#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "tair_client_api.hpp"
#include "packet_factory.hpp"
#include "packet_streamer.hpp"
#include "wait_object.hpp"
#include "item_manager.hpp"
#include "items_packet.hpp"
#include "response_return_packet.hpp"
#include "util.hpp"
#include "dump_data_info.hpp"
#include "remove_packet.hpp"
#include "BlockQueueEx.hpp"
#include "lock_packet.hpp"
#include "get_group_packet.hpp"
#include "group_names_packet.hpp"
#include "invalid_packet.hpp"
#include "hide_packet.hpp"
#include "get_hidden_packet.hpp"
#include "prefix_removes_packet.hpp"
#include "response_mreturn_packet.hpp"
#include "prefix_hides_packet.hpp"
#include "flowrate.h"

namespace tair {


  using namespace std;
  using namespace __gnu_cxx;
  using namespace tair::json;
  using namespace tair::common;

  const int UPDATE_SERVER_TABLE_INTERVAL = 50;
  typedef void (*TAIRCALLBACKFUNC)(int retcode, void* parg);

  typedef map<uint64_t , request_get *> request_get_map;
  typedef map<uint64_t , request_remove *> request_remove_map;

  class tair_client_impl : public tbsys::Runnable, public tbnet::IPacketHandler {
    public:
      tair_client_impl();

      ~tair_client_impl();

      bool startup(const char *master_addr,const char *slave_addr,const char *group_name);
      bool directup(const char *server_addr);
      bool startup(uint64_t data_server);

      void close();
      bool isinited(){return inited;}

    public:

      int put(int area,
          const data_entry &key,
          const data_entry &data,
          int expire,
          int version,
          TAIRCALLBACKFUNC pfunc=NULL,void * arg=NULL);

      //the caller will release the memory
      int get(int area,
          const data_entry &key,
          data_entry*& data);

      int mget(int area,
          const vector<data_entry *> &keys,
          tair_keyvalue_map &data);

      int remove(int area,
          const data_entry &key,
          TAIRCALLBACKFUNC pfunc=NULL,void * arg=NULL);

      int invalidate(int area, const data_entry &key, const char *groupname);

      int invalidate(int area, const data_entry &key);

      int hide(int area, const data_entry &key);

      int get_hidden(int area, const data_entry &key, data_entry *&value);

      int prefix_get(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value);

      int prefix_put(int area, const data_entry &pkey, const data_entry &skey,
          const data_entry &value, int expire, int version);

      int prefix_hide(int area, const data_entry &pkey, const data_entry &skey);

      int prefix_hides(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map);

      int prefix_get_hidden(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value);

      int prefix_remove(int area, const data_entry &pkey, const data_entry &skey);

      int prefix_removes(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map);

      int hides(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map);

      int removes(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map);

      int mdelete(int area,
          const vector<data_entry*> &keys);

      int add_count(int area,
          const data_entry &key,
          int count,
          int *retCount,
          int init_value = 0,
          int expire_time = 0);

      int set_count(int area, const data_entry& key, int count,
          int expire, int version,
          TAIRCALLBACKFUNC pfunc=NULL,void * arg=NULL);

      int lock(int area, const data_entry& key, LockType type,
          TAIRCALLBACKFUNC pfunc=NULL,void * arg=NULL);

      int set_flow_limit_bound(uint64_t addr, int &ns, 
                                int &lower, int &upper, 
                                tair::stat::FlowType &type);

      int get_flow(uint64_t addr, int ns, 
                   tair::stat::Flowrate &rate);

      template< typename IT >
        int add_items(int area,
            const data_entry &key,
            const IT& start,
            const IT& end,
            int version,
            int expired,
            int max_count = 500,
            int type  = ELEMENT_TYPE_INVALID);

      template<typename Type,template <typename T,typename = std::allocator<Type> > class CONT >
        int get_items(int area,
            const data_entry &key,
            int offset,
            int count,
            CONT<Type> &result,
            int type = ELEMENT_TYPE_INVALID);

      int remove_items(int area,
          const data_entry &value,
          int offset,
          int count);

      template<typename Type,template <typename T,typename = std::allocator<Type> > class CONT >
        int get_and_remove(int area,
            const data_entry &key,
            int offset,
            int count,
            CONT<Type>& result,
            int type = ELEMENT_TYPE_INVALID);

      int get_items_count(int area,const data_entry& key);

      void set_timeout(int this_timeout);
      void set_randread(bool rand_flag);

      uint32_t get_bucket_count() const { return bucket_count;}
      uint32_t get_copy_count() const { return copy_count;}

      template<typename Type>
        int find_elements_type(Type element);

      static const std::map<int,string> m_errmsg;
      static std::map<int,string> init_errmsg();
      const char *get_error_msg(int ret);
      void get_server_with_key(const data_entry& key,std::vector<std::string>& servers);
      bool get_group_name_list(uint64_t id1, uint64_t id2, std::vector<std::string> &groupnames);
      void get_servers(std::set<uint64_t> &servers);

      //@override IPacketHandler
      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet *packet, void *args);

      //@override Runnable
      void run(tbsys::CThread *thread, void *arg);



      int remove_area(int area);
      //    int getStatInfo(int type, int area, vector<ResponseStatPacket *> &list);
      int dump_area(std::set<dump_meta_info>& info);

      void force_change_dataserver_status(uint64_t server_id, int cmd);
      void get_migrate_status(uint64_t server_id,vector<pair<uint64_t,uint32_t> >& result);
      void query_from_configserver(uint32_t query_type, const string group_name, map<string, string>&, uint64_t server_id = 0);
      uint32_t get_config_version() const;
      int64_t ping(uint64_t server_id);

#if  0     /* ----- #if 0 : If0Label_1 ----- */
      bool dumpKey(int area, char *file_name, int timeout = 0);

      bool dumpBucket(int db_id, char *path);


      bool ping(int timeout = 0);

      char *getGroupName();

      uint64_t getConfigServerId();

#endif     /* ----- #if 0 : If0Label_1 ----- */
    public:
      int push_waitobject(wait_object * obj);
      static int invoke_callback(void * phandler,wait_object * obj);

    private:

      bool startup(uint64_t master_cfgsvr, uint64_t slave_cfgsvr, const char *group_name);
      bool directup(uint64_t data_server);

      bool initialize();

      bool retrieve_server_addr();

      void parse_invalidate_server(response_get_group *rggp);

      void shrink_invalidate_server(uint64_t server_id);

      void start_tbnet();

      void stop_tbnet();

      void wait_tbnet();

      void reset(); //reset enviroment

      bool get_server_id(const data_entry &key, vector<uint64_t>& server);

      int send_request(uint64_t serverId,base_packet *packet,int waitId);

      //int send_request(vector<uint64_t>& server,TAIRPacket *packet,int waitId);

      int get_response(wait_object *cwo,int waitCount,base_packet*& tpacket);
      int get_response(wait_object *cwo, int wait_count, std::vector<base_packet*>& tpacket);


      bool data_entry_check(const data_entry& data);
      bool key_entry_check(const data_entry& data);

      int mget_impl(int area,
          const vector<data_entry *> &keys,
          tair_keyvalue_map &data ,
          int server_select = 0);

      int init_request_map(int area,
          const vector<data_entry *>& keys,
          request_get_map &request_gets ,
          int server_select = 0);

      int init_request_map(int area,
          const vector<data_entry *>& keys,
          request_remove_map &request_removes);

      //~ do the request operation, delete req if send failed
      template <typename Request, typename Response>
      int do_request(Request *req, Response *&resp, wait_object *cwo, uint64_t server_id) {
        if (req == NULL) {
          log_error("request is null");
          return TAIR_RETURN_SEND_FAILED;
        }
        if (cwo == NULL) {
          log_error("wait object is null");
          return TAIR_RETURN_SEND_FAILED;
        }

        int ret = TAIR_RETURN_SEND_FAILED;
        base_packet *tpacket = NULL;
        do {
          if ((ret = send_request(server_id, req, cwo->get_id())) != TAIR_RETURN_SUCCESS) {
            delete req;
            break;
          }
          if ((ret = get_response(cwo, 1, tpacket)) < 0) {
            break;
          }
          resp = dynamic_cast<Response*>(tpacket);
          if (resp == NULL) {
            ret = TAIR_RETURN_FAILED;
            break;
          }
          ret = resp->get_code();
        } while (false);
        return ret;
      }

    private:
      bool inited;
      bool is_stop;
      tbsys::CThread thread;
      tbsys::CThread response_thread; //thread to response packet.

      bool direct;
      uint64_t data_server;
      tair_packet_factory *packet_factory;
      tair_packet_streamer *streamer;
      tbnet::Transport *transport;
      tbnet::ConnectionManager *connmgr;

      int timeout; // ms

      vector<uint64_t> my_server_list;
      vector<uint64_t> config_server_list;
      vector<uint64_t> invalid_server_list;
      std::string group_name;
      uint32_t config_version;
      uint32_t new_config_version;
      int send_fail_count;
      wait_object_manager *this_wait_object_manager;
      uint32_t bucket_count;
      uint32_t copy_count;
      bool rand_read_flag;
      atomic_t read_seq;
    private:
      void do_queue_response();
      int handle_response_obj(wait_object * obj);
    private:
	    typedef BlockQueueEx<wait_object* > CAsyncCallerQueue;
      CAsyncCallerQueue  m_return_object_queue;
      pthread_rwlock_t m_init_mutex;
  };

  template< typename IT >

    int tair_client_impl::add_items(int area,
        const data_entry &key,
        const IT& start,
        const IT& end,
        int version,
        int expired,
        int max_count /*=500*/,
        int type /* = ELEMENT_TYPE_INVALID*/)

    {
      if(area < 0 || area >= TAIR_MAX_AREA_COUNT || version < 0 || expired < 0){
        return TAIR_RETURN_INVALID_ARGUMENT;
      }

      if( !key_entry_check(key)){
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }
      //1. serialize
      int elements_type = type;
      if(elements_type == ELEMENT_TYPE_INVALID){
        elements_type = find_elements_type(*start);
      }

      TBSYS_LOG(DEBUG,"start addItems,elements_type:%d",elements_type);

      uint32_t attr = 0;
      int elements_count = distance(start,end);
      if(elements_count <= 0 || max_count <= 0){
        return TAIR_RETURN_INVALID_ARGUMENT;
      }
      if(max_count > tair_client_api::MAX_ITEMS){
        max_count = tair_client_api::MAX_ITEMS;
      }
      SET_VAL_COUNT(attr,elements_count);
      SET_VAL_TYPE(attr,elements_type);

      string result(reinterpret_cast<char *>(&attr),sizeof(attr)); //just reserve space
      if( !item_manager::prepare_serialize(elements_type,start,end,result) ){
        return TAIR_RETURN_SERIALIZE_ERROR;
      }
      data_entry data(result.data(),result.size(),true);
      if (!data_entry_check(data)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      item_manager::set_attribute(data,attr); //set attribute
      //2. encode

      wait_object *cwo = this_wait_object_manager->create_wait_object();

      request_add_items *packet = new request_add_items();
      packet->max_count = max_count;
      packet->area = area;
      packet->key = key;
      packet->data = data;
      packet->expired = expired;
      packet->version = version;

      //3. send packet & get response
      vector<uint64_t> serverList;
      if ( !get_server_id(key, serverList)) {
        TBSYS_LOG(DEBUG, "can not find serverId, return false");
        return -1;
      }

      int ret = TAIR_RETURN_SEND_FAILED;
      base_packet *tpacket = 0;
      response_return *resp = 0;

      if( (ret = send_request(serverList[0],packet,cwo->get_id())) < 0){
        delete packet;
        goto FAIL;
      }
      if( (ret = get_response(cwo,1,tpacket)) < 0){
        goto FAIL;
      }

      if (tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
        goto FAIL;
      }
      resp = (response_return*)tpacket;
      new_config_version = resp->config_version;

      ret = resp->get_code();
      if(ret != 0){
        if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){ //just in case
          send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        goto FAIL;
      }
      //4. return

      this_wait_object_manager->destroy_wait_object(cwo);
      return ret;

FAIL:

      this_wait_object_manager->destroy_wait_object(cwo);

      TBSYS_LOG(ERROR, "put failure: %s : %s",
          tbsys::CNetUtil::addrToString(serverList[0]).c_str(),get_error_msg(ret));
      return ret;
    }

  template<typename Type,template <typename T,typename = std::allocator<Type> > class CONT >

    int tair_client_impl::get_items(int area,
        const data_entry &key,
        int offset,
        int count,
        CONT<Type>& result,
        int type/* = ELEMENT_TYPE_INVALID */)
    {
      if( area < 0 || area >= TAIR_MAX_AREA_COUNT || count <= 0){
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

      int elements_type = type;
      if(elements_type == ELEMENT_TYPE_INVALID){
        Type _t;
        elements_type = find_elements_type(_t);
      }

      wait_object *cwo = 0;

      base_packet *tpacket = 0;
      response_get_items *resp  = 0;
      data_entry* data = 0;

      char *start = 0;
      char *end = 0;

      int ret = TAIR_RETURN_SEND_FAILED;
      int send_success = 0;

      for(vector<uint64_t>::iterator it=server_list.begin(); it != server_list.end(); ++it){

        request_get_items *packet = new request_get_items();
        packet->area = area;
        packet->add_key(key.get_data(), key.get_size());
        packet->offset = offset;
        packet->count = count;
        packet->type = elements_type;


        cwo = this_wait_object_manager->create_wait_object();


        if( send_request(*it,packet,cwo->get_id()) < 0 ){
          delete packet;
          continue;
        }
        if(get_response(cwo,1,tpacket) < 0 ){

          this_wait_object_manager->destroy_wait_object(cwo);

          continue;
        }else{
          ++send_success;
          break;
        }
      }

      if( send_success < 1 ){
        return TAIR_RETURN_SEND_FAILED;
      }

      if (tpacket->getPCode() != TAIR_RESP_GETITEMS_PACKET) {
        TBSYS_LOG(DEBUG,"packet incorrect");
        goto FAIL;
      }

      resp = (response_get_items*)tpacket;
      data = resp->data;
      resp->data = 0;
      ret = resp->get_code();
      if(ret != TAIR_RETURN_SUCCESS){
        goto FAIL;
      }
      if( data == 0 || data->get_size() < 4 ){
        //packet incorrect.
        TBSYS_LOG(DEBUG,"packet incorrect");
        goto FAIL;
      }

      new_config_version = resp->config_version;
      this_wait_object_manager->destroy_wait_object(cwo);


      start = item_manager::get_items_addr(*data);
      end = data->get_data() + data->get_size();

      TBSYS_LOG(INFO,"get data:%s",start);

      if( !item_manager::parse_array(start,end,result)){
        // data error;
        TBSYS_LOG(DEBUG,"parse_array failed");
        ret = TAIR_RETURN_SERIALIZE_ERROR;
      }else {
        ret = TAIR_RETURN_SUCCESS;
      }
      TBSYS_LOG(DEBUG,"end getitems");
      delete data;
      return ret;
FAIL:

      if(tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET){
        response_return *r = (response_return *)tpacket;
        ret = r->get_code();
      }
      if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){
        new_config_version = resp->config_version;
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }

      this_wait_object_manager->destroy_wait_object(cwo);

      TBSYS_LOG(ERROR, "get failure: %s:%s",
          tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
          get_error_msg(ret));
      return ret;

    }

  template<typename Type,template <typename T,typename = std::allocator<Type> > class CONT >

    int tair_client_impl::get_and_remove(int area,
        const data_entry &key,
        int offset,
        int count,
        CONT<Type>& result,
        int type /*= ELEMENT_TYPE_INVALID*/)
    {
      if( area < 0 || area >= TAIR_MAX_AREA_COUNT || count < 0){
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

      int elements_type = type;
      if(elements_type == ELEMENT_TYPE_INVALID){
        Type _t;
        elements_type = find_elements_type(_t);
      }

      TBSYS_LOG(DEBUG,"get_and_remove: elements_type:%d",type);


      wait_object *cwo = this_wait_object_manager->create_wait_object();

      request_get_and_remove_items *packet = new request_get_and_remove_items();
      packet->area = area;
      packet->add_key(key.get_data(), key.get_size());
      packet->offset = offset;
      packet->count = count;
      packet->type = elements_type;

      base_packet *tpacket = 0;
      response_get_items *resp  = 0;
      data_entry* data = 0;

      int ret = TAIR_RETURN_SEND_FAILED;
      if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){ //can't retry
        delete packet;
        goto FAIL;
      }

      if( (ret = get_response(cwo,1,tpacket)) < 0){
        goto FAIL;
      }
      if ( tpacket->getPCode() != TAIR_RESP_GETITEMS_PACKET) {
        goto FAIL;
      }

      resp = (response_get_items*)tpacket;
      ret = resp->get_code();
      if(ret != TAIR_RETURN_SUCCESS){
        goto FAIL;
      }
      if (resp->data) {
        data = resp->data;
        resp->data = 0;
      }else{
        //TODO ?
        ret = TAIR_RETURN_DATA_NOT_EXIST;
      }

      new_config_version = resp->config_version;
      this_wait_object_manager->destroy_wait_object(cwo);

      TBSYS_LOG(INFO,"get_and_remove:get data:%s",item_manager::get_items_addr(*data));

      if( !item_manager::parse_array(item_manager::get_items_addr(*data),data->get_data() + data->get_size(),result)){
        // data error;
        ret = TAIR_RETURN_SERIALIZE_ERROR;
      }else {
        ret = TAIR_RETURN_SUCCESS;
      }
      delete data;
      return ret;
FAIL:
      if(tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET){
        response_return *r = (response_return *)tpacket;
        ret = r->get_code();
        if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){
          new_config_version = resp->config_version;
          send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
      }

      this_wait_object_manager->destroy_wait_object(cwo);

      TBSYS_LOG(ERROR, "get failure: %s:%s",
          tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
          get_error_msg(ret));
      return ret;

    }

  //int getItemsCount()

  template<typename Type>
    int tair_client_impl::find_elements_type(Type element)
    {
      const std::type_info& info = typeid(element);
      if(info == typeid(int) || info == typeid(short)){
        return ELEMENT_TYPE_INT;
      }else if(info == typeid(int64_t) || info == typeid(uint32_t)){
        return ELEMENT_TYPE_LLONG;
      }else if(info == typeid(double) || info == typeid(float)){
        return ELEMENT_TYPE_DOUBLE;
      }else if(info == typeid(string) || info == typeid(char *)){
        return ELEMENT_TYPE_STRING;
      }
      return ELEMENT_TYPE_STRING;
    }


} /* tair */
#endif //__TAIR_CLIENT_IMPL_H
