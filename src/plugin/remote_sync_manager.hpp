/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Authors:
 *   xinshu<xinshu.wzx@taobao.com>
 *
 */
#ifndef REMOTE_SYNC_MANAGER_H
#define REMOTE_SYNC_MANAGER_H


#include <tbsys.h>
#include <tbnet.h>

#include "BlockQueueEx.hpp"
#include "tair_client_api_impl.hpp"
#include "scoped_wrlock.hpp"
#include "sync_packet.hpp"
#include "sync_packet_list.hpp"
#include "tair_manager.hpp"

#include <queue>
#include <map>
#include <ext/hash_map>
#include <ext/hash_fun.h>
#include "base_plugin.hpp"

namespace tair {

#define MAX_IP_PORT_LEN 22
#define MAX_GROUP_NAME 64
#define MAX_DUP_THREAD 7

  struct sync_conf
  {
    char master_ip[MAX_IP_PORT_LEN];
    char slave_ip[MAX_IP_PORT_LEN];
    char group_name[MAX_GROUP_NAME];
    char remote_data_dir[NAME_MAX];
    bool split(const char *_conf_str)
    {
      if(4==sscanf(_conf_str,"%[^,],%[^,],%[^,],%s",master_ip,slave_ip,group_name,remote_data_dir))
      {
        if(strcmp(slave_ip,"nop")==0) slave_ip[0]='\0'; //slave_ip is not used.use nop.
        return true;
      }
      return false;
    }
  };

   class remote_sync_manager;
   class async_call_node
   {
     public:
       async_call_node(uint64_t  _pkg_id,data_entry* _key,remote_sync_manager * _pmanager)
       {
         pkg_id=_pkg_id;
         failed=0;
         pkey=_key;
         pmanager=_pmanager;
       }
     public:
       uint64_t  pkg_id;
       uint8_t  failed;
       remote_sync_manager *pmanager;
       data_entry* pkey;
   };

   class remote_sync_manager :public plugin::base_plugin,public tbsys::CDefaultRunnable
  {
    public:
      remote_sync_manager();
      ~remote_sync_manager();
    public:
      int get_hook_point();
      int get_property();
      int get_plugin_type();
      void do_response(int rev,int call_type,int area,const data_entry* key,const data_entry* value, void* exv = NULL);
      bool init();
      bool init(const string& para);
      void clean();
    public:
      void run(tbsys::CThread *thread, void *arg);
    private:
      void handle_tosend_queue(uint16_t index);
      void handle_slow_queue();
      int  do_send_packet(data_entry * pkg);
      int do_direct_send_packet(int call_type,int area,const data_entry* key,const data_entry* value);
      void do_sync_response(int error_code,async_call_node *prsp);
      static void callback_sync_response(int error_code, void * arg);
      bool init_tair_client(tair_client_impl&  _client,struct sync_conf &_conf);
      int get_sended_size();
      int do_invalid_key(data_entry *& pkey);
      bool parse_para(const char* para,struct sync_conf &_remote,struct sync_conf &_local);
    private:
      atomic_t packet_id_creater;
      struct sync_conf  _remote_conf,_local_conf; //para for tairclient.
      tair_client_impl  m_remote_tairclient;
      tair_client_impl  m_local_tairclient;
      char  m_base_home[NAME_MAX];
      char  m_data_dir[NAME_MAX];
      bool m_inited;
      common::RecordLogFile<request_sync> m_sync_log_file;
    private:
      CSlotLocks *m_slots_locks;
      sync_packet_list m_tosend_queue;
      BlockQueueEx< data_entry*>  m_slow_queue;
  };
}
#endif
