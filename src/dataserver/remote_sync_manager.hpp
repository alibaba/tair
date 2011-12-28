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

namespace tair {

#define MAX_SYNC_SENDED_MAP_SIZE 100000
   class remote_sync_manager;
   class async_call_node
   {
     public:
        async_call_node(uint64_t  _pkg_id,remote_sync_manager * _pmanager)
        {
          pkg_id=_pkg_id;
          pmanager=_pmanager;
        }
     public:
       uint64_t  pkg_id;
       remote_sync_manager *pmanager;
   };

   class remote_sender_manager :public tbsys::CDefaultRunnable
  {
    remote_sender_manager();
    private:
      remote_sync_manager * m_sync_manager; 
    public:
      void run(tbsys::CThread *thread, void *arg);
  };

   class remote_sync_manager :public tbsys::CDefaultRunnable
  {
    public:
      remote_sync_manager(tair_manager *tair_mgr,const char *base_home);
      ~remote_sync_manager();
    public:
      bool init();
      int  sync_put_data(int area,const data_entry& key, const data_entry& value,int expired, int version);
      int  sync_remove_data(int area,const data_entry& key);
    public:
      void run(tbsys::CThread *thread, void *arg);
    private:
      void handle_tosend_queue();
      void handle_slow_queue();
      void handle_monitor_init();
      int  do_send_packet(request_sync * pkg);
      void do_sync_response(int error_code, int pkg_id);
      static void callback_sync_response(int error_code, void * arg);
      bool init_local_client();
      int get_sended_size();
    private:
      tair_manager *tair_mgr;
      remote_sender_manager * send_mgr;
      atomic_t packet_id_creater;
      tair_client_impl  m_remote_tairclient;
      tair_client_impl  m_local_tairclient;
      char  m_base_home[NAME_MAX];
      char  m_data_dir[NAME_MAX];
      bool m_b_use_disk;
      bool m_inited;
      common::RecordLogFile<request_sync> m_sync_log_file;
    private:
	 typedef BlockQueueEx<request_sync* > CDupNodeQueue;
     typedef __gnu_cxx::hash_map<uint32_t,request_sync*> CDupNodePkgMap;
     typedef __gnu_cxx::hash_map<uint32_t,request_sync*>::iterator CDupNodePkgMapIter;
     CSlotLocks *m_slots_locks;
     sync_packet_list m_tosend_queue;
     sync_packet_list m_slow_queue;
     CDupNodePkgMap m_sended_map;
     CDupNodeQueue  m_timout_queue;

  };
}
#endif
