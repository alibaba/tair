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
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef TAIR_SERVER_CONF_THREAD_H
#define TAIR_SERVER_CONF_THREAD_H
#include <algorithm>
#include <tbsys.h>
#include <tbnet.h>
#include <utime.h>
#include "mmap_file.hpp"
#include "server_info.hpp"
#include "group_info.hpp"
#include "get_group_packet.hpp"
#include "heartbeat_packet.hpp"
#include "query_info_packet.hpp"
#include "get_server_table_packet.hpp"
#include "set_master_packet.hpp"
#include "conf_heartbeat_packet.hpp"
#include "migrate_finish_packet.hpp"
#include "group_names_packet.hpp"
#include "data_server_ctrl_packet.hpp"
#include "get_migrate_machine.hpp"
#include "wait_object.hpp"

namespace tair {
  namespace config_server {
    class server_conf_thread:public tbsys::CDefaultRunnable,
      public tbnet::IPacketHandler {
    public:
      server_conf_thread();

      ~server_conf_thread();

      void set_thread_parameter(tbnet::Transport * transport,
                                tbnet::Transport *,
                                tbnet::DefaultPacketStreamer * streamer);

      // IPacketHandler interface
        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet * packet,
                                                      void *args);

      // Runnable interface
      void run(tbsys::CThread * thread, void *arg);

      void find_group_host(request_get_group * req,
                           response_get_group * resp);

      void do_heartbeat_packet(request_heartbeat * req,
                               response_heartbeat * resp);

      void do_query_info_packet(request_query_info *, response_query_info *);

      void do_get_server_table_packet(request_get_server_table * req,
                                      response_get_server_table * resp);

      bool do_set_master_packet(request_set_master * req);

      uint64_t get_slave_server_id();

      void do_conf_heartbeat_packet(request_conf_heartbeart * req);

      bool do_set_server_table_packet(response_get_server_table * packet);

      //return vaule -1 response it as error 0 response it ok 1 noresponse
      int do_finish_migrate_packet(request_migrate_finish * packet);

      void do_group_names_packet(response_group_names * resp);

      group_info_map *get_group_info_map();

      server_info_map *get_server_info_map();

      void set_stat_interval_time(int stat_interval_time);

      void force_change_server_status(request_data_server_ctrl * packet);

      void get_migrating_machines(request_get_migrate_machine * req,
                                  response_get_migrate_machine * resp);

    private:
      enum
      {
        GROUP_DATA = 0,
        GROUP_CONF
      };
        server_conf_thread(const server_conf_thread &);
        server_conf_thread & operator =(const server_conf_thread &);

      void load_group_file(const char *file_name, uint32_t version,
                           uint64_t sync_server_id);
      uint32_t get_file_time(const char *file_name);
      void check_server_status(uint32_t loop_count);
      void check_config_server_status(uint32_t loop_count);
      void load_config_server();
      uint64_t get_master_config_server(uint64_t id, int value);
      void get_server_table(uint64_t sync_server_id, const char *group_name,
                            int type);
      bool backup_and_write_file(const char *file_name, const char *data,
                                 int size, int modified_time);
      void read_group_file_to_packet(response_get_server_table * resp);
      void send_group_file_packet();

    private:
      group_info_map group_info_map_data;
      server_info_map data_server_info_map;
      server_info_map config_server_info_map;
      vector<server_info *>config_server_info_list;
      tbsys::CThreadMutex mutex_grp_need_build;
      tbsys::CRWSimpleLock group_info_rw_locker;
      tbsys::CRWSimpleLock server_info_rw_locker;
      int stat_interval_time;
      uint64_t master_config_server_id;
      uint64_t down_slave_config_server;

      // communication
      tbnet::ConnectionManager * connmgr;
      tbnet::ConnectionManager * connmgr_heartbeat;
      tair::common::wait_object_manager my_wait_object_manager;
      int heartbeat_curr_time;
      bool is_ready;

      static const uint32_t server_up_inc_step = 100;
    private:
        class table_builder_thread:public tbsys::CDefaultRunnable
      {
      public:
        explicit table_builder_thread(server_conf_thread *);
        void run(tbsys::CThread * thread, void *arg);
         ~table_builder_thread();
          public:vector<group_info *>group_need_build;
      private:
          server_conf_thread * p_server_conf;
        void build_table(vector<group_info *>&groupe_will_builded);
      };
      friend class table_builder_thread;
      table_builder_thread builder_thread;
    };
  }
}

#endif
