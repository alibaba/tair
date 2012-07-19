/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * heartbeat thread with configserver
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_HEART_BEAT_THREAD_H
#define TAIR_HEART_BEAT_THREAD_H

#include <pthread.h>
#include <tbsys.h>
#include <tbnet.h>
#include "tair_manager.hpp"
#include "stat_helper.hpp"
#include "heartbeat_packet.hpp"

namespace tair {
   class heartbeat_thread : public tbsys::CDefaultRunnable, public tbnet::IPacketHandler {
   public:
      heartbeat_thread();

      ~heartbeat_thread();

      void run(tbsys::CThread *thread, void *arg);

      void set_thread_parameter(tbnet::IServerAdapter *adaptor, tair_packet_streamer *streamer, tair_manager *tair_mgr);

      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet *packet, void *args);

      uint32_t get_client_version();
      uint32_t get_server_version();
      uint32_t get_plugins_version();
      vector<uint64_t> get_config_servers();

   public:
      uint64_t request_count;
      uint32_t curr_load;

   private:
      request_heartbeat heartbeat_packet;
      tbnet::ConnectionManager *conn_mgr;
      tair_manager *tair_mgr;
      tbnet::Transport transport;
      vector<uint64_t> config_server_list;
      bool have_config_server_down;
      uint32_t client_version;
      uint32_t server_version;
      uint32_t plugins_version;
   };

   class server_table_updater : public tbsys::CDefaultRunnable {
   public:
      server_table_updater(tair_manager *tair_mgr, uint64_t *server_list, int server_list_count, uint32_t server_version, int32_t data_need_move, vector<uint64_t> current_state_table, uint32_t copy_count, uint32_t bucket_count)
      {
         this->tair_mgr = tair_mgr;
         this->server_list_count = server_list_count;
         this->server_list = new uint64_t[server_list_count];
         this->server_version = server_version;
         this->data_need_move = data_need_move;
         this->current_state_table = current_state_table;
         memcpy(this->server_list, server_list, server_list_count * sizeof(uint64_t));
         this->copy_count = copy_count;
         this->bucket_count = bucket_count;
      }

      ~server_table_updater()
      {
         delete [] server_list;
      }

      void run(tbsys::CThread *thread, void *arg)
      {
         pthread_detach(pthread_self());
         if (server_list_count > 0 && tair_mgr != NULL) {
            tair_mgr->update_server_table(server_list, server_list_count, server_version,
                                          data_need_move, current_state_table,
                                          copy_count, bucket_count);
         }
         delete this;
      }

   private:
      tair_manager *tair_mgr;
      int server_list_count;
      uint32_t server_version;
      uint64_t *server_list;
      int32_t  data_need_move;
      vector<uint64_t> current_state_table;
      uint32_t copy_count;
      uint32_t bucket_count;
   };
   class plugins_updater : public tbsys::CDefaultRunnable {
   public:
      plugins_updater(tair_manager* tair_mgr, const std::vector<std::string>& plugins_dll_names_list)
         : tair_mgr(tair_mgr)
      {
         for (std::vector<std::string>::const_iterator it = plugins_dll_names_list.begin();
              it != plugins_dll_names_list.end(); ++it) {
            this->plugins_dll_names_list.insert(*it);
         }
      }
      ~plugins_updater()
      {
      }
      void run(tbsys::CThread *thread, void *arg)
      {
         pthread_detach(pthread_self());
         if (tair_mgr->plugins_manager.chang_plugins_to(plugins_dll_names_list)){
            log_info("change plugins ok");
         }else {
            log_info("change plugins error");
         }
         delete this;

      }
   private:
      tair_manager* tair_mgr;
      std::set<std::string>  plugins_dll_names_list;
   };

}

#endif
