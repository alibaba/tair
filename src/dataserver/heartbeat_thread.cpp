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
#include "heartbeat_thread.hpp"

namespace tair {

   heartbeat_thread::heartbeat_thread()
   {
      conn_mgr = NULL;
      tair_mgr = NULL;
      client_version = 0U;
      server_version = 0U;
      plugins_version = 0U;
      heartbeat_packet.set_no_free();
      heartbeat_packet.server_flag = 1; // heartbeat will always with server flag
      request_count = 0;
      have_config_server_down = false;


   }

   heartbeat_thread::~heartbeat_thread()
   {
      transport.stop();
      transport.wait();
      if (conn_mgr) {
         delete conn_mgr;
         conn_mgr = NULL;
      }
   }

   void heartbeat_thread::set_thread_parameter(tbnet::IServerAdapter *adaptor, tair_packet_streamer *streamer, tair_manager *tair_mgr)
   {
      heartbeat_packet.server_id = local_server_ip::ip;
      // init heartbeat
      int port = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_HEARTBEAT_PORT, TAIR_SERVER_DEFAULT_HB_PORT);
      char spec[32];
      sprintf(spec, "tcp::%d", port);
      if (transport.listen(spec, streamer, adaptor) == NULL) {
         log_error("heartbeat transport listen on %d failed", port);
         exit(1);
      } else {
         log_info("heartbeat transport listen on %d successed", port);
      }
      transport.start();

      conn_mgr = new tbnet::ConnectionManager(&transport, streamer, this);
      this->tair_mgr = tair_mgr;
   }

   vector<uint64_t> heartbeat_thread::get_config_servers()
   {
      return config_server_list;
   }

   void heartbeat_thread::run(tbsys::CThread *thread, void *arg)
   {
      if (conn_mgr == NULL) {
         return;
      }
      vector<const char*> str_list = TBSYS_CONFIG.getStringList(TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
      if (str_list.size() == 0U) {
         log_warn("miss config item %s:%s", TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
         return;
      }
      int port = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
      uint64_t id;
      for (uint32_t i=0; i<str_list.size(); i++) {
         id = tbsys::CNetUtil::strToAddr(str_list[i], port);
         if (id == 0) continue;
         config_server_list.push_back(id);
         if (config_server_list.size() == 2U) break;
      }
      if (config_server_list.size() == 0U) {
         return;
      }

      string str_msg;
      double d;

      tzset();
      int log_rotate_time = (time(NULL)-timezone) % 86400;
      while (!_stop) {
         if (getloadavg(&d, 1) == 1) curr_load = (int)(d * 100);

         heartbeat_packet.loop_count ++;
         heartbeat_packet.config_version = server_version;
         heartbeat_packet.plugins_version = plugins_version;
         // set the proxying buckets to heartbeat packet
         heartbeat_packet.vec_bucket_no.clear();
         tair_mgr->get_proxying_buckets(heartbeat_packet.vec_bucket_no);
         heartbeat_packet.pull_migrated_info = tair_mgr->is_working() ? 0 : 1;

         int stat_size = TAIR_STAT.get_size();
         const char *stat_data = TAIR_STAT.get_and_reset();
         heartbeat_packet.set_stat_info(stat_data, stat_size);

         for (uint32_t i=0; i<config_server_list.size(); i++) {
            //if ((config_server_list[i] & TAIR_FLAG_CFG_DOWN)) continue;
            request_heartbeat *new_packet = new request_heartbeat(heartbeat_packet);
            if (conn_mgr->sendPacket(config_server_list[i], new_packet, NULL, (void*)((long)i)) == false) {
               delete new_packet;
            }
         }

         // rotate log
         log_rotate_time ++;
         if (log_rotate_time % 86400 == 86340) {
            log_info("rotateLog End");
            TBSYS_LOGGER.rotateLog(NULL, "%d");
            log_info("rotateLog start");
         }
         if (log_rotate_time % 3600 == 3000) {
            log_rotate_time = (time(NULL)-timezone) % 86400;
         }

         TAIR_SLEEP(_stop, 1);
      }
   }

   tbnet::IPacketHandler::HPRetCode heartbeat_thread::handlePacket(tbnet::Packet *packet, void *args)
   {
      if (!packet->isRegularPacket()) {
         tbnet::ControlPacket *cp = (tbnet::ControlPacket*)packet;
         log_error("ControlPacket, cmd:%d", cp->getCommand());
         return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      if (_stop) {
         log_warn("thread is stop, but receive packet. pcode :%d", packet->getPCode());
      } else {
        int pcode = packet->getPCode();
        int server_index = (int)((long)args);

        switch (pcode) {
          case TAIR_RESP_HEARTBEAT_PACKET: {
            response_heartbeat *resp = (response_heartbeat*) packet;
            bool need_set = true;
            if (server_index == 1 && config_server_list.size() == 2U &&
                (config_server_list[0] & TAIR_FLAG_CFG_DOWN) == 0) {
               need_set = false;
            }
            if (need_set && resp->client_version > 0 && resp->client_version != client_version) {
               log_info("set _clientVersion to %d from %d", resp->client_version, client_version);
               client_version = resp->client_version;
            }

            if (need_set && resp->server_version == 1) {
               tair_mgr->set_solitary();
               server_version = 1;
            }

            if (need_set && resp->server_version >= TAIR_CONFIG_MIN_VERSION && resp->server_version != server_version) {
               log_info("set _serverVersion to %d from %d", resp->server_version, server_version);
               server_version = resp->server_version;


               uint64_t *server_list = resp->get_server_list(resp->bucket_count, resp->copy_count);
               if(server_list != NULL) {

                  // update the serverTable async
                  server_table_updater *stu = new server_table_updater(tair_mgr, server_list, resp->server_list_count, server_version, resp->data_need_move, resp->migrated_info, resp->copy_count, resp->bucket_count);
                  stu->start();
               } else {
                  log_debug("serverTable is NULL");
               }

            }
            if (resp->down_slave_config_server && have_config_server_down == false) {
               for (uint32_t i=0; i<config_server_list.size(); i++) {
                  if ((config_server_list[i]&TAIR_FLAG_SERVER) == resp->down_slave_config_server) {
                     config_server_list[i] |= TAIR_FLAG_CFG_DOWN;
                     log_warn("config server DOWN: %s", tbsys::CNetUtil::addrToString(resp->down_slave_config_server).c_str());
                  }
               }
               have_config_server_down = true;
            } else if (resp->down_slave_config_server == 0 && have_config_server_down == true) {
               for (uint32_t i=0; i<config_server_list.size(); i++) {
                  config_server_list[i] &= TAIR_FLAG_SERVER;
                  log_warn("config server HOST UP: %s", tbsys::CNetUtil::addrToString(config_server_list[i]).c_str());
               }
               have_config_server_down = false;
            }

            if (need_set && resp->plugins_version != plugins_version ){
               log_debug("get plugIns needset=%d, plugInsVersion =%d _plugInsVersion =%d",need_set, resp->plugins_version, plugins_version);
               plugins_version = resp->plugins_version;
               plugins_updater* pud = new plugins_updater(tair_mgr, resp->plugins_dll_names);
               pud->start(); //this will delete itself
            }
            if (need_set && !resp->vec_area_capacity_info.empty() && !tair_mgr->is_localmode()) {
               char area_arry[TAIR_MAX_AREA_COUNT];
               memset(area_arry, 0, sizeof(area_arry));
               log_error("_areaCapacityVersion = %u", resp->area_capacity_version);
               for (vector<pair<uint32_t, uint64_t> >::iterator it = resp->vec_area_capacity_info.begin();
                    it != resp->vec_area_capacity_info.end(); it++) {
                  if (it->first >= 0 && it->first < TAIR_MAX_AREA_COUNT) {
                     area_arry[it->first] = 1;
                     log_error("area %u, capacity %" PRI64_PREFIX "u", it->first, it->second);
                     tair_mgr->set_area_quota(static_cast<int>(it->first),it->second);
                  }
                  for (uint32_t i = 0; i < TAIR_MAX_AREA_COUNT; i++) {
                     if (area_arry[i] != 1) {
                        tair_mgr->set_area_quota(i,0);
                     }
                  }

               }
               heartbeat_packet.area_capacity_version =  resp->area_capacity_version;
            }
            break;
         }
         default:
            log_warn("unknow packet, pcode: %d", pcode);
        }
      }
      packet->free();

      return tbnet::IPacketHandler::KEEP_CHANNEL;
   }

// client_version
   uint32_t heartbeat_thread::get_client_version()
   {
      return client_version;
   }
// server_version
   uint32_t heartbeat_thread::get_server_version()
   {
      return server_version;
   }
   uint32_t heartbeat_thread::get_plugins_version()
   {
      return plugins_version;
   }

}

//////////////////
