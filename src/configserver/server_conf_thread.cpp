/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * server_conf_threa.cpp is the thread of config server
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "server_conf_thread.hpp"
#include "server_info_allocator.hpp"
#include "stat_helper.hpp"
#include "util.hpp"
#include "response_return_packet.hpp"
namespace {
  const int HARD_CHECK_MIG_COMPLETE = 300;
}

namespace tair {
  namespace config_server {

    server_conf_thread::server_conf_thread():builder_thread(this)
    {
      master_config_server_id = 0;
      struct timespec tm;
      clock_gettime(CLOCK_MONOTONIC, &tm);
      heartbeat_curr_time = tm.tv_sec;
      stat_interval_time = 5;
      connmgr = NULL;
      connmgr_heartbeat = NULL;
      is_ready = false;
      down_slave_config_server = 0U;
      load_config_server();
    }

    server_conf_thread::~server_conf_thread()
    {
      group_info_map::iterator it;
      vector<group_info *>group_info_list;
      for(it = group_info_map_data.begin(); it != group_info_map_data.end();
          ++it)
      {
        group_info_list.push_back(it->second);
      }
      for(size_t i = 0; i < group_info_list.size(); i++) {
        delete group_info_list[i];
      }
      if(connmgr) {
        delete connmgr;
      }
      if(connmgr_heartbeat) {
        delete connmgr_heartbeat;
      }
    }

    void server_conf_thread::set_thread_parameter(tbnet::Transport *
                                                  transport,
                                                  tbnet::Transport *
                                                  transport_heartbeat,
                                                  tair_packet_streamer *
                                                  streamer)
    {
      connmgr = new tbnet::ConnectionManager(transport, streamer, this);
      connmgr_heartbeat =
        new tbnet::ConnectionManager(transport_heartbeat, streamer, this);
    }
      /**
       * implementaion of IPacketHandler
       */
    tbnet::IPacketHandler::HPRetCode server_conf_thread::handlePacket(tbnet::
                                                                      Packet *
                                                                      packet,
                                                                      void
                                                                      *args)
    {
      if(!packet->isRegularPacket()) {
        tbnet::ControlPacket * cp = (tbnet::ControlPacket *) packet;
        log_warn("ControlPacket, cmd:%d", cp->getCommand());
        if(cp->getCommand() == tbnet::ControlPacket::CMD_DISCONN_PACKET) {
          return tbnet::IPacketHandler::FREE_CHANNEL;
        }
      }
      int id = (int) ((long) args);
      if(id) {
        if(packet->isRegularPacket()) {
          my_wait_object_manager.wakeup_wait_object(id,
                                                    (base_packet *) packet);
        }
        else {
          my_wait_object_manager.wakeup_wait_object(id, NULL);
        }
      }
      else if(packet->isRegularPacket()) {
        packet->free();
      }
      return tbnet::IPacketHandler::KEEP_CHANNEL;
    }
    uint32_t server_conf_thread::get_file_time(const char *file_name)
    {
      if(file_name == NULL) {
        return 0;
      }
      struct stat stats;
      if(lstat(file_name, &stats) == 0) {
        return stats.st_mtime;
      }
      return 0;
    }

    void server_conf_thread::load_group_file(const char *file_name,
                                             uint32_t version,
                                             uint64_t sync_config_server_id)
    {
      if(file_name == NULL)
        return;
      tbsys::CConfig config;
      if(config.load((char *) file_name) == EXIT_FAILURE) {
        log_error("load config file %s error", file_name);
        return;
      }

      log_info("begin load group file, filename: %s, version: %d", file_name,
               version);
      vector<string> section_list;
      config.getSectionName(section_list);
      // start up, get hash table from an other config server if exist one.
      if(sync_config_server_id
         && sync_config_server_id != util::local_server_ip::ip) {
        get_server_table(sync_config_server_id, NULL, GROUP_CONF);
        for(size_t i = 0; i < section_list.size(); i++) {
          get_server_table(sync_config_server_id,
                           (char *) section_list[i].c_str(), GROUP_DATA);
        }
        // here we need reload group configfile, since the file has been synced
        if(config.load((char *) file_name) == EXIT_FAILURE) {
          log_error("reload config file error: %s", file_name);
          return;
        }
      }

      set<uint64_t> server_id_list;
      group_info_rw_locker.wrlock();
      server_info_rw_locker.wrlock();
      for(size_t i = 0; i < section_list.size(); i++) {
        group_info_map::iterator it =
          group_info_map_data.find(section_list[i].c_str());
        group_info *group_info_tmp = NULL;
        if(it == group_info_map_data.end()) {
          group_info_tmp =
            new group_info((char *) section_list[i].c_str(),
                           &data_server_info_map, connmgr);
          group_info_map_data[group_info_tmp->get_group_name()] =
            group_info_tmp;
        }
        else {
          group_info_tmp = it->second;
        }
        group_info_tmp->load_config(config, version, server_id_list);
        group_info_tmp->find_available_server();
      }

      set <uint64_t> map_id_list;
      set <uint64_t> should_del_id_list;
      server_info_map::iterator sit;
      for(sit = data_server_info_map.begin();
          sit != data_server_info_map.end(); ++sit) {
        map_id_list.insert(sit->second->server_id);
      }
      std::set_difference(map_id_list.begin(), map_id_list.end(),
                          server_id_list.begin(), server_id_list.end(),
                          inserter(should_del_id_list,
                                   should_del_id_list.begin()));
      for(set<uint64_t>::iterator it = should_del_id_list.begin();
          it != should_del_id_list.end(); ++it) {
        sit = data_server_info_map.find(*it);
        if(sit != data_server_info_map.end()) {
          sit->second->server_id = 0;
          data_server_info_map.erase(sit);
        }
      }
      for(group_info_map::iterator it = group_info_map_data.begin();
          it != group_info_map_data.end(); ++it) {
        it->second->correct_server_info(sync_config_server_id
                                        && sync_config_server_id !=
                                        util::local_server_ip::ip);
      }
      log_info("machine count: %d, group count: %d",
               data_server_info_map.size(), group_info_map_data.size());
      server_info_rw_locker.unlock();
      group_info_rw_locker.unlock();
    }

    void server_conf_thread::check_server_status(uint32_t loop_count)
    {
      set<group_info *>change_group_info_list;

      group_info_map::iterator it;
      for(it = group_info_map_data.begin(); it != group_info_map_data.end();
          ++it) {
        if(it->second->is_need_rebuild()) {
          change_group_info_list.insert(it->second);
        }
      }

      server_info_map::iterator sit;
      for(sit = data_server_info_map.begin();
          sit != data_server_info_map.end(); ++sit) {
        //a bad one is alive again, we do not mark it ok unless some one tould us to.
        //administrator can touch group.conf to let these data serve alive again.
        uint32_t now;                // this decide how many seconds since last heart beat, we will mark this data server as a bad one.
        if(sit->second->group_info_data) {
          now =
            heartbeat_curr_time -
            sit->second->group_info_data->get_server_down_time();
        }
        else {
          now = heartbeat_curr_time - TAIR_SERVER_DOWNTIME;
        }
        if(sit->second->status != server_info::DOWN) {
          if((sit->second->last_time < now && (sit->second->status == server_info::ALIVE && !tbnet::ConnectionManager::isAlive(sit->second->server_id))) || sit->second->status == server_info::FORCE_DOWN) {        // downhost
            change_group_info_list.insert(sit->second->group_info_data);
            sit->second->status = server_info::DOWN;
            log_warn("HOST DOWN: %s lastTime is %u now is %u ",
                     tbsys::CNetUtil::addrToString(sit->second->server_id).
                     c_str(), sit->second->last_time, heartbeat_curr_time);

            // if (need add down server config) then set downserver in group.conf
            if (sit->second->group_info_data->get_pre_load_flag() == 1)
            {
              log_error("add down host: %s lastTime is %u now is %u ",
                  tbsys::CNetUtil::addrToString(sit->second->server_id).
                  c_str(), sit->second->last_time, heartbeat_curr_time);
              sit->second->group_info_data->add_down_server(sit->second->server_id);
              sit->second->group_info_data->set_force_send_table();
            }
          }
        }
      }
      // only master config server can update hashtable.
      if(master_config_server_id != util::local_server_ip::ip) {
        return;
      }

      uint64_t slave_server_id = get_slave_server_id();

      group_info_rw_locker.rdlock();

      if(loop_count % HARD_CHECK_MIG_COMPLETE == 0) {
        for(group_info_map::const_iterator it = group_info_map_data.begin();
            it != group_info_map_data.end(); it++) {
          it->second->hard_check_migrate_complete(slave_server_id);
        }
      }

      for(it = group_info_map_data.begin(); it != group_info_map_data.end();
          ++it) {
        it->second->check_migrate_complete(slave_server_id);

        // check if send server_table
        if (1 == it->second->get_send_server_table())
        {
          log_warn("group: %s need send server table", it->second->get_group_name());
          it->second->send_server_table_packet(slave_server_id);
          it->second->reset_force_send_table();
        }
      }

      if(change_group_info_list.size() == 0U) {
        group_info_rw_locker.unlock();
        return;
      }

      set<group_info *>::iterator cit;
      tbsys::CThreadGuard guard(&mutex_grp_need_build);
      for(cit = change_group_info_list.begin();
          cit != change_group_info_list.end(); ++cit) {
        builder_thread.group_need_build.push_back(*cit);
        (*cit)->set_table_builded();
      }
      group_info_rw_locker.unlock();
    }

    void server_conf_thread::check_config_server_status(uint32_t loop_count)
    {
      if(config_server_info_list.size() != 2U) {
        return;
      }
      bool master_status_changed = false;

      uint32_t now = heartbeat_curr_time - TAIR_SERVER_DOWNTIME * 2;
      bool config_server_up = false;
      for(size_t i = 0; i < config_server_info_list.size(); i++) {
        server_info *server_info_tmp = config_server_info_list[i];
        if(server_info_tmp->server_id == util::local_server_ip::ip) {        // this is myself
          continue;
        }
        if(server_info_tmp->last_time < now && server_info_tmp->status == server_info::ALIVE) {        // downhost
          if(tbnet::ConnectionManager::isAlive(server_info_tmp->server_id) ==
             true)
            continue;
          server_info_tmp->status = server_info::DOWN;
          if(i == 0)
            master_status_changed = true;
          log_warn("CONFIG HOST DOWN: %s",
                   tbsys::CNetUtil::addrToString(server_info_tmp->server_id).
                   c_str());
        }
        else if(server_info_tmp->last_time > now && server_info_tmp->status == server_info::DOWN) {        // uphost
          server_info_tmp->status = server_info::ALIVE;
          if(i == 0)
            master_status_changed = true;
          config_server_up = true;
          log_warn("CONFIG HOST UP: %s",
                   tbsys::CNetUtil::addrToString(server_info_tmp->server_id).
                   c_str());
        }
      }

      if(master_status_changed == false) {
        if(config_server_up
           && master_config_server_id == util::local_server_ip::ip) {
          uint64_t slave_id = get_slave_server_id();
          group_info_map::iterator it;
          group_info_rw_locker.rdlock();
          for(it = group_info_map_data.begin();
              it != group_info_map_data.end(); ++it) {
            // for we can't get peer's version from protocol, add 10 to client version and server
            it->second->inc_version(server_up_inc_step);
            it->second->send_server_table_packet(slave_id);
            log_warn("config server up and master not changed, version changed. "
                "group name: %s, client version: %u, server version: %u",
                it->second->get_group_name(), it->second->get_client_version(), it->second->get_server_version());
          }
          group_info_rw_locker.unlock();
        }
        return;
      }

      server_info *server_info_master = config_server_info_list[0];
      if(server_info_master->status == server_info::ALIVE) {
        master_config_server_id = server_info_master->server_id;
      }
      else {
        master_config_server_id = util::local_server_ip::ip;
      }
      if(master_config_server_id == util::local_server_ip::ip) {
        const char *group_file_name =
          TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        if(group_file_name == NULL) {
          log_error("not found %s:%s ", CONFSERVER_SECTION, TAIR_GROUP_FILE);
          return;
        }

        uint32_t curr_version = get_file_time(group_file_name);
        load_group_file(group_file_name, curr_version, 0);
      }
      log_warn("MASTER_CONFIG changed %s",
               tbsys::CNetUtil::addrToString(master_config_server_id).
               c_str());
    }

    void server_conf_thread::load_config_server()
    {
      vector<const char *>str_list =
        TBSYS_CONFIG.getStringList(TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
      int port =
        TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT,
                            TAIR_CONFIG_SERVER_DEFAULT_PORT);
      uint64_t id;
      for(size_t i = 0; i < str_list.size(); i++) {
        id = tbsys::CNetUtil::strToAddr(str_list[i], port);
        if(id == 0)
          continue;
        if(config_server_info_map.find(id) == config_server_info_map.end()) {
          server_info *server_info =
            server_info_allocator::server_info_allocator_instance.
            new_server_info(NULL, id);
          config_server_info_map[id] = server_info;
          config_server_info_list.push_back(server_info);
        }
        // only the first 2 configserver is useful
        if(config_server_info_list.size() == 2)
          break;
      }
      if(config_server_info_list.size() == 0U) {
        master_config_server_id = util::local_server_ip::ip;
        return;
      }
      if(config_server_info_list.size() == 1U
         && config_server_info_list[0]->server_id ==
         util::local_server_ip::ip) {
        master_config_server_id = util::local_server_ip::ip;
        return;
      }
      //we got 2 configserver here
      //uint64_t syncConfigServer = 0;
      master_config_server_id = config_server_info_list[0]->server_id;
      return;
    }

    // implementaion Runnable interface
    void server_conf_thread::run(tbsys::CThread * thread, void *arg)
    {
      uint64_t sync_config_server_id = 0;
      uint64_t tmp_master_config_server_id = master_config_server_id;
      if(tmp_master_config_server_id == util::local_server_ip::ip) {
        tmp_master_config_server_id = get_slave_server_id();
      }
      if(tmp_master_config_server_id)
        sync_config_server_id =
          get_master_config_server(tmp_master_config_server_id, 1);
      if(sync_config_server_id) {
        log_info("syncConfigServer: %s",
                 tbsys::CNetUtil::addrToString(sync_config_server_id).
                 c_str());
      }

      // groupFile
      const char *group_file_name =
        TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
      if(group_file_name == NULL) {
        log_error("not found %s:%s ", CONFSERVER_SECTION, TAIR_GROUP_FILE);
        return;
      }

      uint32_t config_version_from_file = get_file_time(group_file_name);
      //bool rebuild_group = true;
      bool rebuild_this_group = false;
      load_group_file(group_file_name, config_version_from_file,
                      sync_config_server_id);
      //if (syncConfigServer != 0) {
      //    rebuild_group = false;
      //}

      //set myself is master
      if(config_server_info_list.size() == 2U &&
         (config_server_info_list[0]->server_id == util::local_server_ip::ip
          || (sync_config_server_id == 0
              && config_server_info_list[1]->server_id ==
              util::local_server_ip::ip))) {
        master_config_server_id = util::local_server_ip::ip;
        log_info("set MASTER_CONFIG: %s",
                 tbsys::CNetUtil::addrToString(master_config_server_id).
                 c_str());
        {
          group_info_map::iterator it;
          group_info_rw_locker.rdlock();
          for(it = group_info_map_data.begin();
              it != group_info_map_data.end(); ++it) {
            if(rebuild_this_group) {
              it->second->set_force_rebuild();
            }
            else {
              bool need_build_it = false;
              const uint64_t *p_table = it->second->get_hash_table();
              for(uint32_t i = 0; i < it->second->get_server_bucket_count();
                  ++i) {
                if(p_table[i] == 0) {
                  need_build_it = true;
                  break;
                }
              }
              if(need_build_it) {
                it->second->set_force_rebuild();
              }
              else {
                it->second->set_table_builded();
              }
            }
          }
          group_info_rw_locker.unlock();
        }
      }

      // will start loop
      uint32_t loop_count = 0;
      tzset();
      int log_rotate_time = (time(NULL) - timezone) % 86400;
      struct timespec tm;
      clock_gettime(CLOCK_MONOTONIC, &tm);
      heartbeat_curr_time = tm.tv_sec;
      is_ready = true;

      while(!_stop) {
        struct timespec tm;
        clock_gettime(CLOCK_MONOTONIC, &tm);
        heartbeat_curr_time = tm.tv_sec;
        for(size_t i = 0; i < config_server_info_list.size(); i++) {
          server_info *server_info_it = config_server_info_list[i];
          if(server_info_it->server_id == util::local_server_ip::ip)
            continue;
          if(server_info_it->status != server_info::ALIVE
             && (loop_count % 30) != 0) {
            down_slave_config_server = server_info_it->server_id;
            log_debug("local server: %s set down slave config server: %s",
                tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str(),
                tbsys::CNetUtil::addrToString(down_slave_config_server).c_str());
            continue;
          }
          down_slave_config_server = 0;
          request_conf_heartbeart *new_packet = new request_conf_heartbeart();
          new_packet->server_id = util::local_server_ip::ip;
          new_packet->loop_count = loop_count;
          if(connmgr_heartbeat->
             sendPacket(server_info_it->server_id, new_packet) == false) {
            delete new_packet;
          }
        }

        loop_count++;
        if(loop_count <= 0)
          loop_count = TAIR_SERVER_DOWNTIME + 1;
        uint32_t curver = get_file_time(group_file_name);
        if(curver > config_version_from_file) {
          log_info("groupFile: %s, curver:%d configVersion:%d",
                   group_file_name, curver, config_version_from_file);
          load_group_file(group_file_name, curver, 0);
          config_version_from_file = curver;
          send_group_file_packet();
        }

        check_config_server_status(loop_count);

        if(loop_count > TAIR_SERVER_DOWNTIME) {
          check_server_status(loop_count);
        }

        // rotate log
        log_rotate_time++;
        if(log_rotate_time % 86400 == 86340) {
          log_info("rotateLog End");
          TBSYS_LOGGER.rotateLog(NULL, "%d");
          log_info("rotateLog start");
        }
        if(log_rotate_time % 3600 == 3000) {
          log_rotate_time = (time(NULL) - timezone) % 86400;
        }

        TAIR_SLEEP(_stop, 1);
      }
      is_ready = false;
    }

    void server_conf_thread::find_group_host(request_get_group * req,
                                             response_get_group * resp)
    {
      if(is_ready == false) {
        return;
      }

      group_info_map::iterator it;
      group_info_rw_locker.rdlock();
      it = group_info_map_data.find(req->group_name);
      if(it == group_info_map_data.end()) {
        group_info_rw_locker.unlock();
        return;
      }
      group_info *group_info_found = it->second;
      resp->bucket_count = group_info_found->get_server_bucket_count();
      resp->copy_count = group_info_found->get_copy_count();

      resp->config_version = group_info_found->get_client_version();
      if(req->config_version == group_info_found->get_client_version()) {
        group_info_rw_locker.unlock();
        return;
      }

      resp->config_map = *(group_info_found->get_common_map());

      resp->set_hash_table(group_info_found->
                           get_hash_table_deflate_data(req->server_flag),
                           group_info_found->get_hash_table_deflate_size(req->
                                                                         server_flag));
      resp->available_server_ids =
        group_info_found->get_available_server_id();
      group_info_rw_locker.unlock();
    }

    void server_conf_thread::set_stat_interval_time(int stat_interval_time_v)
    {
      if(stat_interval_time_v > 0) {
        stat_interval_time = stat_interval_time_v;
      }
    }
    void server_conf_thread::
      force_change_server_status(request_data_server_ctrl * packet)
    {
      log_warn
        ("receive forceChangeCommand from %s make data server %s chang to %d",
         tbsys::CNetUtil::addrToString(packet->get_connection()->
                                       getServerId()).c_str(),
         tbsys::CNetUtil::addrToString(packet->server_id).c_str(),
         packet->cmd);

      server_info_rw_locker.rdlock();
      server_info_map::iterator it =
        data_server_info_map.find(packet->server_id);
      if(it == data_server_info_map.end()) {
        server_info_rw_locker.unlock();
        log_warn("not found this server(%s) in serverInfoMap",
                 tbsys::CNetUtil::addrToString(packet->server_id).c_str());
        return;
      }
      server_info *p_server = it->second;
      if(packet->cmd == request_data_server_ctrl::CTRL_DOWN
         && p_server->status != server_info::DOWN) {
        log_debug("force it down");
        p_server->status = server_info::FORCE_DOWN;
      }
      else if(packet->cmd == request_data_server_ctrl::CTRL_UP
              && p_server->status != server_info::ALIVE) {
        struct timespec tm;
        clock_gettime(CLOCK_MONOTONIC, &tm);
        p_server->last_time = tm.tv_sec;
        p_server->status = server_info::ALIVE;
        p_server->group_info_data->set_force_rebuild();
        log_debug("force it up");
      }
      server_info_rw_locker.unlock();
    }

    void server_conf_thread::
      get_migrating_machines(request_get_migrate_machine * req,
                             response_get_migrate_machine * resp)
    {
      group_info_rw_locker.rdlock();
      server_info_rw_locker.rdlock();
      server_info_map::iterator it =
        data_server_info_map.find(req->server_id);
      if(it == data_server_info_map.end()) {
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        log_error("ger MigratingMachine can not found server %s ",
                  tbsys::CNetUtil::addrToString(req->server_id).c_str());
        return;
      }
      server_info *p_server = it->second;
      group_info *group_info_found = p_server->group_info_data;
      log_debug("find groupinfo = %p", group_info_found);
      if(group_info_found == NULL) {
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        return;
      }
      group_info_found->get_migrating_machines(resp->vec_ms);
      server_info_rw_locker.unlock();
      group_info_rw_locker.unlock();
      return;
    }

    group_info_map *server_conf_thread::get_group_info_map()
    {
      return &group_info_map_data;
    }

    server_info_map *server_conf_thread::get_server_info_map()
    {
      return &data_server_info_map;
    }

    void server_conf_thread::do_query_info_packet(request_query_info * req,
                                                  response_query_info * resp)
    {
      if(is_ready == false) {
        return;
      }
      group_info_map::iterator it;
      group_info_rw_locker.rdlock();
      if(req->query_type == request_query_info::Q_GROUP_INFO) {
        for(it = group_info_map_data.begin(); it != group_info_map_data.end();
            it++) {
          if(it->first == NULL)
            continue;
          string group_name(it->first);
          log_debug("group_name is %s", group_name.c_str());
          if(it->second == NULL)
            continue;
          if(it->second->get_group_is_OK()) {
            resp->map_k_v[group_name] = "status OK";
          }
          else {
            resp->map_k_v[group_name] = "status error";
          }
        }
        group_info_rw_locker.unlock();
        return;
      }
      it = group_info_map_data.find(req->group_name.c_str());
      if(it == group_info_map_data.end()) {
        group_info_rw_locker.unlock();
        return;
      }
      group_info *group_info_found = it->second;
      char key[32];
      char value[32];
      switch (req->query_type) {
      case request_query_info::Q_AREA_CAPACITY:
        {
          const map<uint32_t, uint64_t> &capacity_info =
            group_info_found->get_area_capacity_info();
          map<uint32_t, uint64_t>::const_iterator capacity_it =
            capacity_info.begin();
          for(; capacity_it != capacity_info.end(); capacity_it++) {
            snprintf(key, 32, "area(%u)", capacity_it->first);
            snprintf(value, 32, "%" PRI64_PREFIX "u", capacity_it->second);
            resp->map_k_v[key] = value;
          }
        }

        break;
      case request_query_info::Q_MIG_INFO:
        {
          bool is_migating = false;
          if(group_info_found->is_migrating()) {
            vector <pair<uint64_t, uint32_t> >mig_machine_info;
            group_info_found->get_migrating_machines(mig_machine_info);
            is_migating = !mig_machine_info.empty();
            for(vector <pair<uint64_t, uint32_t> >::iterator mig_it =
                mig_machine_info.begin(); mig_it != mig_machine_info.end();
                mig_it++) {
              snprintf(key, 32, "%s",
                       tbsys::CNetUtil::addrToString(mig_it->first).c_str());
              snprintf(value, 32, "%u left", mig_it->second);
              resp->map_k_v[key] = value;
            }
          }
          if(is_migating) {
            resp->map_k_v["isMigrating"] = "true";
          }
          else {
            resp->map_k_v["isMigrating"] = "false";
          }
        }
        break;
      case request_query_info::Q_DATA_SEVER_INFO:
        {
          server_info_rw_locker.rdlock();
          node_info_set nodeInfo = group_info_found->get_node_info();
          node_info_set::iterator node_it;
          for(node_it = nodeInfo.begin(); node_it != nodeInfo.end();
              ++node_it) {
            snprintf(key, 32, "%s",
                     tbsys::CNetUtil::addrToString((*node_it)->server->
                                                   server_id).c_str());
            if((*node_it)->server->status == server_info::ALIVE) {
              sprintf(value, "%s", "alive");
            }
            else {
              sprintf(value, "%s", "dead");
            }
            resp->map_k_v[key] = value;
          }
          server_info_rw_locker.unlock();
        }
        break;
      case request_query_info::Q_STAT_INFO:
        {
          node_stat_info node_info;
          group_info_found->get_stat_info(req->server_id, node_info);
          node_info.format_info(resp->map_k_v);
        }
        break;
      default:
        break;
      }
      group_info_rw_locker.unlock();
      return;

    }
    void server_conf_thread::do_heartbeat_packet(request_heartbeat * req,
                                                 response_heartbeat * resp)
    {
      if(is_ready == false) {
        return;
      }
      group_info_rw_locker.rdlock();
      server_info_rw_locker.rdlock();

      server_info_map::iterator it =
        data_server_info_map.find(req->server_id);
      if(it == data_server_info_map.end()) {
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        return;
      }
      server_info *p_server = it->second;
      p_server->last_time = heartbeat_curr_time;
      if (p_server->status == server_info::DOWN)
      {
        if (p_server->group_info_data == NULL ||
            p_server->group_info_data->get_accept_strategy() == GROUP_DEFAULT_ACCEPT_STRATEGY) //default accept strategy
        {
          log_warn("dataserver: %s UP, accept strategy is default, set it to solitary(can not work)",
              tbsys::CNetUtil::addrToString(req->server_id).c_str());
          // this data server should not be in this group
          resp->client_version = resp->server_version = 1;
          // inc table version and force rebuild table for send server table to slave asynchronously
          if (p_server->group_info_data != NULL && p_server->group_info_data->get_send_server_table() == 0)
          {
            p_server->group_info_data->inc_version(server_up_inc_step);
            p_server->group_info_data->set_force_send_table();
            log_warn("ds up, set force send table. version changed. group name: %s, client version: %u, server version: %u",
                p_server->group_info_data->get_group_name(), p_server->group_info_data->get_client_version(),
                p_server->group_info_data->get_server_version());
          }
          server_info_rw_locker.unlock();
          group_info_rw_locker.unlock();
          return;
        }
        else if (p_server->group_info_data->get_accept_strategy() == GROUP_AUTO_ACCEPT_STRATEGY)
        {
          log_warn("dataserver: %s UP, accept strategy is auto, we will rebuild the table.",
              tbsys::CNetUtil::addrToString(req->server_id).c_str());
          // accept ds automatically
          struct timespec tm;
          clock_gettime(CLOCK_MONOTONIC, &tm);
          p_server->last_time = tm.tv_sec;
          p_server->status = server_info::ALIVE;
          p_server->group_info_data->set_force_rebuild();
        }
        else
        {
          log_error("dataserver: %s UP, accept strategy is: %d illegal.",
              p_server->group_info_data->get_accept_strategy(), tbsys::CNetUtil::addrToString(req->server_id).c_str());
          server_info_rw_locker.unlock();
          group_info_rw_locker.unlock();
          return;
        }
      }
      else if (p_server->status == server_info::FORCE_DOWN)
      {
        resp->client_version = resp->server_version = 1;
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        return;
      }

      group_info *group_info_founded = p_server->group_info_data;
      if(group_info_founded == NULL) {
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        return;
      }
      if(group_info_founded->get_group_status() == false) {
        log_error("group:%s status is abnorm. set ds: %s to solitary(can not work)",
            group_info_founded->get_group_name(), tbsys::CNetUtil::addrToString(req->server_id).c_str());
        resp->client_version = resp->server_version = 1;
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        return;
      }
      resp->client_version = group_info_founded->get_client_version();
      resp->down_slave_config_server = down_slave_config_server;
      resp->data_need_move = group_info_founded->get_data_need_move();
      tair_stat *stat = req->get_stat();

      if(stat) {
        log_debug("have stat info");
        node_stat_info node_stat;
        node_stat.set_last_update_time(heartbeat_curr_time);
        for(int i = 0; i < STAT_LENGTH; i++) {
          stat_info_detail statDetail;
          if(stat[i].get_count()) {
            statDetail.set_unit_value(stat_info_detail::GETCOUNT,
                                      stat[i].get_count());
            //log_debug("stat[i].getCount() =%d",stat[i].getCount());
          }
          if(stat[i].put_count()) {
            statDetail.set_unit_value(stat_info_detail::PUTCOUNT,
                                      stat[i].put_count());
            //log_debug("stat[i].putCount =%d",stat[i].putCount());
          }
          if(stat[i].evict_count()) {
            statDetail.set_unit_value(stat_info_detail::EVICTCOUNT,
                                      stat[i].evict_count());
            //log_debug("stat[i].evictCount() =%d",stat[i].evictCount());
          }
          if(stat[i].remove_count()) {
            statDetail.set_unit_value(stat_info_detail::REMOVECOUNT,
                                      stat[i].remove_count());
            //log_debug("stat[i].removeCount() =%d",stat[i].removeCount());
          }
          if(stat[i].hit_count()) {
            statDetail.set_unit_value(stat_info_detail::HITCOUNT,
                                      stat[i].hit_count());
            //log_debug("stat[i].hitCount() =%d",stat[i].hitCount());
          }
          if(stat[i].data_size()) {
            statDetail.set_unit_value(stat_info_detail::DATASIZE,
                                      stat[i].data_size());
            //log_debug("stat[i].dataSize() =%d",stat[i].dataSize());
          }
          if(stat[i].use_size()) {
            statDetail.set_unit_value(stat_info_detail::USESIZE,
                                      stat[i].use_size());
            //log_debug("stat[i].useSize() =%d",stat[i].useSize());
          }
          if(stat[i].item_count()) {
            statDetail.set_unit_value(stat_info_detail::ITEMCOUNT,
                                      stat[i].item_count());
            //log_debug("stat[i].itemCount() =%d",stat[i].itemCount());
          }
          if(statDetail.get_unit_size()) {
            node_stat.insert_stat_detail(i, statDetail);
          }
        }
        group_info_founded->set_stat_info(req->server_id, node_stat);
      }

      //TODO interval of next stat report
      //heartbeat_curr_time will be set as last update time
      if(req->config_version != group_info_founded->get_server_version()) {
        // set the groupname into the packet
        int len = strlen(group_info_founded->get_group_name()) + 1;
        if(len > 64)
          len = 64;
        memcpy(resp->group_name, group_info_founded->get_group_name(), len);
        resp->group_name[63] = '\0';
        resp->server_version = group_info_founded->get_server_version();
        resp->bucket_count = group_info_founded->get_server_bucket_count();
        resp->copy_count = group_info_founded->get_copy_count();
        resp->set_hash_table(group_info_founded->
                             get_hash_table_deflate_data(req->server_flag),
                             group_info_founded->
                             get_hash_table_deflate_size(req->server_flag));
        log_info("config verion diff, return hash table size: %d",
                 resp->get_hash_table_size());
      }
      else {
        if(master_config_server_id == util::local_server_ip::ip) {
          group_info_founded->do_proxy_report(*req);
        }
      }

      if(req->plugins_version != group_info_founded->get_plugins_version()) {
        resp->set_plugins_names(group_info_founded->get_plugins_info());
      }
      resp->area_capacity_version =
        group_info_founded->get_area_capacity_version();
      if(req->area_capacity_version != resp->area_capacity_version) {
        resp->set_area_capacity(group_info_founded->get_area_capacity_info(),
                                group_info_founded->get_copy_count(),
                                group_info_founded->get_available_server_id().
                                size());
      }
      resp->plugins_version = group_info_founded->get_plugins_version();
      if(req->pull_migrated_info != 0) {
        const uint64_t *p_m_table = group_info_founded->get_hash_table(1);
        const uint64_t *p_s_table = group_info_founded->get_hash_table(0);
        for(uint32_t i = 0; i < group_info_founded->get_server_bucket_count();
            ++i) {
          if(req->server_id == p_s_table[i]) {
            resp->migrated_info.push_back(i);
            for(uint32_t j = 0; j < group_info_founded->get_copy_count(); ++j) {
              resp->migrated_info.
                push_back(p_m_table
                          [i +
                           j *
                           group_info_founded->get_server_bucket_count()]);
            }
          }

        }

      }
      server_info_rw_locker.unlock();
      group_info_rw_locker.unlock();
    }

    // do_conf_heartbeat_packet
    void server_conf_thread::
      do_conf_heartbeat_packet(request_conf_heartbeart * req)
    {
      server_info_map::iterator it =
        config_server_info_map.find(req->server_id);
      if(it == config_server_info_map.end()) {
        return;
      }
      it->second->last_time = heartbeat_curr_time;
    }

    int server_conf_thread::do_finish_migrate_packet(request_migrate_finish *
                                                     req)
    {
      int ret = 0;
      log_info("receive migrate finish packet from [%s]",
               tbsys::CNetUtil::addrToString(req->server_id).c_str());
      if(is_ready == false) {
        return 1;
      }
      if(master_config_server_id != util::local_server_ip::ip) {
        //this is a slve config server. no response
        ret = 1;
      }
      uint64_t slave_server_id = get_slave_server_id();
      group_info_rw_locker.rdlock();
      server_info_rw_locker.rdlock();
      server_info_map::iterator it =
        data_server_info_map.find(req->server_id);
      if(it != data_server_info_map.end()
         && it->second->group_info_data != NULL) {
        if(it->second->group_info_data->
           do_finish_migrate(req->server_id, req->version, req->bucket_no,
                             slave_server_id) == false) {
          if(ret == 0) {
            ret = -1;
          }
        }
      }
      server_info_rw_locker.unlock();
      group_info_rw_locker.unlock();
      return ret;
    }

    uint64_t server_conf_thread::get_master_config_server(uint64_t id,
                                                          int value)
    {
      uint64_t master_id = 0;
      common::wait_object * cwo = my_wait_object_manager.create_wait_object();
      request_set_master *packet = new request_set_master();
      packet->value = value;
      if(connmgr->
         sendPacket(id, packet, NULL,
                    (void *) ((long) cwo->get_id())) == false) {
        log_error("Send RequestSetMasterPacket to %s failure.",
                  tbsys::CNetUtil::addrToString(id).c_str());
        delete packet;
      }
      else {
        cwo->wait_done(1, 1000);
        base_packet *tpacket = cwo->get_packet();
        if(tpacket != NULL && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
          if(((response_return *) tpacket)->get_code() == 0) {
            master_id = id;
          }
        }
      }
      my_wait_object_manager.destroy_wait_object(cwo);
      return master_id;
    }

    void server_conf_thread::get_server_table(uint64_t sync_config_server_id,
                                              const char *group_name,
                                              int type)
    {
      bool ret = false;
      uint32_t hashcode = 0;
      common::wait_object * cwo = my_wait_object_manager.create_wait_object();
      request_get_server_table *packet = new request_get_server_table();
      packet->config_version = type;
      packet->set_group_name(group_name);
      log_info("begin syncserver table with [%s] for group [%s]",
               tbsys::CNetUtil::addrToString(sync_config_server_id).c_str(),
               group_name);
      if(connmgr->
         sendPacket(sync_config_server_id, packet, NULL,
                    (void *) ((long) cwo->get_id())) == false) {
        log_error("Send RequestGetServerInfoPacket to %s failure.",
                  tbsys::CNetUtil::addrToString(sync_config_server_id).
                  c_str());
        delete packet;
      }
      else {
        cwo->wait_done(1, 1000);
        base_packet *tpacket = cwo->get_packet();
        if(tpacket != NULL
           && tpacket->getPCode() == TAIR_RESP_GET_SVRTAB_PACKET) {
          response_get_server_table *resp =
            (response_get_server_table *) tpacket;
          if(resp->type == 0) {
            if(resp->size > 0) {        //write to file
              char file_name[256];
              const char *sz_data_dir =
                TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_DATA_DIR,
                                       TAIR_DEFAULT_DATA_DIR);
              snprintf(file_name, 256, "%s/%s_server_table", sz_data_dir,
                       group_name);
              ret =
                backup_and_write_file(file_name, resp->data, resp->size,
                                      resp->modified_time);
              if(ret) {
                hashcode =
                  util::string_util::mur_mur_hash(resp->data, resp->size);
              }
            }
          }
          else {
            const char *file_name =
              TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE,
                                     NULL);
            if(file_name != NULL) {
              ret =
                backup_and_write_file(file_name, resp->data, resp->size,
                                      resp->modified_time);
              if(ret) {
                hashcode =
                  util::string_util::mur_mur_hash(resp->data, resp->size);
              }
            }
          }
        }
      }
      my_wait_object_manager.destroy_wait_object(cwo);

      if(type == GROUP_DATA) {
        log_info("%s, load server_table%s, hashcode: %u",
                 group_name, (ret ? "ok" : "error"), hashcode);
      }
      else {
        log_info("load groupFile%s, hashcode: %u", (ret ? "ok" : "error"),
                 hashcode);
      }
    }

    // do_set_master_packet
    bool server_conf_thread::do_set_master_packet(request_set_master * req)
    {
      uint64_t old_master_config_server_id = master_config_server_id;
      bool ret = true;
      if(master_config_server_id == 0 ||
         master_config_server_id != util::local_server_ip::ip) {
        ret = false;
      }
      if(req == NULL)
        return ret;

      if(req->value == 1 && config_server_info_list.size() > 0U &&
         config_server_info_list[0]->server_id != util::local_server_ip::ip) {
        master_config_server_id = 0;
      }
      uint64_t server_id = 0;
      if(req->get_connection() != NULL) {
        server_id = req->get_connection()->getServerId();
      }
      server_info_map::iterator it = config_server_info_map.find(server_id);
      if(it != config_server_info_map.end()) {
        it->second->last_time = heartbeat_curr_time;
      }

      log_info("I am %s MASTER_CONFIG", (ret ? "" : "not"));
      if(old_master_config_server_id != master_config_server_id) {
        group_info_rw_locker.wrlock();
        group_info_map::iterator it = group_info_map_data.begin();
        for(; it != group_info_map_data.end(); it++) {
          it->second->inc_version();

        }
        group_info_rw_locker.unlock();
      }

      return ret;
    }

    void server_conf_thread::do_op_cmd(request_op_cmd *req) {
      int rc = TAIR_RETURN_SUCCESS;
      response_op_cmd *resp = new response_op_cmd();
      // only master can op cmd
      if(master_config_server_id != util::local_server_ip::ip) {
        rc = TAIR_RETURN_FAILED;
      } else {
        const char *group_file_name = TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        ServerCmdType cmd = req->cmd;
        switch (cmd) {
        case TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER:
        {
          rc = get_group_config_value(resp, req->params, group_file_name, TAIR_TMP_DOWN_SERVER, "");
          break;
        }
        case TAIR_SERVER_CMD_GET_GROUP_STATUS:
        {
          rc = get_group_config_value(resp, req->params, group_file_name, TAIR_GROUP_STATUS, "off");
          break;
        }
        case TAIR_SERVER_CMD_SET_GROUP_STATUS:
        {
          rc = set_group_status(resp, req->params, group_file_name);
          break;
        }
        case TAIR_SERVER_CMD_RESET_DS:
        {
          rc = do_reset_ds_packet(resp, req->params);
          break;
        }
        default:
        {
          log_error("unknown command %d received", cmd);
          rc = TAIR_RETURN_FAILED;
          break;
        }
        }
      }
      resp->code = rc;
      resp->setChannelId(req->getChannelId());
      if (req->get_connection()->postPacket(resp) == false) {
        delete resp;
      }
    }

    int server_conf_thread::get_group_config_value(response_op_cmd *resp,
                                                   const vector<string> &params, const char *group_file_name,
                                                   const char *config_key, const char* default_value) {
      if (group_file_name == NULL) {
        log_error("group_file_name is NULL");
        return TAIR_RETURN_FAILED;
      }
      if (config_key == NULL) {
        log_error("get config value but key is NULL");
        return TAIR_RETURN_FAILED;
      }
      tbsys::CConfig config;
      if (config.load((char*) group_file_name) == EXIT_FAILURE) {
        log_error("load group file %s failed", group_file_name);
        return TAIR_RETURN_FAILED;
      }

      for (size_t i = 0; i < params.size(); ++i) {
        const char *config_value = config.getString(params[i].c_str(), config_key, default_value);
        if (config_value == NULL) {
          config_value = "none";
        }
        char buf[128];
        snprintf(buf, sizeof(buf), "%s: %s=%s", params[i].c_str(), config_key, config_value);
        resp->infos.push_back(string(buf));
      }

      return TAIR_RETURN_SUCCESS;
    }

    int server_conf_thread::set_group_status(response_op_cmd *resp,
        const vector<string> &params, const char *group_file_name) {
      if (group_file_name == NULL) {
        log_error("group_file_name is NULL");
        return TAIR_RETURN_FAILED;
      }
      if (params.size() != 2) {
        log_error("set group status but have no group/status");
        return TAIR_RETURN_FAILED;
      }

      log_info("set group status: %s = %s", params[0].c_str(), params[1].c_str());
      return util::file_util::change_conf(group_file_name, params[0].c_str(), TAIR_GROUP_STATUS, params[1].c_str());
    }

    int server_conf_thread::do_reset_ds_packet(response_op_cmd* resp, const vector<string>& params)
    {
      int ret = TAIR_RETURN_SUCCESS;
      if (params.size() <= 0)
      {
        log_error("reset ds cmd but no group parameter");
        return TAIR_RETURN_FAILED;
      }

      const char* cmd_group = params[0].c_str();
      group_info_map::iterator mit = group_info_map_data.find(cmd_group);
      if (mit == group_info_map_data.end())
      {
        log_error("rest ds cmd but invalid group name: %s", cmd_group);
        return TAIR_RETURN_FAILED;
      }

      vector<uint64_t> ds_ids;
      vector<string>::const_iterator it = params.begin();
      ++it;
      log_info("resetds group: %s, requeset resetds size: %d", cmd_group, params.size() - 1);
      for (; it != params.end(); it++)
      {
        log_info("reset ds: %s", it->c_str());
        uint64_t ds_id = tbsys::CNetUtil::strToAddr(it->c_str(), 0);
        // check ds's status: must be alive
        server_info_rw_locker.rdlock();
        server_info_map::iterator mit =
          data_server_info_map.find(ds_id);
        if (mit == data_server_info_map.end())
        {
          server_info_rw_locker.unlock();
          log_error("can not find reset ds: %s", it->c_str());
          return TAIR_RETURN_FAILED;
        }
        else if (mit->second->status != server_info::ALIVE)
        {
          server_info_rw_locker.unlock();
          log_error("reset ds: %s status is: %d illegal", it->c_str(), mit->second->status);
          return TAIR_RETURN_FAILED;
        }
        server_info_rw_locker.unlock();
        ds_ids.push_back(ds_id);
      }
      group_info_rw_locker.wrlock();
      mit->second->clear_down_server(ds_ids);
      mit->second->inc_version();
      mit->second->set_force_send_table();
      group_info_rw_locker.unlock();
      return ret;
    }

    void server_conf_thread::
      read_group_file_to_packet(response_get_server_table * resp)
    {
      resp->type = GROUP_CONF;
      const char *file_name =
        TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
      if(file_name == NULL)
        return;
      file_mapper mmap_file;
      mmap_file.open_file(file_name);
      char *data = (char *) mmap_file.get_data();
      if(data == NULL)
        return;
      resp->set_data(data, mmap_file.get_size());
      resp->modified_time = mmap_file.get_modify_time();
      log_info("load groupfile, hashcode: %u",
               util::string_util::mur_mur_hash(resp->data, resp->size));
    }
    uint64_t server_conf_thread::get_slave_server_id()
    {
      uint64_t slave_server_id = 0;
      if(config_server_info_list.size() == 2U &&
         config_server_info_list[0]->server_id == util::local_server_ip::ip &&
         config_server_info_list[1]->status == server_info::ALIVE) {
        slave_server_id = config_server_info_list[1]->server_id;
      }
      return slave_server_id;
    }

    void server_conf_thread::send_group_file_packet()
    {
      uint64_t slave_server_id = get_slave_server_id();
      if(slave_server_id == 0)
        return;

      response_get_server_table *packet = new response_get_server_table();
      read_group_file_to_packet(packet);

      log_info("send groupFile to %s: hashcode: %u",
               tbsys::CNetUtil::addrToString(slave_server_id).c_str(),
               util::string_util::mur_mur_hash(packet->data, packet->size));

      if(connmgr->sendPacket(slave_server_id, packet, this, NULL) == false) {
        log_error("Send ResponseGetServerTablePacket %s failure.",
                  tbsys::CNetUtil::addrToString(slave_server_id).c_str());
        delete packet;
      }
    }
    // do_get_server_table_packet
    void server_conf_thread::
      do_get_server_table_packet(request_get_server_table * req,
                                 response_get_server_table * resp)
    {
      if(req->config_version == GROUP_CONF) {
        read_group_file_to_packet(resp);
        return;
      }

      group_info_map::iterator it;
      group_info_rw_locker.rdlock();
      it = group_info_map_data.find(req->group_name);
      if(it == group_info_map_data.end()) {
        group_info_rw_locker.unlock();
        return;
      }
      group_info *group_info_founded = it->second;
      const char *data = group_info_founded->get_server_table_data();
      uint32_t size = group_info_founded->get_server_table_size();
      if(size == group_info_founded->get_SVR_table_size()) {
        resp->set_data(data, size);
        log_info("%s get servertable, hashcode: %u",
                 req->group_name,
                 util::string_util::mur_mur_hash(resp->data, resp->size));
      }
      group_info_rw_locker.unlock();
    }

    // do_set_server_table_packet
    bool server_conf_thread::
      do_set_server_table_packet(response_get_server_table * packet)
    {
      if (NULL == packet)
      {
        log_error("do set server table packet is null");
        return false;
      }

      // if config server is default master and now is master role, discard this packet.
      // if config server is not default master and now is master role, accept this packet.
      if (master_config_server_id == util::local_server_ip::ip)
      {
        string peer_ip = tbsys::CNetUtil::addrToString(packet->get_connection()->getPeerId()).c_str();
        size_t peer_pos = peer_ip.find(":");
        string default_m_ip = tbsys::CNetUtil::addrToString(config_server_info_list[0]->server_id).c_str();
        size_t default_m_pos = default_m_ip.find(":");
        log_warn("master conflict. local id: %s, peer id: %s, pos: %u, ip: %s, default id: %s, pos: %u, ip: %s",
            tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str(),
            peer_ip.c_str(), peer_pos, peer_ip.substr(0, peer_pos).c_str(),
            default_m_ip.c_str(), default_m_pos, default_m_ip.substr(0, default_m_pos).c_str());
        if (peer_ip.substr(0, peer_pos) != default_m_ip.substr(0, default_m_pos))
        {
          log_error("master conflict, master ip: %s, local ip: %s, default ip: %s, peer ip: %s,"
              " this packet is not from default master, discard set server table packet",
              tbsys::CNetUtil::addrToString(master_config_server_id).c_str(),
              tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str(),
              tbsys::CNetUtil::addrToString(config_server_info_list[0]->server_id).c_str(),
              tbsys::CNetUtil::addrToString(packet->get_connection()->getPeerId()).c_str());
          return false;
        }
      }

      group_info_rw_locker.wrlock();
      if(packet->type == GROUP_CONF) {
        bool ret = false;
        uint32_t hashcode = 0;
        const char *file_name =
          TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        if(file_name != NULL) {
          ret =
            backup_and_write_file(file_name, packet->data, packet->size,
                                  packet->modified_time);
          if(ret) {
            hashcode =
              util::string_util::mur_mur_hash(packet->data, packet->size);
          }
        }
        group_info_rw_locker.unlock();
        log_info("receive groupFile%s, hashcode: %u", (ret ? "ok" : "error"),
                 hashcode);
        return ret;
      }

      group_info_map::iterator it;
      it = group_info_map_data.find(packet->group_name);
      if(it == group_info_map_data.end()) {
        group_info_rw_locker.unlock();
        return false;
      }
      group_info *group_info_founded = it->second;
      server_info_rw_locker.rdlock();
      bool retv =
        group_info_founded->write_server_table_packet(packet->data,
                                                      packet->size);
      server_info_rw_locker.unlock();
      group_info_rw_locker.unlock();
      return retv;
    }

    // do_group_names_packet
    void server_conf_thread::do_group_names_packet(response_group_names *
                                                   resp)
    {
      //tbsys::CThreadGuard guard(&_mutex);
      group_info_rw_locker.rdlock();

      group_info_map::iterator sit;
      resp->status = (is_ready ? 1 : 0);
      for(sit = group_info_map_data.begin(); sit != group_info_map_data.end();
          ++sit) {
        resp->add_group_name(sit->first);
      }
      group_info_rw_locker.unlock();
    }

    bool server_conf_thread::backup_and_write_file(const char *file_name,
                                                   const char *data, int size,
                                                   int modified_time)
    {
      bool ret = false;
      if(data == NULL)
        return ret;
      if(1) {
        file_mapper mmap_file;
        mmap_file.open_file(file_name);
        char *tdata = (char *) mmap_file.get_data();
        int tsize = mmap_file.get_size();
        //same, not need to write
        if(tdata != NULL && tsize == size && memcmp(tdata, data, size) == 0) {
          log_info("%s not changed, set modify time  to %d", file_name,
                   modified_time);
          if(modified_time > 0) {
            struct utimbuf times;
            times.actime = modified_time;
            times.modtime = modified_time;
            utime(file_name, &times);
          }
          return true;
        }
      }
      //back up first
      char backup_file_name[256];
      time_t t;
      time(&t);
      struct tm *tm =::localtime((const time_t *) &t);
      snprintf(backup_file_name, 256, "%s.%04d%02d%02d%02d%02d%02d.backup",
               file_name, tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
               tm->tm_hour, tm->tm_min, tm->tm_sec);
      rename(file_name, backup_file_name);
      int fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC, 0644);
      if(write(fd, data, size) == size) {
        ret = true;
      }
      close(fd);
      if(ret == false) {
        rename(backup_file_name, file_name);
      }
      else if(modified_time > 0) {
        struct utimbuf times;
        times.actime = modified_time;
        times.modtime = modified_time;
        if(utime(file_name, &times)) {
          log_warn("can not change modified timestamp: %s", file_name);
        }
      }
      if(ret) {
        log_info("back up file  %s to  %s, new file modify time: %d",
                 file_name, backup_file_name, modified_time);
      }
      return ret;
    }
    server_conf_thread::table_builder_thread::
      table_builder_thread(server_conf_thread * serveconf) {
      p_server_conf = serveconf;
      start();
    }
    server_conf_thread::table_builder_thread::~table_builder_thread() {
      stop();
      wait();

    }
    void server_conf_thread::table_builder_thread::run(tbsys::CThread *
                                                       thread, void *arg)
    {
      while(!_stop) {
        vector<group_info *>tmp;
        {
          tbsys::CThreadGuard guard(&(p_server_conf->mutex_grp_need_build));
          tmp = group_need_build;
          group_need_build.clear();
        }
        build_table(tmp);
        sleep(1);
      }
    }
    void server_conf_thread::table_builder_thread::build_table(vector <
                                                               group_info *
                                                               >&groupe_will_builded)
    {
      for(vector<group_info *>::iterator it = groupe_will_builded.begin();
          it != groupe_will_builded.end(); ++it) {
        (*it)->rebuild(p_server_conf->get_slave_server_id(),
                       &(p_server_conf->group_info_rw_locker),
                       &(p_server_conf->server_info_rw_locker));
      }
    }

  }
}
