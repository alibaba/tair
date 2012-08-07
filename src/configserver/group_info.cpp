/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * group_info.cpp is to manage all the infos belong to a group
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "group_info.hpp"
#include "server_info_allocator.hpp"
#include "table_builder1.hpp"
#include "table_builder2.hpp"
//#include "table_builder3.hpp"
#include "get_server_table_packet.hpp"

namespace tair {
  namespace config_server {
    using namespace std;
    using namespace __gnu_cxx;

    group_info::group_info(const char *p_group_name,
                           server_info_map * p_server_info_map,
                           tbnet::ConnectionManager * connmgr)
    {
      this->connmgr = connmgr;
      need_rebuild_hash_table = 0;
      need_send_server_table = 0;
      assert(p_group_name != NULL);
      assert(p_server_info_map != NULL);
      group_name = strdup(p_group_name);
      server_info_maps = p_server_info_map;
      load_config_count = 0;
      min_config_version = TAIR_CONFIG_MIN_VERSION;
      should_syn_mig_info = false;
      group_can_work = true;
      group_is_OK = true;
      build_strategy = 1;
      lost_tolerance_flag = NO_DATA_LOST_FLAG;
      accept_strategy = GROUP_DEFAULT_ACCEPT_STRATEGY;
      bucket_place_flag = 0;
      diff_ratio = 0.6;
      pos_mask = TAIR_POS_MASK;
      server_down_time = TAIR_SERVER_DOWNTIME;
      min_data_server_count = 0;
      pre_load_flag = 0;
      tmp_down_server.clear();
      // server table name
      char server_table_file_name[256];
      const char *sz_data_dir =
        TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_DATA_DIR,
                               TAIR_DEFAULT_DATA_DIR);
        snprintf(server_table_file_name, 256, "%s/%s_server_table",
                 sz_data_dir, group_name);
      if(server_table_manager.open(server_table_file_name))
      {
        if((*server_table_manager.migrate_block_count) > 0) {
          log_info("restore migrate machine list, size: %d",
                   *server_table_manager.migrate_block_count);
          fill_migrate_machine();
        }
        deflate_hash_table();
      }

    }

    int group_info::fill_migrate_machine()
    {
      int migrate_count = 0;
      migrate_machine.clear();
      for(uint32_t i = 0; i < server_table_manager.get_server_bucket_count();
          i++) {
        bool migrate_this_bucket = false;
        if(server_table_manager.m_hash_table[i] != 0) {
          if(server_table_manager.m_hash_table[i] !=
             server_table_manager.d_hash_table[i]) {
            migrate_this_bucket = true;
          }
          else {
            for(uint32_t j = 1; j < server_table_manager.get_copy_count();
                j++) {
              bool found = false;
              for(uint32_t k = 1; k < server_table_manager.get_copy_count();
                  k++) {
                if(server_table_manager.
                   d_hash_table[j *
                                server_table_manager.
                                get_server_bucket_count() + i]
                   == server_table_manager.m_hash_table[k *
                                                        server_table_manager.
                                                        get_server_bucket_count
                                                        () + i]) {
                  found = true;
                  break;
                }
              }
              if(found == false) {
                migrate_this_bucket = true;
                break;
              }
            }
          }
          if(migrate_this_bucket) {
            migrate_machine[server_table_manager.m_hash_table[i]]++;
            migrate_count++;
            log_info("added migrating machine: %s bucket %d ",
                     tbsys::CNetUtil::addrToString(server_table_manager.
                                                   m_hash_table[i]).c_str(),
                     i);
          }
        }
      }
      return migrate_count;
    }
    group_info::~group_info() {
      if(group_name) {
        free(group_name);
      }
      node_info_set::iterator it;
      for(it = node_list.begin(); it != node_list.end(); ++it) {
        delete(*it);
      }
    }
    void group_info::deflate_hash_table()
    {
      server_table_manager.deflate_hash_table();
    }

    uint32_t group_info::get_client_version() const
    {
      return *server_table_manager.client_version;
    }
    uint32_t group_info::get_server_version() const
    {
      return *server_table_manager.server_version;
    }
    uint32_t group_info::get_plugins_version() const
    {
      return *server_table_manager.plugins_version;
    }
    uint32_t group_info::get_area_capacity_version() const
    {
      return *server_table_manager.area_capacity_version;
    }
    const map<uint32_t, uint64_t> &group_info::get_area_capacity_info() const
    {
      return area_capacity_info;
    }
    const uint64_t *group_info::get_hash_table(int mode) const
    {
      if(mode == 0)
        return server_table_manager.hash_table;
      else if(mode == 1) {
        return server_table_manager.m_hash_table;
      }
      return server_table_manager.d_hash_table;
    }
    const char *group_info::get_group_name() const
    {
      return group_name;
    }
    tbsys::STR_STR_MAP * group_info::get_common_map()
    {
      return &common_map;
    }
    const char *group_info::get_hash_table_deflate_data(int mode) const
    {
      if(mode == 0)
        return server_table_manager.hash_table_deflate_data_for_client;
      return server_table_manager.hash_table_deflate_data_for_data_server;
    }
    int group_info::get_hash_table_deflate_size(int mode) const
    {
      if(mode == 0)
        return server_table_manager.hash_table_deflate_data_for_client_size;
      return server_table_manager.
        hash_table_deflate_data_for_data_server_size;
    }
    // server_list=ip:port,ip:port;...
    void group_info::parse_server_list(node_info_set & list,
                                       const char *server_list,
                                       set<uint64_t> &server_id_list)
    {
      log_debug("parse server list: %s", server_list);
      char *str = strdup(server_list);

      pair <node_info_set::iterator, bool> ret;
      vector <char *>node_list;
      tbsys::CStringUtil::split(str, ";", node_list);
      int default_port =
        TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PORT,
                            TAIR_SERVER_DEFAULT_PORT);
      for(uint32_t i = 0; i < node_list.size(); i++) {
        vector<char *>ip_list;
        tbsys::CStringUtil::split(node_list[i], ",", ip_list);
        server_info *sinfo;
        for(uint32_t j = 0; j < ip_list.size(); j++) {
          uint64_t id = tbsys::CNetUtil::strToAddr(ip_list[j], default_port);
          if(id == 0)
            continue;
          server_id_list.insert(id);
          server_info_map::iterator it = server_info_maps->find(id);
          sinfo = NULL;
          if(it == server_info_maps->end()) {
            sinfo =
              server_info_allocator::server_info_allocator_instance.
              new_server_info(this, id);
            server_info_maps->insert(server_info_map::value_type(id, sinfo));
          }
          else {
            sinfo = it->second;
            log_debug("add server: %s", ip_list[j]);
            break;
          }
        }

        node_info *node = new node_info(sinfo);
        if(sinfo->group_info_data != this
           || sinfo->status == server_info::DOWN) {
          sinfo->last_time = 0;
          if(sinfo->group_info_data != this) {
            sinfo->group_info_data->node_list.erase(node);
            sinfo->group_info_data = this;
          }
        }
        ret = list.insert(node);
        if(!ret.second) {
          delete node;
        }
        else {
          sinfo->node_info_data = node;
        }
      }
      free(str);
    }
    void group_info::parse_plugins_list(vector<const char *>&plugins)
    {
      set<string> plugins_set;
      for(vector<const char *>::iterator it = plugins.begin();
          it != plugins.end(); it++) {
        log_debug("parse PlugIns list: %s", *it);
        vector<char *>plugin_list;
        char tmp_str[strlen(*it) + 1];
        strcpy(tmp_str, *it);
        tbsys::CStringUtil::split(tmp_str, ";", plugin_list);
        for(uint32_t i = 0; i < plugin_list.size(); i++) {
          string plugin_name = plugin_list[i];
          if(plugin_name != "") {
            plugins_set.insert(plugin_name);
          }
        }
      }
      if(plugins_set != plugins_name_info) {
        plugins_name_info = plugins_set;
        inc_version(server_table_manager.plugins_version);
      }

    }
    //input will be like  area,capacity;area,capacity
    void group_info::parse_area_capacity_list(vector<const char *>&a_c)
    {
      map <uint32_t, uint64_t> area_capacity_info_tmp;
      for(vector<const char *>::iterator it = a_c.begin();
          it != a_c.end(); it++) {
        log_debug("parse area capacity list: %s", *it);
        vector<char *>info;
        char tmp_str[strlen(*it) + 1];
        strcpy(tmp_str, *it);
        tbsys::CStringUtil::split(tmp_str, ";", info);
        for(uint32_t i = 0; i < info.size(); i++) {
          char *p = strchr(info[i], ',');
          if(p) {
            uint32_t area = strtoll(info[i], NULL, 10);
            uint64_t capacity = strtoll(p + 1, NULL, 10);
            area_capacity_info_tmp[area] = capacity;
            log_debug("area %u capacity %" PRI64_PREFIX "u", area, capacity);
          }

        }
      }
      bool need_update = false;
      if(area_capacity_info.size() != area_capacity_info_tmp.size()) {
        need_update = true;
      }
      else {
        for(map<uint32_t, uint64_t>::iterator new_it =
            area_capacity_info_tmp.begin();
            new_it != area_capacity_info_tmp.end(); new_it++) {
          map<uint32_t, uint64_t >::iterator old_it =
            area_capacity_info.find(new_it->first);
          if(old_it == area_capacity_info.end()
             || new_it->second != old_it->second) {
            need_update = true;
            break;
          }
        }
      }
      if(need_update) {
        area_capacity_info = area_capacity_info_tmp;
        map<uint32_t, uint64_t>::iterator it = area_capacity_info.begin();
        for(; it != area_capacity_info.end(); it++) {
          log_info("set area %u capacity is %" PRI64_PREFIX "u", it->first,
                   it->second);
        }
        inc_version(server_table_manager.area_capacity_version);
      }
    }
    void group_info::correct_server_info(bool is_sync)
    {
      set <uint64_t> live_machines;
      const uint64_t *p = get_hash_table(2);
      //get dest hash table. all server avialbe should in this table. we believe bucket count > machine count;
      for(uint32_t i = 0; i < get_hash_table_size(); ++i) {
        if(p[i] != 0) {
          live_machines.insert(p[i]);
        }
      }
      if(!live_machines.empty()) {
        node_info_set::iterator sit;
        for(sit = node_list.begin(); sit != node_list.end(); ++sit) {
          set<uint64_t >::iterator it =
            live_machines.find((*sit)->server->server_id);
          if(it == live_machines.end()) {
            if(is_sync) {
              (*sit)->server->status = server_info::DOWN;
              //illegal server
              log_warn("set server %s status is down",
                        tbsys::CNetUtil::addrToString((*sit)->server->
                                                      server_id).c_str());

              if (get_pre_load_flag() == 1)
              {
                add_down_server((*sit)->server->server_id);
              }
            }
          }
          else {
            (*sit)->server->status = server_info::ALIVE;
          }
        }
      }
    }
    bool group_info::load_config(tbsys::CConfig & config, uint32_t version,
                                 set<uint64_t> &server_id_list)
    {
      load_config_count++;
      node_info_set server_list;
      tbsys::STR_STR_MAP common_map_tmp;

      data_need_move =
        config.getInt(group_name, TAIR_STR_GROUP_DATA_NEED_MOVE,
                      TAIR_DATA_NEED_MIGRATE);
      int bucket_count =
        config.getInt(group_name, TAIR_STR_BUCKET_NUMBER,
                      TAIR_DEFAULT_BUCKET_NUMBER);
      int copy_count =
        config.getInt(group_name, TAIR_STR_COPY_COUNT,
                      TAIR_DEFAULT_COPY_COUNT);
      if(!server_table_manager.is_file_opened()) {
        char file_name[256];
        const char *sz_data_dir =
          TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_DATA_DIR,
                                 TAIR_DEFAULT_DATA_DIR);
        snprintf(file_name, 256, "%s/%s_server_table", sz_data_dir,
                 group_name);

        bool tmp_ret = server_table_manager.
          create(file_name, bucket_count, copy_count);
        assert(tmp_ret);

        log_info("set bucket count = %u ok",
                 server_table_manager.get_server_bucket_count());
        log_info("set copy count = %u ok",
                 server_table_manager.get_copy_count());
      }
      if(server_table_manager.get_copy_count() > 1
         && data_need_move != TAIR_DATA_NEED_MIGRATE) {
        data_need_move = TAIR_DATA_NEED_MIGRATE;
        log_error("set _dataNeedMove to %d", TAIR_DATA_NEED_MIGRATE);
      }

      log_debug("loadconfig, version: %d, lastVersion: %d", version,
                *server_table_manager.last_load_config_time);
      min_config_version =
        config.getInt(group_name, TAIR_STR_MIN_CONFIG_VERSION,
                      TAIR_CONFIG_MIN_VERSION);
      if(min_config_version < TAIR_CONFIG_MIN_VERSION) {
        log_error("%s must large than %d. set it to %d",
                  TAIR_STR_MIN_CONFIG_VERSION, TAIR_CONFIG_MIN_VERSION,
                  TAIR_CONFIG_MIN_VERSION);
        min_config_version = TAIR_CONFIG_MIN_VERSION;
      }

      server_down_time =
        config.getInt(group_name, TAIR_STR_SERVER_DOWNTIME,
                      TAIR_SERVER_DOWNTIME);

      vector<const char *>str_list =
        config.getStringList(group_name, TAIR_STR_SERVER_LIST);
      for(uint32_t i = 0; i < str_list.size(); i++) {
        parse_server_list(server_list, str_list[i], server_id_list);
      }

      float f_min_data_server_count =
        strtof(config.
               getString(group_name, TAIR_STR_MIN_DATA_SRVER_COUNT, "0"),
               NULL);
      if(f_min_data_server_count < 1 && f_min_data_server_count > 0) {
        min_data_server_count =
          (uint32_t) (f_min_data_server_count * server_list.size());
      }
      else {
        if(f_min_data_server_count < 0)
          f_min_data_server_count = 0;
        min_data_server_count = (uint32_t) f_min_data_server_count;
      }
      if(min_data_server_count < server_table_manager.get_copy_count()) {
        log_error("%s must large than %d. set it to %d",
                  TAIR_STR_MIN_DATA_SRVER_COUNT,
                  server_table_manager.get_copy_count(),
                  server_table_manager.get_copy_count());
        min_data_server_count = server_table_manager.get_copy_count();
      }
      log_info(" %s = %d", TAIR_STR_MIN_DATA_SRVER_COUNT,
               min_data_server_count);

      int old_build_strategy = build_strategy;
      build_strategy =
        config.getInt(group_name, TAIR_STR_BUILD_STRATEGY,
                      TAIR_BUILD_STRATEGY);

      //modify accept_strategy & lost_tolerance_flag : no need to rebulid table
      accept_strategy =
        static_cast<GroupAcceptStrategy>(config.getInt(group_name, TAIR_STR_ACCEPT_STRATEGY,
                      static_cast<int>(GROUP_DEFAULT_ACCEPT_STRATEGY)));
      lost_tolerance_flag =
        static_cast<DataLostToleranceFlag>(config.getInt(group_name, TAIR_STR_DATALOST_FLAG,
                      static_cast<int>(NO_DATA_LOST_FLAG)));

      bucket_place_flag =
        config.getInt(group_name, TAIR_STR_BUCKET_DISTRI_FLAG,
                      0);

      log_info("build_strategy: %d, accept_strategy: %d, lost_tolerance_flag: %d, bucket_place_flag: %d",
          build_strategy, static_cast<int>(accept_strategy), static_cast<int>(lost_tolerance_flag), bucket_place_flag);

      float old_ratio = diff_ratio;
      diff_ratio =
        strtof(config.
               getString(group_name, TAIR_STR_BUILD_DIFF_RATIO,
                         TAIR_BUILD_DIFF_RATIO), NULL);
      uint64_t old_pos_mask = pos_mask;
      pos_mask =
        strtoll(config.getString(group_name, TAIR_STR_POS_MASK, "0"), NULL,
                10);
      if(pos_mask == 0)
        pos_mask = TAIR_POS_MASK;
      log_info(" %s = %llX", TAIR_STR_POS_MASK, pos_mask);

      pre_load_flag = config.getInt(group_name, TAIR_PRE_LOAD_FLAG, 0);
      const char* down_servers = config.getString(group_name, TAIR_TMP_DOWN_SERVER, "");
      log_debug("pre load flag: %d", pre_load_flag);
      log_debug("down servers: %s", down_servers);
      if (pre_load_flag == 0)
      {
        if (strlen(down_servers) != 0)
        {
          log_error("no %s config or config 0, tmp down server %s config is ignored", TAIR_PRE_LOAD_FLAG, down_servers);
        }
        tmp_down_server.clear();
      }
      else
      {
        // parse down server list and init tmp_down_server
        vector<char*> down_server_vec;
        char* tmp_servers = new char[strlen(down_servers) + 1];
        strcpy(tmp_servers, down_servers);
        tbsys::CStringUtil::split(tmp_servers, ";", down_server_vec);
        vector<char*>::iterator vit;
        for (vit = down_server_vec.begin(); vit != down_server_vec.end(); ++vit)
        {
          tmp_down_server.insert(tbsys::CNetUtil::strToAddr(*vit, 0));
        }
        delete []tmp_servers;
      }


      vector<string> key_list;
      config.getSectionKey(group_name, key_list);
      for(uint32_t i = 0; i < key_list.size(); i++) {
        const char *key = key_list[i].c_str();
        if(key == NULL || key[0] == '_')
          continue;
        common_map_tmp[key] = config.getString(group_name, key);
      }
      vector<const char *>str_plugins_list =
        config.getStringList(group_name, TAIR_STR_PLUGINS_LIST);
      parse_plugins_list(str_plugins_list);
      vector<const char *>str_area_capacity_list =
        config.getStringList(group_name, TAIR_STR_AREA_CAPACITY_LIST);
      parse_area_capacity_list(str_area_capacity_list);

      if(*server_table_manager.last_load_config_time >= version) {
        log_info("[%s] config file unchanged, load config count: %d",
                 group_name, load_config_count);
        if(load_config_count == 1) {
          common_map = common_map_tmp;
          node_list = server_list;
        }
        return true;
      }

      struct timespec tm;
      clock_gettime(CLOCK_MONOTONIC, &tm);
      time_t now = tm.tv_sec;
      need_rebuild_hash_table = now;
      interval_seconds =
        config.getInt(group_name, TAIR_STR_REPORT_INTERVAL,
                      TAIR_DEFAULT_REPORT_INTERVAL);
      *server_table_manager.last_load_config_time = version;

      bool changed1 = false;
      bool changed2 = false;

      if(common_map_tmp.size() == common_map.size()) {
        for(tbsys::STR_STR_MAP::iterator it = common_map_tmp.begin();
            it != common_map_tmp.end(); ++it) {
          tbsys::STR_STR_MAP::iterator it1 = common_map.find(it->first);
          if(it1 == common_map.end() || it1->second != it->second) {
            changed1 = true;
            break;
          }
        }
      }
      else {
        changed1 = true;
      }

      if(server_list.size() == node_list.size()) {
        node_info_set::iterator it1 = node_list.begin();
        for(; it1 != node_list.end(); it1++) {
          node_info_set::iterator it = server_list.find(*it1);
          if(it == server_list.end()) {
            changed2 = true;
            break;
          }
        }
      }
      else {
        changed2 = true;
      }

      for(node_info_set::iterator it = server_list.begin();
          it != server_list.end(); ++it) {
        if((*it)->server->status == server_info::DOWN) {
          if(tbnet::ConnectionManager::isAlive((*it)->server->server_id)) {
            (*it)->server->status = server_info::ALIVE;
            (*it)->server->last_time = now;
            log_debug("set server %s alive lastTime = %u",
                      tbsys::CNetUtil::addrToString((*it)->server->server_id).
                      c_str(), now);
            changed2 = true;
          }
        }
      }

      if(old_build_strategy != build_strategy) {
        need_rebuild_hash_table = now;
      }
      if(old_pos_mask != pos_mask) {
        need_rebuild_hash_table = now;
      }

      float min =
        old_ratio >
        diff_ratio ? old_ratio - diff_ratio : diff_ratio - old_ratio;
      if(build_strategy != 1 && min > 0.0001) {
        need_rebuild_hash_table = now;
      }

      if(changed1 || changed2) {
        if(changed1) {
          common_map.clear();
          common_map = common_map_tmp;
          inc_version(server_table_manager.client_version);
          inc_version(server_table_manager.server_version);
          log_warn("[%s] version changed: clientVersion: %u", group_name,
                   *server_table_manager.client_version);
        }

        if(changed2) {
          node_info_set::iterator it;
          for(it = node_list.begin(); it != node_list.end(); ++it) {
            delete(*it);
          }
          node_list.clear();
          node_list = server_list;

          // update node stat info
          stat_info_rw_locker.wrlock();
          server_info tmp_server_info;
          node_info tmp_node_info(&tmp_server_info);
          for (std::map<uint64_t, node_stat_info>::iterator stat_it = stat_info.begin(); stat_it != stat_info.end();)
          {
            tmp_node_info.server->server_id = stat_it->first;
            // This stat info is not needed any more.
            // NOTE: We only clear stat info here, 'cause this ds has been removed from _server_list,
            //       we do nothing when ds is DOWN (maybe also clear).
            if (node_list.find(&tmp_node_info) == node_list.end())
            {
              log_info("erase stat info of server: %s", tbsys::CNetUtil::addrToString(stat_it->first).c_str());
              stat_info.erase(stat_it++);
            }
            else
            {
              ++stat_it;
            }
          }
          stat_info_rw_locker.unlock();

          log_warn("nodeList changed");
          need_rebuild_hash_table = now;
        }
      }
      return true;
    }
    // isNeedRebuild
    bool group_info::is_need_rebuild() const
    {
      if(need_rebuild_hash_table == 0)
        return false;
      struct timespec tm;
      clock_gettime(CLOCK_MONOTONIC, &tm);
      time_t now = tm.tv_sec;
      if(need_rebuild_hash_table < now - get_server_down_time())
          return true;
        return false;
    }
    void group_info::get_up_node(set<node_info *>& upnode_list)
    {
      struct timespec tm;
      clock_gettime(CLOCK_MONOTONIC, &tm);
      time_t now = tm.tv_sec;

      now -= (get_server_down_time() + 1) / 2;
      node_info_set::iterator it;
      available_server.clear();
      for(it = node_list.begin(); it != node_list.end(); ++it) {
        node_info *node = (*it);
        // 1 means down, 0 alive, 2 initing, 3 synced
        if(node->server->status != server_info::DOWN) {
          if((node->server->last_time < now && (node->server->status == server_info::ALIVE && !tbnet::ConnectionManager::isAlive(node->server->server_id)))
             || node->server->status == server_info::FORCE_DOWN) {
            log_info("node: <%s> DOWN",
                     tbsys::CNetUtil::addrToString(node->server->server_id).c_str());
            node->server->status = server_info::DOWN;

            if (get_pre_load_flag() == 1)
            {
              add_down_server(node->server->server_id);
            }
          }
          else {
            log_info("get up node: <%s>",
                     tbsys::CNetUtil::addrToString(node->server->server_id).c_str());
            upnode_list.insert(node);
            available_server.insert(node->server->server_id);
          }
        }
      }
    }
    void group_info::rebuild(uint64_t slave_server_id,
                             tbsys::CRWSimpleLock * p_grp_locker,
                             tbsys::CRWSimpleLock * p_server_locker)
    {
      log_warn("start rebuild table of %s, build_strategy: %d", group_name, build_strategy);
      set<node_info *>upnode_list;
      table_builder::hash_table_type quick_table_tmp;
      table_builder::hash_table_type hash_table_for_builder_tmp;        // will put rack info to this
      table_builder::hash_table_type dest_hash_table_for_builder_tmp;
      bool first_run = true;
      p_grp_locker->wrlock();
      p_server_locker->rdlock();
      get_up_node(upnode_list);
      p_server_locker->unlock();
      int size = upnode_list.size();
      log_info("[%s] upnodeList.size = %d", group_name, size);
      for(uint32_t i = 0; i < server_table_manager.get_hash_table_size(); i++) {
        if(server_table_manager.m_hash_table[i])
          first_run = false;
      }
      if(size < (int) min_data_server_count) {
        if(first_run == false) {
          log_error("can not get enough data servers. need %d lef %d ",
                    min_data_server_count, size);
        }
        inc_version(server_table_manager.client_version);
        inc_version(server_table_manager.server_version);
        send_server_table_packet(slave_server_id);
        p_grp_locker->unlock();
        return;
      }

      table_builder *p_table_builder;
      if (build_strategy == 1) {
        p_table_builder =
          new table_builder1(server_table_manager.get_server_bucket_count(),
                             server_table_manager.get_copy_count());
      }
      else if (build_strategy == 2) {
        p_table_builder =
          new table_builder2(diff_ratio,
                             server_table_manager.get_server_bucket_count(),
                             server_table_manager.get_copy_count());
      }
      else if (build_strategy == 3) {
        int tmp_strategy = select_build_strategy(upnode_list);
        if (tmp_strategy == 1)
        {
          p_table_builder =
            new table_builder1(server_table_manager.get_server_bucket_count(),
                server_table_manager.get_copy_count());
        }
        else if (tmp_strategy == 2)
        {
          p_table_builder =
            new table_builder2(diff_ratio,
                server_table_manager.get_server_bucket_count(),
                server_table_manager.get_copy_count());
        }
        else
        {
          log_error("can not find the table_builder object, build strategy: %d, tmp stategy: %d\n", build_strategy, tmp_strategy);
          p_grp_locker->unlock();
          return;
        }
      }
      else
      {
        log_error("can not find the table_builder object, build strategy: %d\n", build_strategy);
        p_grp_locker->unlock();
        return ;
      }

      p_table_builder->set_pos_mask(pos_mask);
      p_table_builder->set_bucket_place_flag(bucket_place_flag);
      p_table_builder->set_available_server(upnode_list);
      p_table_builder->set_data_lost_flag(lost_tolerance_flag);
#ifdef TAIR_DEBUG
      log_debug("print available server");
      p_table_builder->print_available_server();
#endif
      // use current mhash table
      p_table_builder->load_hash_table(hash_table_for_builder_tmp,
                                       server_table_manager.m_hash_table);
      quick_table_tmp = hash_table_for_builder_tmp;
      if((data_need_move == TAIR_DATA_NEED_MIGRATE
          && server_table_manager.get_copy_count() > 1)
         && first_run == false) {
        log_debug("will build quick table");
        if(p_table_builder->build_quick_table(quick_table_tmp) == false) {
          set_group_is_OK(false);
          delete p_table_builder;
          inc_version(server_table_manager.client_version);
          inc_version(server_table_manager.server_version);
          send_server_table_packet(slave_server_id);
          p_grp_locker->unlock();
          return;
        }
        log_debug("quick table build ok");
      }
      p_grp_locker->unlock();
      int ret =
        p_table_builder->rebuild_table(hash_table_for_builder_tmp,
                                       dest_hash_table_for_builder_tmp, true);
      p_grp_locker->wrlock();
      if(ret == 0) {
        log_error("build table fail. fatal error.");
        set_group_status(false);
        set_group_is_OK(false);
        delete p_table_builder;
        inc_version(server_table_manager.client_version);
        inc_version(server_table_manager.server_version);
        send_server_table_packet(slave_server_id);
        p_grp_locker->unlock();
        return;
      }

      p_table_builder->write_hash_table(dest_hash_table_for_builder_tmp,
                                        server_table_manager.d_hash_table);
      if((data_need_move == TAIR_DATA_NEED_MIGRATE) && first_run == false) {
        log_info("need migrate write quick table to _p_hashTable");
        p_table_builder->write_hash_table(quick_table_tmp,
                                          server_table_manager.m_hash_table);
        p_table_builder->write_hash_table(quick_table_tmp,
                                          server_table_manager.hash_table);
        if(server_table_manager.get_copy_count() <= 1) {
          for(uint32_t i = 0;
              i <
              server_table_manager.get_server_bucket_count() *
              server_table_manager.get_copy_count(); i++) {
            if(available_server.find(server_table_manager.m_hash_table[i])
               == available_server.end()) {
              server_table_manager.m_hash_table[i] =
                server_table_manager.hash_table[i] =
                server_table_manager.d_hash_table[i];
            }
          }
        }
        else if (lost_tolerance_flag == ALLOW_DATA_LOST_FALG) {
          for (uint32_t i = 0; i < server_table_manager.get_server_bucket_count(); ++i) {
            // if master bucket is not available, use d_hash_table to fill m_hash_table
            if (available_server.find(server_table_manager.m_hash_table[i]) == available_server.end()) {
              log_info("index %u:%s in m_hash_table is invalid, use d_hash_table: %s",
                  i, tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[i]).c_str(),
                  tbsys::CNetUtil::addrToString(server_table_manager.d_hash_table[i]).c_str());
              for (uint32_t j = 0; j < server_table_manager.get_copy_count(); ++j) {
                uint32_t bucket_index = i + j * server_table_manager.get_server_bucket_count();
                server_table_manager.m_hash_table[bucket_index] =
                  server_table_manager.hash_table[bucket_index] =
                  server_table_manager.d_hash_table[bucket_index];
              }
            }
          }
        }
      }
      else {
        log_info("need`t migrate write original table to _p_hashTable");
        p_table_builder->write_hash_table(dest_hash_table_for_builder_tmp,
                                          server_table_manager.m_hash_table);
        p_table_builder->write_hash_table(dest_hash_table_for_builder_tmp,
                                          server_table_manager.hash_table);
      }
#ifdef TAIR_DEBUG
      log_info("_confServerTableManager._p_hashTable");
      for(uint32_t i = 0;
          i <
          server_table_manager.get_server_bucket_count() *
          server_table_manager.get_copy_count(); i++)
        log_info("+++ line: %d bucket: %d => %s",
                 i / server_table_manager.get_server_bucket_count(), i % server_table_manager.get_server_bucket_count(),
                 tbsys::CNetUtil::addrToString(server_table_manager.
                                               hash_table[i]).c_str());
      log_info("_mhashTable");
      for(uint32_t i = 0;
          i <
          server_table_manager.get_server_bucket_count() *
          server_table_manager.get_copy_count(); i++)
        log_info("+++ line: %d bucket: %d => %s",
                 i / server_table_manager.get_server_bucket_count(), i % server_table_manager.get_server_bucket_count(),
                 tbsys::CNetUtil::addrToString(server_table_manager.
                                               m_hash_table[i]).c_str());
      log_info("_dhashTable");
      for(uint32_t i = 0;
          i <
          server_table_manager.get_server_bucket_count() *
          server_table_manager.get_copy_count(); i++)
        log_info("+++ line: %d bucket: %d => %s",
                 i / server_table_manager.get_server_bucket_count(), i % server_table_manager.get_server_bucket_count(),
                 tbsys::CNetUtil::addrToString(server_table_manager.
                                               d_hash_table[i]).c_str());
#endif
      //_p_hashTable is the table for client and data server to use now,
      //_p_m_hashTable is the state of migration process
      //_p_d_hashTable is the table when migration finished correctly
      //migration is a process which move data based on _p_m_hashTable to data based on _p_d_hashTable
      //when the migration is finished _p_hashTable will be covered by _p_d_hashTable, so client can use the new table
      //but if something interrupt the migrationg process, for example lost a data server,
      //we will use _p_m_hashTable to build a new _p_d_hashTable.
      //data server will receive _p_hashTable and  _p_d_hashTable so they know how to start a migration.
      //client will receive _p_hashTable they will use this table to decide which data server they should ask for a key.
      int diff_count = 0;
      if(first_run == false) {
        diff_count = fill_migrate_machine();
      }
      (*server_table_manager.migrate_block_count) =
        diff_count > 0 ? diff_count : -1;
      inc_version(server_table_manager.client_version);
      inc_version(server_table_manager.server_version);
      inc_version(server_table_manager.area_capacity_version);        //data server count changed, so capacity for every data server will be change at the same time
      log_warn("[%s] version changed: clientVersion: %u",
               group_name, *server_table_manager.client_version,
               *server_table_manager.server_version);
      deflate_hash_table();
      server_table_manager.sync();
      {
        //will caculate different ratio
        diff_count = 0;
        for(uint32_t i = 0;
            i < server_table_manager.get_server_bucket_count(); i++) {
          for(uint32_t j = 0; j < server_table_manager.get_copy_count(); j++) {
            bool migrate_it = true;
            for(uint32_t k = 0; k < server_table_manager.get_copy_count();
                ++k) {
              if(server_table_manager.
                 d_hash_table[i +
                              server_table_manager.get_server_bucket_count() *
                              j] ==
                 server_table_manager.m_hash_table[i +
                                                   server_table_manager.
                                                   get_server_bucket_count() *
                                                   k]) {
                migrate_it = false;
                break;
              }
            }
            if(migrate_it) {
              diff_count++;
            }
          }
        }
      }
      log_info("[%s] different ratio is : migrateCount:%d/%d=%.2f%%",
               group_name, diff_count,
               server_table_manager.get_server_bucket_count() *
               server_table_manager.get_copy_count(),
               100.0 * diff_count /
               (server_table_manager.get_server_bucket_count() *
                server_table_manager.get_copy_count()));

      if(diff_count > 0) {
        log_warn("need migrate, count: %d", diff_count);
      }

      print_server_count();
      send_server_table_packet(slave_server_id);
      p_grp_locker->unlock();
      set_group_status(true);
      set_group_is_OK(true);
      delete p_table_builder;
      return;
    }

    bool group_info::do_finish_migrate(uint64_t server_id,
                                       uint32_t server_version, int bucket_id,
                                       uint64_t slave_server_id)
    {
      if(data_need_move != TAIR_DATA_NEED_MIGRATE)
        return false;
      log_info
        ("migrate finish: from[%s] base version: [%d], now version: [%d] ",
         tbsys::CNetUtil::addrToString(server_id).c_str(), server_version,
         *server_table_manager.server_version);
      if(server_version != *server_table_manager.server_version) {
        log_error("migrate version conflict, ds version: %d, cs version: %d", server_version,
            *server_table_manager.server_version);
        return false;
      }
      bool ret_code = true;        //migrate ok
      if(ret_code) {
        set_migrating_hashtable(bucket_id, server_id);
      }
      //else {
      //  set_force_rebuild();
      //}
      return true;
    }
    bool group_info::check_migrate_complete(uint64_t slave_server_id)
    {
      if(data_need_move != TAIR_DATA_NEED_MIGRATE
         || (*server_table_manager.migrate_block_count) == -1) {
        return true;
      }
      if(migrate_machine.size() > 0U) {
        map<uint64_t, int>::iterator it;
        for(it = migrate_machine.begin(); it != migrate_machine.end(); ++it) {
          server_info_map::iterator ait = server_info_maps->find(it->first);
          if(ait == server_info_maps->end()
             || ait->second->status == server_info::DOWN) {
            log_info("migrate machine %s down",
                     tbsys::CNetUtil::addrToString(it->first).c_str());
            return true;
          }
        }
        log_info("[%s] still have %d data server are in migrating.",
                 group_name, migrate_machine.size());
        if(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG) {
          for(it = migrate_machine.begin(); it != migrate_machine.end(); it++) {
            log_debug
              ("server %s still have %d buckets waiting to be migrated",
               tbsys::CNetUtil::addrToString(it->first).c_str(), it->second);
          }
        }
        if(should_syn_mig_info) {
          send_server_table_packet(slave_server_id);
          should_syn_mig_info = false;
        }
        return false;
      }
      assert(memcmp(server_table_manager.m_hash_table,
                    server_table_manager.d_hash_table,
                    server_table_manager.get_hash_table_byte_size()) == 0);
      inc_version(server_table_manager.client_version);
      inc_version(server_table_manager.server_version);
      log_warn("[%s] version changed: clientVersion: %u",
               group_name, *server_table_manager.client_version,
               *server_table_manager.server_version);
      memcpy((void *) server_table_manager.hash_table,
             (const void *) server_table_manager.d_hash_table,
             server_table_manager.get_hash_table_byte_size());
      deflate_hash_table();
      (*server_table_manager.migrate_block_count) = -1;
      server_table_manager.sync();
      log_warn("migrate all done");
      log_info("[%s] migrating is completed", group_name);
      reported_serverid.clear();
      print_server_count();
      should_syn_mig_info = false;
      send_server_table_packet(slave_server_id);
      return true;
    }
    void group_info::set_force_rebuild()
    {
      need_rebuild_hash_table = 1;
    }
    void group_info::print_server_count()
    {
      hash_map<uint64_t, int, hash<int> >m;
      for(uint32_t i = 0; i < server_table_manager.get_server_bucket_count();
          i++) {
        m[server_table_manager.hash_table[i]]++;
      }
      for(hash_map<uint64_t, int, hash<int> >::iterator it = m.begin();
          it != m.end(); ++it) {
        log_info("[%s] %s => %d", group_name,
                 tbsys::CNetUtil::addrToString(it->first).c_str(),
                 it->second);
      }
    }
    void group_info::send_server_table_packet(uint64_t slave_server_id)
    {
      if(slave_server_id == 0)
        return;

      response_get_server_table *packet = new response_get_server_table();
      packet->set_data(server_table_manager.get_data(),
                       server_table_manager.get_size());
      packet->set_group_name(group_name);

      log_info("[%s] send servertable to %s: hashcode: %u", group_name,
               tbsys::CNetUtil::addrToString(slave_server_id).c_str(),
               util::string_util::mur_mur_hash(packet->data, packet->size));

      if(connmgr->sendPacket(slave_server_id, packet, this, NULL) == false) {
        log_error("[%s] Send ResponseGetServerTablePacket %s failure.",
                  group_name,
                  tbsys::CNetUtil::addrToString(slave_server_id).c_str());
        delete packet;
      }
    }
    bool group_info::write_server_table_packet(char *data, int size)
    {
      char *mdata = server_table_manager.get_data();
      // header len
      int hlen = ((char *) server_table_manager.hash_table) - mdata;
      if(server_table_manager.get_size() == size && mdata != NULL
         && data != NULL && hlen < size) {
        memcpy(mdata + hlen, data + hlen, size - hlen);
        deflate_hash_table();
        memcpy(mdata, data, hlen);
        server_table_manager.sync();
        (*server_table_manager.last_load_config_time) = 0;        // force to reload config file, since the file is changed
        log_info
          ("[%s] accept servertable successed, hashcode: %u, migrateBlockCount: %d, "
           "version changed: clientVersion: %u",
           group_name, util::string_util::mur_mur_hash(mdata, size),
           *server_table_manager.migrate_block_count,
           *server_table_manager.client_version,
           *server_table_manager.server_version);
        log_info("clear local migrate machines");
        if((*server_table_manager.migrate_block_count) > 0) {
          fill_migrate_machine();
        }
        need_rebuild_hash_table = 0;
        correct_server_info(true);
        find_available_server();
        return true;
      }
      log_error("size does not match, check group.conf");
      return false;
    }
    tbnet::IPacketHandler::HPRetCode group_info::handlePacket(tbnet::Packet *
                                                              packet,
                                                              void *args) {
      if(!packet->isRegularPacket()) {
        tbnet::ControlPacket * cp = (tbnet::ControlPacket *) packet;
        log_info("[%s] syn servertable, controlPacket, cmd:%d", group_name,
                 cp->getCommand());
      }
      else {
        delete packet;
      }
      return tbnet::IPacketHandler::KEEP_CHANNEL;
    }
    const char *group_info::get_server_table_data() const
    {
      return (const char *) server_table_manager.mmap_file.get_data();
    }
    int group_info::get_server_table_size() const
    {
      return server_table_manager.mmap_file.get_size();
    }
    bool group_info::is_migrating() const
    {
      return (*server_table_manager.migrate_block_count) != -1;
    }
    void group_info::set_migrating_hashtable(size_t bucket_id,
                                             uint64_t server_id)
    {
      bool ret = false;
      if(bucket_id > server_table_manager.get_server_bucket_count()
         || is_migrating() == false)
        return;
      if(server_table_manager.m_hash_table[bucket_id] != server_id) {
        log_error("where you god this ? old hashtable? bucket_id: %u, m_server: %s, server: %s",
            bucket_id, tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[bucket_id]).c_str(),
            tbsys::CNetUtil::addrToString(server_id).c_str());
        return;
      }
      tbsys::CThreadGuard guard(&hash_table_set_mutex);

      for(uint32_t i = 0; i < server_table_manager.get_copy_count(); i++) {
        uint32_t idx =
          i * server_table_manager.get_server_bucket_count() + bucket_id;
        if(server_table_manager.m_hash_table[idx] !=
           server_table_manager.d_hash_table[idx]) {
          server_table_manager.m_hash_table[idx] =
            server_table_manager.d_hash_table[idx];
          ret = true;
        }
        if(server_table_manager.hash_table[idx] !=
           server_table_manager.d_hash_table[idx]) {
          server_table_manager.hash_table[idx] =
            server_table_manager.d_hash_table[idx];
        }
      }
      if(ret == true) {
        inc_version(server_table_manager.client_version);
        deflate_hash_table();
        log_info("[%s] version changed: clientVersion: %u, serverVersion: %u",
               group_name, *server_table_manager.client_version,
               *server_table_manager.server_version);
        (*server_table_manager.migrate_block_count)--;
        map<uint64_t, int>::iterator it = migrate_machine.find(server_id);
        assert(it != migrate_machine.end());
        it->second--;
        if(it->second == 0) {
          migrate_machine.erase(it);
        }
        log_info("setMigratingHashtable bucketNo %d serverId %s, finish migrate this bucket.",
                 bucket_id, tbsys::CNetUtil::addrToString(server_id).c_str());
        should_syn_mig_info = true;
      }
      return;
    }
    void group_info::hard_check_migrate_complete(uint64_t /* nouse */ )
    {
      if(memcmp(server_table_manager.m_hash_table,
                server_table_manager.d_hash_table,
                server_table_manager.get_hash_table_byte_size()) == 0) {
        migrate_machine.clear();
      }
    }
    void group_info::get_migrating_machines(vector <pair< uint64_t, uint32_t > >&vec_server_id_count) const
    {
      log_debug("machine size = %d", migrate_machine.size());
      map<uint64_t, int>::const_iterator it = migrate_machine.begin();
      for(; it != migrate_machine.end(); ++it)
      {
        vec_server_id_count.push_back(make_pair(it->first, it->second));
      }
    }

    void group_info::inc_version(uint32_t* value, const uint32_t inc_step)
    {
      (*value) += inc_step;
      if(min_config_version > (*value)) {
        (*value) = min_config_version;
      }

    }
    void group_info::find_available_server()
    {
      available_server.clear();
      node_info_set::iterator it;
      for(it = node_list.begin(); it != node_list.end(); ++it) {
        node_info *node = (*it);
        if(node->server->status != server_info::DOWN) {
          available_server.insert(node->server->server_id);
        }
      }
    }
    void group_info::inc_version(const uint32_t inc_step)
    {
      inc_version(server_table_manager.client_version, inc_step);
      inc_version(server_table_manager.server_version, inc_step);

    }
    void group_info::set_stat_info(uint64_t server_id,
                                   const node_stat_info & node_info)
    {
      if(server_id == 0)
        return;
      stat_info_rw_locker.wrlock();
      //map<uint64_t, node_stat_info>::iterator it =
      //  stat_info.find(server_id);
      //if(it == stat_info.end()) {
      //  stat_info_rw_locker.unlock();
      //  stat_info_rw_locker.wrlock();
      //}
      stat_info[server_id] = node_info;
      stat_info_rw_locker.unlock();
      return;
    }
    void group_info::get_stat_info(uint64_t server_id,
                                   node_stat_info & node_info) const
    {
      map<uint64_t, node_stat_info>::const_iterator it;
      stat_info_rw_locker.wrlock();        //here we use write lock so no data server can update its stat info
      if(server_id == 0) {
        for(it = stat_info.begin(); it != stat_info.end(); it++)
        {
          node_info.update_stat_info(it->second);
        }

      }
      else
      {
        it = stat_info.find(server_id);
        if(it != stat_info.end()) {
          node_info.update_stat_info(it->second);
        }
      }
      stat_info_rw_locker.unlock();
      return;
    }

    int group_info::add_down_server(uint64_t server_id)
    {
      int ret = EXIT_SUCCESS;
      std::set<uint64_t>::const_iterator sit = tmp_down_server.find(server_id);
      if (sit == tmp_down_server.end())
      {
        log_debug("add server: %"PRI64_PREFIX"u not exist", server_id);
        // add
        tmp_down_server.insert(server_id);
        // serialize to group.conf
        const char *group_file_name =
          TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        if (group_file_name == NULL) {
          log_error("not found %s:%s ", CONFSERVER_SECTION, TAIR_GROUP_FILE);
          return EXIT_FAILURE;
        }
        assert(group_name != NULL);
        string down_server_list;
        down_server_list.reserve(tmp_down_server.size() * 32);
        for (sit = tmp_down_server.begin(); sit != tmp_down_server.end(); ++sit)
        {
          string str = tbsys::CNetUtil::addrToString(*sit);
          down_server_list += str;
          down_server_list += ';';
        }
        log_debug("current down servers: %s", down_server_list.c_str());
        ret = tair::util::file_util::change_conf(group_file_name, group_name, TAIR_TMP_DOWN_SERVER, down_server_list.c_str());
        if (ret != TAIR_RETURN_SUCCESS)
        {
          log_error("change conf failed, ret: %d, group_file: %s, group_name: %s", ret, group_file_name, group_name);
        }
      }
      else //do nothing
      {
        log_debug("add server: %"PRI64_PREFIX"u exist", server_id);
      }
      return ret;
    }

    // when reset;
    int group_info::clear_down_server(const vector<uint64_t>& server_ids)
    {
      int ret = TAIR_RETURN_SUCCESS;
      log_debug("clear down server. server size: %d, clear size: %d", tmp_down_server.size(), server_ids.size());
      //do it when has down server
      if (!tmp_down_server.empty())
      {
        const char *group_file_name =
          TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        if (group_file_name == NULL) {
          log_error("not found %s:%s ", CONFSERVER_SECTION, TAIR_GROUP_FILE);
          return TAIR_RETURN_FAILED;
        }

        std::string down_servers_str("");

        if (server_ids.empty()) { // mean clear all down servers
          tmp_down_server.clear();
        } else {
          for (vector<uint64_t>::const_iterator it = server_ids.begin(); it != server_ids.end(); ++it) {
            tmp_down_server.erase(*it);
          }
          if (!tmp_down_server.empty()) {
            down_servers_str.reserve(tmp_down_server.size() * 32);
            for (std::set<uint64_t>::iterator sit = tmp_down_server.begin(); sit != tmp_down_server.end(); ++sit)
            {
              down_servers_str.append(tbsys::CNetUtil::addrToString(*sit));
              down_servers_str.append(";");
            }
            log_info("current down servers: %s", down_servers_str.c_str());
          }
        }

        ret = tair::util::file_util::change_conf(group_file_name, group_name, TAIR_TMP_DOWN_SERVER, down_servers_str.c_str());
        if (ret != TAIR_RETURN_SUCCESS)
        {
          log_error("change conf failed, ret: %d, group_file: %s, group_name: %s", ret, group_file_name, group_name);
        }
      }
      return ret;
    }


    int group_info::get_server_down_time() const
    {
      return server_down_time;
    }

    uint32_t group_info::get_server_bucket_count() const
    {
      return server_table_manager.get_server_bucket_count();
    }
    uint32_t group_info::get_copy_count() const
    {
      return server_table_manager.get_copy_count();
    }
    uint32_t group_info::get_hash_table_size() const
    {
      return server_table_manager.get_hash_table_size();
    }
    uint32_t group_info::get_hash_table_byte_size() const
    {
      return server_table_manager.get_hash_table_byte_size();
    }
    uint32_t group_info::get_SVR_table_size() const
    {
      return server_table_manager.get_SVR_table_size();
    }
    void group_info::do_proxy_report(const request_heartbeat & req)
    {
      if(is_migrating()) {
        set<uint64_t>::iterator it = reported_serverid.find(req.server_id);
        if(it == reported_serverid.end()) {
          reported_serverid.insert(req.server_id);        //this will clear when migrate complete.
          log_info("insert reported server: %s, bucket_count: %u",
              tbsys::CNetUtil::addrToString(req.server_id).c_str(), req.vec_bucket_no.size());
          for(size_t i = 0; i < req.vec_bucket_no.size(); i++) {
            set_migrating_hashtable(req.vec_bucket_no[i], req.server_id);
          }
        }
      }

    }

    int group_info::select_build_strategy(const std::set<node_info*>& ava_server)
    {
      int strategy = 2;
      //choose which strategy to select
      //serverid in ava_server will not be repeated
      map<uint32_t, int> pos_count;
      pos_count.clear();
      for(set<node_info *>::const_iterator it = ava_server.begin();
          it != ava_server.end(); it++)
      {
        log_info("mask %"PRI64_PREFIX"u:%s & %"PRI64_PREFIX"u -->%"PRI64_PREFIX"u",
            (*it)->server->server_id, tbsys::CNetUtil::addrToString((*it)->server->server_id).c_str(),
            pos_mask, (*it)->server->server_id & pos_mask);
        (pos_count[(*it)->server->server_id & pos_mask])++;
      }

      if (pos_count.size() <= 1)
      {
        log_warn("pos_count size: %u, table builder3 use build_strategy 1", pos_count.size());
        strategy = 1;
      }
      else
      {
        int pos_max = 0;
        for (map<uint32_t, int>::const_iterator it = pos_count.begin();
            it != pos_count.end(); ++it)
        {
          log_info("pos rack: %d count: %d", it->first, it->second);
          if (it->second > pos_max)
          {
            pos_max = it->second;
          }
        }

        int diff_server = ava_server.size() - pos_max - pos_max;
        if (diff_server <= 0)
          diff_server = 0 - diff_server;
        float ratio = diff_server / (float) pos_max;
        if (ratio > diff_ratio || ava_server.size() < server_table_manager.get_copy_count()
            || ratio > 0.9999)
        {
          log_warn("pos_count size: %u, ratio: %f, table builder3 use build_strategy 1", pos_count.size(), ratio);
          strategy = 1;
        }
        else
        {
          log_warn("pos_count size: %u, ratio: %f, table builder3 use build_strategy 2", pos_count.size(), ratio);
          strategy = 2;
        }
        log_warn("diff_server = %d ratio = %f diff_ratio=%f",
            diff_server, ratio, diff_ratio);

      }

      return strategy;
    }
  }
}
