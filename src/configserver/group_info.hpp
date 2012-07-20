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
 *  Daoan <daoan@taobao.com>
 *
 */
#ifndef TAIR_GROUP_INFO_H
#define TAIR_GROUP_INFO_H
#include <string>
#include <map>
#include <set>
#include "server_info.hpp"
#include "table_builder.hpp"
#include "conf_server_table_manager.hpp"
#include "stat_info.hpp"
#include "heartbeat_packet.hpp"

namespace tair {
  namespace config_server {

    enum GroupAcceptStrategy {
      GROUP_DEFAULT_ACCEPT_STRATEGY = 0,
      GROUP_AUTO_ACCEPT_STRATEGY
    };

    class group_info:public tbnet::IPacketHandler {
    public:
      group_info(const char *group_name, server_info_map * serverInfoMap,
                 tbnet::ConnectionManager * connmgr);
       ~group_info();
      // IPacketHandler interface
        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet * packet,
                                                      void *args);
      uint32_t get_client_version() const;
      uint32_t get_server_version() const;
      uint32_t get_plugins_version() const;
      uint32_t get_area_capacity_version() const;
      const std::map<uint32_t, uint64_t> &get_area_capacity_info() const;
      const uint64_t *get_hash_table(int mode = 0) const;
      const char *get_group_name() const;
        tbsys::STR_STR_MAP * get_common_map();
      const char *get_hash_table_deflate_data(int mode = 0) const;
      int get_hash_table_deflate_size(int mode = 0) const;
      bool is_need_rebuild() const;
      const char *get_server_table_data() const;
      int get_server_table_size() const;
      bool is_migrating() const;
      uint32_t get_server_bucket_count() const;
      uint32_t get_copy_count() const;
      uint32_t get_hash_table_size() const;
      uint32_t get_hash_table_byte_size() const;
      uint32_t get_SVR_table_size() const;
      int get_server_down_time() const;

      bool load_config(tbsys::CConfig & config, uint32_t version,
                       std::set<uint64_t> &server_id_list);
      void correct_server_info(bool is_sync);
      void set_force_rebuild();
      void rebuild(uint64_t slave_server_id,
                   tbsys::CRWSimpleLock * p_grp_locker,
                   tbsys::CRWSimpleLock * p_server_locker);
      bool do_finish_migrate(uint64_t server_id, uint32_t server_version,
                             int bucket_id, uint64_t slave_server_id);
      bool check_migrate_complete(uint64_t slave_server_id);
      bool write_server_table_packet(char *data, int size);
      void set_migrating_hashtable(size_t bucket_no, uint64_t server_id);
      void hard_check_migrate_complete(uint64_t slave_server_id);

      uint32_t get_plugins_count() const
      {
        return plugins_name_info.size();
      }
      std::set<std::string>::const_iterator find_plugin(const std::string & dll_name) const
      {
        return plugins_name_info.find(dll_name);
      }
      std::set<std::string>::const_iterator plugin_end() const
      {
        return plugins_name_info.end();
      }
      std::set<std::string> get_plugins_info() const
      {
        return plugins_name_info;
      }

      int get_data_need_move() const
      {
        return data_need_move;
      }

      GroupAcceptStrategy get_accept_strategy() const
      {
        return accept_strategy;
      }

      bool get_group_status() const
      {
        return group_can_work;
      }
      void set_group_status(bool status)
      {
        group_can_work = status;
      }
      bool get_group_is_OK() const
      {
        return group_is_OK;
      }
      void set_group_is_OK(bool status)
      {
        group_is_OK = status;

      }
      std::set<uint64_t> get_available_server_id() const
      {
        return available_server;
      }

      node_info_set get_node_info() const
      {
        return node_list;
      }

      void get_migrating_machines(vector<pair <uint64_t,uint32_t > >&vec_server_id_count) const;

      void set_table_builded()
      {
        need_rebuild_hash_table = 0;
      }

      void set_force_send_table()
      {
        need_send_server_table = 1;
      }

      void reset_force_send_table()
      {
        need_send_server_table = 0;
      }

      int get_send_server_table() const
      {
        return need_send_server_table;
      }

      int get_pre_load_flag() const
      {
        return pre_load_flag;
      }

      void send_server_table_packet(uint64_t slave_server_id);
      void find_available_server();
      void inc_version(const uint32_t inc_step = 1);
      void do_proxy_report(const request_heartbeat & req);
      void set_stat_info(uint64_t server_id, const node_stat_info &);
      void get_stat_info(uint64_t server_id, node_stat_info &) const;

      int add_down_server(uint64_t server_id);
      int clear_down_server(const vector<uint64_t>& server_ids);
    private:
      group_info(const group_info &);
      group_info & operator =(const group_info &);
      int fill_migrate_machine();
      void deflate_hash_table();
      // parse server list
      void parse_server_list(node_info_set & list, const char *server_list,
                             std::set<uint64_t> &server_id_list);
      void parse_plugins_list(vector<const char *>& plugins);
      void parse_area_capacity_list(vector<const char *>&);
      void get_up_node(std::set<node_info *>&up_node_list);
      void print_server_count();
      void inc_version(uint32_t* value, const uint32_t inc_step = 1);
      int select_build_strategy(const std::set<node_info*>& ava_server);
    private:
      tbsys::STR_STR_MAP common_map;
      node_info_set node_list;

      char *group_name;
      conf_server_table_manager server_table_manager;

      server_info_map *server_info_maps;        //  => server
      int need_rebuild_hash_table;
      int need_send_server_table;
      uint32_t load_config_count;
      int data_need_move;
      uint32_t min_config_version;
      uint32_t min_data_server_count;        // if we can not get at lesat min_data_server_count data servers,
      //config server will stop serve this group;
      std::map<uint64_t, int>migrate_machine;
      tbnet::ConnectionManager * connmgr;
      bool should_syn_mig_info;
      std::set<std::string> plugins_name_info;
      bool group_can_work;
      bool group_is_OK;
      int build_strategy;
      DataLostToleranceFlag lost_tolerance_flag;
      GroupAcceptStrategy accept_strategy;
      int bucket_place_flag;
      int server_down_time;
      float diff_ratio;
      uint64_t pos_mask;
      int pre_load_flag;  // 1: need preload; 0: no need; default 0
      std::set<uint64_t> available_server;

      std::set<uint64_t> reported_serverid;
      std::map<uint32_t, uint64_t> area_capacity_info;        //<area, capacity>

      std::map<uint64_t, node_stat_info> stat_info;        //<server_id, statInfo>
      mutable tbsys::CRWSimpleLock stat_info_rw_locker;        //node_stat_info has its own lock, this only for the map stat_info
      uint32_t interval_seconds;

      std::set<uint64_t> tmp_down_server;  //save tmp down server in DATA_PRE_LOAD_MODE
      tbsys::CThreadMutex hash_table_set_mutex;

    };
    typedef __gnu_cxx::hash_map <const char *, group_info *,
      __gnu_cxx::hash <const char *>, tbsys::char_equal> group_info_map;
  }
}

#endif
