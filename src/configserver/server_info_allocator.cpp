/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * server_info_allocator.cpp is to create a data server's info in storage.
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "server_info_allocator.hpp"
#include "group_info.hpp"
namespace tair {
  namespace config_server {
    server_info_allocator server_info_allocator::
      server_info_allocator_instance;
      server_info_allocator::server_info_allocator()
    {
      use_size = 0;

    }
    server_info_allocator::~server_info_allocator()
    {
      vector<server_info_file_mapper *>::iterator it;
      for(it = vec_server_infos.begin(); it != vec_server_infos.end(); ++it)
      {
        delete *it;
      }
      vec_server_infos.clear();
    }
    server_info *server_info_allocator::new_server_info(group_info * group,
                                                        uint64_t id)
    {

      if(use_size >=
         vec_server_infos.size() *
         server_info_file_mapper::CELL_COUNT_PER_FILE) {

        char file_name[256];
        const char *sz_data_dir =
          TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_DATA_DIR,
                                 TAIR_DEFAULT_DATA_DIR);
        snprintf(file_name, 256, "%s/server_info.%d", sz_data_dir,
                 use_size / server_info_file_mapper::CELL_COUNT_PER_FILE);
        server_info_file_mapper *psif = new server_info_file_mapper();
        bool tmp_ret = psif->open(file_name);
        assert(tmp_ret);
        vec_server_infos.push_back(psif);
      }
      int vecidx = use_size / server_info_file_mapper::CELL_COUNT_PER_FILE;
      assert((size_t) vecidx < vec_server_infos.size());
      server_info_file_mapper *psif = vec_server_infos[vecidx];
      int mapfile_idx =
        use_size - server_info_file_mapper::CELL_COUNT_PER_FILE * vecidx;

      server_info *p_server_Info = psif->get_server_info(mapfile_idx);
      psif->set_zero(mapfile_idx);

      p_server_Info->group_info_data = group;
      p_server_Info->server_id = id;

      if(tbnet::ConnectionManager::isAlive(id)) {
        log_debug("new server [%s] is alive",
                  tbsys::CNetUtil::addrToString(id).c_str());
        p_server_Info->status = 0;
        struct timespec tm;
        clock_gettime(CLOCK_MONOTONIC, &tm);
        p_server_Info->last_time = tm.tv_sec;
      }
      else {
        log_debug("new server [%s] is down",
                  tbsys::CNetUtil::addrToString(id).c_str());
        p_server_Info->status = 1;
        struct timespec tm;
        clock_gettime(CLOCK_MONOTONIC, &tm);
        if(group != NULL) {
          p_server_Info->last_time =
            tm.tv_sec - group->get_server_down_time() * 2;
        }
        else {
          p_server_Info->last_time = tm.tv_sec - TAIR_SERVER_DOWNTIME * 2;
        }
      }
      const char *group_name = NULL;
      if(group != NULL)
        group_name = group->get_group_name();
      psif->set_group_name(mapfile_idx, group_name);
      (*(psif->getp_used_counter()))++;

      use_size++;
      return p_server_Info;
    }
  }
}
