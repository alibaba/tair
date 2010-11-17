/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * server_info_file_mapper.cpp is a interface to manage the file which store the server's info
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "server_info_file_mapper.hpp"
#include "log.hpp"
#include "define.hpp"

namespace tair {
  namespace config_server {
    server_info_file_mapper::server_info_file_mapper()
    {
      init();
    }
    void server_info_file_mapper::init()
    {
      p_magic_number = NULL;
      p_cell_size = NULL;
      p_used_counter = NULL;
      data_start = NULL;

    }
    server_info_file_mapper::~server_info_file_mapper()
    {
      close();
    }
    bool server_info_file_mapper::open(const string & file_name,
                                       bool file_exist)
    {
      close();
      int fok = access(file_name.c_str(), F_OK);
      if(file_exist && fok) {
        log_error("file not exist :%s\n", file_name.c_str());
        return false;
      }
      int length = CELL_SIZE * CELL_COUNT_PER_FILE + sizeof(int32_t) * 3;
      if(map_file.open_file(file_name.c_str(), length) == false) {
        log_error("open file %s error", file_name.c_str());
        return false;
      }
      data_start = (char *) map_file.get_data();
      assert(data_start != NULL);
      p_magic_number = (int32_t *) data_start;
      data_start += sizeof(int32_t);

      p_cell_size = (int32_t *) data_start;
      data_start += sizeof(int32_t);

      p_used_counter = (int32_t *) data_start;
      data_start += sizeof(int32_t);

      if(!file_exist || fok || (*p_magic_number) != TAIR_DTM_VERSION) {        //first open
        if(file_exist) {
          printf("format is error %s\n", file_name.c_str());
          close();
          return false;
        }
        *p_magic_number = TAIR_DTM_VERSION;
        *p_cell_size = CELL_SIZE;
        *p_used_counter = 0;
      }
      return true;
    }
    void server_info_file_mapper::close()
    {
      map_file.close_file();
      init();
    }

    server_info *server_info_file_mapper::get_server_info(int idx)
    {
      assert(data_start != NULL && idx < CELL_COUNT_PER_FILE);
      return (server_info *) (data_start + idx * CELL_SIZE);
    }
    void server_info_file_mapper::set_zero(int idx)
    {
      char *data = (char *) get_server_info(idx);
      memset(data, 0, CELL_SIZE);
    }
    char *server_info_file_mapper::getp_group_name(int idx)
    {
      return (char *) (get_server_info(idx)) + sizeof(server_info);
    }
    void server_info_file_mapper::set_group_name(int idx, const char *name)
    {
      char *data = getp_group_name(idx);
      int len = 0;
      if(name != NULL) {
        len = strlen(name);
        if(len >= GROUP_NAME_LEN)
          len = GROUP_NAME_LEN - 1;
        memcpy(data, name, len);

      }
      data[len] = '\0';
      return;
    }
    void server_info_file_mapper::print_head()
    {
      assert(data_start != NULL);
      printf
        ("-----------------------------file head---------------------------\n");
      printf("magic_number:%d  CELL_SIZE:%d  used_count:%d\n",
             *p_magic_number, *p_cell_size, *p_used_counter);
    }
    void server_info_file_mapper::print_cell(int idx)
    {
      server_info *p = get_server_info(idx);
      p->print();
      printf("group name is |%s|\n", getp_group_name(idx));
    }
  }
}
