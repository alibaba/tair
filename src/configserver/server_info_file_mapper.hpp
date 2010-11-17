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
#ifndef SERVER_INFO_FILE_MAPPER_H
#define SERVER_INFO_FILE_MAPPER_H
#include "server_info.hpp"
namespace tair {
  namespace config_server {
    class server_info_file_mapper {
    public:
      static const int GROUP_NAME_LEN = 64;
      static const int CELL_SIZE = sizeof(server_info) + GROUP_NAME_LEN;
      static const int CELL_COUNT_PER_FILE = 512;

        server_info_file_mapper();
       ~server_info_file_mapper();
      bool open(const string & file_name, bool file_exist = false);
      void close();

      server_info *get_server_info(int idx);
      char *getp_group_name(int idx);
      void set_group_name(int idx, const char *name);

      void set_zero(int idx);

      int32_t *&getp_magic_number()
      {
        return p_magic_number;
      }
      int32_t *&getp_cell_size()
      {
        return p_cell_size;
      }
      int32_t *&getp_used_counter()
      {
        return p_used_counter;
      }

      void print_head();
      void print_cell(int idx);


    private:
      server_info_file_mapper(const server_info_file_mapper &);
      server_info_file_mapper & operator=(const server_info_file_mapper &);
      void init();
      int32_t *p_magic_number;
      int32_t *p_cell_size;
      int32_t *p_used_counter;

      char *data_start;

      file_mapper map_file;
    };
  }
}

#endif
