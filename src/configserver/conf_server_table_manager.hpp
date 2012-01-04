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
#ifndef CONF_SERVER_TABLE_MANAGER_H
#define CONF_SERVER_TABLE_MANAGER_H
#include "define.hpp"
#include "mmap_file.hpp"
#include "string"
namespace tair {
  namespace config_server {
    using namespace std;
    class group_info;
    class conf_server_table_manager {
    public:
      friend class group_info;
      conf_server_table_manager();
      ~conf_server_table_manager();
      bool open(const std::string & file_name);
      bool create(const std::string & file_name, uint32_t bucket_count,
                  uint32_t copy_count);
      inline std::string get_file_name() const
      {
        return file_name;
      }
      void close();
      void sync();


      void print_info() const;
      void print_hash_table(const uint64_t * root) const;
      void print_table() const;

      bool translate_from_txt2binary(const std::string & src_file_name,
                                     const std::string & out_file_name);

      void deflate_hash_table();

      void deflate_hash_table(int &hash_table_deflate_size,
                              char *&hash_table_deflate_data,
                              const char *hash_table, const int table_count);

      char *get_data();
      int get_size();

      bool set_server_bucket_count(uint32_t bucket_count);
      bool set_copy_count(uint32_t copy_count);

      uint32_t get_server_bucket_count() const;
      uint32_t get_copy_count() const;
      uint32_t get_hash_table_size() const;
      uint32_t get_hash_table_byte_size() const;
      uint32_t get_SVR_table_size() const;
      uint32_t get_mig_data_skip() const;
      uint32_t get_dest_data_skip() const;
      bool is_file_opened() const;

      uint64_t * get_hash_table(){
        return hash_table;
      }
      uint64_t * get_m_hash_table(){
        return m_hash_table;
      }
      uint64_t * get_d_hash_table(){
        return d_hash_table;
      }
    private:
        conf_server_table_manager(const conf_server_table_manager &);
        conf_server_table_manager & operator=(const conf_server_table_manager
                                              &);
      void init();
      char *map_meta_data();
        std::string file_name;

      uint32_t *flag;
      uint32_t *client_version;
      uint32_t *server_version;
      uint32_t *plugins_version;
      uint32_t *area_capacity_version;
      uint32_t *last_load_config_time;
      int32_t *migrate_block_count;

      uint64_t *hash_table;

      uint64_t *m_hash_table;

      uint64_t *d_hash_table;

      uint32_t *server_copy_count;

      uint32_t *server_bucket_count;

      char *hash_table_deflate_data_for_client;
      int hash_table_deflate_data_for_client_size;

      char *hash_table_deflate_data_for_data_server;
      int hash_table_deflate_data_for_data_server_size;
      bool file_opened;

      file_mapper mmap_file;
    };
  }
}
#endif
