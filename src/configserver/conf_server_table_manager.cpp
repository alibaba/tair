/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * conf_server_table_manager.cpp is to store and load the hashtable 
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "conf_server_table_manager.hpp"
#include "server_info.hpp"
#include <zlib.h>
namespace tair {
  namespace config_server {
    using namespace std;
    void conf_server_table_manager::init()
    {
      flag = client_version = server_version = server_bucket_count =
        server_copy_count = plugins_version = area_capacity_version =
        last_load_config_time = NULL;
      migrate_block_count = NULL;
      hash_table = m_hash_table = d_hash_table = NULL;
      file_name = "";
      hash_table_deflate_data_for_client =
        hash_table_deflate_data_for_data_server = NULL;
      hash_table_deflate_data_for_client_size =
        hash_table_deflate_data_for_data_server_size = 0;
      file_opened = false;

    }
    conf_server_table_manager::conf_server_table_manager()
    {
      init();
    }
    conf_server_table_manager::~conf_server_table_manager()
    {
      if(hash_table_deflate_data_for_client) {
        free(hash_table_deflate_data_for_client);
      }
      if(hash_table_deflate_data_for_data_server) {
        free(hash_table_deflate_data_for_data_server);
      }
      close();
    }
    void conf_server_table_manager::print_table() const
    {
      print_info();
      printf("--------------------hashtable-----------------------\n");
      print_hash_table(hash_table);
      printf("--------------------Mhashtable-----------------------\n");
      print_hash_table(m_hash_table);
      printf("--------------------Dhashtable-----------------------\n");
      print_hash_table(d_hash_table);
    }

    void conf_server_table_manager::print_info() const
    {
      if(mmap_file.get_data() == NULL) {
        printf("open table first\n");
        return;
      }
      printf("copy_count:%u\nbucket_count:%u\n",
             *server_copy_count, *server_bucket_count);
      printf
        ("flag:%u\nclientVersion:%u\nserverVersion:%u\npluginsVersion:%u\ncapacityVersion:%u\n",
         *flag, *client_version, *server_version, *plugins_version,
         *area_capacity_version);
      printf("lastLoadConfigTime:%u\nmigrateBlockCount:%d\n",
             *last_load_config_time, *migrate_block_count);

    }
    void conf_server_table_manager::print_hash_table(const uint64_t * root) const
    {
      if(root == NULL)
        return;
      for(uint32_t j = 0; j < get_copy_count(); j++)
        for(uint32_t i = 0; i < get_server_bucket_count(); ++i)
        {
          printf("bucket:%u,line:%u,ip:%s\n", i, j,
                 tbsys::CNetUtil::
                 addrToString(root[j * get_server_bucket_count() + i]).
                 c_str());
        }
    }
    bool conf_server_table_manager::
      translate_from_txt2binary(const string & src_file_name,
                                const string & out_file_name)
    {
      FILE *fp;
      char buf[4096];
      uint64_t *p_table = NULL;
      if((fp = fopen(src_file_name.c_str(), "r")) == NULL) {
        return false;
      }
      uint32_t copy_count = 0;
      uint32_t server_bucket_count = 0;
      while(!feof(fp) && (fgets(buf, 4096, fp))) {
        char *p = NULL;
        p = strstr(buf, "copy_count:");
        if(p != NULL) {
          copy_count = strtoul(p + strlen("copy_count:"), NULL, 10);
          continue;
        }
        p = strstr(buf, "bucket_count:");
        if(p != NULL) {
          server_bucket_count =
            strtoul(p + strlen("bucket_count:"), NULL, 10);
          if(!this->create(out_file_name, server_bucket_count, copy_count)) {
            return false;
          }
          printf(" got copy_count=%d bucket=%d\n", copy_count,
                 server_bucket_count);
          continue;
        }
        p = strstr(buf, "-hashtable");
        if(p != NULL) {
          p_table = hash_table;
          continue;
        }
        p = strstr(buf, "-Mhashtable");
        if(p != NULL) {
          p_table = m_hash_table;
          continue;
        }
        p = strstr(buf, "-Dhashtable");
        if(p != NULL) {
          p_table = d_hash_table;
          continue;
        }

        p = strstr(buf, "flag:");
        if(p != NULL) {
          *flag = strtoul(p + strlen("flag:"), NULL, 10);
        }
        p = strstr(buf, "clientVersion:");
        if(p != NULL) {
          *client_version = strtoul(p + strlen("clientVersion:"), NULL, 10);
          continue;
        }
        p = strstr(buf, "serverVersion:");
        if(p != NULL) {
          *server_version = strtoul(p + strlen("serverVersion:"), NULL, 10);
          continue;
        }
        p = strstr(buf, "pluginsVersion:");
        if(p != NULL) {
          *plugins_version = strtoul(p + strlen("pluginsVersion:"), NULL, 10);
          continue;
        }
        p = strstr(buf, "capacityVersion:");
        if(p != NULL) {
          *area_capacity_version =
            strtoul(p + strlen("capacityVersion:"), NULL, 10);
          continue;
        }
        p = strstr(buf, "lastLoadConfigTime:");
        if(p != NULL) {
          *last_load_config_time =
            strtoul(p + strlen("lastLoadConfigTime:"), NULL, 10);
          continue;
        }
        p = strstr(buf, "migrateBlockCount:");
        if(p != NULL) {
          *migrate_block_count =
            strtoul(p + strlen("migrateBlockCount:"), NULL, 10);
          continue;
        }
        p = strstr(buf, "bucket:");
        if(p != NULL) {
          int bucket = 0;
          int line = 0;
          uint64_t serverid = 0;
          p += strlen("bucket:");
          bucket = strtol(p, NULL, 10);

          p = strstr(p, "line:");
          assert(p != NULL);
          p += strlen("line:");
          line = strtol(p, NULL, 10);

          p = strstr(p, "ip:");
          assert(p != NULL);
          p += strlen("ip:");
          serverid = tbsys::CNetUtil::strToAddr(p, 0);
          p_table[line * get_server_bucket_count() + bucket] = serverid;

        }
      }
      sync();
      return true;

    }
    char *conf_server_table_manager::map_meta_data()
    {
      char *data = (char *) mmap_file.get_data();
      assert(data != NULL);

      flag = (uint32_t *) data;
      data += sizeof(uint32_t);
      client_version = (uint32_t *) data;
      data += sizeof(uint32_t);
      server_version = (uint32_t *) data;
      data += sizeof(uint32_t);
      plugins_version = (uint32_t *) data;
      data += sizeof(uint32_t);
      area_capacity_version = (uint32_t *) data;
      data += sizeof(uint32_t);
      last_load_config_time = (uint32_t *) data;
      data += sizeof(uint32_t);
      migrate_block_count = (int32_t *) data;
      data += sizeof(uint32_t);
      server_copy_count = (uint32_t *) data;
      data += sizeof(uint32_t);
      server_bucket_count = (uint32_t *) data;
      data += sizeof(uint32_t);
      return data;
    }
    bool conf_server_table_manager::create(const string & file_name,
                                           uint32_t bucket_count,
                                           uint32_t copy_count)
    {
      log_debug("will create group server table");
      int fok = access(file_name.c_str(), F_OK);
      if(fok == 0) {
        return false;
      }
      close();                        //this will do nothing but init
      server_bucket_count = &bucket_count;
      server_copy_count = &copy_count;
      if(mmap_file.open_file(file_name.c_str(), get_SVR_table_size()) ==
         false) {
        server_bucket_count = server_copy_count = NULL;
        return false;
      }
      char *data = map_meta_data();

      *flag = TAIR_HTM_VERSION;
      *client_version = 1;
      *server_version = 1;
      *plugins_version = 1;
      *area_capacity_version = 1;
      (*migrate_block_count) = -1;
      *server_copy_count = copy_count;
      *server_bucket_count = bucket_count;

      hash_table = (uint64_t *) data;
      m_hash_table = hash_table + get_mig_data_skip();
      d_hash_table = hash_table + get_dest_data_skip();
      memset(data, 0, get_hash_table_byte_size());
      sync();
      file_opened = true;
      log_debug("create %s ok", file_name.c_str());
      return true;
    }
    bool conf_server_table_manager::open(const string & file_name)
    {
      close();
      int fok = access(file_name.c_str(), F_OK);
      if(fok) {
        log_error("file (%s) not exit\n", file_name.c_str());
        return false;
      }
      if(mmap_file.open_file(file_name.c_str()) == false) {        //read only so we can get meta info
        return false;
      }
      this->file_name = file_name;
      char *data = map_meta_data();
      if(TAIR_HTM_VERSION != *flag) {
        log_error("flag error");
        close();
        return false;
      }


      log_debug
        ("CConfServerTableManager:open file_name %s start, client version: %d, server version: %d, plugins version:%d",
         file_name.c_str(), *client_version, *server_version,
         *plugins_version);
      uint32_t size = get_SVR_table_size();
      log_debug("will reopen it size = %d", size);
      //reopen it with the right size;
      close();
      if(mmap_file.open_file(file_name.c_str(), size) == false) {        //
        return false;
      }
      data = map_meta_data();

      hash_table = (uint64_t *) data;
      m_hash_table = hash_table + get_mig_data_skip();
      d_hash_table = hash_table + get_dest_data_skip();
      file_opened = true;
      return true;
    }

    void conf_server_table_manager::close()
    {
      mmap_file.close_file();
      init();
    }
    void conf_server_table_manager::sync()
    {
      mmap_file.sync_file();
    }

    void conf_server_table_manager::deflate_hash_table()
    {
      char *p_tmp_table = (char *) malloc(get_hash_table_byte_size() * 2);
      assert(p_tmp_table != NULL);
      memcpy(p_tmp_table, (const char *) hash_table,
             get_hash_table_byte_size());
      memcpy(p_tmp_table + get_hash_table_byte_size(),
             (const char *) d_hash_table, get_hash_table_byte_size());
      deflate_hash_table(hash_table_deflate_data_for_client_size,
                         hash_table_deflate_data_for_client, p_tmp_table, 1);
      deflate_hash_table(hash_table_deflate_data_for_data_server_size,
                         hash_table_deflate_data_for_data_server, p_tmp_table,
                         2);
      free(p_tmp_table);
    }
    char *conf_server_table_manager::get_data()
    {
      return (char *) mmap_file.get_data();
    }
    int conf_server_table_manager::get_size()
    {
      return mmap_file.get_size();
    }
    void conf_server_table_manager::
      deflate_hash_table(int &hash_table_deflate_size,
                         char *&hash_table_deflate_data,
                         const char *hash_table, const int table_count)
    {
      hash_table_deflate_size = 0;
      if(hash_table_deflate_data) {
        free(hash_table_deflate_data);
        hash_table_deflate_data = NULL;
      }

      int index = get_hash_table_size() * table_count;
      while(index > 0) {
        if(hash_table[--index] != 0) {
          break;
        }
      }
      if(index == 0) {
        log_info("all in [%s] _hashTable is 0", file_name.c_str());
        return;
      }

      if(index % get_server_bucket_count()) {
        index -= (index % get_server_bucket_count());
        index += get_server_bucket_count();
      }

      // source len
      unsigned long source_len = index * sizeof(uint64_t);
      unsigned long dest_len = compressBound(source_len);
      unsigned char *dest = (unsigned char *) malloc(dest_len);

      int ret =
        compress(dest, &dest_len, (unsigned char *) hash_table, source_len);
      if(ret == Z_OK) {
        hash_table_deflate_data = (char *) malloc(dest_len);
        hash_table_deflate_size = dest_len;
        memcpy(hash_table_deflate_data, dest, hash_table_deflate_size);

        log_info("[%s] hashCount: %d, compress: %d => %d", file_name.c_str(),
                 index, source_len, dest_len);
      }
      else {
        log_error("[%s] compress error: %d", file_name.c_str(), ret);
      }
      free(dest);
    }
    bool conf_server_table_manager::
      set_server_bucket_count(uint32_t bucket_count)
    {
      if(server_bucket_count == NULL || *server_bucket_count != 0) {
        return false;                // can not change this dynamicly
      }
      *server_bucket_count = bucket_count;
      return true;
    }
    bool conf_server_table_manager::set_copy_count(uint32_t copy_count)
    {
      if(server_copy_count == NULL || *server_copy_count != 0) {
        return false;                // can not change this dynamicly
      }
      *server_copy_count = copy_count;
      return true;
    }
    uint32_t conf_server_table_manager::get_server_bucket_count() const
    {
      return server_bucket_count != NULL ? *server_bucket_count : 0;
    }
    uint32_t conf_server_table_manager::get_copy_count() const
    {
      return server_copy_count != NULL ? *server_copy_count : 0;
    }
    uint32_t conf_server_table_manager::get_hash_table_size() const
    {
      return get_server_bucket_count() * get_copy_count();
    }
    uint32_t conf_server_table_manager::get_hash_table_byte_size() const
    {
      return get_hash_table_size() * sizeof(uint64_t);
    }
    uint32_t conf_server_table_manager::get_SVR_table_size() const
    {
      return get_hash_table_byte_size() * 3 + sizeof(uint32_t) * 9;
    }
    uint32_t conf_server_table_manager::get_mig_data_skip() const
    {
      return get_hash_table_size();
    }
    uint32_t conf_server_table_manager::get_dest_data_skip() const
    {
      return get_hash_table_size() * 2;
    }
    bool conf_server_table_manager::is_file_opened() const
    {
      return file_opened;
    }
  }
}
