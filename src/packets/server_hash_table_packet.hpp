/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for configserver sync server table between each other
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_SERVER_HASH_TABLE_H
#define TAIR_SERVER_HASH_TABLE_H
#include "base_packet.hpp"
namespace tair{
   class server_hash_table_packet {
   public:
      server_hash_table_packet()
      {
         hash_table_size = 0;
         hash_table_data = NULL;
         server_list = NULL;
         server_list_count = -1;
      }

      ~server_hash_table_packet()
      {
         if (hash_table_data) {
            ::free(hash_table_data);
         }
         if (server_list) {
            ::free(server_list);
         }
      }

      int get_hash_table_size() const 
      {
         return hash_table_size;
      }

      void set_hash_table(const char *data_h, int size_h)
      {
         if (hash_table_data) {
            ::free(hash_table_data);
            hash_table_data = NULL;
         }
         hash_table_size = size_h ;

         if (hash_table_size > 0 && hash_table_size <= TAIR_MAX_DATA_SIZE) {
            hash_table_data = (char*) malloc(hash_table_size);
            if (data_h != NULL)
               memcpy(hash_table_data, data_h, hash_table_size);
         }
      }

      uint64_t *get_server_list(int bucket_count, int copy_count)
      {
         if (server_list_count == 0 || hash_table_data == NULL || hash_table_size == 0) {
            return NULL;
         }
         if (server_list != NULL) {
            return server_list;
         }
         server_list_count = 0;
         unsigned long dest_len = bucket_count * copy_count * 3 * sizeof(uint64_t);
         unsigned long src_len = hash_table_size;
         server_list = (uint64_t*)malloc(dest_len);
         memset(server_list, 0, dest_len);
         log_info("uncompress server list, data size: %d", src_len);
         if (uncompress((Bytef*)server_list, &dest_len, (const Bytef*)hash_table_data, src_len) != Z_OK) {
            log_warn("uncompress error");
            ::free(server_list);
            server_list = NULL;
         } else {
            log_debug("uncompress successed dest_len=%d",dest_len);
            server_list_count = (dest_len / sizeof(uint64_t));
         }
         return server_list;
      }
   public:
      int server_list_count;
   protected:
      int hash_table_size;
      char *hash_table_data;
      uint64_t *server_list;
   private:
      server_hash_table_packet(const server_hash_table_packet&);
   };
}
#endif
