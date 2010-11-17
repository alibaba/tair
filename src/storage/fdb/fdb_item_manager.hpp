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
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef FDB_ITEM_MANAGER_H
#define FDB_ITEM_MANAGER_H

#include <unistd.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include "fdb_item.hpp"
#include "file_op.hpp"
#include "free_blocks_manager.hpp"
#include "data_entry.hpp"
#include "define.hpp"
#include "stat_info.hpp"        // for tair_pstat

namespace tair {
  namespace storage {
    const static int DBHEADER_MAGIC_SIZE = 16;
    namespace fdb
    {

      using namespace tair::common;
      const static int IO_BUF_SIZE = 8192;        // IO buffer size, 8KB
      const static char INDEX_SUFFIX[4] = "idx";
      const static char DATA_SUFFIX[4] = "dat";
      const static char INDEX_MAGIC[16] = "TAIR_IDX_200";
      const static char DATA_MAGIC[16] = "TAIR_DAT_200";
      const static int FILENAME_MAX_LENGTH = 100;
      const static int DATA_PADDING_SIZE = 128;
      const static int MAX_LOOP_COUNT = 10000;
      const static int MAX_FAIL_COUNT = 1000;
      const static int TAIR_DATA_MAGIC_NUMBER = 0xAC9B;
      const static int FDB_BUCKET_NUMBER = 10223;

      struct db_header
      {
        char magic[DBHEADER_MAGIC_SIZE];
        uint8_t flag;
        uint8_t fb_power;        // free block power
        uint32_t item_count;        // item total count
        uint32_t idx_file_size;
        uint64_t dat_file_size;
        uint32_t free_idx_head;        // the head of free index block
        tair_pstat stat[TAIR_MAX_AREA_COUNT];
      };
      const static size_t DBHEADER_SIZE = sizeof(db_header);

      enum
      {
        INIT_SUCC,
        INIT_NEW,
        INIT_FAILED,
      };

      class fdb_item_manager {
      public:
        enum
        {
          READ_DATA_META,
          READ_DATA_KEY,
          READ_DATA_VALUE,
        };

          fdb_item_manager();
         ~fdb_item_manager();

        int initialize(int bucket_number);

        int get_item(data_entry & key, fdb_item & ret_item);
        int update_item(fdb_item & up_item);

        void free_item(fdb_item & old_item);
        void free_meta(fdb_item & old_item);
        void free_data(fdb_item & old_item);

        void new_item(fdb_item & ret_item);
        void new_data(fdb_item & ret_item, bool exclusive = true);

        // this will rename data and index file
        void backup();

        void stat_add(int area, int data_size, int use_size);
        void stat_sub(int area, int data_size, int use_size);

        tair_pstat *get_stat() const;

      private:
          bool read_meta(fdb_item & ret_item);
        bool write_meta(fdb_item & old_item);
        bool read_data(fdb_item & ret_item, char *data, int read_type);
        bool write_data(fdb_item & old_item);

      private:
        void dump_default_db_header(db_header & header);

      private:
          file_operation * index_file;
        file_operation *data_file;
        free_blocks_manager *fb_manager;
        struct db_header *header;
        uint32_t *buckets;
          tbsys::CThreadMutex header_lock;
      };
    }                                /* fdb */
  }                                /* storage */
}                                /* tair */
#endif
