/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * kyotocabinet storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *
 */

#ifndef TAIR_KDB_BUCKET_H
#define TAIR_KDB_BUCKET_H

#include "storage/kdb/kyotocabinet/kchashdb.h"
#include "common/data_entry.hpp"
#include "common/stat_info.hpp"
#include "stat_manager.h"
#include "locker.hpp"

namespace tair {
  namespace storage {
    namespace kdb {
      struct kdb_item_meta {
        kdb_item_meta() : flag(0), reserved(0), version(0), cdate(0), mdate(0), edate(0) {}
        uint8_t  flag;
        uint8_t  reserved;
        uint16_t version;
        uint32_t cdate;
        uint32_t mdate;
        uint32_t edate;
      };
      const size_t KDB_META_SIZE = sizeof(kdb_item_meta);

      class kdb_item {
        public:
          kdb_item() : meta(), value(NULL), value_size(0), full_value(NULL), full_value_size(0) {}

          bool encode() {
            bool ret = true;

            free_full_value();

            full_value_size = KDB_META_SIZE + value_size;
            full_value = (char*)malloc(full_value_size);
            if (full_value == NULL) {
              ret = false;
              TBSYS_LOG(ERROR, "alloc memory failed");
            }

            if (ret) {
              char* p = full_value;
              memcpy(p, &meta, KDB_META_SIZE);
              p += KDB_META_SIZE;

              memcpy(p, value, value_size);
            }

            return ret;
          }

          bool decode() {
            bool ret = true;

            if (full_value == NULL) {
              ret = false;
            }

            if (ret) {
              meta = *((kdb_item_meta*)full_value);
              value = full_value + KDB_META_SIZE;
              value_size = full_value_size - KDB_META_SIZE;
            }

            return ret;
          }

          bool is_expired() {
            time_t timeNow = time(NULL);
            return (meta.edate != 0 && meta.edate < (uint32_t) timeNow);
          }

          void free_full_value() {
            if (full_value != NULL) {
              free(full_value);
              full_value = NULL;
            }
          }

        public:
          kdb_item_meta meta;
          char* value;
          size_t value_size;
          char* full_value;
          size_t full_value_size;
      };

      class kdb_bucket {
        const static int PATH_MAX_LENGTH = 1024;
        public:
          kdb_bucket();
          ~kdb_bucket();

          bool start(int bucket_number);
          void stop();

          int put(tair::common::data_entry& key, tair::common::data_entry& value, bool version_care, uint32_t expire_time);

          int get(tair::common::data_entry& key, tair::common::data_entry& value);

          int remove(tair::common::data_entry& key, bool version_care);

          void destory(); // this will remove the data file

          void get_stat(tair_stat* stat);

          bool begin_scan();
          bool end_scan();

          int get_next_item(item_data_info* &data);

        private:
          void print_db_error(const char* prefix);

        private:
          char filename[PATH_MAX_LENGTH];
          bool is_item_expired();
          locker* locks;
          stat_manager stat_mgr;
          
          kyotocabinet::HashDB db;
          kyotocabinet::DB::Cursor* cursor;
      };
    }
  }
}

#endif
