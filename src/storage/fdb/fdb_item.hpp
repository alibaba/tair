/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * commonly used data structures of fdb storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef FDB_ITEM_H
#define FDB_ITEM_H

#include "define.hpp"
#include "item_data_info.hpp"

namespace tair {
  namespace storage {
    namespace fdb {
      typedef struct _item_index
      {
        uint32_t left;
        uint32_t right;
        uint64_t size:24;
        uint64_t offset:40;
        uint64_t hashcode;
      } item_index;
      const static int FDB_ITEM_META_SIZE = sizeof(item_index);

      const static int FDB_ITEM_DATA_SIZE = sizeof(item_meta_info);

      class fdb_item {
      public:
        fdb_item()
        {
          meta_offset = 0;
          bucket_index = 0;
          parent_off = 0;
          is_right = 0;
          is_new = 0;
          key = NULL;
          value = NULL;
          memset(&meta, 0, sizeof(meta));
          memset(&data, 0, sizeof(data));
        }
        uint32_t meta_offset;
        uint32_t bucket_index;
        uint32_t parent_off;
        bool is_right;
        bool is_new;
        item_index meta;
        item_meta_info data;
        char *key;
        char *value;

        void free_value()
        {
          if(value != NULL) {
            free(value);
            value = NULL;
          }
        }

        void log_self(const char *hit = "") {
          log_debug
            ("%s - item: {meta_offset=%u, bucket_index=%u, parent_off=%u, is_right=%d, is_new=%d, ItemIndex{left=%u, right=%u, size: %u, offset: %lu, hashcode: %llu}, ItemMeta{keysize=%u, valsize=%u, padsize=%u, version=%u, flag=%d, cdate=%u, mdata=%u, edate=%u}",
             hit, meta_offset, bucket_index, parent_off, is_right, is_new,
             meta.left, meta.right, meta.size, meta.offset, meta.hashcode,
             data.keysize, data.valsize, data.prefixsize, data.version,
             data.flag, data.cdate, data.mdate, data.edate);
        }
      };
    }                                /* fdb */
  }                                /* storage */
}                                /* tair */
#endif
