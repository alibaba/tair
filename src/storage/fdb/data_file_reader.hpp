/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this class is for reader value from fdb data file
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_FDB_DATA_FILE_READ_HPP
#define TAIR_FDB_DATA_FILE_READ_HPP
#include "file_op.hpp"
#include "define.hpp"
#include "item_data_info.hpp"
namespace tair {
  namespace storage {
    namespace fdb {
      static const uint32_t SYNC_READ_FILE_LEN = 64 * 1024;

      class data_reader{
      public:
        data_reader();
        ~data_reader();
        void close();
        item_data_info *get_next_item();
        void set_file(file_operation * file);
        void set_length(uint64_t len);

      private:
          uint32_t read_file_by_lock(uint32_t size);
        bool has_read;
        uint32_t want_length;
        uint64_t file_size;
        uint64_t offset;
        uint64_t read_length;
        uint64_t buffer_offset;
        uint32_t last_length;
        char *buffer;
        file_operation *data_file;
      };
    }
  }
}
#endif
