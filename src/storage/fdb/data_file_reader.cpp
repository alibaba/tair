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
#include "data_file_reader.hpp"
namespace tair {
  namespace storage {
    namespace fdb {

      data_reader::data_reader()
      {
        want_length = 0;
        file_size = 0;
        offset = 0;
        read_length = 0;
        buffer_offset = 0;
        last_length = 0;
        buffer = NULL;
        has_read = false;
      }

      data_reader::~data_reader()
      {
      }
      item_data_info *data_reader::get_next_item()
      {
        uint16_t key_size = 0;
        uint32_t pad_size = 0;
        uint32_t value_size = 0;
        uint8_t is_free;
        if(!has_read)
        {
          want_length = SYNC_READ_FILE_LEN;
          buffer = (char *) malloc(want_length);
          if(offset == 0) {
            offset = 16;
          }
          read_length = read_file_by_lock(want_length);
          has_read = true;
        }
        if(offset + buffer_offset >= file_size) {
          return NULL;
        }
      _Restart:
        if(read_length == 0) {
          TBSYS_LOG(DEBUG, "read data file with no data!");
          return NULL;
        }
        if(read_length - buffer_offset >= ITEM_HEADER_LEN) {
          item_meta_info *header =
            (item_meta_info *) (buffer + buffer_offset);
          key_size = header->keysize;
          assert(key_size > 2);
          value_size = header->valsize;
          pad_size = header->padsize;
          is_free = (uint8_t) (header->flag);
          uint32_t total_size =
            ITEM_HEADER_LEN + key_size + value_size + pad_size;
          if(read_length - buffer_offset >= total_size) {
            time_t time_now = time(NULL);

            if(!(is_free & TAIR_ITEM_FLAG_DELETED)
               && !(header->edate != 0
                    && header->edate < (uint32_t) time_now)) {
              item_data_info *item = (item_data_info *) malloc(total_size);
              memcpy(item, buffer + buffer_offset, total_size);
              buffer_offset += total_size;
              return item;
            }
            else {
              buffer_offset += total_size;
              goto _Restart;
            }
          }
          else {
            offset += buffer_offset;
            last_length = want_length;
            want_length = SYNC_READ_FILE_LEN;
            if(is_free & TAIR_ITEM_FLAG_DELETED) {
              offset += total_size;
            }
            else {
              if(total_size > SYNC_READ_FILE_LEN) {
                want_length = total_size;
              }
            }
            if(want_length != last_length) {
              free(buffer);
              buffer = (char *) malloc(want_length);
            }
            read_length = read_file_by_lock(total_size);
            buffer_offset = 0;
            goto _Restart;
          }
        }
        else {
          offset += buffer_offset;
          last_length = want_length;
          want_length = SYNC_READ_FILE_LEN;
          if(want_length != last_length) {
            free(buffer);
            buffer = (char *) malloc(want_length);
          }
          read_length = read_file_by_lock(ITEM_HEADER_LEN);
          buffer_offset = 0;
          goto _Restart;
        }
      }

      uint32_t data_reader::read_file_by_lock(uint32_t size)
      {
        if(offset >= file_size)
          return 0;
        if(file_size - offset < want_length) {
          want_length = file_size - offset;
          if(want_length < size) {
            want_length = size;
          }
        }
        while(true) {
          bool is_locked =
            data_file->lock((off_t) offset, (size_t) want_length, false);
          if(is_locked) {
            break;
          }
        }
        read_length =
          data_file->read(buffer, (size_t) want_length, (off_t) offset);
        data_file->unlock((off_t) offset, (size_t) want_length);
        return read_length;
      }


      void data_reader::set_file(file_operation * file)
      {
        data_file = file;
      }

      void data_reader::set_length(uint64_t len)
      {
        file_size = len;
      }

      void data_reader::close()
      {
        delete data_file;
        free(buffer);
        delete this;
      }

    }
  }
}
