/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * some common RecordLogger implementation
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_COMMON_RECORD_LOGGER_UTIL_H
#define TAIR_COMMON_RECORD_LOGGER_UTIL_H

#include "record_logger.hpp"

namespace tair
{
  namespace common
  {
    // ring buffer RecordLogger wrapped on file(mmap)
    // TODO: data sync interval ..
    class RingBufferRecordLogger : public RecordLogger
    {
    public:
      explicit RingBufferRecordLogger(const char* file_path, int64_t max_mem_size);
      virtual ~RingBufferRecordLogger();

      int init();
      int add_record(int32_t index, int32_t type,
                     data_entry* key, data_entry* value);
      int get_record(int32_t index, int32_t& type, int32_t& bucket_num,
                     data_entry*& key, data_entry*& value, bool& force_reget);

    private:
      char* calc_mem_pos(int32_t size);
      void fill_skip_tailer(char* pos, int32_t size);
      bool is_skip_tailer(const char* pos);
      int cleanup();

    private:
      static const int32_t LOGGER_SKIP_TYPE;
      static const int32_t HEADER_SIZE;
      static const int32_t RECORD_HEADER_SIZE;

    private:
      tbsys::CThreadMutex mutex_;
      std::string file_path_;
      int fd_;
      int64_t mem_size_;

      char* base_;
      // r_offset_/w_offset should always be aligned to one record boundry
      int64_t* w_offset_;
      int64_t* r_offset_;
      // consider r_offset > w_offset in ring buffer as reverse,
      // we need this flag to decide how to process when r_offset == w_offset.
      char* reverse_;
    };

    class FileOperation;
    // This is a shoddy implementation.
    // NOTE: Read or write file sequentially, ONLY one type operation
    //       can operated over file once open.
    class SequentialFileRecordLogger : public RecordLogger
    {
    public:
      // actually, auto_rotate is for write
      SequentialFileRecordLogger(const char* file_path, int64_t max_file_size, bool auto_rotate);
      virtual ~SequentialFileRecordLogger();

      int init();
      int add_record(int32_t index, int32_t type,
                     data_entry* key, data_entry* value);
      int get_record(int32_t index, int32_t& type, int32_t& bucket_num,
                     data_entry*& key, data_entry*& value, bool& force_reget);

    private:
      int reserve_file_size(int64_t size);
      int reserve_one_record();

    private:
      tbsys::CThreadMutex mutex_;
      std::string file_path_;

      FileOperation* file_;
      int64_t max_file_size_;
      bool auto_rotate_;

      // r_offset_/w_offset should always be aligned to one record boundry
      int64_t w_offset_;
      int64_t r_offset_;
      std::string last_data_;
    };
  }
}

#endif

