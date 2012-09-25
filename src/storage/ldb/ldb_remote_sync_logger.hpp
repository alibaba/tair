/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * RecordLogger implementation of ldb
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_LDB_REMOTE_SYNC_LOGGER_H
#define TAIR_STORAGE_LDB_REMOTE_SYNC_LOGGER_H

#include "common/data_entry.hpp"
#include "common/record_logger.hpp"

namespace leveldb
{
  class Slice;
  namespace log
  {
    class Reader;
  }
}

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      class LdbManager;
      class LdbInstance;
      class LdbRemoteSyncLogReader
      {
      public:
        explicit LdbRemoteSyncLogReader(LdbInstance* instance);
        ~LdbRemoteSyncLogReader();

        int init();
        int restart();
        int get_record(int32_t& type, int32_t& bucket_num,
                       tair::common::data_entry*& key, tair::common::data_entry*& value, bool& force_reget);

      private:
        bool update_last_sequence();
        int start_new_reader(uint64_t min_number);
        int get_log_record();
        int parse_one_kv_record(int32_t& type, int32_t& bucket_num,
                                tair::common::data_entry*& key, tair::common::data_entry*& value,
                                bool skip_value);
        bool try_skip_one_kv_record(char& type);
        bool parse_one_key(int32_t& bucket_num, tair::common::data_entry*& key);
        bool parse_one_value(tair::common::data_entry* key, tair::common::data_entry*& value);
        bool parse_one_entry_tailer(data_entry* entry);
        bool skip_one_entry();

      private:
        LdbInstance* instance_;
        // first sequence number when startup
        uint64_t first_sequence_;
        // current reading logfile's filenumber
        uint64_t reading_logfile_number_;
        leveldb::log::Reader* reader_;
        bool last_logfile_refed_;

        // one log record can contain several kv entry(record),
        // following is for temporary data buffer of last gotten log record
        leveldb::Slice* last_log_record_;
        std::string last_log_scratch_;
        uint64_t last_sequence_;
      };

      class LdbRemoteSyncLogger : public tair::common::RecordLogger
      {
      public:
        explicit LdbRemoteSyncLogger(LdbManager* manager);
        virtual ~LdbRemoteSyncLogger();

        int init();
        int restart();
        int add_record(int32_t index, int32_t type,
                       tair::common::data_entry* key, tair::common::data_entry* value);
        int get_record(int32_t index, int32_t& type, int32_t& bucket_num,
                       tair::common::data_entry*& key, tair::common::data_entry*& value,
                       bool& force_reget);

      private:
        LdbManager* manager_;
        LdbRemoteSyncLogReader** reader_;
      };

    }
  }
}
#endif
