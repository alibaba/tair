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

// leveldb etc.
#include "util/coding.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "leveldb/slice.h"

#include "ldb_define.hpp"
#include "ldb_manager.hpp"
#include "ldb_instance.hpp"
#include "ldb_remote_sync_logger.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      using common::data_entry;

      //////////////////////////////////
      // LdbRemoteSyncLogReader
      //////////////////////////////////
      LdbRemoteSyncLogReader::LdbRemoteSyncLogReader(LdbInstance* instance) :
        instance_(instance), first_sequence_(0), reader_(NULL), last_logfile_refed_(false)
      {
        last_log_record_ = new leveldb::Slice();
        start_new_reader(0);
      }

      LdbRemoteSyncLogReader::~LdbRemoteSyncLogReader()
      {
        if (last_log_record_ != NULL)
        {
          delete last_log_record_;
        }

        if (reader_ != NULL)
        {
          if (last_logfile_refed_)
          {
            dynamic_cast<leveldb::ReadableAndWritableFile*>(reader_->File())->Unref();
          }
          else
          {
            delete reader_->File();
          }
          delete reader_;
        }
      }

      int LdbRemoteSyncLogReader::init()
      {
        return TAIR_RETURN_SUCCESS;
      }

      int LdbRemoteSyncLogReader::restart()
      {
        if (instance_->db_ != NULL)
        {
          // restart first sequence
          first_sequence_ = dynamic_cast<leveldb::DBImpl*>(instance_->db_)->LastSequence() + 1;
        }
        return TAIR_RETURN_SUCCESS;
      }

      int LdbRemoteSyncLogReader::get_record(int32_t& type, int32_t& bucket_num,
                                             data_entry*& key, data_entry*& value, bool& force_reget)
      {
        if (NULL == instance_->db_)
        {
          log_debug("db not init yet");
          return TAIR_RETURN_SERVER_CAN_NOT_WORK;
        }

        key = value = NULL;
        bucket_num = -1;

        int ret = TAIR_RETURN_SUCCESS;

        while (true)
        {
          if (last_log_record_->size() <= 0)
          {
            ret = get_log_record();
            log_debug("@@ get new record: %d", last_log_record_->size());
          }

          // maybe read over all current writing log record
          if (TAIR_RETURN_SUCCESS == ret && last_log_record_->size() > 0)
          {
            force_reget = update_last_sequence();
            ret = parse_one_kv_record(type, bucket_num, key, value, force_reget);
            // this kv record is skipped, just try next
            if (TAIR_RETURN_SUCCESS == ret && NULL == key)
            {
              continue;
            }
            else
            {
              break;
            }
          }
          else
          {
            // read over or fail
            type = TAIR_REMOTE_SYNC_TYPE_PUT;
            break;
          }
        }

        return ret;
      }

      int LdbRemoteSyncLogReader::start_new_reader(uint64_t min_number)
      {
        int ret = TAIR_RETURN_SUCCESS;

        if (instance_->db_ != NULL)
        {
          leveldb::DBImpl* db = dynamic_cast<leveldb::DBImpl*>(instance_->db_);
          if (first_sequence_ <= 0)
          {
            first_sequence_ = db->LastSequence() + 1;
            log_warn("first sequence of ldb rsync log reader %d: %"PRI64_PREFIX"u", instance_->index_, first_sequence_);
          }

          leveldb::Env* db_env = db->GetEnv();
          const std::string& db_log_dir = db->DBLogDir();
          std::vector<std::string> filenames;
          leveldb::Status s = db_env->GetChildren(db_log_dir, &filenames);

          uint64_t number = 0;
          leveldb::FileType type;
          std::vector<uint64_t> logs;
          for (size_t i = 0; i < filenames.size(); i++)
          {
            if (leveldb::ParseFileName(filenames[i], &number, &type) && type == leveldb::kLogFile)
            {
              logs.push_back(number);
            }
          }

          uint64_t new_logfile_number = 0;
          uint64_t db_logfile_number = db->LogFileNumber();

          if (!logs.empty())
          {
            std::sort(logs.begin(), logs.end());
            // maybe binary-search..
            for (size_t i = 0; i < logs.size(); ++i)
            {
              if (logs[i] > min_number && logs[i] <= db_logfile_number)
              {
                new_logfile_number = logs[i];
                break;
              }
            }
          }

          if (0 == new_logfile_number)
          {
            log_warn("no ldb log for reader");
            ret = TAIR_RETURN_FAILED;
          }
          else
          {
            // file will be Ref()
            leveldb::SequentialFile* file = db->LogFile(new_logfile_number);
            bool refed = (file != NULL);

            // not current writing log, current writing db logger will be ReadableAndWritableFile
            if (NULL == file)
            {
              std::string fname = leveldb::LogFileName(db_log_dir, new_logfile_number);
              s = db_env->NewSequentialFile(fname, &file);
              if (!s.ok())
              {
                log_error("init to read log file %s fail: %s", fname.c_str(), s.ToString().c_str());
                ret = TAIR_RETURN_FAILED;
              }
            }

            if (TAIR_RETURN_SUCCESS == ret)
            {
              reading_logfile_number_ = new_logfile_number;
              if (reader_ != NULL)
              {
                if (last_logfile_refed_)
                {
                  dynamic_cast<leveldb::ReadableAndWritableFile*>(reader_->File())->Unref();
                }
                else
                {
                  delete reader_->File();
                }
                delete reader_;
                db_env->DeleteFile(leveldb::LogFileName(db_log_dir, min_number));
              }

              // TODO: reporter
              log_debug("start new ldb rsync reader, filenumber: %"PRI64_PREFIX"u", reading_logfile_number_);
              reader_ = new leveldb::log::Reader(file, NULL, true, 0);
              last_logfile_refed_ = refed;
            }
          }
        }

        return ret;
      }

      bool LdbRemoteSyncLogReader::update_last_sequence()
      {
        if (0 == last_sequence_)
        {
          log_debug("@@ new last seq");
          last_sequence_ = leveldb::DecodeFixed64(last_log_record_->data());
          // this is a new log record, skip sequence number(uint64_t) and count(int32_t),
          // see format in db/write_batch.cpp.
          last_log_record_->remove_prefix(sizeof(uint64_t) + sizeof(int32_t));
        }
        else
        {
          log_debug("@@ more than");
          ++last_sequence_;
        }

        // records before this startup should be re-get again to get current status.
        return (last_sequence_ < first_sequence_);
      }

      int LdbRemoteSyncLogReader::get_log_record()
      {
        int ret = TAIR_RETURN_SUCCESS;
        bool need_new_reader = (NULL == reader_);
        leveldb::DBImpl* db = dynamic_cast<leveldb::DBImpl*>(instance_->db_);

        while (true)
        {
          last_log_scratch_.clear();
          last_sequence_ = 0;

          uint64_t db_logfile_size = db->LogWriter()->Size();
          uint64_t db_logfile_number = db->LogFileNumber();

          if (need_new_reader)
          {
            ret = start_new_reader(reading_logfile_number_);
            if (ret != TAIR_RETURN_SUCCESS)
            {
              log_error("start new log reader fail: %d", ret);
              break;
            }
            need_new_reader = false;
          }

          // reading earlier log
          if (reading_logfile_number_ < db_logfile_number)
          {
            log_debug("@@ from earlier log: %d", reading_logfile_number_);
            // read over one earlier whole log
            if (!reader_->ReadRecord(last_log_record_, &last_log_scratch_, ~((uint64_t)0)))
            {
              log_debug("@@ read over");
              need_new_reader = true;
              continue;
            }
            break;
          }
          else                  // reading current writting log
          {
            log_debug("@@ from now log: %d %lu", reading_logfile_number_, db_logfile_size);
            reader_->ReadRecord(last_log_record_, &last_log_scratch_, db_logfile_size);
            // read one record OR read over all written record, both OK.
            break;
          }
        }

        return ret;
      }

      int LdbRemoteSyncLogReader::parse_one_kv_record(int32_t& type, int32_t& bucket_num,
                                                      data_entry*& key, data_entry*& value, bool skip_value)
      {
        int ret = TAIR_RETURN_SUCCESS;

        char record_type;
        bool skipped = try_skip_one_kv_record(record_type);

        if (!skipped)
        {
          switch (record_type)
          {
          case leveldb::kTypeValue:
          {
            log_debug("@@ type value");
            type = TAIR_REMOTE_SYNC_TYPE_PUT;
            parse_one_key(bucket_num, key);

            if (skip_value)
            {
              skip_one_entry();
            }
            else
            {
              parse_one_value(key, value);
            }
            break;
          }
          case leveldb::kTypeDeletion:
          {
            log_debug("@@ type del");
            type = TAIR_REMOTE_SYNC_TYPE_DELETE;
            parse_one_key(bucket_num, key);
            break;
          }
          case leveldb::kTypeDeletionWithTailer:
          {
            log_debug("@@ type del tail");
            type = TAIR_REMOTE_SYNC_TYPE_DELETE;
            parse_one_key(bucket_num, key);
            parse_one_entry_tailer(key);
            break;
          }
          default:
          {
            log_error("bad record type: %d", record_type);
            // bad record type, we can't determine what to skip(one or two entry?), so we just abandon this record
            last_log_record_->clear();
            last_log_scratch_.clear();
            last_sequence_ = 0;

            ret = TAIR_RETURN_FAILED;
            break;
          }
          }
        }

        return ret;
      }

      bool LdbRemoteSyncLogReader::parse_one_key(int32_t& bucket_num, data_entry*& key)
      {
        leveldb::Slice key_slice;
        bool ret = leveldb::GetLengthPrefixedSlice(last_log_record_, &key_slice);
        log_debug("@@ parse key ret : %d", ret);
        if (ret)
        {
          LdbKey ldb_key;
          ldb_key.assign(const_cast<char*>(key_slice.data()), key_slice.size());
          bucket_num = ldb_key.get_bucket_number();
          key = new data_entry(ldb_key.key(), ldb_key.key_size(), true);
          key->has_merged = true;
        }
        return ret;
      }

      // key != NULL
      bool LdbRemoteSyncLogReader::parse_one_value(data_entry* key, data_entry*& value)
      {
        leveldb::Slice value_slice;
        bool ret = leveldb::GetLengthPrefixedSlice(last_log_record_, &value_slice);
        log_debug("@@ parse value ret : %d", ret);
        if (ret)
        {
          LdbItem ldb_item;
          ldb_item.assign(const_cast<char*>(value_slice.data()), value_slice.size());
          value = new data_entry(ldb_item.value(), ldb_item.value_size());

          key->data_meta.flag = value->data_meta.flag = ldb_item.flag();
          key->data_meta.cdate = value->data_meta.cdate = ldb_item.cdate();
          key->data_meta.edate = value->data_meta.edate = ldb_item.edate();
          key->data_meta.mdate = value->data_meta.mdate = ldb_item.mdate();
          key->data_meta.version = value->data_meta.version = ldb_item.version();
          key->data_meta.keysize = value->data_meta.keysize = key->get_size();
          key->data_meta.valsize = value->data_meta.valsize = ldb_item.value_size();
          key->set_prefix_size(ldb_item.prefix_size());
        }
        return ret;
      }

      bool LdbRemoteSyncLogReader::parse_one_entry_tailer(data_entry* entry)
      {
        leveldb::Slice tailer_slice; 
        bool ret = leveldb::GetLengthPrefixedSlice(last_log_record_, &tailer_slice);
        log_debug("@@ parse del tail ret : %d", ret);
        if (ret)
        {
          entry_tailer tailer(tailer_slice.data(), tailer_slice.size());
          tailer.consume_tailer(*entry);
        }

        return ret;
      }

      bool LdbRemoteSyncLogReader::skip_one_entry()
      {
        return leveldb::SkipLengthPrefixedSlice(last_log_record_);
      }

      bool LdbRemoteSyncLogReader::try_skip_one_kv_record(char& type)
      {
        type = last_log_record_->data()[0];
        last_log_record_->remove_prefix(1);

        // synced(duplicate/migrate) data need no remote synchronization
        bool skipped = leveldb::TestSyncMask(type);
        if (skipped)
        {
          int32_t skip_entry_count = 0;
          type = leveldb::OffSyncMask(type);
          switch (type)
          {
          case leveldb::kTypeValue:
          case leveldb::kTypeDeletionWithTailer:
            skip_entry_count = 2;
            break;
          case leveldb::kTypeDeletion:
            skip_entry_count = 1;
            break;
          default:
            skip_entry_count = 0;
            break;
          }

          log_debug("@@ skip count %d %d", skip_entry_count, type);
          for (int32_t i = 0; i < skip_entry_count; ++i)
          {
            skip_one_entry();
          }
        }

        return skipped;
      }


      //////////////////////////////////
      // LdbRemoteSyncLogger
      //////////////////////////////////
      LdbRemoteSyncLogger::LdbRemoteSyncLogger(LdbManager* manager) : manager_(manager)
      {
        // we use leveldb's bin-log here where key/value has already been
        // added in function logic, so need no writer
        writer_count_ = 0;
        // several reader, each ldb instance has one reader(leveldb log reader)
        reader_count_ = manager_->db_count_;
        reader_ = new LdbRemoteSyncLogReader*[reader_count_];
        for (int32_t i = 0; i < reader_count_; ++i)
        {
          reader_[i] = new LdbRemoteSyncLogReader(manager_->ldb_instance_[i]);
        }
      }

      LdbRemoteSyncLogger::~LdbRemoteSyncLogger()
      {
        if (reader_ != NULL)
        {
          for (int32_t i = 0; i < reader_count_; ++i)
          {
            delete reader_[i];
          }
          delete [] reader_;
        }
      }

      int LdbRemoteSyncLogger::init()
      {
        int ret = TAIR_RETURN_SUCCESS;
        for (int32_t i = 0; i < reader_count_; ++i)
        {
          if ((ret = reader_[i]->init()) != TAIR_RETURN_SUCCESS)
          {
            log_error("init reader %d fail, ret: %d", i, ret);
            break;
          }
        }
        return ret;
      }

      int LdbRemoteSyncLogger::restart()
      {
        int ret = TAIR_RETURN_SUCCESS;
        for (int32_t i = 0; i < reader_count_; ++i)
        {
          if ((ret = reader_[i]->init()) != TAIR_RETURN_SUCCESS)
          {
            log_error("restart reader %d fail, ret: %d", i, ret);
            break;
          }
        }
        return ret;
      }

      int LdbRemoteSyncLogger::add_record(int32_t index, int32_t type,
                                          data_entry* key, data_entry* value)
      {
        // no writer, do nothing.
        return TAIR_RETURN_SUCCESS;
      }

      int LdbRemoteSyncLogger::get_record(int32_t index, int32_t& type, int32_t& bucket_num,
                                          data_entry*& key, data_entry*& value,
                                          bool& force_reget)
      {
        int ret = TAIR_RETURN_SUCCESS;
        if (index < 0 || index >= reader_count_)
        {
          ret = TAIR_RETURN_FAILED;
        }
        else
        {
          ret = reader_[index]->get_record(type, bucket_num, key, value, force_reget);
        }
        return ret;
      }

    }
  }
}
