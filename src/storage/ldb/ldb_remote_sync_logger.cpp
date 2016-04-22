/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
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

#include "common/result.hpp"
#include "common/record.hpp"

namespace tair {
namespace storage {
namespace ldb {
using tair::common::data_entry;

//////////////////////////////////
// LdbRemoteSyncLogReader
//////////////////////////////////
LdbRemoteSyncLogReader::LdbRemoteSyncLogReader(LdbInstance *instance, bool new_way, bool delete_file) :
        instance_(instance), first_sequence_(0), reading_logfile_number_(0), reader_(NULL), last_logfile_refed_(false),
        last_sequence_(0), min_position_(), delete_file_(delete_file) {
    new_way_ = new_way;
    last_record_offset_ = 0;
    last_log_record_ = new leveldb::Slice();
    records_head_of_log_file_ = new std::queue<tair::common::Record *>();
    max_bucket_count_ = 0;
    buffer_size_ = 10;
    start_new_reader(0, 0);
}

LdbRemoteSyncLogReader::LdbRemoteSyncLogReader(LdbInstance *instance,
                                               int max_bucket_count, tair::common::RecordPosition &position,
                                               bool new_way, bool delete_file) {
    new_way_ = new_way;
    instance_ = instance;
    reading_logfile_number_ = 0;
    reader_ = NULL;
    last_logfile_refed_ = false;
    last_sequence_ = 0;
    delete_file_ = delete_file;
    last_record_offset_ = 0;
    last_log_record_ = new leveldb::Slice();
    records_head_of_log_file_ = new std::queue<tair::common::Record *>();
    max_bucket_count_ = max_bucket_count;
    buffer_size_ = 10;

    first_sequence_ = position.sequence_;
    uint64_t file_number = position.filenumber_;
    file_number = (file_number == 0 ? 0 : (file_number - 1));
    start_new_reader(file_number, position.fileoffset_);
}

LdbRemoteSyncLogReader::~LdbRemoteSyncLogReader() {
    if (last_log_record_ != NULL) {
        delete last_log_record_;
    }

    if (records_head_of_log_file_ != NULL) {
        while (!records_head_of_log_file_->empty()) {
            delete records_head_of_log_file_->front();
            records_head_of_log_file_->pop();
        }
        delete records_head_of_log_file_;
    }

    clear_reader(0);
}

LdbRemoteSyncLogReader *LdbRemoteSyncLogReader::clone() {
    // not copy record at head of log file,
    // because we use its position in bin log, so it will read again
    return new LdbRemoteSyncLogReader(instance_, max_bucket_count_, min_position_, true, delete_file_);
}

int LdbRemoteSyncLogReader::init() {
    return TAIR_RETURN_SUCCESS;
}

uint64_t LdbRemoteSyncLogReader::restart() {
    if (instance_->db_ != NULL) {
        // restart first sequence
        first_sequence_ = dynamic_cast<leveldb::DBImpl *>(instance_->db_)->LastSequence() + 1;
        log_warn("reader restart, first_sequence: %"PRI64_PREFIX"u", first_sequence_);
    }
    return first_sequence_;
}

void LdbRemoteSyncLogReader::delete_log_file(uint64_t filenumber) {
    dynamic_cast<leveldb::DBImpl *>(instance_->db_)->DeleteLogFile(filenumber);
}

tair::common::Result<std::queue<tair::common::Record *> *> LdbRemoteSyncLogReader::get_records() {
    size_t size = records_head_of_log_file_->size();
    size_t count = buffer_size_ > size ? buffer_size_ - size : 0;
    for (size_t i = 0; i < count; i++) {
        int32_t type = 0;
        int32_t bucket = -1;
        int32_t origin_bucket = -1;
        data_entry *key = NULL;
        data_entry *value = NULL;
        bool force_reget = false;
        int ret = get_record(type, bucket, origin_bucket, key, value, force_reget);
        if (ret == TAIR_RETURN_SUCCESS && key != NULL) {
            tair::common::RecordPosition position;
            position.instance_ = instance_->index_;
            position.filenumber_ = reading_logfile_number_;
            position.fileoffset_ = last_record_offset_;
            position.sequence_ = last_sequence_;

            if (min_position_.valid() == false) {
                log_warn("just happen at start rsync or new log reader");
                min_position_ = position;
            }
            Record *record = new KVRecord(position, (TairRemoteSyncType) type, bucket, force_reget, key, value);
            record->set_origin_bucket(origin_bucket);
            records_head_of_log_file_->push(record);
        } else {
            break;
        }
    }

    // position will update at Function update
    // get_records will never increase position

    if (!records_head_of_log_file_->empty()) {
        return Result<std::queue<tair::common::Record *> *>(records_head_of_log_file_, TAIR_RETURN_SUCCESS);
    } else {
        // all kvs are slave kv
        if (min_position_.valid() == false || reading_logfile_number_ > min_position_.filenumber_) {
            min_position_.instance_ = instance_->index_;
            min_position_.filenumber_ = reading_logfile_number_;
            min_position_.set_undefined();
        }
    }

    return Result<std::queue<tair::common::Record *> *>(NULL, TAIR_RETURN_DATA_NOT_EXIST);
}

void LdbRemoteSyncLogReader::update() {
    assert(records_head_of_log_file_->empty() == false);
    tair::common::Record *record = records_head_of_log_file_->front();
    min_position_ = record->position_;
    records_head_of_log_file_->pop();
}

int LdbRemoteSyncLogReader::get_record(int32_t &type, int32_t &bucket_num, int32_t &origin_bucket_num,
                                       data_entry *&key, data_entry *&value, bool &force_reget) {
    if (NULL == instance_->db_) {
        log_debug("db not init yet");
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    key = value = NULL;
    bucket_num = -1;

    int ret = TAIR_RETURN_SUCCESS;

    while (true) {
        if (last_log_record_->size() <= 0) {
            ret = get_log_record();
            log_debug("@@ get new record: %zu", last_log_record_->size());
        }

        // maybe read over all current writing log record
        if (TAIR_RETURN_SUCCESS == ret && last_log_record_->size() > 0) {
            force_reget = update_last_sequence();
            ret = parse_one_kv_record(type, bucket_num, key, value, force_reget);
            // this kv record is skipped, just try next
            if (TAIR_RETURN_SUCCESS == ret && NULL == key) {
                continue;
            } else {
                break;
            }
        } else {
            // read over or fail
            type = TAIR_REMOTE_SYNC_TYPE_PUT;
            break;
        }
    }

    origin_bucket_num = bucket_num;
    if (ret == TAIR_RETURN_SUCCESS && key != NULL && max_bucket_count_ > 0) {
        assert((new_way_ == false && max_bucket_count_ == 0) ||
               (new_way_ == true && max_bucket_count_ > 0));
        // re-compute new bucket instead of local cluster bucket
        bucket_num = recalc_dest_bucket(key);
        log_debug("max_bucket_count %d, bucket_num %d", max_bucket_count_, bucket_num);
    }

    return ret;
}

int LdbRemoteSyncLogReader::recalc_dest_bucket(const data_entry *key) {
    uint32_t hash;
    int prefix_size = key->get_prefix_size();
    int32_t diff_size = key->has_merged ? TAIR_AREA_ENCODE_SIZE : 0;
    if (prefix_size == 0) {
        hash = util::string_util::mur_mur_hash(key->get_data() + diff_size, key->get_size() - diff_size);
    } else {
        hash = util::string_util::mur_mur_hash(key->get_data() + diff_size, prefix_size);
    }

    return hash % max_bucket_count_;
}

tair::common::RecordPosition LdbRemoteSyncLogReader::get_min_position() {
    return min_position_;
}

int LdbRemoteSyncLogReader::start_new_reader(uint64_t min_number, uint64_t offset) {
    int ret = TAIR_RETURN_SUCCESS;
    if (instance_->db_ != NULL) {
        clear_reader(min_number);

        leveldb::DBImpl *db = dynamic_cast<leveldb::DBImpl *>(instance_->db_);
        if (first_sequence_ <= 0) {
            first_sequence_ = db->LastSequence() + 1;
            log_warn("first sequence of ldb rsync log reader %d: %"PRI64_PREFIX"u", instance_->index_, first_sequence_);
        }

        leveldb::Env *db_env = db->GetEnv();
        const std::string &db_log_dir = db->DBLogDir();
        std::vector<std::string> filenames;
        leveldb::Status s = db_env->GetChildren(db_log_dir, &filenames);

        uint64_t number = 0;
        leveldb::FileType type;
        std::vector<uint64_t> logs;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (leveldb::ParseFileName(filenames[i], &number, &type) && type == leveldb::kLogFile) {
                logs.push_back(number);
            }
        }

        uint64_t new_logfile_number = 0;
        uint64_t db_logfile_number = db->LogFileNumber();

        if (!logs.empty()) {
            std::sort(logs.begin(), logs.end());
            // maybe binary-search..
            for (size_t i = 0; i < logs.size(); ++i) {
                if (logs[i] > min_number && logs[i] <= db_logfile_number) {
                    new_logfile_number = logs[i];
                    break;
                }
            }
        }

        if (0 == new_logfile_number) {
            log_info("no ldb log for reader");
        } else {
            ret = init_reader(new_logfile_number, offset);
        }
    }

    return ret;
}

int LdbRemoteSyncLogReader::init_reader(uint64_t number, uint64_t offset) {
    leveldb::Status s;
    leveldb::DBImpl *db = dynamic_cast<leveldb::DBImpl *>(instance_->db_);
    // file will be Ref()
    leveldb::RandomAccessFile *rfile = db->LogFile(number);
    bool refed = (rfile != NULL);
    leveldb::SequentialFile *sfile = NULL;

    // not current writing log, current writing db logger will be ReadableAndWritableFile
    if (NULL == rfile) {
        std::string fname = leveldb::LogFileName(db->DBLogDir(), number);
        s = db->GetEnv()->NewSequentialFile(fname, &sfile);
        if (!s.ok()) {
            log_error("init to read log file %s fail: %s", fname.c_str(), s.ToString().c_str());
        }
    }

    if (s.ok()) {
        reading_logfile_number_ = number;
        // TODO: reporter
        log_debug("start new ldb rsync reader, filenumber: %"PRI64_PREFIX"u offset %"PRI64_PREFIX"u",
                  reading_logfile_number_, offset);
        reader_ = rfile != NULL ?
                  new leveldb::log::Reader(rfile, NULL, true, offset) :
                  new leveldb::log::Reader(sfile, NULL, true, offset);
        last_logfile_refed_ = refed;
    }
    return s.ok() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
}

void LdbRemoteSyncLogReader::clear_reader(uint64_t number) {
    if (reader_ != NULL) {
        if (last_logfile_refed_) {
            dynamic_cast<leveldb::ReadableAndWritableFile *>(reader_->RFile())->Unref();
        } else {
            delete reader_->SFile();
        }
        delete reader_;
        reader_ = NULL;
        if (delete_file_ && number > 0) {
            dynamic_cast<leveldb::DBImpl *>(instance_->db_)->DeleteLogFile(number);
        }
    }
}

bool LdbRemoteSyncLogReader::update_last_sequence() {
    if (0 == last_sequence_) {
        log_debug("@@ new last seq");
        last_sequence_ = leveldb::DecodeFixed64(last_log_record_->data());
        // this is a new log record, skip sequence number(uint64_t) and count(int32_t),
        // see format in db/write_batch.cpp.
        last_log_record_->remove_prefix(sizeof(uint64_t) + sizeof(int32_t));
    } else {
        log_debug("@@ more than");
        ++last_sequence_;
    }

    if (new_way_ == true) return false; // new way never reget
    // records before this startup should be re-get again to get current status.
    return (last_sequence_ < first_sequence_);
}

int LdbRemoteSyncLogReader::get_log_record() {
    int ret = TAIR_RETURN_SUCCESS;
    bool need_new_reader = (NULL == reader_);
    leveldb::DBImpl *db = dynamic_cast<leveldb::DBImpl *>(instance_->db_);

    while (true) {
        last_log_scratch_.clear();
        last_sequence_ = 0;

        uint64_t db_logfile_size = db->LogWriter()->Size();
        uint64_t db_logfile_number = db->LogFileNumber();

        if (need_new_reader) {
            ret = start_new_reader(reading_logfile_number_, 0);
            if (ret != TAIR_RETURN_SUCCESS) {
                log_error("start new log reader fail: %d", ret);
                break;
            }
            // no log for reader
            if (reader_ == NULL) {
                break;
            }

            need_new_reader = false;
        }

        // reading earlier log
        if (reading_logfile_number_ < db_logfile_number) {
            log_debug("@@ from earlier log: %lu", reading_logfile_number_);
            // read over one earlier whole log
            if (!reader_->ReadRecord(last_log_record_, &last_log_scratch_, ~((uint64_t) 0))) {
                log_debug("@@ read over");
                need_new_reader = true;
                continue;
            }
            break;
        } else                  // reading current writting log
        {
            log_debug("@@ from now log: %lu %lu", reading_logfile_number_, db_logfile_size);
            reader_->ReadRecord(last_log_record_, &last_log_scratch_, db_logfile_size);
            // read one record OR read over all written record, both OK.
            break;
        }
    }

    if (reader_ != NULL) {
        last_record_offset_ = reader_->LastRecordOffset();
    }

    return ret;
}

int LdbRemoteSyncLogReader::parse_one_kv_record(int32_t &type, int32_t &bucket_num,
                                                data_entry *&key, data_entry *&value, bool skip_value) {
    char record_type;
    int ret = TAIR_RETURN_SUCCESS;

    bool is_from_unit = false;

    bool skipped = try_skip_one_kv_record(record_type, is_from_unit);

    if (!skipped) {
        switch (record_type) {
            case leveldb::kTypeValue: {
                log_debug("@@ type value");
                type = TAIR_REMOTE_SYNC_TYPE_PUT;
                parse_one_key(bucket_num, key);

                if (skip_value) {
                    skip_one_entry();
                } else {
                    parse_one_value(key, value);
                }
                break;
            }
            case leveldb::kTypeDeletion: {
                log_debug("@@ type del");
                type = TAIR_REMOTE_SYNC_TYPE_DELETE;
                parse_one_key(bucket_num, key);
                break;
            }
            case leveldb::kTypeDeletionWithTailer: {
                log_debug("@@ type del tail");
                type = TAIR_REMOTE_SYNC_TYPE_DELETE;
                parse_one_key(bucket_num, key);
                parse_one_entry_tailer(key);
                break;
            }
            default: {
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

    if (is_from_unit == true && ret == TAIR_RETURN_SUCCESS && key != NULL) {
        set_meta_flag_unit(key->data_meta.flag);
    }

    return ret;
}

bool LdbRemoteSyncLogReader::parse_one_key(int32_t &bucket_num, data_entry *&key) {
    leveldb::Slice key_slice;
    bool ret = leveldb::GetLengthPrefixedSlice(last_log_record_, &key_slice);
    log_debug("@@ parse key ret : %d", ret);
    if (ret) {
        LdbKey ldb_key;
        ldb_key.assign(const_cast<char *>(key_slice.data()), key_slice.size());
        bucket_num = ldb_key.get_bucket_number();
        key = new data_entry(ldb_key.key(), ldb_key.key_size(), true);
        key->has_merged = true;
    }
    return ret;
}

// key != NULL
bool LdbRemoteSyncLogReader::parse_one_value(data_entry *key, data_entry *&value) {
    leveldb::Slice value_slice;
    bool ret = leveldb::GetLengthPrefixedSlice(last_log_record_, &value_slice);
    log_debug("@@ parse value ret : %d", ret);
    if (ret) {
        LdbItem ldb_item;
        ldb_item.assign(const_cast<char *>(value_slice.data()), value_slice.size());
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

bool LdbRemoteSyncLogReader::parse_one_entry_tailer(data_entry *entry) {
    leveldb::Slice tailer_slice;
    bool ret = leveldb::GetLengthPrefixedSlice(last_log_record_, &tailer_slice);
    log_debug("@@ parse del tail ret : %d", ret);
    if (ret) {
        entry_tailer tailer(tailer_slice.data(), tailer_slice.size());
        tailer.consume_tailer(*entry);
    }

    return ret;
}

bool LdbRemoteSyncLogReader::skip_one_entry() {
    return leveldb::SkipLengthPrefixedSlice(last_log_record_);
}

bool LdbRemoteSyncLogReader::try_skip_one_kv_record(char &type, bool &is_from_unit) {
    type = last_log_record_->data()[0];
    last_log_record_->remove_prefix(1);

    // synced(duplicate/migrate) data need no remote synchronization
    bool skipped = leveldb::TestSyncMask(type);
    if (leveldb::TestUnitMask(type)) {
        type = leveldb::OffUnitMask(type);
        is_from_unit = true;
    } else {
        is_from_unit = false;
    }

    if (skipped) {
        int32_t skip_entry_count = 0;
        type = leveldb::OffSyncMask(type);
        switch (type) {
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
        for (int32_t i = 0; i < skip_entry_count; ++i) {
            skip_one_entry();
        }
    }

    return skipped;
}


//////////////////////////////////
// LdbRemoteSyncLogger
//////////////////////////////////
LdbRemoteSyncLogger::LdbRemoteSyncLogger(LdbManager *manager) : manager_(manager) {
    // we use leveldb's bin-log here where key/value has already been
    // added in function logic, so need no writer
    writer_count_ = 0;
    // several reader, each ldb instance has one reader(leveldb log reader)
    reader_count_ = manager_->db_count_;
    reader_ = new LdbRemoteSyncLogReader *[reader_count_];
    for (int32_t i = 0; i < reader_count_; ++i) {
        // because this logger use for old remote sync manager, so we just use TAIR_DEFAULT_BUCKET_NUMBER
        reader_[i] = new LdbRemoteSyncLogReader(manager_->ldb_instance_[i]);
    }
}

LdbRemoteSyncLogger::~LdbRemoteSyncLogger() {
    if (reader_ != NULL) {
        for (int32_t i = 0; i < reader_count_; ++i) {
            delete reader_[i];
        }
        delete[] reader_;
    }
}

int LdbRemoteSyncLogger::init() {
    int ret = TAIR_RETURN_SUCCESS;
    for (int32_t i = 0; i < reader_count_; ++i) {
        if ((ret = reader_[i]->init()) != TAIR_RETURN_SUCCESS) {
            log_error("init reader %d fail, ret: %d", i, ret);
            break;
        }
    }
    return ret;
}

int LdbRemoteSyncLogger::restart() {
    for (int32_t i = 0; i < reader_count_; ++i) {
        uint64_t first_seq = reader_[i]->restart();
        log_error("restart instance %d reader, seq: %llu", i, (unsigned long long) first_seq);
    }
    return TAIR_RETURN_SUCCESS;
}

int LdbRemoteSyncLogger::add_record(int32_t index, int32_t type,
                                    data_entry *key, data_entry *value) {
    // no writer, do nothing.
    return TAIR_RETURN_SUCCESS;
}

int LdbRemoteSyncLogger::get_record(int32_t index, int32_t &type, int32_t &bucket_num,
                                    data_entry *&key, data_entry *&value,
                                    bool &force_reget) {
    int ret = TAIR_RETURN_SUCCESS;
    if (index < 0 || index >= reader_count_) {
        ret = TAIR_RETURN_FAILED;
    } else {
        int32_t origin_bucket_num = -1;
        ret = reader_[index]->get_record(type, bucket_num, origin_bucket_num, key, value, force_reget);
    }
    return ret;
}
}
}
}
