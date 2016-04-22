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

#include "db/db_impl.h"
#include "db/log_reader.h"
#include "db/filename.h"
#include "db/dbformat.h"

#include "common/log.hpp"
#include "ldb_define.hpp"

namespace tair {
namespace storage {
namespace ldb {

bool do_gc_file_check(leveldb::Iterator *input, leveldb::Logger *info_log) {
    bool valid = true;
    //gc sst file should have only one bucket
    input->SeekToFirst();
    leveldb::ParsedInternalKey ikey;
    bool first = true;
    int32_t bucket = 0;
    for (; input->Valid();) {
        leveldb::Slice origin_key = input->key();
        leveldb::Slice key(origin_key.data(), origin_key.size());
        if (!leveldb::ParseInternalKey(key, &ikey)) {
            // Do not hide error keys
            leveldb::Log(info_log, "ERROR: bad key");
        } else {
            int32_t cur_bucket = LdbKey::get_bucket_number(ikey.user_key.data());
            if (first) {
                bucket = cur_bucket;
                first = false;
            }
            if (cur_bucket != bucket) {
                valid = false;
                leveldb::Log(info_log, "ERROR: bad bucket, cur_bucket(%d), last_bucket(%d)", cur_bucket, bucket);
                break;
            }
        }
        input->Next();
    }
    return valid;
}

bool do_file_repair_check(leveldb::Iterator *iter, leveldb::ManualCompactionType type, leveldb::Logger *info_log) {
    bool valid = true;
    if (iter == NULL) {
        return false;
    }
    if (type == leveldb::KCompactSelfRepairRemoveExtraSSTFile ||
        type == leveldb::KCompactSelfRepairRemoveCorruptionSSTFile) {
        leveldb::Status s = iter->status();
        if (s.ok()) {
            valid = false;
        }
    } else if (type == leveldb::KCompactSelfRepairRemoveGCSSTFile) {
        valid = do_gc_file_check(iter, info_log);
    }
    return valid;
}

void ldb_key_printer(const leveldb::Slice &key, std::string &output) {
    // we only care bucket number, area and first byte of key now
    if (key.size() < LDB_KEY_META_SIZE + LDB_KEY_AREA_SIZE + 1) {
        log_error("invalid ldb key. igore print");
        output.append("DiRtY");
    } else {
        char buf[32];
        int32_t skip = 0;
        // bucket number
        skip += snprintf(buf + skip, sizeof(buf) - skip, "%d",
                         LdbKey::decode_bucket_number(key.data() + LDB_EXPIRED_TIME_SIZE));
        // area
        skip += snprintf(buf + skip, sizeof(buf) - skip, "-%d", LdbKey::decode_area(key.data() + LDB_KEY_META_SIZE));
        // first byte of key
        skip += snprintf(buf + skip, sizeof(buf) - skip, "-0x%X",
                         *(key.data() + LDB_KEY_META_SIZE + LDB_KEY_AREA_SIZE));
        output.append(buf);
    }
}

bool get_db_stat(leveldb::DB *db, std::string &value, const char *property) {
    bool ret = db != NULL && property != NULL;

    if (ret) {
        value.clear();

        char name[32];
        snprintf(name, sizeof(name), "leveldb.%s", property);
        std::string stat_value;

        if (!(ret = db->GetProperty(leveldb::Slice(std::string(name)), &stat_value, ldb_key_printer))) {
            log_error("get db stats fail");
        } else {
            value += stat_value;
        }
    }
    return ret;
}

bool get_db_stat(leveldb::DB *db, uint64_t &value, const char *property) {
    std::string str_value;
    bool ret = false;
    if ((ret = get_db_stat(db, str_value, property))) {
        value = atoll(str_value.c_str());
    }

    return ret;
}

int32_t get_level_num(leveldb::DB *db) {
    uint64_t level = 0;
    get_db_stat(db, level, "levelnums");
    return static_cast<int32_t>(level);
}

bool get_level_range(leveldb::DB *db, int32_t level, std::string *smallest, std::string *largest) {
    bool ret = false;
    if (db != NULL) {
        ret = db->GetLevelRange(level, smallest, largest);
    }
    return ret;
}

std::string get_back_path(const char *path) {
    if (path == NULL) {
        return std::string("");
    }

    char back_path[TAIR_MAX_PATH_LEN + 16];
    char *pos = back_path;
    pos += snprintf(back_path, TAIR_MAX_PATH_LEN, "%s.bak.", path);
    tbsys::CTimeUtil::timeToStr(time(NULL), pos);
    return std::string(back_path);
}

//////////////////// LdbLogsReader
LdbLogsReader::LdbLogsReader(leveldb::DB *db, Filter *filter, uint64_t start_lognumber, bool delete_file) :
        db_(db), filter_(filter), delete_file_(delete_file),
        reading_logfile_number_(start_lognumber), reader_(NULL), last_logfile_refed_(false),
        last_log_record_(new leveldb::Slice()), last_sequence_(0) {
}

LdbLogsReader::~LdbLogsReader() {
    if (last_log_record_ != NULL) {
        delete last_log_record_;
    }

    clear_reader(0);
}

int LdbLogsReader::get_record(int32_t &type, leveldb::Slice &key, leveldb::Slice &value, bool &alloc) {
    alloc = false;

    int ret = TAIR_RETURN_SUCCESS;

    while (true) {
        if (last_log_record_->size() <= 0) {
            ret = get_log_record();
            log_debug("@@ get new record: %zu", last_log_record_->size());
        }

        // read one log record
        if (TAIR_RETURN_SUCCESS == ret && last_log_record_->size() > 0) {
            update_last_sequence();
            ret = parse_one_kv_record(type, key, value);
            // this kv record is skipped, just try next
            if (TAIR_RETURN_SUCCESS == ret && key.empty()) {
                continue;
            } else {
                // read fail or got a kv
                break;
            }
        } else {
            // read fail or read over
            break;
        }
    }

    return ret;
}

int LdbLogsReader::start_new_reader(uint64_t min_number) {
    int ret = TAIR_RETURN_SUCCESS;

    clear_reader(min_number);

    leveldb::DBImpl *db = dynamic_cast<leveldb::DBImpl *>(db_);
    leveldb::Env *db_env = db->GetEnv();
    const std::string &db_log_dir = db->DBLogDir();
    std::vector <std::string> filenames;
    leveldb::Status s = db_env->GetChildren(db_log_dir, &filenames);

    uint64_t number = 0;
    leveldb::FileType type;
    std::vector <uint64_t> logs;
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
        ret = init_reader(new_logfile_number);
    }

    return ret;
}

int LdbLogsReader::init_reader(uint64_t number) {
    leveldb::Status s;
    leveldb::DBImpl *db = dynamic_cast<leveldb::DBImpl *>(db_);
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
        log_debug("start new ldb rsync reader, filenumber: %"PRI64_PREFIX"u", reading_logfile_number_);
        reader_ = rfile != NULL ?
                  new leveldb::log::Reader(rfile, NULL, true, 0) :
                  new leveldb::log::Reader(sfile, NULL, true, 0);
        last_logfile_refed_ = refed;
    }
    return s.ok() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
}

void LdbLogsReader::clear_reader(uint64_t number) {
    if (reader_ != NULL) {
        if (last_logfile_refed_) {
            dynamic_cast<leveldb::ReadableAndWritableFile *>(reader_->RFile())->Unref();
        } else {
            delete reader_->SFile();
        }
        delete reader_;
        reader_ = NULL;
        if (delete_file_ && number > 0) {
            dynamic_cast<leveldb::DBImpl *>(db_)->DeleteLogFile(number);
        }
    }
}

void LdbLogsReader::update_last_sequence() {
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
}

int LdbLogsReader::get_log_record() {
    int ret = TAIR_RETURN_SUCCESS;
    bool need_new_reader = (NULL == reader_);
    leveldb::DBImpl *db = dynamic_cast<leveldb::DBImpl *>(db_);

    while (true) {
        last_log_scratch_.clear();
        last_sequence_ = 0;

        uint64_t db_logfile_size = db->LogWriter()->Size();
        uint64_t db_logfile_number = db->LogFileNumber();

        if (need_new_reader) {
            ret = start_new_reader(reading_logfile_number_);
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

    return ret;
}

int LdbLogsReader::parse_one_kv_record(int32_t &type, leveldb::Slice &key, leveldb::Slice &value) {
    int ret = TAIR_RETURN_SUCCESS;

    // get type
    char record_type = last_log_record_->data()[0];
    last_log_record_->remove_prefix(1);
    // off mask for sync
    type = leveldb::OffSyncMask(record_type);

    switch (type) {
        case leveldb::kTypeValue: {
            log_debug("@@ type value");
            // pass record_type to do filter
            parse_one_kv(record_type, key, value);
            break;
        }
        case leveldb::kTypeDeletion: {
            log_debug("@@ type del");
            parse_one_key(record_type, key);
            break;
        }
        case leveldb::kTypeDeletionWithTailer: {
            log_debug("@@ type del tail");
            // tailer will be value
            parse_one_kv(record_type, key, value);
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

    return ret;
}

bool LdbLogsReader::parse_one_kv(int32_t type, leveldb::Slice &key, leveldb::Slice &value) {
    bool ret = leveldb::GetLengthPrefixedSlice(last_log_record_, &key);
    log_debug("@@ parse key ret : %d", ret);
    if (ret) {
        if (filter_ == NULL || filter_->ok(type, key, last_sequence_)) {
            ret = leveldb::GetLengthPrefixedSlice(last_log_record_, &value);
        } else {
            // skip this key
            key.clear();
            // skip value
            ret = leveldb::SkipLengthPrefixedSlice(last_log_record_);
        }
    }
    return ret;
}

bool LdbLogsReader::parse_one_key(int32_t type, leveldb::Slice &key) {
    bool ret = leveldb::GetLengthPrefixedSlice(last_log_record_, &key);
    log_debug("@@ parse key ret : %d", ret);
    if (ret && filter_ != NULL && filter_->ok(type, key, last_sequence_)) {
        // skip this key
        key.clear();
    }
    return ret;
}

//////////////////// LdbBucketDataIter
LdbBucketDataIter::LdbBucketDataIter(int32_t bucket, leveldb::DB *db) :
        bucket_(bucket), db_(db), db_it_(NULL),
        log_filter_(NULL), log_reader_(NULL), log_snapshot_(NULL), alloc_(false) {
}

LdbBucketDataIter::~LdbBucketDataIter() {
    clear();

    if (log_snapshot_ != NULL) {
        db_->ReleaseLogSnapshot(log_snapshot_);
    }
    if (log_reader_ != NULL) {
        delete log_reader_;
    }
    if (log_filter_ != NULL) {
        delete log_filter_;
    }
    if (db_it_ != NULL) {
        delete db_it_;
    }
}

void LdbBucketDataIter::seek_to_first() {
    char scan_key[LDB_KEY_META_SIZE];
    LdbKey::build_key_meta(scan_key, bucket_);

    log_snapshot_ = dynamic_cast<const leveldb::LogSnapshotImpl *>(db_->GetLogSnapshot());
    log_filter_ = new LdbBucketDataIter::LogFilter(bucket_, log_snapshot_->number_/*sequence*/);
    log_reader_ = new LdbLogsReader(db_, log_filter_,
                                    log_snapshot_->log_number_ - 1/* read current log*/,
                                    false/* not delete log file*/);

    // get db iterator
    leveldb::ReadOptions scan_options;
    scan_options.verify_checksums = false;
    scan_options.fill_cache = false;
    scan_options.hold_for_long = true;

    db_it_ = db_->NewIterator(scan_options);
    db_it_->Seek(leveldb::Slice(scan_key, sizeof(scan_key)));
    db_sanity();
}

void LdbBucketDataIter::next() {
    clear();
    if (db_it_ != NULL) {
        db_it_->Next();
        // make sure db sanity when next next()
        db_sanity();
    } else {
        log_reader_->get_record(type_, key_, value_, alloc_);
    }
}

bool LdbBucketDataIter::valid() {
    return key_.size() > 0;
}

leveldb::Slice &LdbBucketDataIter::key() {
    return key_;
}

leveldb::Slice &LdbBucketDataIter::value() {
    return value_;
}

int32_t LdbBucketDataIter::type() {
    return type_;
}

void LdbBucketDataIter::db_sanity() {
    if (db_it_->Valid() &&
        LdbKey::decode_bucket_number_with_key(db_it_->key().data()) == bucket_) {
        key_ = db_it_->key();
        value_ = db_it_->value();
        type_ = leveldb::kTypeValue;
    } else {
        // db data over.
        delete db_it_;
        db_it_ = NULL;
        // next one
        next();
    }
}

void LdbBucketDataIter::clear() {
    if (alloc_) {
        delete key_.data();
        delete value_.data();
    }
    key_.clear();
    value_.clear();
    alloc_ = false;
}

}
}
}
