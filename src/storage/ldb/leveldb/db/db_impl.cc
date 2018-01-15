// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "../../rt_profiler.hpp"

namespace leveldb {
using namespace tair::storage;
const char* DBImpl::kBackupTableFileDir = "backup";
// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,            20,     200000);
  ClipToRange(&result.write_buffer_size,         64<<10, 1<<30);
  ClipToRange(&result.block_size,                1<<10,  4<<20);
  ClipToRange(&result.block_cache_size,          8<<20,  1<<30);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (s.ok()) {
      s = result.info_log->Rotate();
    }
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(result.block_cache_size);
  }

  // config sort of
  config::setConfig(result);
  return result;
}

DBImpl::DBImpl(const Options& options, const std::string& dbname)
    : env_(options.env),
      internal_comparator_(options.comparator),
      internal_filter_policy_(options.filter_policy),
      options_(SanitizeOptions(
          dbname, &internal_comparator_, &internal_filter_policy_, options)),
      owns_info_log_(options_.info_log != options.info_log),
      owns_cache_(options_.block_cache != options.block_cache),
      dbname_(dbname),
      dblog_dir_(dbname + "/logs/"),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(new MemTable(internal_comparator_, env_)),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      tmp_batch_(new WriteBatch),
      min_snapshot_log_number_(~(uint64_t)0),
      // @@ for multi-bucket update
      imm_list_count_(0),
      bu_head_(NULL),
      bu_tail_(NULL),
      logger_(NULL),
      logger_cv_(&mutex_),
      // @@
      has_limited_delete_obsolete_file_count_(0),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL),
      today_start_(env_->TodayStart()) {
  mem_->Ref();
  has_imm_.Release_Store(NULL);

  // // Reserve ten files or so for other uses and give the rest to TableCache.
  // const int table_cache_size = options.max_open_files - 10;
  // table_cache_ = new TableCache(dbname_, &options_, table_cache_size);
  // use memory charge limit for TableCache
  table_cache_ = new TableCache(dbname_, &options_, options_.table_cache_size);

  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  //delete logfile_;
  if (logfile_ != NULL) {
    logfile_->Unref();
  }
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (++has_limited_delete_obsolete_file_count_ < config::kLimitDeleteObsoleteFileInterval) {
    Log(options_.info_log, "limit delete %lu", has_limited_delete_obsolete_file_count_);
    // we limit delete obsolete file now
    return ;
  }
  has_limited_delete_obsolete_file_count_ = 0;
  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  PROFILER_BEGIN("del addchild+");
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  if (!options_.reserve_log) {
    env_->GetChildren(dblog_dir_, &filenames);
  }
  PROFILER_END();
  PROFILER_BEGIN("del file+");
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= min_snapshot_log_number_) ||
                  (number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
        case kBucketLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        if (type == kLogFile) {
          env_->DeleteFile(dblog_dir_ + "/" + filenames[i]);
        } else if ((type == kTableFile || type == kDescriptorFile) && config::kDoBackUpSSTFile) {
            env_->RenameFile(dbname_ + "/" + filenames[i], dbname_ + "/" + DBImpl::kBackupTableFileDir + "/" + filenames[i]);
        }else {
          env_->DeleteFile(dbname_ + "/" + filenames[i]);
        }
      }
    }
  }
  PROFILER_END();
}

void DBImpl::DeleteLogFile(uint64_t min_number)
{
  if (min_number < min_snapshot_log_number_)
  {
    std::vector<std::string> filenames;
    PROFILER_BEGIN("del addchild+");
    env_->GetChildren(dblog_dir_, &filenames);
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type == kLogFile && number <= min_number) {
        if (config::kLogFileKeepInterval > 0) {
          uint64_t mtime = 0;
          Status s = env_->GetFileMTime(dblog_dir_ + "/" + filenames[i], &mtime);
          if (s.ok()) {
            if (env_->NowSecs() - mtime >= config::kLogFileKeepInterval) {
              env_->DeleteFile(dblog_dir_ + "/" + filenames[i]);
            }
          }
        } else {
          env_->DeleteFile(dblog_dir_ + "/" + filenames[i]);
        }
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  env_->CreateDir(dblog_dir_);
  env_->CreateDir(dbname_ + "/" + DBImpl::kBackupTableFileDir);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover();
  if (s.ok()) {
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
      return s;
    }
    s = env_->GetChildren(dblog_dir_, &filenames);
    if (!s.ok()) {
      return s;
    }
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)
          && type == kLogFile
          && ((number >= min_log) || (number == prev_log))) {
        logs.push_back(number);
      }
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
      s = RecoverLogFile(logs[i], edit, &max_sequence);

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  // Note: binlog will be in directory dbname_/logs/ now,
  //       we also try to find it in directory dbname_/ to
  //       be compatible to recovering from old version db.
  bool old_log = false;
  std::string fname = LogFileName(dblog_dir_, log_number);
  if (!env_->FileExists(fname)) {
    Log(options_.info_log, "try to find log file in db dir, recover from old version db.");
    fname = LogFileName(dbname_, log_number);
    old_log = true;
  }
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_, env_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = WriteLevel0Table(mem, edit, NULL);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      mem->Unref();
      mem = NULL;
    }
  }

  if (status.ok() && mem != NULL) {
    status = WriteLevel0Table(mem, edit, NULL);
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.

    // delete log file when recover from old version log file
    if (status.ok() && old_log) {
      env_->DeleteFile(fname);
    }
  }

  if (mem != NULL) mem->Unref();
  delete file;
  return status;
}

// actually WriteLevel0Table() can be run by only one thread(compaction-thread)
// and its mutlti-thread write-action(versionset::NewFileNumber()/table_cache_) is all thread-safe now,
// so no mutex here.
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  Iterator* iter = mem->NewIterator();
  iter->SeekToFirst();
  Status s;
  while (iter->Valid() && s.ok()) {
    const uint64_t start_micros = env_->NowMicros();
    FileMetaData meta;
    meta.number = versions_->NewFileNumber();
    pending_outputs_.insert(meta.number);
    Log(options_.info_log, "Level-0 table #%llu: started",
        (unsigned long long) meta.number);

    {
      PROFILER_BEGIN("buildtab-");
      s = BuildTable(dbname_, env_, options_, config::kDoSplitMmtCompaction ? user_comparator() : NULL,
                     table_cache_, iter, &meta);
      PROFILER_END();
    }

    Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
        (unsigned long long) meta.number,
        (unsigned long long) meta.file_size,
        s.ToString().c_str());
    pending_outputs_.erase(meta.number);


    // Note that if file_size is zero, the file has been deleted and
    // should not be added to the manifest.
    int level = 0;
    if (s.ok() && meta.file_size > 0) {
      const Slice min_user_key = meta.smallest.user_key();
      const Slice max_user_key = meta.largest.user_key();
      if (base != NULL) {
        PROFILER_BEGIN("picklevel+");
        level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
        PROFILER_END();
      }
      edit->AddFile(level, meta.number, meta.file_size,
                    meta.smallest, meta.largest);
    }

    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros;
    stats.bytes_written = meta.file_size;
    stats_[level].Add(stats);
  }
  delete iter;

  return s;
}

Status DBImpl::CompactMemTable(bool compact_mlist) {
  Status s;
  if (compact_mlist && !imm_list_.empty()) {
    s = CompactMemTableList();
    if (!s.ok()) {
      return s;
    }
  }

  if (NULL == imm_) {
    return s;
  }

  VersionEdit edit;
  mutex_.Lock();
  // Save the contents of the memtable as a new Table
  Version* base = versions_->current();
  base->Ref();
  mutex_.Unlock();
  PROFILER_BEGIN("wL0Tab+");
  s = WriteLevel0Table(imm_, &edit, base);
  PROFILER_END();
  mutex_.Lock();
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }
  mutex_.Unlock();

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    // logfile_number
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    PROFILER_BEGIN("log apply+");
    s = versions_->LogAndApply(&edit, &mutex_);
    PROFILER_END();
  }

  if (s.ok()) {
    mutex_.Lock();
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    mutex_.Unlock();
    // do not DeleteObsoleteFiles() here
    // DeleteObsoleteFiles();
  }

  return s;
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

Status DBImpl::ForceCompactMemTable() {
  {
    MutexLock l(&mutex_);
    // TODO: emit..
    LoggerId self;
    AcquireLoggingResponsibility(&self);
    // switch all memtable to imm memtable list
    for (BucketMap::iterator it = bucket_map_.begin(); it != bucket_map_.end(); ++it) {
      delete it->second->log_;
      delete it->second->logfile_;
      imm_list_.push_front(it->second);
      imm_list_count_++;
    }
    bu_head_ = bu_tail_ = NULL;
    bucket_map_.clear();

    ReleaseLoggingResponsibility(&self);
    MaybeScheduleCompaction();
  }
  Status s;
  // s = Write(WriteOptions(), NULL);
  // don't wait, just return
  return s;
}

Status DBImpl::ForceFlush() {
  Status s;
  {
    MutexLock l(&mutex_);
    if (imm_ == NULL) {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      Log(options_.info_log, "new mem");
      uint64_t new_log_number = versions_->NewFileNumber();
      // use ReadableAndWritableFile here to support outer reading
      ReadableAndWritableFile* lfile = NULL;
      s = env_->NewReadableAndWritableFile(LogFileName(dblog_dir_, new_log_number), &lfile);
      if (s.ok()) {
        delete log_;
        //delete logfile_;
        logfile_->Unref();
        lfile->Ref();

        logfile_ = lfile;
        logfile_number_ = new_log_number;
        log_ = new log::Writer(lfile);
        imm_ = mem_;
        has_imm_.Release_Store(imm_);
        mem_ = new MemTable(internal_comparator_, env_);
        mem_->Ref();
        MaybeScheduleCompaction();
      }
    } else {
      s = Status::Corruption(Slice());
    }
  }
  return s;
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done) {
    while (manual_compaction_ != NULL) {
      bg_cv_.Wait();
    }
    manual_compaction_ = &manual;
    MaybeScheduleCompaction();
    while (manual_compaction_ == &manual) {
      bg_cv_.Wait();
    }
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

//////////////////////////////////////////
// special compact func to support      //
// compact range only in one level.     //
//////////////////////////////////////////

// There is at most one thread that is the current logger.  This call
// waits until preceding logger(s) have finished and becomes the
// current logger.
void DBImpl::AcquireLoggingResponsibility(LoggerId* self) {
  while (logger_ != NULL) {
    logger_cv_.Wait();
  }
  logger_ = self;
}

void DBImpl::ReleaseLoggingResponsibility(LoggerId* self) {
  assert(logger_ == self);
  logger_ = NULL;
  logger_cv_.SignalAll();
}

// each memtable is up to one bucket's update
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates, int bucket) {
  BucketUpdate* bucket_update = NULL;
  MutexLock l(&mutex_);
  LoggerId self;
  AcquireLoggingResponsibility(&self);
  Status status = MakeRoomForWrite(false, bucket, &bucket_update);
  uint64_t last_sequence = versions_->LastSequence();

  if (status.ok()) {
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);
    versions_->SetLastSequence(last_sequence);

    // Add to log and apply to memtable.  We can release the lock during
    // this phase since the "logger_" flag protects against concurrent
    // loggers and concurrent writes into mem_.
    {
      assert(logger_ == &self);
      mutex_.Unlock();
      // status = bucket_update->log_->AddRecord(WriteBatchInternal::Contents(updates));
      // if (status.ok() && options.sync) {
      //   status = bucket_update->logfile_->Sync();
      // }
      // if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, bucket_update->mem_);
      // }
      mutex_.Lock();
      assert(logger_ == &self);
    }

  }

  ReleaseLoggingResponsibility(&self);
  return status;
}

Status DBImpl::MakeRoomForWrite(bool force, int bucket, BucketUpdate** bucket_update) {
  static int kMaxMemTableCount = options_.max_mem_usage_for_memtable / options_.write_buffer_size;
  static int kRetryCount = 3;
  static int kEvictMemTableCount = 5;

  mutex_.AssertHeld();
  assert(logger_ != NULL);
  if (bucket < 0) {
    return Status::InvalidArgument("bucket is invalid");
  }
  if (!bg_error_.ok()) {
    // Yield previous error
    return bg_error_;
  }

  if (ShouldLimitWrite((config::kL0_SlowdownWritesTrigger * 2) / 3)) {
    return Status::SlowWrite("too many L0 sst");
  }

  Status s;
  BucketUpdate* bu = NULL;
  BucketMap::iterator bm_it = bucket_map_.find(bucket);
  if (bm_it != bucket_map_.end()) {
    bu = bm_it->second;
    // not force and current memtale has remaining space
    if (!force && bu->mem_->ApproximateMemoryUsage() <= options_.write_buffer_size) {
      *bucket_update = bu;
      return s;
    }

    // this bucketupdate should be compacted
    bucket_map_.erase(bm_it);
    EvictBucketUpdate(bu);
    // sentinel
    has_imm_.Release_Store(bu->mem_);
    MaybeScheduleCompaction();
  }

  // control max memtable count now.
  int retry = 0;
  while (bucket_map_.size() + imm_list_count_ >= static_cast<uint32_t>(kMaxMemTableCount) && retry++ < kRetryCount) {
    // too many memtable now. try evict some
    int evict_count = 0;
    BucketUpdate* evicted_bu = NULL;
    while (evict_count++ < kEvictMemTableCount && bu_head_ != NULL) {
      // bu_head_ ponit to oldest memtale
      BucketMap::iterator it = bucket_map_.find(bu_head_->bucket_number_);
      assert(it != bucket_map_.end());
      evicted_bu = it->second;
      Log(options_.info_log, "evict mmt [%d %lu]",
          bu_head_->bucket_number_, evicted_bu->mem_->ApproximateMemoryUsage());
      EvictBucketUpdate(evicted_bu);
      bucket_map_.erase(it);
    }

    if (evicted_bu != NULL) {
      has_imm_.Release_Store(evicted_bu->mem_);
    }

    MaybeScheduleCompaction();

    mutex_.Unlock();
    env_->SleepForMicroseconds(10000);
    mutex_.Lock();
    Log(options_.info_log, "wait for less mmt. now %zd + %d", bucket_map_.size(), imm_list_count_);
  }

  // can't get space for new memtable
  if (retry > kRetryCount) {
    return Status::SlowWrite("too many mmt");
  }

  // we can switch a new memtable now.
  assert(versions_->PrevLogNumber() == 0);
  Log(options_.info_log, "new mem %d", bucket);

  uint64_t new_log_number = versions_->NewFileNumber();
  WritableFile* lfile = NULL;

  s = env_->NewWritableFile(BucketLogFileName(dbname_, new_log_number), &lfile);
  if (!s.ok()) {
    return s;
  }

  // not dirty logfile_number_

  Log(options_.info_log, "new log %lu", new_log_number);
  bu = new BucketUpdate();
  bu->bucket_number_ = bucket;
  bu->log_number_ = new_log_number;
  bu->logfile_ = lfile;
  bu->log_ = new log::Writer(lfile);
  bu->mem_ = new MemTable(internal_comparator_, env_);
  bu->mem_->Ref();
  // link to bucket list tail
  bu->next_ = NULL;
  bu->prev_ = bu_tail_;
  if (NULL == bu_head_) {
    bu_head_ = bu_tail_ = bu;
  } else {
    bu_tail_->next_ = bu;
    bu_tail_ = bu;
  }
  // add to bucket map
  bucket_map_[bucket] = bu;

  *bucket_update = bu;
  return s;
}

void DBImpl::EvictBucketUpdate(BucketUpdate* bu) {
  mutex_.AssertHeld();
  if (NULL == bu) {
    return;
  }

  if (bu == bu_tail_) {
    bu_tail_ = bu->prev_;
  }
  if (bu == bu_head_) {
    bu_head_ = bu->next_;
  }
  if (bu->prev_ != NULL) {
    bu->prev_->next_ = bu->next_;
  }
  if (bu->next_ != NULL) {
    bu->next_->prev_ = bu->prev_;
  }
  delete bu->log_;
  delete bu->logfile_;
  // add to imm list
  imm_list_.push_front(bu);
  imm_list_count_++;
}

Status DBImpl::CompactMemTableList() {
  // Save the contents of the memtable as a new Table
  const uint64_t imm_start = env_->NowMicros();
  Status s;
  Log(options_.info_log, "memlist : %d", imm_list_count_);

  std::vector<BucketList::iterator> imms;
  {
    MutexLock l(&mutex_);
    for (BucketList::iterator it = imm_list_.begin(); it != imm_list_.end(); ++it) {
      imms.push_back(it);
    }
  }

  size_t count = 0;
  for (; count < imms.size(); ++count) {
    if (imm_ != NULL) {           // we compcat imm_ at highest priority
      s = CompactMemTable(false); // MUST only compact imm_
      if (!s.ok()) {
        break;
      }
      bg_cv_.SignalAll();
    }

    Version* base;
    {
      MutexLock l(&mutex_);
      base = versions_->current();
      base->Ref();
    }

    VersionEdit edit;
    BucketUpdate* bu = *imms[count];
    s = WriteLevel0Table(bu->mem_, &edit, base);
    if (!s.ok()) {
      break;
    }
    // DeleteObsoleteFiles() can't check to delete bucket log file base on current log_file_number_,
    // so delete bucket log file here.
    env_->DeleteFile(BucketLogFileName(dbname_, bu->log_number_));
    bu->mem_->Unref();

    base->Unref();

    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
    if (!s.ok()) {
      break;
    }
  }

  // cleanup
  {
    MutexLock l(&mutex_);
    for (size_t i = 0; i < count; ++i) {
      BucketUpdate* bu = *imms[i];
      imm_list_.erase(imms[i]);
      --imm_list_count_;
      delete bu;
    }
  }

  Log(options_.info_log, "com imm list: %lu now: %d cost %ld", count, imm_list_count_, env_->NowMicros() - imm_start);

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  if (s.ok() && imm_list_.empty()) {
    // Commit to the new state
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  }

  return s;
}
////////////////////////////////////////


////////////////////////////////////////
//compact for repair
//
Status DBImpl::CompactRepairSST(ManualCompactionType type, std::vector<uint64_t>& files, bool check) {
  ManualCompaction manual;
  manual.done = false;
  manual.reschedule = false;    // only run once
  manual.type = type;
  manual.check_sstfile = check;

  // avoid to miss bg_cv's signal() occasionally(wait() compete somewhere), use TimedCond() here
  int64_t timed_us = 1000000;   // 1s
  MutexLock l(&mutex_);
  if(manual.type == KCompactSelfRepairRemoveExtraSSTFile
         || manual.type == KCompactSelfRepairRemoveGCSSTFile
         || manual.type == KCompactSelfRepairRemoveCorruptionSSTFile) {
    //repair by file number
    manual.bg_compaction_func = &DBImpl::BackgroundCompactionRepairSST;
    manual.files = files;

    ManualCompaction each_manual = manual;
    while (each_manual.compaction_status.ok() && !each_manual.done) {
      // still have other compaction running
      while (bg_compaction_scheduled_) {
        bg_cv_.TimedWait(timed_us);
      }
      manual_compaction_ = &each_manual;
      MaybeScheduleCompaction();
      while (manual_compaction_ == &each_manual) {
        bg_cv_.TimedWait(timed_us);
      }
    }
    manual.compaction_status = each_manual.compaction_status;
  }

  return manual.compaction_status;
}

void DBImpl::BackgroundCompactionRepairSST() {
  assert(bg_compaction_scheduled_);
  assert(manual_compaction_ != NULL); // this must be a manual compaction
  leveldb::Status status;
  ManualCompaction* m = manual_compaction_;
  int num = manual_compaction_->files.size();
  for (int i = 0; i < num; i++) {
    if (config::kLimitCompactRepair) {
      Log(options_.info_log, "SelfLevel Repair SST Compacting cancel!");
      break;
    }
    Log(options_.info_log, "SelfLevel Repair SST Compacting file(%"PRI64_PREFIX"u)", manual_compaction_->files[i]);
    Compaction* c = versions_->CompactSingleSSTFile(manual_compaction_->files[i]);
    if (c == NULL) {
      Log(options_.info_log, "SelfLevel Repair SST Compacting pick file(%"PRI64_PREFIX"u) failed!", manual_compaction_->files[i]);
      continue;
    }
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWorkRepairSST(compact, c->input(0, 0));

    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();

    delete c;
  }

  if (shutting_down_.Acquire_Load()) {
  // Ignore compaction errors found during shutting down
  } else if (!status.ok()) {
    Log(options_.info_log, "Background compact repair sst failed, error: %s",
          status.ToString().c_str());
    m->compaction_status = status; // save error
    if (bg_error_.ok()) {          // no matter paranoid_checks
      bg_error_ = status;
    }
  } else if (status.ok()) {
      m->done = true;
  }
  // Mark it as done
  manual_compaction_ = NULL;

}

Status DBImpl::DoCompactionWorkRepairSST(CompactionState* compact, FileMetaData* file_meta) {
  leveldb::Status status;
  Log(options_.info_log, "SelfLevel Repair SST Compacting %d files, type(%d), file(%"PRI64_PREFIX"u)",
      compact->compaction->num_input_files(0), manual_compaction_->type, file_meta->number);
  leveldb::Iterator* input = versions_->MakeSingleFileInputIterator(file_meta);
  if (input == NULL) {
    return Status::Corruption("Make file input iterator failed");
  }
  bool valid = true;
  if (manual_compaction_->check_sstfile) {
    valid = options_.fileRepairCheckFunc(input, manual_compaction_->type, options_.info_log);
  }

  if (valid) {
    status = InstallCompactionResults(compact, false);
  } else {
    Log(options_.info_log, "SelfLevel Repair SST Compacting file check invalid.");
    status = Status::Corruption("sst check invalid.");
  }
  delete input;
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
		  "repair compacted to: %s", versions_->LevelSummary(&tmp));

  return status;
}

// compact file(sstable) whoes filenumber is less than limit_filenumber and range is in [begin, end).
// Only compacting level 0 files output to level 1, files in level n, output files is in level n too.
Status DBImpl::CompactRangeSelfLevel(
  uint64_t limit_filenumber,
  const Slice* begin,
  const Slice* end) {
  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.limit_filenumber = limit_filenumber;
  manual.done = false;
  manual.reschedule = false;    // only run once

  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    // maybe specify sequnece number
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  // avoid to miss bg_cv's signal() occasionally(wait() compete somewhere), use TimedCond() here
  int64_t timed_us = 1000000;   // 1s
  MutexLock l(&mutex_);
  manual.bg_compaction_func = &DBImpl::BackgroundCompactionSelfLevel;
  for (int level = 0; level < config::kNumLevels && manual.compaction_status.ok(); ++level) {
    ManualCompaction each_manual = manual;
    each_manual.level = level;
    while (each_manual.compaction_status.ok() && !each_manual.done) {
      // still have other compaction running
      while (bg_compaction_scheduled_) {
        bg_cv_.TimedWait(timed_us);
      }
      manual_compaction_ = &each_manual;
      MaybeScheduleCompaction();
      while (manual_compaction_ == &each_manual) {
        bg_cv_.TimedWait(timed_us);
      }
    }
    manual.compaction_status = each_manual.compaction_status;
  }

  return manual.compaction_status;
}

void DBImpl::BackgroundCompactionSelfLevel() {
  assert(bg_compaction_scheduled_);
  assert(manual_compaction_ != NULL); // this must be a manual compaction

  Compaction* c = NULL;
  InternalKey manual_end;
  ManualCompaction* m = manual_compaction_;
  Status status;
  do {
    // level-0 is dumped by memtable, apply no filter, so ignore filenumber limit
    c = versions_->
      CompactRangeOneLevel(m->level, m->level > 0 ? m->limit_filenumber : ~(static_cast<uint64_t>(0)), m->begin, m->end);
    if (NULL == c) {            // no compact for this level
      Log(options_.info_log, "need no selfcom in level: %d\n", m->level);
      m->done = true;           // done all.
      break;
    }
    manual_end = c->input(0, c->num_input_files(0) - 1)->largest;

#if 0
    int input_size = c->num_input_files(0);
    for (int i = 0; i < input_size; ++i) {
      Log(options_.info_log, "[%lu]", c->input(0, i)->number);
    }
#endif

    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWorkSelfLevel(compact);
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();

    delete c;

    if (shutting_down_.Acquire_Load()) {
      // Ignore compaction errors found during shutting down
    } else if (!status.ok()) {
      Log(options_.info_log, "compactrangeself fail. level: %d, error: %s",
          m->level, status.ToString().c_str());
      m->compaction_status = status; // save error
      if (bg_error_.ok()) {          // no matter paranoid_checks
        bg_error_ = status;
      }
      break;                    // exit once fail.
    }
  } while (false);

  if (!m->done && 0 != m->level) {
    // We only compacted part of the requested range.  Update *m
    // to the range that is left to be compacted.
    m->tmp_storage = manual_end;
    m->begin = &m->tmp_storage;
  }
  // Mark it as done
  manual_compaction_ = NULL;
}

// do compact in self level, input: N files in level-n, output (<= N) files in level-n.
// NOT harm any other range sequence.
Status DBImpl::DoCompactionWorkSelfLevel(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "SelfLevel Compacting %d@%d files: %s",
      compact->compaction->num_input_files(0),
      compact->compaction->level(), compact->compaction->DebugString().c_str());
  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);

  // consider snapshot here, but ShouldDrop() ignore it.
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      if (imm_ != NULL) {
        Log(options_.info_log, "com mem");
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    // if (compact->compaction->ShouldStopBefore(key) &&
    //     compact->builder != NULL) {
    //   status = FinishCompactionOutputFile(compact, input);
    //   if (!status.ok()) {
    //     break;
    //   }
    // }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      // only drop same entry and ShouldDrop() entrys here.
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (user_comparator()->ShouldDrop(ikey.user_key.data(), ikey.sequence, 1 /* will gc */)) {
        // user-defined should drop, no matter what conditon.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        " SelfLevel Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    } else{
      // drop data, update stat
      DEC_STAT(ikey.user_key.data(), ikey.user_key.size() + input->value().size());
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  // only one level
  for (int i = 0; i < compact->compaction->num_input_files(0); i++) {
    stats.bytes_read += compact->compaction->input(0, i)->file_size;
  }

  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  // stat add this level
  stats_[compact->compaction->level()].Add(stats);

  if (status.ok()) {
    Log(options_.info_log,  "SelfLevel Compacted %d@%d (%ld) bytes => %ld bytes, [%ld + %ld]",
        compact->compaction->num_input_files(0),
        compact->compaction->level(),
        stats.bytes_read,
        static_cast<int64_t>(compact->total_bytes),
        imm_micros, stats.micros
      );
    status = InstallCompactionResults(compact, false); // output files is in current level, not level + 1
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

//////////////////////////////////////////
// special write to support multi-bucket update
//////////////////////////////////////////


//////////////////////////////////////////////////

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    Log(options_.info_log, "com running");
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (imm_ == NULL &&
             imm_list_.empty() && // @@ imm list
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    Log(options_.info_log, "need no com");
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  PROFILER_START("compact start");
  PROFILER_BEGIN("mutex");
  MutexLock l(&mutex_);
  PROFILER_END();
  assert(bg_compaction_scheduled_);
  if (!shutting_down_.Acquire_Load()) {
    if (manual_compaction_ != NULL) {
      mutex_.Unlock();
      (this->*manual_compaction_->bg_compaction_func)(); // use user-defined compaction function
      mutex_.Lock();
    } else {
      PROFILER_BEGIN("do com+");
      mutex_.Unlock();
      BackgroundCompaction();   // use default compaction
      mutex_.Lock();
      PROFILER_END();
    }
  }

  bool reschedule = manual_compaction_ != NULL ? manual_compaction_->reschedule : true;
  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  if (reschedule) {
    PROFILER_BEGIN("maybesche+");
    MaybeScheduleCompaction();
    PROFILER_END();
  }
  bg_cv_.SignalAll();
  PROFILER_DUMP();
  PROFILER_STOP();
}

void DBImpl::BackgroundCompaction() {
  if (imm_ != NULL || !imm_list_.empty()) {
    const uint64_t imm_start = env_->NowMicros();
    Log(options_.info_log, "only com mem");
    PROFILER_BEGIN("com mem+");
    CompactMemTable();
    PROFILER_END();
    Log(options_.info_log, "only com mem cost %ld", env_->NowMicros() - imm_start);
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    PROFILER_BEGIN("pickcom+");
    c = versions_->PickCompaction();
    PROFILER_END();
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    PROFILER_BEGIN("com move lAa+");
    status = versions_->LogAndApply(c->edit(), &mutex_);
    PROFILER_END();
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    PROFILER_BEGIN("do com work+");
    status = DoCompactionWork(compact);
    PROFILER_END();
    PROFILER_BEGIN("cleanupcom+");
    CleanupCompaction(compact);
    PROFILER_END();
    c->ReleaseInputs();
    PROFILER_BEGIN("del obsofile+");
    DeleteObsoleteFiles();
    PROFILER_END();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
    if (options_.paranoid_checks && bg_error_.ok()) {
      bg_error_ = status;
    }
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    // Mark it as done
    manual_compaction_ = NULL;
  }

  MaybeRotate();
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact, bool uppen_level) {
  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  const int output_level = uppen_level ? level + 1 : level;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        output_level,
        out.number, out.file_size, out.smallest, out.largest);
  }
  PROFILER_BEGIN("lAa+");
  Status s = versions_->LogAndApply(compact->compaction->edit(), &mutex_);
  PROFILER_END();
  return s;
}

Status DBImpl::MaybeRotate() {
  static const uint32_t DAY_S = 86400;
  Status s;
  uint32_t now = env_->NowSecs();
  if (now - today_start_ > DAY_S) {
    // rotate log
    if (options_.info_log != NULL) {
      s = options_.info_log->Rotate(true);
      if (!s.ok()) {
        Log(options_.info_log, "rotate info log fail: %s", s.ToString().c_str());
      }
    }

    // retart versionset manifest
    s = versions_->Restart();

    // reset timeline
    today_start_ = env_->TodayStart();
  }
  return s;
}

bool DBImpl::ShouldLimitWrite(int32_t trigger) {
  // without split, then limit with count, or with size
  return !config::kDoSplitMmtCompaction ? (versions_->NumLevelFiles(0) >= trigger) :
    (versions_->NumLevelBytes(0) >= static_cast<int64_t>(options_.write_buffer_size * trigger));
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files: %s",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1, compact->compaction->DebugString().c_str());

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  PROFILER_BEGIN("do real file com-");

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  // @ caculate expired end time only once for speed
  // @ consider cost time of one compaction (range matters), the precision (maybe) is tolerable
  uint32_t expired_end_time = start_micros/1000000;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      if (imm_ != NULL || !imm_list_.empty()) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (user_comparator()->ShouldDrop(ikey.user_key.data(), ikey.sequence, 1 /* will gc */)) {
        // user-defined should drop, no matter what conditon.
        drop = true;
      } else if (ikey.sequence <= compact->smallest_snapshot &&
                 (ikey.type == kTypeDeletion || // deleted or ..
                  user_comparator()->ShouldDropMaybe(ikey.user_key.data(),
                                                     ikey.sequence, expired_end_time)) &&
                 // .. user-defined should drop(maybe),
                 // based on some condition(eg. this key only has this update.).
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    } else{
      // drop data, update stat
      DEC_STAT(ikey.user_key.data(), ikey.user_key.size() + input->value().size());
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  PROFILER_END();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %ld bytes, [%ld + %ld]",
        compact->compaction->num_input_files(0),
        compact->compaction->level(),
        compact->compaction->num_input_files(1),
        compact->compaction->level() + 1,
        static_cast<int64_t>(compact->total_bytes),
        imm_micros, stats.micros
      );
    PROFILER_BEGIN("install com result+");
    status = InstallCompactionResults(compact);
    PROFILER_END();
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

static void CleanupLongHold(void* arg1, void* arg2) {
  VersionSet* version_set = reinterpret_cast<VersionSet*>(arg1);
  version_set->RelealseLongHold();
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  if (options.hold_for_long) {
    versions_->AllocLongHold();
    internal_iter->RegisterCleanup(CleanupLongHold, versions_, NULL);
  }

  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  return NewInternalIterator(ReadOptions(), &ignored);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  RTProbe probe(PSN_LEVELDB_GET);
  Status s;
  PROFILER_BEGIN("db mutex");
  RTProbe probe_mtx;
  probe_mtx.enter(PSN_LEVELDB_MUTEX_LOCK);
  MutexLock l(&mutex_);
  probe_mtx.leave();
  PROFILER_END();
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      PROFILER_BEGIN("db sst get");
      RTProbe probe(PSN_SST_GET);
      s = current->Get(options, lkey, value, &stats);
      PROFILER_END();
      have_stat_update = true;
    }
    {
      RTProbe probe(PSN_LEVELDB_MUTEX_LOCK);
      mutex_.Lock();
    }
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  Iterator* internal_iter = NewInternalIterator(options, &latest_snapshot);
  return NewDBIterator(
      &dbname_, env_, user_comparator(), internal_iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot));
}

Iterator* DBImpl::NewRawIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  Iterator* internal_iter = NewInternalIterator(options, &latest_snapshot);
  return NewRawDBIterator(
      &dbname_, env_, user_comparator(), internal_iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot));
}

int DBImpl::NowSpecifyCompactLevel() const {
  return versions_->NowSpecifyCompactLevel();
}

int DBImpl::SpecifyCompactStatus() const {
  return versions_->SpecifyCompactStatus();
}

time_t DBImpl::LastFinishSpecifyCompactTime() const {
  return versions_->LastFinishSpecifyCompactTime();
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

const Snapshot* DBImpl::GetLogSnapshot() {
  MutexLock l(&mutex_);
  const Snapshot* s = log_snapshots_.New(versions_->LastSequence(), versions_->LogNumber());
  min_snapshot_log_number_ = dynamic_cast<LogSnapshotImpl*>(log_snapshots_.oldest())->log_number_;
  return s;
}

void DBImpl::ReleaseLogSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  log_snapshots_.Delete(reinterpret_cast<const LogSnapshotImpl*>(s));
  min_snapshot_log_number_ = log_snapshots_.empty() ? ~((uint64_t)0) :
    dynamic_cast<LogSnapshotImpl*>(log_snapshots_.oldest())->log_number_;
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val,
    bool synced, bool from_other_unit) {
  RTProbe probe(PSN_LEVELDB_PUT);
  return DB::Put(o, key, val, synced, from_other_unit);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key,
    bool synced, bool from_other_unit) {
  return DB::Delete(options, key, synced, from_other_unit);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key, const Slice& tailer,
    bool synced, bool from_other_unit) {
  return DB::Delete(options, key, tailer, synced, from_other_unit);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  RTProbe probe(PSN_LEVELDB_WRITE);
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  PROFILER_BEGIN("db mutex");
  RTProbe probe_mtx;
  probe_mtx.enter(PSN_LEVELDB_MUTEX_LOCK);
  MutexLock l(&mutex_);
  probe_mtx.leave();
  PROFILER_END();

  writers_.push_back(&w);
  PROFILER_BEGIN("db wait");
  RTProbe probe_cv;
  probe_cv.enter(PSN_LEVELDB_CV_WAIT);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  probe_cv.leave();
  PROFILER_END();
  if (w.done) {
    return w.status;
  }

  PROFILER_BEGIN("db makeroom");
  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == NULL);
  PROFILER_END();
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    PROFILER_BEGIN("db builtbatch");
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    PROFILER_END();
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);
    versions_->SetLastSequence(last_sequence);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      PROFILER_BEGIN("db addrecord");
      {
        RTProbe probe(PSN_LEVELDB_LOG_ADD_RECORD);
        status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      }
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
      }
      PROFILER_END();
      if (status.ok()) {
        PROFILER_BEGIN("db insertmem");
        RTProbe probe(PSN_LEVELDB_INSERT_MMT);
        status = WriteBatchInternal::InsertInto(updates, mem_);
        PROFILER_END();
      }
      {
        RTProbe probe(PSN_LEVELDB_MUTEX_LOCK);
        mutex_.Lock();
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

  }

  PROFILER_BEGIN("db lastwait");
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }
  PROFILER_END();

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *reuslt
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  RTProbe probe(PSN_LEVELDB_MAKE_ROOM_FOR_WRITE);
  mutex_.AssertHeld();
  assert(force || !writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        ShouldLimitWrite(config::kL0_SlowdownWritesTrigger)) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      Log(options_.info_log, "wait slow");
      mutex_.Unlock();
      RTProbe probe_sleep;
      probe_sleep.enter(PSN_LEVELDB_MAKEROOM_SLEEP);
      env_->SleepForMicroseconds(1000);
      probe_sleep.leave();
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "wait imm ");
      RTProbe probe_wait;
      probe_wait.enter(PSN_LEVELDB_MAKEROOM_BGWAIT_IMM);
      MaybeScheduleCompaction();
      bg_cv_.Wait();
      probe_wait.leave();
      Log(options_.info_log, "wait imm over");
    } else if (ShouldLimitWrite(config::kL0_StopWritesTrigger)) {
      // There are too many level-0 files.
      Log(options_.info_log, "waiting...\n");
      RTProbe probe_wait(PSN_LEVELDB_MAKEROOM_BGWAIT_LEVEL0);
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      Log(options_.info_log, "new mem");
      uint64_t new_log_number = versions_->NewFileNumber();
      // use ReadableAndWritableFile here to support outer reading
      ReadableAndWritableFile* lfile = NULL;
      s = env_->NewReadableAndWritableFile(LogFileName(dblog_dir_, new_log_number), &lfile);
      if (!s.ok()) {
        break;
      }
      delete log_;
      //delete logfile_;
      logfile_->Unref();
      lfile->Ref();

      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_, env_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

uint64_t DBImpl::LastSequence() {
  return versions_->LastSequence();
}

ReadableAndWritableFile* DBImpl::LogFile(uint64_t limit_logfile_number) {
  MutexLock l(&mutex_);
  if (logfile_ != NULL && logfile_number_ <= limit_logfile_number) {
    logfile_->Ref();
    return logfile_;
  }
  return NULL;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value,
                         void (*key_printer)(const Slice&, std::string&)) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || static_cast<int64_t>(level) >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }

    mutex_.Unlock();
    // append cache statistics
    if (table_cache_ != NULL) {
      value->append("Table Cache: ");
      table_cache_->Stats(*value);
    }
    if (options_.block_cache != NULL) {
      value->append("Block Cache: ");
      options_.block_cache->Stats(*value);
    }
    mutex_.Lock();
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "levelnums") {
    char buf[10];
    snprintf(buf, sizeof(buf), "%d", config::kNumLevels);
    value->append(buf);
    return true;
  } else if (in == "sequence") {
    char buf[50];
    snprintf(buf, sizeof(buf), "%lu", versions_->LastSequence());
    value->append(buf);
    return true;
  } else if (in == "largest-filenumber") {
    char buf[50];
    snprintf(buf, sizeof(buf), "%lu", versions_->NextFileNumber());
    value->append(buf);
    return true;
  } else if (in == "smallest-filenumber") {
    char buf [50];
    snprintf(buf, sizeof(buf), "%lu", versions_->SmallestFileNumber());
    value->append(buf);
    return true;
  } else if (in == "ranges") {
    Version* version = versions_->current();
    version->Ref();
    mutex_.Unlock();
    version->GetAllRange(*value, key_printer);
    mutex_.Lock();
    version->Unref();
    return true;
  }

  return false;
}

Status DBImpl::OpCmd(int cmd, const std::vector<std::string>* params, std::vector<std::string>* result) {
  MutexLock l(&mutex_);
  Status s;
  switch (cmd) {
  case kCmdBackupDB:
    s = versions_->BackupCurrentVersion();
    break;
  case kCmdUnloadBackupedDB:
  {
    uint64_t id = 0;
    if (params != NULL && !params->empty() && !(*params)[0].empty()) {
      id = atoll((*params)[0].c_str());
    }
    s = versions_->UnloadBackupedVersion(id);
    break;
  }
  default:
    return Status::InvalidArgument("unkonwn cmd type");
  }
  return s;
}

bool DBImpl::GetLevelRange(int level, std::string* smallest, std::string* largest) {
  MutexLock l(&mutex_);
  if (level < 0 || level > config::kNumLevels) {
    return false;
  }
  return versions_->current()->Range(level, smallest, largest);
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value,
    bool synced, bool from_other_unit) {
  WriteBatch batch;
  {
   RTProbe probe(PSN_LEVELDB_WRITEBATCH_PUT);
   batch.Put(key, value, synced, from_other_unit);
  }
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key,
    bool synced, bool from_other_unit) {
  WriteBatch batch;
  batch.Delete(key, synced, from_other_unit);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key, const Slice& tailer,
    bool synced, bool from_other_unit) {
  WriteBatch batch;
  batch.Delete(key, tailer, synced, from_other_unit);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  Status s = impl->Recover(&edit); // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    ReadableAndWritableFile* lfile;
    s = options.env->NewReadableAndWritableFile(LogFileName(impl->dblog_dir_, new_log_number),
                                                &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      lfile->Ref();
      impl->log_ = new log::Writer(lfile);
      impl->mutex_.Unlock();
      s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
      impl->mutex_.Lock();
    }
    if (s.ok()) {
      impl->mutex_.Unlock();
      impl->DeleteObsoleteFiles();
      impl->mutex_.Lock();
      impl->MaybeScheduleCompaction();
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

// open with specified manifest,
// resemble read_only mode.
// TODO: maybe support open db with read_only_mode.
Status DB::Open(const Options& options, const std::string& dbname,
                const std::string& manifest, DB** dbptr) {
  *dbptr = NULL;

  // not overlap current using LOG
  if (options.info_log == NULL) {
    return Status::InvalidArgument("not provide info log");
  }

  Status s;
  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  if (!impl->env_->FileExists(manifest)) {
    s = Status::InvalidArgument("manifest file not exist");
  } else {
    // recover with specified manifest
    s = impl->versions_->Recover(manifest.c_str());
  }
  impl->mutex_.Unlock();

  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
