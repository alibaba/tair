// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <map>
#include <list>
#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

struct LoggerId;

struct BucketUpdate
{
  int bucket_number_;
  int log_number_;
  log::Writer* log_;
  WritableFile* logfile_;
  MemTable* mem_;
  BucketUpdate* prev_;
  BucketUpdate* next_;
  // TODO: fine-grained lock
  LoggerId* logger_;            // NULL, or the id of the current logging thread
  port::CondVar* logger_cv_;     // For threads waiting to log
};

typedef std::map<int, BucketUpdate*> BucketMap;
typedef std::list<BucketUpdate*> BucketList;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value, bool synced = false);
  virtual Status Delete(const WriteOptions&, const Slice& key, bool synced = false);
  virtual Status Delete(const WriteOptions&, const Slice& key, const Slice& tailer, bool synced = false);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates, int bucket);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value,
                           void (*key_printer)(const Slice&, std::string&) = NULL);
  virtual Status OpCmd(int cmd);
  virtual bool GetLevelRange(int level, std::string* smallest, std::string* largest);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);
  virtual Status CompactRangeSelfLevel(uint64_t limit_filenumber, const Slice* begin, const Slice* end);
  virtual Status ForceCompactMemTable();
  virtual void ResetDbName(const std::string& dbname) { dbname_ = dbname; }

  // Compact any files in the named level that overlap [begin,end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Extra methods (for testing) that are not in the public DB interface

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // util function
  Env* GetEnv() { return env_; }
  uint64_t LastSequence();
  uint64_t LogFileNumber() { return logfile_number_; }
  log::Writer* LogWriter() { return log_; }
  ReadableAndWritableFile* LogFile(uint64_t limit_logfile_number);
  const std::string& DBLogDir() { return dblog_dir_; }

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status CompactMemTable(bool compact_mlist = true);
  Status RecoverLogFile(uint64_t log_number,
                        VersionEdit* edit,
                        SequenceNumber* max_sequence);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  // multi-bucket memtable update
  Status MakeRoomForWrite(bool force, int bucket, BucketUpdate** bucket_update);
  void EvictBucketUpdate(BucketUpdate* bu);
  Status CompactMemTableList();
  // Only thread is allowed to log at a time.
  // TODO: group commit
  struct LoggerId { };          // Opaque identifier for logging thread
  void AcquireLoggingResponsibility(LoggerId* self);
  void ReleaseLoggingResponsibility(LoggerId* self);

  void MaybeScheduleCompaction();
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction();
  void CleanupCompaction(CompactionState* compact);
  Status DoCompactionWork(CompactionState* compact);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact, bool uppen_level = true);

  // specified selflevel compaction
  void BackgroundCompactionSelfLevel();
  Status DoCompactionWorkSelfLevel(CompactionState* compact);

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  bool owns_info_log_;
  bool owns_cache_;
  std::string dbname_;
  const std::string dblog_dir_;
  // whether binlog should be reserved after dumping memtable(maybe for remote synchronization, etc)
  bool reserve_log_;

  // table_cache_ provides its own synchronization
  TableCache* table_cache_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;          // Signalled when background work finishes
  MemTable* mem_;
  MemTable* imm_;                // Memtable being compacted
  port::AtomicPointer has_imm_;  // So bg thread can detect non-NULL imm_
  // actually WritableFile can read to support remote synchronization need.
  // remote synchronization need to read log file while writing,
  // to make sure that reader can read real data(writer may not sync data to disk),
  // remote sync reader and write will use one logfile.
  ReadableAndWritableFile* logfile_;
  uint64_t logfile_number_;
  log::Writer* log_;

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  SnapshotList snapshots_;

  // for multi-bucket update
  BucketMap bucket_map_;
  BucketList imm_list_;
  int32_t imm_list_count_;      // list.size() cost much
  // list all bucket-memtable, new memtable is linked in the list tail.
  BucketUpdate* bu_head_;
  BucketUpdate* bu_tail_;
  LoggerId* logger_;            // NULL, or the id of the current logging thread
  port::CondVar logger_cv_;     // For threads waiting to log

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;

  // how many times to delete obsolete files continuously
  uint64_t has_limited_delete_obsolete_file_count_;

  // Has a background compaction been scheduled or is running?
  bool bg_compaction_scheduled_;

  // Information for a manual compaction
  typedef void (leveldb::DBImpl::* BgCompactionFunc)();
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;   // NULL means beginning of key range
    const InternalKey* end;     // NULL means end of key range
    InternalKey tmp_storage;    // Used to keep track of compaction progress
    uint64_t limit_filenumber;  // just specified for special use
    BgCompactionFunc bg_compaction_func; // specified compaction function
    bool reschedule;            // whether re-schecheled other compaction when this compaction is completed
    Status compaction_status;
    ManualCompaction() : bg_compaction_func(NULL), reschedule(true) {}
  };
  ManualCompaction* manual_compaction_;

  VersionSet* versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;

    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) { }

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  CompactionStats stats_[config::kNumLevels];

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
