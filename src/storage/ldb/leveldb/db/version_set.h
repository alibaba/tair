// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <stdint.h>
#ifndef PRI64_PREFIX
# if __WORDSIZE == 64
#define PRI64_PREFIX "l"
# else
#define PRI64_PREFIX "ll"
# endif
#endif

#include <map>
#include <set>
#include <vector>
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "util/random.h"

namespace leveldb {

namespace log { class Writer; }

class Random;
class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
extern int FindFile(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==NULL represents a key smaller than all keys in the DB.
// largest==NULL represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
extern bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key);

class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  uint32_t GetRef();
  void Ref();
  void Unref();

  void GetOverlappingInputs(
    int level,
    const InternalKey* begin,         // NULL means before all keys
    const InternalKey* end,           // NULL means after all keys
    std::vector<FileMetaData*>* inputs);

  void GetOverlappingInputsOneLevel(
    int level,
    uint64_t limit_filenumber,
    uint64_t limit_filesize,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs);

  //get one file's level and inputs by one file number
  void GetInputsByFileNumber(uint64_t file, std::vector<FileMetaData*>* inputs, uint32_t& file_level);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  bool OverlapInLevel(int level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return files_[level].size(); }

  std::vector<FileMetaData*>* FileMetas() { return files_; }

  // return smallest and largest key in level
  bool Range(int level, std::string* smallest, std::string* largest);

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

  // Return all range of sstable, print base 'key_printer
  void GetAllRange(std::string& str, void (*key_printer)(const Slice&, std::string&));

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;
  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  VersionSet* vset_;            // VersionSet to which this Version belongs
  Version* next_;               // Next version in linked list
  Version* prev_;               // Previous version in linked list
  // int refs_;                    // Number of live refs to this version
  port::AtomicCount<uint32_t> refs_; // Number of live refs to this version

  // List of files per level
  std::vector<FileMetaData*> files_[config::kNumLevels];
  // bytesize per level
  int64_t file_sizes_[config::kNumLevels];

  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_;
  int compaction_level_;

  explicit Version(VersionSet* vset)
      : vset_(vset), next_(this), prev_(this), refs_(0),
        file_to_compact_(NULL),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {
    memset(file_sizes_, 0, sizeof(file_sizes_));
  }

  ~Version();

  // No copying allowed
  Version(const Version&);
  void operator=(const Version&);
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname,
             const Options* options,
             TableCache* table_cache,
             const InternalKeyComparator*);
  ~VersionSet();

  uint32_t LongHoldCnt();
  void AllocLongHold();
  void RelealseLongHold();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(const char* manifest = NULL);

  // Restart manifest
  Status Restart() { pending_restart_manifest_ = true; return Status::OK(); }

  // when recover over, maybe some backupversion should be loaded to db.
  // backupversion is used to maintain some version(snapshot).
  Status LoadBackupVersion();
  // backup current version for future use.
  Status BackupCurrentVersion();
  // unload backuped version, `id is MANIFEST filenumber
  Status UnloadBackupedVersion(uint64_t id);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  // uint64_t NewFileNumber() { return next_file_number_++; }
  uint64_t NewFileNumber() { return next_file_number_.GetAndInc(); }

  // Return the next filenumber
  // uint64_t NextFileNumber() const { return next_file_number_; }
  uint64_t NextFileNumber() const { return next_file_number_.Get(); }

  // Set next filenumber
  void SetNextFileNumber(uint64_t file_number) { next_file_number_.Set(file_number); }

  // Return the smallest filenumber
  uint64_t SmallestFileNumber() const;

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the number to Table files of all level
  int64_t NumFiles() const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the combined file size of all files
  int64_t NumBytes() const;

  // Return the last sequence number.
  // uint64_t LastSequence() const { return last_sequence_; }
  uint64_t LastSequence() const { return last_sequence_.Get(); }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_.Get());
    // last_sequence_ = s;
    last_sequence_.Set(s);
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns NULL if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns NULL if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(
      int level,
      const InternalKey* begin,
      const InternalKey* end);

  // Return a compaction object for one file
  Compaction* CompactSingleSSTFile(uint64_t file);

  // Return a compaction object for compacting the range [begin,end] and
  // whose filenumber is less than limit_filenumber in
  // the specified level.  Returns NULL if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRangeOneLevel(
    int level,
    uint64_t limit_filenumber,
    const InternalKey* begin,
    const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  Iterator* MakeSingleFileInputIterator(const FileMetaData* file);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (config::kDoSeekCompaction && v->file_to_compact_ != NULL);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // now specify compact level, if not specify compacting, return 0
  int NowSpecifyCompactLevel() const;
  // specify cimpact status
  int SpecifyCompactStatus() const;
  // last time finishing specify compact
  time_t LastFinishSpecifyCompactTime() const;

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };
  static const char* kBackupVersionDir;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  void Finalize(Version* v);

  bool ShouldLimitCompact(int level);
  bool LimitCompactByLevel(int level);
  bool LimitCompactByInterval();

  void GetRange(const std::vector<FileMetaData*>& inputs,
                InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest,
                 InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);
  void CleanupVersion();

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  port::AtomicCount<uint64_t> next_file_number_;

  Random* random_;
  // second per level, just use for specify time get best level, it is not accurate
  int may_next_compact_level_;
  // finish compact time last time
  time_t last_finish_specify_compact_time_;
  // specify compact status
  int specify_compact_status_;
  // uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  port::AtomicCount<uint64_t> last_sequence_;
  // uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted
  int64_t has_limited_compact_count_;
  // compaction(especially seek-compaction by Db::Get()) may be triggered very frequently,
  // so only limiting compaction count may make us got this situation soon(maybe
  // just next MaybeScheduleCompaction()), so we need limit compaction for a specified time.
  uint64_t first_limited_compact_time_;
  int32_t current_max_level_;

  // pending restart manifest
  bool pending_restart_manifest_;

  // Opened lazily
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  // loaded versions, id => Version*, id is version's MANIFEST filenumber
  std::map<uint64_t, Version*> loaded_versions_;
  Version* current_;        // == dummy_versions_.prev_
  port::AtomicCount<uint32_t> long_hold_refs_; // the num of long-hold-version

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

  std::string DebugString() const {
    std::string str;
    for (int input = 0; input < 2; input++) {
      char buf[64];
      memset(buf, 0, sizeof(buf));
      sprintf(buf, "inputs[%d]: ", input);
      str.append(buf, strlen(buf));
      for (size_t i = 0; i < inputs_[input].size(); i++) {
        FileMetaData * f = inputs_[input].at(i);
        memset(buf, 0, sizeof(buf));
        sprintf(buf, "%" PRI64_PREFIX "u ", f->number);
        str.append(buf, strlen(buf));
      }
    }
    return str;
  }

 private:
  friend class Version;
  friend class VersionSet;

  explicit Compaction(int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[2];      // The two sets of inputs

  // State used to check for number of of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
