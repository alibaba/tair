// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_CONFIG_H_
#define STORAGE_LEVELDB_UTIL_CONFIG_H_
#include <stdint.h>

namespace leveldb {
class Options;
struct config {

static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
static int kL0_CompactionTrigger;

// Soft limit on number of level-0 files.  We slow down writes at this point.
static int kL0_SlowdownWritesTrigger;

// Maximum number of level-0 files.  We stop writes at this point.
static int kL0_StopWritesTrigger;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static int kMaxMemCompactLevel;

// sstable size
static int kTargetFileSize;

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t kMaxGrandParentOverlapBytes;

// arena block size
static int kArenaBlockSize;

// filter base 
static int kFilterBaseLg;
// filter base size 1 << kFilterBaseLg
static int kFilterBase;

// base size for each level.
// level-0 & level-1 : kBaseLevelSize
// level-2.. : kBaseLevelSize * 10 * (level - 1)
static int kBaseLevelSize;

// whether use mmap() to speed random read file(sstable)
static bool kUseMmapRandomAccess;


// how many highest levels to limit compaction
static int kLimitCompactLevelCount;
// limit compaction ratio: allow doing one compaction every kLimitCompactCountInterval.
static int kLimitCompactCountInterval;
// limit compaction time interval (s)
static int kLimitCompactTimeInterval;
// start time to limit compaction
static int kLimitCompactTimeStart;
// end time to limit compaction
static int kLimitCompactTimeEnd;
// limit compaction time reverse mode (4-3, eg.)
static bool kLimitCompactTimeReverse;
// Db will delete obsolete files when finishing one compaction.
// DeleteObsoleteFiles() cost too much and does NOT need been done
// each compaction actually, so wen can do this action each 'delete_obsolete_file_interval
// times compaction.
static int kLimitDeleteObsoleteFileInterval;
// whether do compaction scheduled by seek count over-threshold
static bool kDoSeekCompaction;

static bool IsLimitCompactTime();
static void SetLimitCompactTimeRange(int time_start, int time_end);

// get config from user option.
static void setConfig(const Options& option);
};

// db cmd type
enum {
  kCmdBackupDB = 1,
};
}
#endif  // STORAGE_LEVELDB_UTIL_CONFIG_H_
