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
static int kBlockSize;

// base size for each level.
// level-0 & level-1 : kBaseLevelSize
// level-2.. : kBaseLevelSize * 10 * (level - 1)
static int kBaseLevelSize;

// get config from user option.
static void setConfig(const Options& option);
};
}
#endif  // STORAGE_LEVELDB_UTIL_CONFIG_H_
