// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(4<<20),
      max_mem_usage_for_memtable(1<<30),
      max_open_files(1000),
      block_cache(NULL),
      block_cache_size(8<<20),
      block_size(4096),
      block_restart_interval(16),
      compression(kSnappyCompression),
      filter_policy(NULL),
      reserve_log(false),
      load_backup_version(false),
      kL0_CompactionTrigger(4),
      kL0_SlowdownWritesTrigger(8),
      kL0_StopWritesTrigger(12),
      kMaxMemCompactLevel(2),
      kTargetFileSize(2 * 1048576),
      kMaxGrandParentOverlapBytes(10 * kTargetFileSize),
      kArenaBlockSize(4096),
      kFilterBaseLg(12),        // kFilterBase will be (1 << 12) == (default block_size)
      kBaseLevelSize(10485760),  // 10M
      kUseMmapRandomAccess(false),
      kLimitCompactLevelCount(0),
      kLimitCompactCountInterval(0),
      kLimitCompactTimeInterval(0),
      kLimitCompactTimeStart(0),
      kLimitCompactTimeEnd(0),
      kLimitDeleteObsoleteFileInterval(0),
      kDoSeekCompaction(true) {
}


}  // namespace leveldb
