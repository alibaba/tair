// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <time.h>
#include "config.h"
#include "leveldb/options.h"

namespace leveldb {
  int config::kL0_CompactionTrigger;

  int config::kL0_SlowdownWritesTrigger;

  int config::kL0_StopWritesTrigger;

  int config::kMaxMemCompactLevel;

  int config::kTargetFileSize;

  int64_t config::kMaxGrandParentOverlapBytes;

  int config::kArenaBlockSize;

  int config::kFilterBaseLg;
  int config::kFilterBase;

  int config::kBaseLevelSize;

  bool config::kUseMmapRandomAccess;

  int config::kLimitCompactLevelCount;
  int config::kLimitCompactCountInterval;
  int config::kLimitCompactTimeInterval;
  int config::kLimitCompactTimeStart;
  int config::kLimitCompactTimeEnd;
  bool config::kLimitCompactTimeReverse;
  int config::kLimitDeleteObsoleteFileInterval;
  bool config::kDoSeekCompaction;

  void config::setConfig(const Options& src) {
    config::kL0_CompactionTrigger = src.kL0_CompactionTrigger;
    config::kL0_SlowdownWritesTrigger = src.kL0_SlowdownWritesTrigger;
    config::kL0_StopWritesTrigger = src.kL0_StopWritesTrigger;
    config::kMaxMemCompactLevel = src.kMaxMemCompactLevel;
    config::kTargetFileSize = src.kTargetFileSize;
    config::kMaxGrandParentOverlapBytes = src.kMaxGrandParentOverlapBytes;
    config::kArenaBlockSize = src.kArenaBlockSize;
    config::kBaseLevelSize = src.kBaseLevelSize;
    config::kUseMmapRandomAccess = src.kUseMmapRandomAccess;
    // we make kFilterBase <= block_size here, actually can >. see filter_block.cc;
    int base_lg = src.kFilterBaseLg;
    while ((1 << base_lg) > src.block_size) {
      --base_lg;
    }
    config::kFilterBaseLg = base_lg;
    config::kFilterBase = 1 << kFilterBaseLg;
    
    // limit compact etc.
    config::kLimitCompactLevelCount = src.kLimitCompactLevelCount;
    config::kLimitCompactCountInterval = src.kLimitCompactCountInterval;
    config::kLimitCompactTimeInterval = src.kLimitCompactTimeInterval;
    SetLimitCompactTimeRange(src.kLimitCompactTimeStart, src.kLimitCompactTimeEnd);
    config::kLimitDeleteObsoleteFileInterval = src.kLimitDeleteObsoleteFileInterval;
    config::kDoSeekCompaction = src.kDoSeekCompaction;
  }

  void config::SetLimitCompactTimeRange(int time_start, int time_end) {
    config::kLimitCompactTimeReverse = (time_start > time_end);
    if (config::kLimitCompactTimeReverse) {
      config::kLimitCompactTimeStart = time_end;
      config::kLimitCompactTimeEnd = time_start;
    } else {
      config::kLimitCompactTimeStart = time_start;
      config::kLimitCompactTimeEnd = time_end;
    }
  }

  bool config::IsLimitCompactTime() {
    if (config::kLimitCompactTimeStart == config::kLimitCompactTimeEnd) {
      return true;
    }
    time_t t = time(NULL);
    struct tm *tm = localtime((const time_t*) &t);
    bool in_range = tm->tm_hour >= config::kLimitCompactTimeStart && tm->tm_hour <= kLimitCompactTimeEnd;
    return config::kLimitCompactTimeReverse ? !in_range : in_range;
  }
}


