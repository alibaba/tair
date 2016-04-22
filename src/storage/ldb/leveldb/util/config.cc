// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <time.h>
#include <vector>
#include <sstream>
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

  uint64_t config::kLogFileKeepInterval;

  int config::kLimitCompactLevelCount;
  int config::kLimitCompactCountInterval;
  int config::kLimitCompactTimeInterval;
  int config::kLimitCompactTimeStart;
  int config::kLimitCompactTimeEnd;
  bool config::kLimitCompactTimeReverse;
  int config::kLimitDeleteObsoleteFileInterval;
  bool config::kDoSeekCompaction;
  bool config::kDoSplitMmtCompaction;
  bool config::kDoBackUpSSTFile;

  int config::kSpecifyCompactScoreThreshold;
  int config::kSpecifyCompactTimeStart;
  int config::kSpecifyCompactTimeEnd;
  int config::kSpecifyCompactMaxThreshold;
  int config::kSpecifyCompactThreshold;
  bool config::kSpecifyCompactTimeReverse;
  bool config::kLimitCompactRepair;

  int config::kLimitGetRange = 1024;
  int config::kLimitRangeScanTimes= 100000;

  template<class T>
  inline static std::string FormatKeyValue(const char* key, T value) {
    std::ostringstream os;
    os<<key<<" = "<<value;
    return os.str();
  }

  void config::getConfig(std::vector<std::string>& infos) {
#define push_config_property(name) \
   infos.push_back(FormatKeyValue(#name, config::name));

    push_config_property(kNumLevels);
    push_config_property(kL0_CompactionTrigger);
    push_config_property(kL0_SlowdownWritesTrigger);
    push_config_property(kL0_StopWritesTrigger);
    push_config_property(kMaxMemCompactLevel);
    push_config_property(kTargetFileSize);
    push_config_property(kMaxGrandParentOverlapBytes);
    push_config_property(kArenaBlockSize);
    push_config_property(kFilterBaseLg);
    push_config_property(kFilterBase);
    push_config_property(kBaseLevelSize);
    push_config_property(kUseMmapRandomAccess);
    push_config_property(kLogFileKeepInterval);
    push_config_property(kSpecifyCompactTimeReverse);
    push_config_property(kSpecifyCompactTimeStart);
    push_config_property(kSpecifyCompactTimeEnd);
    push_config_property(kSpecifyCompactThreshold);
    push_config_property(kSpecifyCompactMaxThreshold);
    push_config_property(kSpecifyCompactScoreThreshold);
    push_config_property(kLimitCompactLevelCount);
    push_config_property(kLimitCompactCountInterval);
    push_config_property(kLimitCompactTimeStart);
    push_config_property(kLimitCompactTimeEnd);
    push_config_property(kLimitCompactTimeReverse);
    push_config_property(kLimitDeleteObsoleteFileInterval);
    push_config_property(kDoSeekCompaction);
    push_config_property(kDoSplitMmtCompaction);
    push_config_property(kDoBackUpSSTFile);
#undef push_config_property
  }

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
    config::kLogFileKeepInterval = src.kLogFileKeepInterval;
    // we make kFilterBase <= block_size here, actually can >. see filter_block.cc;
    int base_lg = src.kFilterBaseLg;
    while ((1LLU << base_lg) > src.block_size) {
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
    config::kDoSplitMmtCompaction = src.kDoSplitMmtCompaction;
    config::kDoBackUpSSTFile = src.kDoBackUpSSTFile;

    config::kSpecifyCompactScoreThreshold = src.kSpecifyCompactScoreThreshold;
    config::kSpecifyCompactTimeStart = src.kSpecifyCompactTimeStart;
    config::kSpecifyCompactTimeEnd = src.kSpecifyCompactTimeEnd;
    config::kSpecifyCompactMaxThreshold = src.kSpecifyCompactMaxThreshold;
    config::kSpecifyCompactThreshold = src.kSpecifyCompactThreshold;
    config::kLimitCompactRepair = false;
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

  void config::SetSpecifyCompactTimeRange(int time_start, int time_end) {
    config::kSpecifyCompactTimeReverse = (time_start > time_end);
    if (config::kSpecifyCompactTimeReverse) {
      config::kSpecifyCompactTimeStart = time_end;
      config::kSpecifyCompactTimeEnd = time_start;
    } else {
      config::kSpecifyCompactTimeStart = time_start;
      config::kSpecifyCompactTimeEnd = time_end;
    }
  }

  inline static bool IsInTimeRange(int start, int end) {
    if (start == end) {
      if (start == -1) {
        return false;
      }
      return true;
    }
    time_t t = time(NULL);
    struct tm *tm = localtime((const time_t*) &t);
    bool in_range = tm->tm_hour >= start && tm->tm_hour <= end;
    return in_range;
  }

  bool config::IsLimitCompactTime() {
    bool in_range = IsInTimeRange(config::kLimitCompactTimeStart,
            config::kLimitCompactTimeEnd);
    return config::kLimitCompactTimeReverse ? !in_range : in_range;
  }

  bool config::IsSpecifyCompactTime() {
    bool in_range = IsInTimeRange(config::kSpecifyCompactTimeStart,
            config::kSpecifyCompactTimeEnd);
    return config::kSpecifyCompactTimeReverse ? !in_range : in_range;
  }
}


