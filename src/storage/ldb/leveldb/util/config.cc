// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "config.h"
#include "leveldb/options.h"

namespace leveldb {
  int config::kL0_CompactionTrigger;

  int config::kL0_SlowdownWritesTrigger;

  int config::kL0_StopWritesTrigger;

  int config::kMaxMemCompactLevel;

  int config::kTargetFileSize;

  int64_t config::kMaxGrandParentOverlapBytes;

  int config::kBlockSize;

  int config::kBaseLevelSize;

  void config::setConfig(const Options& src) {
    config::kL0_CompactionTrigger = src.kL0_CompactionTrigger;
    config::kL0_SlowdownWritesTrigger = src.kL0_SlowdownWritesTrigger;
    config::kL0_StopWritesTrigger = src.kL0_StopWritesTrigger;
    config::kMaxMemCompactLevel = src.kMaxMemCompactLevel;
    config::kTargetFileSize = src.kTargetFileSize;
    config::kMaxGrandParentOverlapBytes = src.kMaxGrandParentOverlapBytes;
    config::kBlockSize = src.block_size;
    config::kBaseLevelSize = src.kBaseLevelSize;
  }
}


