/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#ifndef MDB_DEFINE_H_
#define MDB_DEFINE_H_
#include <stdint.h>
#define TAIR_SLAB_LARGEST            100
#define TAIR_SLAB_BLOCK              1048576
#define TAIR_SLAB_ALIGN_BYTES        8
#define TAIR_SLAB_HASH_MAXDEPTH      8


struct mdb_param
{
  static const char *mdb_type;
  static const char *mdb_path;
  static int64_t size;
  static int slab_base_size;
  static int page_size;
  static double factor;
  static int hash_shift;

  static int chkexprd_time_low;
  static int chkexprd_time_high;

  static int chkslab_time_low;
  static int chkslab_time_high;
};

bool hour_range(int min, int max);


#endif /*MDB_DEFINE_H_ */
