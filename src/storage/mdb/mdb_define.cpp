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
#include "mdb_define.hpp"
#include <algorithm>
#include <time.h>

const char *
  mdb_param::mdb_path = "mdb_shm_path01";
int64_t
  mdb_param::size = (1 << 31);
int
  mdb_param::page_size = (1 << 20);
double
  mdb_param::factor = 1.1;
int
  mdb_param::hash_shift = 23;


int
  mdb_param::chkexprd_time_low = 2;
int
  mdb_param::chkexprd_time_high = 4;

int
  mdb_param::chkslab_time_low = 5;
int
  mdb_param::chkslab_time_high = 7;


bool
hour_range(int min, int max)
{
  time_t
    t = time(NULL);
  struct tm *
    tm = localtime((const time_t *) &t);
  if(min > max) {
    std::swap(min, max);
  }
  bool
    inrange = tm->tm_hour >= min && tm->tm_hour <= max;
  return inrange;
}
