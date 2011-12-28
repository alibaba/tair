/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * view cache stat file
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "ldb_cache_stat.hpp"

using namespace tair::storage::ldb;
using namespace tair;

void print_usage(const char* name)
{
  printf("Usage: %s -f file [-t time_range]\n"
         "timerange can be:\n"
         "\t20111010101010-20111010202320\n"
         "\t20111010101010-\n"
         "\t-20111010101010\n"
         "\t20111010101010 (same as 20111010101010-)\n"
         , name);
}

int get_time_range(const char* range, uint32_t& start, uint32_t& end)
{
  int ret = TAIR_RETURN_SUCCESS;
  start = 0;
  end = ~0U;
  if (range != NULL)
  {
    char buf[128];
    memcpy(buf, range, strlen(range));
    // range maybe:
    // "a"  means [a, ..) or
    // "a-" same as "a" or
    // "-a" means [0, a] or
    // "a-b" means [a, b], or
    std::vector<char*> range_list;
    tbsys::CStringUtil::split(buf, "-", range_list);

    if (range_list.empty())
    {
      ret = TAIR_RETURN_FAILED;
    }
    else if (range_list.size() > 1) // "a-b"
    {
      start = tbsys::CTimeUtil::strToTime(range_list[0]);
      end = tbsys::CTimeUtil::strToTime(range_list[1]);
    }
    else if (range_list.size() > 0) // "a" or "a-" or "-a"
    {
      const char* pos = strchr(range, '-');
      bool has_start = true;
      if (NULL != pos)
      {
        while (pos != range && ' ' == *pos)
        {
          --pos;
        }
        if ((' ' == *pos || '-' == *pos) && pos == range)
        {
          has_start = false;
        }
      }
      uint32_t when = tbsys::CTimeUtil::strToTime(range_list[0]);
      if (has_start)
      {
        start = when;
      }
      else
      {
        end = when;
      }
    }
  }

  return ret;
}

int main(int argc, char* argv[])
{
  int i = 0;
  char* file = NULL;
  char* time_range = NULL;

  while ((i = getopt(argc, argv, "f:t:")) != EOF)
  {
    switch (i)
    {
    case 'f':
      file = optarg;
      break;
    case 't':
      time_range = optarg;
      break;
    default:
      print_usage(argv[0]);
      return TAIR_RETURN_FAILED;
    }
  }

  if (NULL == file)
  {
    print_usage(argv[0]);
    return TAIR_RETURN_FAILED;
  }

  StatDumpFilter filter;
  if (get_time_range(time_range, filter.start_time_, filter.end_time_) != TAIR_RETURN_SUCCESS)
  {
    printf("time range invalid: %s\n", time_range);
    print_usage(argv[0]);
    return TAIR_RETURN_FAILED;
  }

  printf("== dump cache stat range: %s %u => %u ==\n", time_range, filter.start_time_, filter.end_time_);
  CacheStat cache_stat;
  if (cache_stat.load_file(file) == TAIR_RETURN_SUCCESS)
  {
    return cache_stat.dump(&filter);
  }

  return TAIR_RETURN_FAILED;
}
