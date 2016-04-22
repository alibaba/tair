/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong
 *
 */

#include "tbtimeutil.h"
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace tbsys {

/*
 * 得到当前时间
 */
int64_t CTimeUtil::getTime() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_usec));
}

int64_t CTimeUtil::getMonotonicTime() {
  timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_nsec/1000));
}

/**
 * 把int转成20080101101010的格式
 */ 
char *CTimeUtil::timeToStr(time_t t, char *dest)
{
  struct tm r;
  memset(&r, 0, sizeof(r));
  if (localtime_r((const time_t*)&t, &r) == NULL) {
    fprintf(stderr, "TIME: %s (%d)\n", strerror(errno), errno);
    dest[0] = '\0';
    return dest;
  }
  sprintf(dest, "%04d%02d%02d%02d%02d%02d",
      r.tm_year+1900, r.tm_mon+1, r.tm_mday,
      r.tm_hour, r.tm_min, r.tm_sec);
  return dest;
}

/**
 * 把字节串转成时间(当地时间)
 */
int CTimeUtil::strToTime(char *str)
{
  if (str == NULL || strlen(str) != 14) {
    return 0;
  }
  char *p = str;
  while((*p)) {
    if ((*p) < '0' || (*p) > '9') return 0;
    p ++;
  }

  struct tm t;
  t.tm_year = (str[0] - '0') * 1000 + (str[1] - '0') * 100 + (str[2] - '0') * 10 + (str[3] - '0') - 1900;
  t.tm_mon = (str[4] - '0') * 10 + (str[5] - '0') - 1;
  t.tm_mday = (str[6] - '0') * 10 + (str[7] - '0');
  t.tm_hour = (str[8] - '0') * 10 + (str[9] - '0');
  t.tm_min = (str[10] - '0') * 10 + (str[11] - '0');
  t.tm_sec = (str[12] - '0') * 10 + (str[13] - '0');
  t.tm_isdst = 0;
  int t1 = mktime(&t);
  return t1;
}

}
