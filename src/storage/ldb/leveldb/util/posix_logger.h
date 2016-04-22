// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include "leveldb/env.h"

namespace leveldb {

class PosixLogger : public Logger {
 private:
  FILE* file_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread
  std::string filename_;
 public:
  PosixLogger(FILE* f, uint64_t (*gettid)(), const char* filename) :
    file_(f), gettid_(gettid), filename_(filename == NULL ? "" : filename) { }
  virtual ~PosixLogger() {
    fclose(file_);
  }
  virtual void Logv(const char* format, va_list ap) {
    const uint64_t thread_id = (*gettid_)();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, NULL);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;       // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      fwrite(base, 1, p - base, file_);
      fflush(file_);
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }
  virtual Status Rotate(bool force) {
    std::string rotate_filename = RotateFileName(force);
    // no need rotate
    if (rotate_filename.empty()) {
      return Status::OK();
    }

    const char* oldname = filename_.c_str();
    const char* newname = rotate_filename.c_str();
    int err = rename(oldname, newname);
    if (err != 0) {
      return Status::IOError(oldname, strerror(err));
    }

    FILE* f = fopen(oldname, "a");
    err = errno;
    if (f == NULL) {
      // ignore fail.
      rename(newname, oldname);
      return Status::IOError(oldname, strerror(err));
    }
    FILE* old_f = file_;
    file_ = f;
    usleep(200000);             // just wait for last fwrite
    fclose(old_f);
    return Status::OK();
  }

 private:
  std::string RotateFileName(bool force) {
    int suffix_year = 0;
    int suffix_month = 0;
    int suffix_day = 0;

    // now time
    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);

    if (force) {
      // suffix with yesterday
      time_t yesterday_s = now_tv.tv_sec - 86400;
      struct tm yesterday_tm;
      localtime_r(&yesterday_s, &yesterday_tm);
      suffix_year = yesterday_tm.tm_year + 1900;
      suffix_month = yesterday_tm.tm_mon + 1;
      suffix_day = yesterday_tm.tm_mday;
    } else {
      struct tm now_tm;
      localtime_r(&now_tv.tv_sec, &now_tm);

      struct stat filestat;
      int err = stat(filename_.c_str(), &filestat);
      if (err != 0) {
        return std::string();
      }
      struct tm file_tm;
      localtime_r(&filestat.st_mtime, &file_tm);

      if (now_tv.tv_sec - filestat.st_mtime < 86400 && file_tm.tm_mday == now_tm.tm_mday) {
        return std::string();
      }
      // suffix with last modify day
      suffix_year = file_tm.tm_year + 1900;
      suffix_month = file_tm.tm_mon + 1;
      suffix_day = file_tm.tm_mday;
    }

    char buf[32];
    snprintf(buf, sizeof(buf), ".%.4d-%.2d-%.2d", suffix_year, suffix_month, suffix_day);
    return filename_ + buf;
  }
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
