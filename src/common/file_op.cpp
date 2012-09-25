/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <Memory.hpp>

#include "define.hpp"
#include "file_op.hpp"

namespace tair
{
  namespace common
  {
    FileOperation::FileOperation(const std::string& file_name, const int open_flags) :
      fd_(-1), open_flags_(open_flags)
    {
      file_name_ = strdup(file_name.c_str());
    }

    FileOperation::~FileOperation()
    {
      if (fd_ > 0)
      {
        ::close(fd_);
        fd_ = -1;
      }

      if (NULL != file_name_)
      {
        free(file_name_);
        file_name_ = NULL;
      }
    }

    void FileOperation::close_file()
    {
      if (fd_ < 0)
      {
        return;
      }
      ::close(fd_);
      fd_ = -1;
    }

    int64_t FileOperation::pread_file(char* buf, const int32_t nbytes, const int64_t offset)
    {
      int32_t left = nbytes;
      int64_t read_offset = offset;
      int32_t read_len = 0;
      char* p_tmp = buf;

      int i = 0;
      while (left > 0)
      {
        ++i;
        if (i >= MAX_DISK_TIMES)
        {
          break;
        }

        if (check_file() < 0)
          return -errno;

        if ((read_len = ::pread64(fd_, p_tmp, left, read_offset)) < 0)
        {
          read_len = -errno;
          if (EINTR == -read_len || EAGAIN == -read_len)
          {
            continue; /* call pread64() again */
          }
          else if (EBADF == -read_len)
          {
            fd_ = -1;
            continue;
          }
          else
          {
            return read_len;
          }
        }
        else if (0 == read_len)
        {
          break; //reach end
        }

        left -= read_len;
        p_tmp += read_len;
        read_offset += read_len;
      }

      return (p_tmp - buf);
    }

    int64_t FileOperation::pwrite_file(const char* buf, const int32_t nbytes, const int64_t offset)
    {
      int32_t left = nbytes;
      int64_t write_offset = offset;
      int32_t written_len = 0;
      const char* p_tmp = buf;

      int i = 0;
      while (left > 0)
      {
        ++i;
        // disk io time over
        if (i >= MAX_DISK_TIMES)
        {
          break;
        }

        if (check_file() < 0)
          return -errno;

        if ((written_len = ::pwrite64(fd_, p_tmp, left, write_offset)) < 0)
        {
          written_len = -errno;
          if (EINTR == -written_len || EAGAIN == -written_len)
          {
            continue;
          }
          if (EBADF == -written_len)
          {
            fd_ = -1;
            continue;
          }
          else
          {
            return written_len;
          }
        }
        else if (0 == written_len)
        {
          break;
        }

        left -= written_len;
        p_tmp += written_len;
        write_offset += written_len;
      }

      return (p_tmp - buf);
    }

    int FileOperation::write_file(const char* buf, const int32_t nbytes)
    {
      const char *p_tmp = buf;
      int32_t left = nbytes;
      int32_t written_len = 0;
      int i = 0;
      while (left > 0)
      {
        ++i;
        if (i >= MAX_DISK_TIMES)
        {
          break;
        }

        if (check_file() < 0)
          return -errno;

        if ((written_len = ::write(fd_, p_tmp, left)) <= 0)
        {
          written_len = -errno;
          if (EINTR == -written_len || EAGAIN == -written_len)
          {
            continue;
          }
          if (EBADF == -written_len)
          {
            fd_ = -1;
            continue;
          }
          else
          {
            return written_len;
          }
        }

        left -= written_len;
        p_tmp += written_len;
      }

      return p_tmp - buf;
    }

    int64_t FileOperation::get_file_size()
    {
      int fd = check_file();
      if (fd < 0)
        return fd;

      struct stat statbuf;
      if (fstat(fd, &statbuf) != 0)
      {
        return -1;
      }
      return statbuf.st_size;
    }

    int FileOperation::ftruncate_file(const int64_t length)
    {
      int fd = check_file();
      if (fd < 0)
        return fd;

      return ftruncate(fd, length);
    }

    int FileOperation::seek_file(const int64_t offset)
    {
      int fd = check_file();
      if (fd < 0)
        return fd;

      return lseek(fd, offset, SEEK_SET);
    }

    int32_t FileOperation::current_pos()
    {
      int fd = check_file();
      if (fd < 0)
        return fd;

      return lseek(fd, 0, SEEK_CUR);
    }

    int FileOperation::flush_file()
    {
      if (open_flags_ & O_SYNC)
      {
        return 0;
      }

      int fd = check_file();
      if (fd < 0)
        return fd;

      return fsync(fd);
    }

    int FileOperation::flush_data()
    {
      if (open_flags_ & O_SYNC)
      {
        return 0;
      }

      int fd = check_file();
      if (fd < 0)
        return fd;

      return fdatasync(fd);
    }

    int FileOperation::unlink_file()
    {
      close_file();
      return ::unlink(file_name_);
    }

    int FileOperation::rename_file(const char* new_name)
    {
      int ret = TAIR_RETURN_FAILED;
      if (NULL != new_name || new_name[0] != '\0')
      {
        size_t new_name_len = strlen(new_name);
        if (strlen(file_name_) != new_name_len || memcmp(new_name, file_name_, new_name_len) != 0)
        {
          if (fd_ > 0)
          {
            fsync(fd_);
            ::close(fd_);
            fd_ = -1;
          }

          if (::rename(file_name_, new_name) == 0)
          {
            ret = TAIR_RETURN_SUCCESS;
            free(file_name_);
            file_name_ = strdup(new_name);
          }
        }
      }
      return ret;
    }

    int FileOperation::open_file()
    {
      if (fd_ > 0)
      {
        close(fd_);
        fd_ = -1;
      }

      fd_ = ::open(file_name_, open_flags_, OPEN_MODE);
      if (fd_ < 0)
      {
        return -errno;
      }
      return fd_;
    }

    int FileOperation::check_file()
    {
      if (fd_ < 0)
      {
        fd_ = open_file();
      }

      return fd_;
    }

  }
}
