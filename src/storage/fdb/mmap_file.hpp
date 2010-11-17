/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * mmap wraps
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_MMAP_FILE_HPP
#define TAIR_MMAP_FILE_HPP

#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "log.hpp"

namespace tair {
  namespace storage {
    namespace fdb {
      const static int MB_SIZE = (1 << 20);
      class mmap_file {

      public:

        mmap_file()
        {
          data = NULL;
          max_size = 0;
          size = 0;
          fd = -1;
        }

        mmap_file(int size, int fd)
        {
          max_size = size;
          this->fd = fd;
          data = NULL;
          this->size = 0;
        }

         ~mmap_file()
        {
          if(data) {
            msync(data, size, MS_SYNC);        // make sure synced
            munmap(data, size);
            log_debug("mmap unmapped, size is: [%d]", size);
            data = NULL;
            size = 0;
            fd = -1;
          }
        }

        bool sync_file()
        {
          if(data != NULL && size > 0) {
            return msync(data, size, MS_ASYNC) == 0;
          }
          return true;
        }

        bool map_file(bool write = false) {
          int flags = PROT_READ;

          if(write)
            flags |= PROT_WRITE;

          if(fd < 0)
            return false;

          if(0 == max_size)
            return false;

          if(max_size <= (1024 * MB_SIZE)) {
            size = max_size;
          }
          else {
            size = 1 * MB_SIZE;
          }

          if(!ensure_file_size(size)) {
            log_error("ensure file size failed");
            return false;
          }

          data = mmap(0, size, flags, MAP_SHARED, fd, 0);

          if(data == MAP_FAILED) {
            log_error("map file failed: %s", strerror(errno));
            fd = -1;
            data = NULL;
            size = 0;
            return false;
          }

          log_info("mmap file successed, maped size is: [%d]", size);

          return true;
        }

        bool remap()
        {
          if(fd < 0 || data == NULL) {
            log_error("mremap not mapped yet");
            return false;
          }

          if(size == max_size) {
            log_info("already mapped max size, currSize: [%u], maxSize: [%u]",
                     size, max_size);
            return false;
          }

          size_t new_size = size * 2;
          if(new_size > max_size)
            new_size = max_size;

          if(!ensure_file_size(new_size)) {
            log_error("ensure file size failed in mremap");
            return false;
          }

          //void *newMapData = mremap(m_data, m_size, newSize, MREMAP_MAYMOVE);
          void *new_map_data = mremap(data, size, new_size, 0);
          if(new_map_data == MAP_FAILED) {
            log_error("mremap file failed: %s", strerror(errno));
            return false;
          }
          else {
            log_info("remap success, oldSize: [%u], newSize: [%u]", size,
                     new_size);
          }

          log_info("mremap successed, new size: [%d]", new_size);
          data = new_map_data;
          size = new_size;
          return true;
        }


        void *get_data()
        {
          return data;
        }

        size_t get_size()
        {
          return size;
        }

      private:
        bool ensure_file_size(int size) {
          struct stat s;
          if(fstat(fd, &s) < 0) {
            log_error("fstat error, {%s}", strerror(errno));
            return false;
          }
          if(s.st_size < size) {
            if(ftruncate(fd, size) < 0) {
              log_error("ftruncate file to size: [%u] failed. {%s}", size,
                        strerror(errno));
              return false;
            }
          }

          return true;
        }

      private:
        size_t max_size;
        size_t size;
        int fd;
        void *data;
      };
    }
  }
}

#endif
