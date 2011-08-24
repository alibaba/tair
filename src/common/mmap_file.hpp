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
#ifndef TAIR_MMAP_FILE_H
#define TAIR_MMAP_FILE_H

#include <unistd.h>
#include <sys/mman.h>
#include "log.hpp"

namespace tair {
   class file_mapper {
   public:
      file_mapper()
      {
         data = NULL;
         size = 0;
         fd = -1;
      }

      ~file_mapper()
      {
         close_file();
      }

      void close_file()
      {
         if (data) {
            munmap(data, size);
            close(fd);
            data = NULL;
            size = 0;
            fd = -1;
         }
      }


      void sync_file()
      {
         if (data != NULL && size > 0) {
            msync(data, size, MS_ASYNC);
         }
      }

      /**
       * open file
       *
       * createLength == 0 means read only
       */
      bool open_file(const char* file_name, int create_length = 0) 
      {
         int flags = PROT_READ;
         if (create_length > 0) {
            fd = open(file_name, O_RDWR|O_LARGEFILE|O_CREAT, 0644);
            flags = PROT_READ | PROT_WRITE;
         } else {
            fd = open(file_name, O_RDONLY|O_LARGEFILE);
         }

         if (fd < 0) {
            log_error("open file : %s failed, errno: %d", file_name, errno);
            return false;
         }

         if (create_length > 0) {
            if (ftruncate(fd, create_length) != 0) {
               log_error("ftruncate file: %s failed", file_name);
               close(fd);
               fd = -1;
               return false;
            }
            size = create_length;
         } else {
            struct stat stbuff;
            fstat(fd, &stbuff);
            size = stbuff.st_size;
         }

         data = mmap(0, size, flags, MAP_SHARED, fd, 0);
         if (data == MAP_FAILED) {
            log_error("map file: %s failed ,err is %s(%d)", file_name, strerror(errno), errno);
            close(fd);
            fd = -1;
            data = NULL;
            return false;
         }
         return true;
      }

      void *get_data() const
      {
         return data;
      }

      int get_size() const
      {
         return size;
      }

      int get_modify_time() const
      {
         struct stat buffer;
         if (fd>=0 && fstat(fd, &buffer) == 0) {
            return (int)buffer.st_mtime;
         }
         return 0;
      }

   private:
      file_mapper(const file_mapper&);
      file_mapper& operator = (const file_mapper&);
      void *data;
      int size;
      int fd;
   };

}
#endif
