/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * file operations wrap calss
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include <stdio.h>
#include "file_op.hpp"
#include "log.hpp"
namespace tair {
  namespace storage {
    namespace fdb {

      file_operation::file_operation()
      {
        file_name = NULL;
        is_mapped = false;
        map_file = NULL;
        fd = -1;
      }

      file_operation::~file_operation()
      {
        if(map_file != NULL) {
          delete map_file;
            map_file = NULL;
        }
        close();
        if(file_name != NULL)
        {
          free(file_name);
          file_name = NULL;
        }
      }

      bool file_operation::open(char *file_nname, int flag, int mode)
      {
        //assert(m_fd < 0);
        if(file_name != NULL) {
          free(file_name);
          file_name = NULL;
        }

        file_name = strdup(file_nname);

        fd =::open(file_name, flag, mode);
        if(fd < 0) {
          log_error("open file [%s] failed: %s", file_name, strerror(errno));
          return false;
        }

        return true;
      }

      bool file_operation::close()
      {
        if(!is_opened()) {
          log_info("file [%s] not opened, need not close", file_name);
          return true;
        }

        if(::close(fd) == -1) {
          log_error("close file [%s] failed: %s", file_name, strerror(errno));
          return false;
        }

        return true;
      }

      int file_operation::get_size()
      {
        int size = -1;
        struct stat s;
        if(fstat(fd, &s) == 0)
          size = s.st_size;
        return size;
      }

      bool file_operation::lock(off_t offset, size_t size, bool write)
      {
        if(!is_opened())
          return false;

        bool rc = false;
        struct flock lock;
        memset(&lock, 0, sizeof(lock));

        lock.l_start = offset;
        lock.l_len = size;
        lock.l_pid = 0;
        lock.l_whence = SEEK_SET;
        lock.l_type = write ? F_WRLCK : F_RDLCK;

        rc = (fcntl(fd, F_SETLK, &lock) != -1);

        return rc;
      }

      bool file_operation::unlock(off_t offset, size_t length)
      {
        if(!is_opened())
          return false;

        bool rc = false;
        struct flock lock;
        memset(&lock, 0, sizeof(lock));

        lock.l_start = offset;
        lock.l_len = length;
        lock.l_pid = 0;
        lock.l_whence = SEEK_SET;
        lock.l_type = F_UNLCK;

        rc = (fcntl(fd, F_SETLK, &lock) != -1);

        return rc;
      }

      bool file_operation::pread(void *buffer, size_t size, off_t offset)
      {
        if(!is_opened())
          return false;

        if(is_mapped && (offset + size) > map_file->get_size())
          map_file->remap();

        if(is_mapped && (offset + size) <= map_file->get_size()) {
          // use mmap first
          log_debug("read data from mmap[%s], offset [%llu], size [%u]",
                    file_name, offset, size);
          memcpy(buffer, (char *) map_file->get_data() + offset, size);
          return true;
        }

        log_debug("read from [%s], offset: [%llu], size: [%u]", file_name,
                  offset, size);
        return::pread(fd, buffer, size, offset) == (ssize_t) size;
      }

      bool file_operation::sync(void)
      {
        if(!is_opened())
          return false;

        if(is_mapped) {
          return map_file->sync_file();
        }
        else {
          return fsync(fd) == 0;
        }
      }

      ssize_t file_operation::read(void *buffer, size_t size, off_t offset)
      {
        if(!is_opened())
          return false;
        if(is_mapped && (offset + size) > map_file->get_size())
          map_file->remap();

        if(is_mapped && (offset + size) <= map_file->get_size()) {
          // use mmap first
          log_debug("read data from mmap[%s], offset [%llu], size [%u]",
                    file_name, offset, size);
          memcpy(buffer, (char *) map_file->get_data() + offset, size);
          return size;
        }
        log_debug("read from [%s], offset: [%llu], size: [%u]", file_name,
                  offset, size);
        return::pread(fd, buffer, size, offset);
      }

      bool file_operation::write(void *buffer, size_t size)
      {
        if(!is_opened())
          return false;

        log_debug("write data into with size of [%u] at offset [%llu]", size,
                  get_position());

        off_t offset = get_position();
        if(is_mapped && (offset + size) > map_file->get_size()) {
          map_file->remap();
        }

        if(is_mapped && (offset + size) <= map_file->get_size()) {
          log_debug("write data use mmap at offset [%llu] with size [%u]",
                    offset, size);
          memcpy((char *) map_file->get_data() + offset, buffer, size);
          return true;
        }

        return::write(fd, buffer, size) == (ssize_t) size;
      }

      bool file_operation::pwrite(void *buffer, size_t size, off_t offset)
      {
        if(!is_opened())
          return false;

        if(offset < 0)
          offset = get_position();

        log_debug("sizeof(off_t): %d, write[%s]: size [%u] at offset [%llu]",
                  sizeof(off_t), file_name, size, offset);

        if(is_mapped && (offset + size) > map_file->get_size()) {
          map_file->remap();
        }

        if(is_mapped && (offset + size) <= map_file->get_size()) {
          // use mmap first
          log_debug("pwrite data use mmap at offset [%llu] with size [%u]",
                    offset, size);
          memcpy((char *) map_file->get_data() + offset, buffer, size);
          return true;
        }

        return::pwrite(fd, buffer, size, offset) == (ssize_t) size;
      }

      bool file_operation::rename(char *new_name)
      {
        if(::rename(file_name, new_name) == 0) {
          log_warn("filename renamed from [%s] to [%s]", file_name, new_name);
          return true;
        }
        return false;
      }

      bool file_operation::remove()
      {
        close();
        if(::remove(file_name) == 0) {
          log_warn("remove file [%s]", file_name);
          return true;
        }
        return false;
      }

      bool file_operation::append_name(char *app_str)
      {
        char new_name[256];
        snprintf(new_name, 256, "%s.%s", file_name, app_str);
        new_name[255] = '\0';
        return rename(new_name);
      }

      bool file_operation::mmap(int map_size)
      {
        if(map_size == 0)
          return true;

        if(!is_opened()) {
          log_warn("file not opened");
          return false;
        }

        if(!is_mapped) {
          // do map if not mapped yet
          map_file = new mmap_file(map_size, fd);
          is_mapped = map_file->map_file(true);
        }

        return is_mapped;
      }

      void *file_operation::get_map_data()
      {
        if(is_mapped)
          return map_file->get_data();

        return NULL;
      }

      bool file_operation::truncate(off_t size)
      {
        return::ftruncate(fd, size) == 0;

      }

    }
  }
}
