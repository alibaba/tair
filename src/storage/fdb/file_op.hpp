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
#ifndef TAIR_FILEOP_HPP
#define TAIR_FILEOP_HPP
#include "mmap_file.hpp"

namespace tair
{
  namespace storage
  {
    namespace fdb
    {
      class file_operation {
      public:
        file_operation();
        ~file_operation();

        bool open(char *file_name, int flag, int mode);

        bool close(void);

        bool is_opened()
        {
          return fd >= 0;
        }

        bool lock(off_t offset, size_t size, bool write = false);
        bool unlock(off_t offset, size_t size);

        //bool read(char *buffer, size_t size);
        bool pread(void *buffer, size_t size, off_t offset);
        ssize_t read(void *buffer, size_t size, off_t offset);

        bool write(void *buffer, size_t size);
        bool pwrite(void *buffer, size_t size, off_t offset = -1);

        bool rename(char *new_name);

        bool append_name(char *app_str);

        bool set_position(off_t position)
        {
          off_t p = lseek(fd, position, SEEK_SET);

            return p == position;
        }

        off_t get_position()
        {
          return lseek(fd, 0, SEEK_CUR);
        }

        int get_size();

        bool is_empty()
        {
          return get_size() == 0;
        }

        bool remove();

        bool sync(void);

        bool mmap(int map_size);

        void *get_map_data();

        char *get_file_name()
        {
          return file_name;
        }

        int get_maped_size()
        {
          if(is_mapped)
            return map_file->get_size();
          return 0;
        }
        bool truncate(off_t size);
      private:
        int fd;
        char *file_name;
        bool is_mapped;
        mmap_file *map_file;
      };
    }
  }
}
#endif
