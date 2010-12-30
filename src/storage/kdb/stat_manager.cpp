/*
 * stat_manager.cpp
 *
 *  Created on: 2010-12-1
 *      Author: huzhenxiong.pt
 */

#include "stat_manager.h"

namespace tair {
  namespace storage {
    namespace kdb {
      stat_manager::stat_manager()
      {
        fd = -1;
        stat_info = NULL;
      }

      stat_manager::~stat_manager()
      {
        stop();
      }

      bool stat_manager::start(int bucket_number, const char *file_dir)
      {
        // open file
        snprintf(file_name, PATH_MAX_LENGTH, "%s/tair_kdb_%06d.stat", file_dir, bucket_number);
        fd =::open(file_name, O_RDWR | O_CREAT | O_LARGEFILE, 0600);
        if(fd < 0) {
          log_error("open file [%s] failed: %s", file_name, strerror(errno));
          return false;
        }
        // check the file is new or not
        if(get_size() == 0) {
          if(!initial_stat_file()) {
            return false;
          }
        }
        // mmap
        int flags = PROT_READ | PROT_WRITE;
        int size = KDBSTATINFO_SIZE;
        if(!ensure_file_size(size)) {
          log_error("ensure file size failed");
          return false;
        }
        void* data = mmap(0, size, flags, MAP_SHARED, fd, 0);
        if(data == MAP_FAILED) {
          log_error("map file failed: %s", strerror(errno));
          return false;
        }
        log_info("mmap file successed, maped size is: [%d]", size);
        stat_info = (kdb_stat_info *)data;
        return true;
      }

      void stat_manager::stop()
      {
        if(stat_info) {
          msync(stat_info, KDBSTATINFO_SIZE, MS_SYNC);
          munmap(stat_info, KDBSTATINFO_SIZE);
          stat_info = NULL;
          log_debug("mmap unmapped, size is: [%d]", KDBSTATINFO_SIZE);
        }
        if(fd < 0) {
          log_info("file [%s] not opened, need not close", file_name);
        }
        else if(::close(fd) == -1) {
          log_error("close file [%s] failed: %s", file_name, strerror(errno));
        } else {
          fd = -1;
        }
      }
      
      void stat_manager::destory()
      {
        stop();
        if (::remove(file_name) == -1) {
          log_error("remove file [%s] failed: %s", file_name, strerror(errno));
        }
      }

      bool stat_manager::sync(void)
      {
        if(fd < 0 || stat_info == NULL)
          return false;
        return msync(stat_info, KDBSTATINFO_SIZE, MS_ASYNC) == 0;
      }

      void stat_manager::stat_add(int area, int data_size, int use_size)
      {
        assert(area < TAIR_MAX_AREA_COUNT);
        tbsys::CThreadGuard guard(&stat_lock);
        stat_info->stat[area].add_data_size(data_size);
        stat_info->stat[area].add_use_size(use_size);
        stat_info->stat[area].add_item_count();
      } 

      void stat_manager::stat_sub(int area, int data_size, int use_size)
      {
        assert(area < TAIR_MAX_AREA_COUNT);
        tbsys::CThreadGuard guard(&stat_lock);
        stat_info->stat[area].sub_data_size(data_size);
        stat_info->stat[area].sub_use_size(use_size);
        stat_info->stat[area].sub_item_count();
      }

      tair_pstat* stat_manager::get_stat() const
      {
        return stat_info->stat;
      }

      int stat_manager::get_size()
      {
        int size = -1;
        struct stat s;
        if(fstat(fd, &s) == 0)
          size = s.st_size;
        return size;
      }

      bool stat_manager::initial_stat_file()
      {
        kdb_stat_info temp_stat;
        snprintf(temp_stat.magic, KDBSTATINFO_MAGIC_SIZE, "%s", STAT_MAGIC);
        temp_stat.flag = 0;
        memset(&temp_stat.stat, 0, sizeof(tair_pstat)); 
        char data[KDBSTATINFO_SIZE];
        memset(data, 0, KDBSTATINFO_SIZE);
        memcpy(data, &temp_stat, KDBSTATINFO_SIZE);
        return ::pwrite(fd, data, KDBSTATINFO_SIZE, 0) == (ssize_t) KDBSTATINFO_SIZE;
      }

      bool stat_manager::ensure_file_size(int size)
      {
        struct stat s;
        if(fstat(fd, &s) < 0) {
          log_error("fstat error, {%s}", strerror(errno));
          return false;
        }
        if(s.st_size < size) {
          if(ftruncate(fd, size) < 0) {
            log_error("ftruncate file to size: [%u] failed. {%s}", size, strerror(errno));
            return false;
          }
        }
        return true;
      }
    }
  }
}
