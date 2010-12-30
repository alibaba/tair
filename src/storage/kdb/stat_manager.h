/*
 * stat_manager.h
 *
 *  Created on: 2010-12-1
 *      Author: huzhenxiong.pt
 */

#ifndef TAIR_KDB_STAT_MANAGER_H
#define TAIR_KDB_STAT_MANAGER_H

#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdint.h>
#include <tbsys.h>
#include "define.hpp"
#include "log.hpp"
#include "stat_info.hpp"        // for tair_pstat

namespace tair {
  namespace storage {
    const static int KDBSTATINFO_MAGIC_SIZE = 16;
    namespace kdb {

      struct kdb_stat_info {
        char magic[KDBSTATINFO_MAGIC_SIZE];
        uint8_t flag;
        tair_pstat stat[TAIR_MAX_AREA_COUNT];
      };
      const static size_t KDBSTATINFO_SIZE = sizeof(kdb_stat_info);
      const static char STAT_MAGIC[16] = "TAIR_STAT_200";
    
      class stat_manager {
        const static int PATH_MAX_LENGTH = 1024;
        public:
          stat_manager();
          virtual ~stat_manager();
        
          bool start(int bucket_number, const char *file_dir);
          void stop();
          void destory();
        
          void stat_add(int area, int data_size, int use_size);
          void stat_sub(int area, int data_size, int use_size);
          tair_pstat * get_stat() const;
        
          bool sync(void);
      
        private:
          int get_size();
          bool initial_stat_file();
          bool ensure_file_size(int size);

        private:
          int fd;
          char file_name[PATH_MAX_LENGTH];
          tbsys::CThreadMutex stat_lock;
          kdb_stat_info * stat_info;
      };
    }
  }
}
#endif /* TAIR_KDB_STAT_MANAGER_H */
