/*
 * stat_manager.h
 *
 *  Created on: 2010-12-1
 *      Author: huzhenxiong.pt
 */

#ifndef TAIR_LDB_STAT_MANAGER_H
#define TAIR_LDB_STAT_MANAGER_H

#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdint.h>

#include <tbsys.h>

#include "common/define.hpp"
#include "common/log.hpp"
#include "common/stat_info.hpp"        // for tair_pstat

namespace tair {
  namespace storage {
    namespace ldb {
      const static int DBSTATINFO_MAGIC_SIZE = 16;

      struct db_stat_info {
        char magic[DBSTATINFO_MAGIC_SIZE];
        uint8_t flag;
        tair_pstat stat[TAIR_MAX_AREA_COUNT];
      };
      const static size_t DBSTATINFO_SIZE = sizeof(db_stat_info);
      const static char STAT_MAGIC[16] = "TAIR_STAT_200";

      class stat_manager {
      public:
        stat_manager();
        virtual ~stat_manager();

        bool start(int bucket_number, const char *file_dir);
        void stop();
        void destroy();

        void stat_add(int area, int data_size, int use_size, int item_count = 1);
        void stat_sub(int area, int data_size, int use_size, int item_count = 1);
        void stat_reset(int area);

        tair_pstat * get_stat() const;

        bool sync(void);

      private:
        int get_size();
        bool initial_stat_file();
        bool ensure_file_size(int size);

      private:
        int fd;
        char file_name[TAIR_MAX_PATH_LEN];
        tbsys::CThreadMutex stat_lock;
        db_stat_info * stat_info;
      };
    }
  }
}
#endif /* TAIR_LDB_STAT_MANAGER_H */
