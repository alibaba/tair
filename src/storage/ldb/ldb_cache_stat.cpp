/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * cache stat
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */
#include <tbsys.h>
#include "common/log.hpp"

#include "ldb_cache_stat.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      CacheStat::CacheStat() : file_(NULL), is_load_(false), file_offset_(RESERVE_SIZE),
                               max_file_size_(MIN_FILE_SIZE),
                               buf_pos_(buf_ + CACHE_STAT_SIZE), remain_buf_stat_count_(BUFFER_STAT_COUNT - 1)
      {
      }

      CacheStat::~CacheStat()
      {
        stop();
      }

      bool CacheStat::start(const char* dir, int64_t max_file_size)
      {
        bool ret = true;
        if (file_ != NULL)
        {
          log_info("cache stat already start");
        }
        else if (dir != NULL)
        {
          char file_name[PATH_MAX];
          snprintf(file_name, sizeof(file_name), "%s/ldb_cache_stat", dir);
          if (init_file(file_name) != TAIR_RETURN_SUCCESS)
          {
            log_error("start cache stat file %s fail. error: %s", file_name, strerror(errno));
            delete file_;
            file_ = NULL;
            ret = false;
          }
          else
          {
            int64_t file_size = file_->get_file_size();
            file_offset_ = file_size <= RESERVE_SIZE ? RESERVE_SIZE : file_size;
            buf_pos_ = buf_ + CACHE_STAT_SIZE; // for sentinel
            remain_buf_stat_count_ = BUFFER_STAT_COUNT - 1;

            if (max_file_size > max_file_size_)
            {
              max_file_size_ = max_file_size;
            }
            log_info("start cache stat success, file_offset: %"PRI64_PREFIX"d, max_file_size: %"PRI64_PREFIX"d", file_offset_, max_file_size_);
          }
        }
        return ret;
      }

      void CacheStat::stop()
      {
        if (check_init())
        {
          if (!is_load_)
          {
            save_file();
          }
          delete file_;
          file_ = NULL;
        }
      }

      void CacheStat::destroy()
      {
        if (check_init())
        {
          file_->unlink_file();
          delete file_;
          file_ = NULL;
        }
      }

      int CacheStat::save(cache_stat* stat, int32_t stat_count)
      {
        int ret = check_init() && stat != NULL ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        if (TAIR_RETURN_SUCCESS == ret)
        {
          int32_t adjacent_stat_count;
          cache_stat* stat_begin;
          bool need_sentinel = true;
          char* old_buf_pos = buf_pos_;
          int32_t i = 0;

          while (i < stat_count)
          {
            if (stat_valid(stat + i++)) // valid
            {
              set_area(stat + i - 1, i - 1);
              // find adjencent valid data to avoid too many memcpy
              adjacent_stat_count = 1;
              stat_begin = stat + i - 1;
              while (i < stat_count && adjacent_stat_count < remain_buf_stat_count_)
              {
                if (stat_valid(stat + i))
                {
                  log_debug("adjent %d", i);
                  set_area(stat + i, i);
                  ++adjacent_stat_count;
                  ++i;
                }
                else
                {
                  ++i;
                  break;
                }
              }

              memcpy(buf_pos_, reinterpret_cast<char*>(stat_begin),
                     adjacent_stat_count * CACHE_STAT_SIZE);
              buf_pos_ += adjacent_stat_count * CACHE_STAT_SIZE;
              remain_buf_stat_count_ -= adjacent_stat_count;

              if (remain_buf_stat_count_ <= 1)
              {
                if (need_sentinel)
                {
                  // set sentinel before save
                  set_sentinel_stat(reinterpret_cast<cache_stat*>(old_buf_pos - CACHE_STAT_SIZE));
                  need_sentinel = false;
                }
                save_file();
              }
            }
          }

          // not save
          if (need_sentinel)
          {
            if (old_buf_pos != buf_pos_) // has valid stat info
            {
              set_sentinel_stat(reinterpret_cast<cache_stat*>(old_buf_pos - CACHE_STAT_SIZE));
              // remain_buf_stat_count_ > 1
              --remain_buf_stat_count_;
              buf_pos_ += CACHE_STAT_SIZE;
            }
          }
          else                  // already save,
          {
            --remain_buf_stat_count_;
            buf_pos_ += CACHE_STAT_SIZE; // for next sentinel
          }
        }
        return ret;
      }

      int CacheStat::save_file()
      {
        int ret = check_init() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        int32_t size = buf_pos_ - buf_;
        log_debug("save file size: %d %d", size, ret);
        if (TAIR_RETURN_SUCCESS == ret && size > 0)
        {
          int32_t write_len = 0;
          if ((write_len = file_->pwrite_file(buf_, size, file_offset_)) < 0)
          {
            log_error("save cache stat fail, ret: %d", write_len);
            ret = TAIR_RETURN_FAILED;
          }
          else
          {
            file_->flush_file();
            file_offset_ += size;
            rotate();
          }
          // ignore one fail write
          // rewind
          buf_pos_ = buf_;
          remain_buf_stat_count_ = BUFFER_STAT_COUNT;
        }
        return ret;
      }

      void CacheStat::rotate()
      {
        if (file_offset_ > max_file_size_)
        {
          char buf[PATH_MAX + 128];
          int32_t pos = snprintf(buf, sizeof(buf), "%s", file_->get_file_name());
          tbsys::CTimeUtil::timeToStr(time(NULL), buf + pos);
          if (check_init())
          {
            file_->rename_file(buf);
            delete file_;
            file_ = NULL;
            buf[pos] = '\0';
            // reinit file
            init_file(buf);
          }
          file_offset_ = RESERVE_SIZE;
        }
      }

      int CacheStat::load_file(const char* file_name)
      {
        int ret = init_file(file_name);
        if (ret != TAIR_RETURN_SUCCESS)
        {
          log_error("init file fail. %s", file_name);
        }

        is_load_ = true;
        return ret;
      }

      int CacheStat::dump(const StatDumpFilter* dump_filter)
      {
        int ret = check_init() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;

        if (ret != TAIR_RETURN_SUCCESS)
        {
          log_error("dump fail. not init");
        }
        else if (file_->get_file_size() <= RESERVE_SIZE)
        {
          log_error("cache stat contain no data");
        }
        else
        {
          char buf[CACHE_STAT_SIZE * 1024];
          int64_t offset = RESERVE_SIZE;
          int32_t count = 0;
          int32_t read_len;
          cache_stat total_stat, last_stats[TAIR_MAX_AREA_COUNT];
          StatDumpFilter default_filter;
          const StatDumpFilter* filter = (NULL == dump_filter) ? &default_filter : dump_filter;
          bool should_dump = true;

          print_head();
          while (true)
          {
            if ((read_len = file_->pread_file(buf, sizeof(buf), offset)) < 0)
            {
              log_error("read file fail. ret: %d", ret);
              ret = read_len;
              break;
            }
            else if (read_len == 0)
            {
              break;
            }
            else
            {
              count = read_len / CACHE_STAT_SIZE;
              dump_stat(reinterpret_cast<cache_stat*>(buf), count, last_stats, &total_stat, filter, should_dump);
              offset += read_len - read_len % CACHE_STAT_SIZE;
            }
          }
          print_total_stat(&total_stat); // maybe read no sentinel at last
          print_head();
        }
        return ret;
      }

      void CacheStat::dump_stat(cache_stat* stats, int32_t count,
                                cache_stat* last_stats, cache_stat* total_stat,
                                const StatDumpFilter* filter, bool& should_dump)
      {
        cache_stat* pstat;

        for (int i = 0; i < count; ++i)
        {
          pstat = stats + i;

          if (is_sentinel(pstat)) // reach one stat record start
          {
            if (sentinel_valid(pstat))
            {
              should_dump = filter->ok(get_time(pstat));
            }
            else        // sentinel is invalid. it's the end of whole last stat.
            {
              memset(last_stats, 0, CACHE_STAT_SIZE * TAIR_MAX_AREA_COUNT);
              if (should_dump)
              {
                print_total_stat(total_stat);      // print last total stat
                total_stat->reset();
                fprintf(stderr, "\t============== ONE WHOLE STAT END ================\n");
              }
              should_dump = false;
            }

            if (should_dump)
            {
              print_total_stat(total_stat);      // print last total stat
              total_stat->reset();
              print_sentinel(pstat); // print sentinel
            }
          }
          else if (should_dump)
          {
            print_and_add_total_stat(pstat, last_stats, total_stat); // just print
          }
        }
      }

      void CacheStat::print_and_add_total_stat(cache_stat* stat, cache_stat* last_stats, cache_stat* total_stat)
      {
        int32_t area = get_area_and_clear(stat);
        cache_stat* p_last_stat = last_stats + area;
        cache_stat cur_stat = *stat;

        // get current stat
        cur_stat.sub(p_last_stat);
        // set last stat
        *p_last_stat = *stat;
        // add total stat
        total_stat->add(&cur_stat, true);
        // print stat
        print_stat(area, &cur_stat);
      }

      void CacheStat::print_total_stat(const cache_stat* stat)
      {
        if (stat_valid(stat)) // valid.
        {
          fprintf(stderr, "%s\n", "ToTal:");
          print_stat(0, stat);
          fprintf(stderr, "----------------------------------------------------------------------------\n");
        }
      }

      void CacheStat::print_sentinel(const cache_stat* stat)
      {
        if (sentinel_valid(stat))
        {
          fprintf(stderr, "%s\n", get_time_str(stat).c_str());
        }
      }

      void CacheStat::print_stat(int32_t area, const cache_stat* stat)
      {
        fprintf(stderr,
                "%6d %8"PRI64_PREFIX"u %8"PRI64_PREFIX"u %8"PRI64_PREFIX"u %8"PRI64_PREFIX"u %8"PRI64_PREFIX"u %6.2lf %8s %8s %8s %8s\n",
                area, stat->evict_count, stat->remove_count, stat->put_count,
                stat->get_count, stat->hit_count,
                0 == stat->get_count ? 0.00 : static_cast<double>(stat->hit_count)/stat->get_count,
                tbsys::CStringUtil::formatByteSize(stat->item_count).c_str(),
                tbsys::CStringUtil::formatByteSize(stat->data_size).c_str(),
                tbsys::CStringUtil::formatByteSize(stat->space_usage).c_str(),
                tbsys::CStringUtil::formatByteSize(stat->quota).c_str()
          );
      }

      void CacheStat::print_head()
      {
        fprintf(stderr, "-------------------------------------------------------------------------------\n");
        fprintf(stderr, "%6s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s\n",
                "Area", "Evic", "Remove", "Put", "Get", "Hit", "HitRatio", "ItemCount", "DataSize", "UseSize", "Quota");
        fprintf(stderr, "-------------------------------------------------------------------------------\n");
      }

      int CacheStat::init_file(const char* file_name)
      {
        int ret = TAIR_RETURN_SUCCESS;
        if (!check_init())
        {
          file_ = new tair::common::FileOperation(file_name);
          ret = file_->open_file() > 0 ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
        }
        return ret;
      }
    }
  }
}
