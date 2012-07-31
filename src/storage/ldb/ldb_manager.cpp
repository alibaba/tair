/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "storage/mdb/mdb_factory.hpp"
#include "storage/mdb/mdb_manager.hpp"
#include "ldb_manager.hpp"
#include "ldb_instance.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      extern void get_bloom_stats(cache_stat* ldb_cache_stat);

      LdbManager::LdbManager() : use_bloomfilter_(false), cache_(NULL), scan_ldb_(NULL),
                                 migrate_wait_us_(0), last_release_time_(0)
      {
        std::string cache_stat_path;
        bool use_cache = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USE_CACHE, 1) > 0;
        use_bloomfilter_ = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USE_BLOOMFILTER, 0) > 0;
        if (use_cache)
        {
          cache_ = dynamic_cast<tair::mdb_manager*>(mdb_factory::create_embedded_mdb());
          if (NULL == cache_)
          {
            log_error("init ldb memory cache fail.");
          }
        }

        if (cache_ != NULL || use_bloomfilter_)
        {
          const char* log_file_path = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_LOG_FILE, "server.log");
          const char* pos = strrchr(log_file_path, '/');
          if (NULL == pos)
          {
            cache_stat_path.assign("./");
          }
          else
          {
            cache_stat_path.assign(log_file_path, pos - log_file_path);
          }

          if (!cache_stat_.start(cache_stat_path.c_str(),
                                 TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_CACHE_STAT_FILE_SIZE, (20*1<<20))))
          {
            log_error("start cache stat fail.");
          }
        }

        bool db_version_care = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DB_VERSION_CARE, 1) > 0;
        db_count_ = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DB_INSTANCE_COUNT, 1);
        ldb_instance_ = new LdbInstance*[db_count_];
        for (int32_t i = 0; i < db_count_; ++i)
        {
          ldb_instance_[i] = new LdbInstance(i + 1, db_version_care, cache_);
        }

        log_warn("ldb storage engine construct count: %d, db version care: %s, use cache: %s, cache stat path: %s",
                 db_count_, db_version_care ? "yes" : "no", cache_ != NULL ? "yes" : "no", cache_stat_path.c_str());
      }

      LdbManager::~LdbManager()
      {
        if (ldb_instance_ != NULL)
        {
          for (int32_t i = 0; i < db_count_; ++i)
          {
            delete ldb_instance_[i];
          }
        }

        delete[] ldb_instance_;

        if (cache_ != NULL)
        {
          delete cache_;
        }
      }

      int LdbManager::put(int bucket_number, data_entry& key, data_entry& value, bool version_care, int expire_time)
      {
        log_debug("ldb::put");
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = db_instance->put(bucket_number, key, value, version_care, expire_time);
        }

        return rc;
      }

      int LdbManager::get(int bucket_number, data_entry& key, data_entry& value)
      {
        log_debug("ldb::get");
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = db_instance->get(bucket_number, key, value);
        }

        return rc;
      }

      int LdbManager::get_range(int bucket_number, data_entry& key_start, data_entry& key_end, int offset, int limit, int type, std::vector<data_entry*>& result, bool &has_next)
      {
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else 
        {
          PROFILER_BEGIN("db_instance get_range");
          rc = db_instance->get_range(bucket_number, key_start, key_end, offset, limit, type, result, has_next);
          log_debug("after get_range key_count:%d", result.size());
          PROFILER_END();
        }
        return rc;
      }

      int LdbManager::remove(int bucket_number, data_entry& key, bool version_care)
      {
        log_debug("ldb::remove");
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = db_instance->remove(bucket_number, key, version_care);
        }

        return rc;
      }

      int LdbManager::clear(int32_t area)
      {
        log_debug("ldb::clear %d", area);
        int ret = TAIR_RETURN_SUCCESS;

        for (int32_t i = 0; i < db_count_; ++i)
        {
          int tmp_ret = ldb_instance_[i]->clear_area(area);
          if (tmp_ret != TAIR_RETURN_SUCCESS)
          {
            ret = tmp_ret;
            log_error("clear area %d for instance %d fail.", area, i+1); // just continue
          }
        }

        // clear cache
        if (cache_ != NULL)
        {
          cache_->clear(area);
        }
        return ret;
      }

      bool LdbManager::init_buckets(const std::vector<int>& buckets)
      {
        log_warn("ldb::init buckets: %d", buckets.size());
        tbsys::CThreadGuard guard(&lock_);
        bool ret = false;

        if (1 == db_count_)
        {
          ret = ldb_instance_[0]->init_buckets(buckets);
        }
        else
        {
          std::vector<int32_t>* tmp_buckets = new std::vector<int32_t>[db_count_];

          for (std::vector<int>::const_iterator it = buckets.begin(); it != buckets.end(); ++it)
          {
            tmp_buckets[hash(*it) % db_count_].push_back(*it);
          }

          for (int32_t i = 0; i < db_count_; ++i)
          {
            log_warn("instance %d own %d buckets.", i+1, tmp_buckets[i].size());
            ret = ldb_instance_[i]->init_buckets(tmp_buckets[i]);
            if (!ret)
            {
              log_error("init buckets for db instance %d fail", i + 1);
              break;
            }
          }
          delete[] tmp_buckets;
        }

        return ret;
      }

      void LdbManager::close_buckets(const std::vector<int>& buckets)
      {
        log_warn("ldb::close buckets: %d", buckets.size());
        tbsys::CThreadGuard guard(&lock_);
        if (1 == db_count_)
        {
          ldb_instance_[0]->close_buckets(buckets);
        }
        else
        {
          std::vector<int32_t>* tmp_buckets = new std::vector<int32_t>[db_count_];
          for (std::vector<int>::const_iterator it = buckets.begin(); it != buckets.end(); ++it)
          {
            tmp_buckets[hash(*it) % db_count_].push_back(*it);
          }

          for (int32_t i = 0; i < db_count_; ++i)
          {
            ldb_instance_[i]->close_buckets(tmp_buckets[i]);
          }
          delete[] tmp_buckets;
        }

        // clear mdb cache
        if (cache_ != NULL)
        {
          cache_->close_buckets(buckets);
        }
      }

      // only one bucket can scan at any time ?
      void LdbManager::begin_scan(md_info& info)
      {
        if ((scan_ldb_ = get_db_instance(info.db_id)) == NULL)
        {
          log_error("scan bucket[%u] not exist", info.db_id);
        }
        else
        {
          if (!scan_ldb_->begin_scan(info.db_id))
          {
            log_error("begin scan bucket[%u] fail", info.db_id);
          }
        }
      }

      void LdbManager::end_scan(md_info& info)
      {
        if (scan_ldb_ != NULL)
        {
          scan_ldb_->end_scan();
          scan_ldb_ = NULL;
        }
      }

      bool LdbManager::get_next_items(md_info& info, std::vector<item_data_info*>& list)
      {
        bool ret = true;
        if (NULL == scan_ldb_)
        {
          ret = false;
          log_error("scan bucket not opened");
        }
        else
        {
          // @@ avoid to migrate data to one server too roughly, especially when several servers do
          // at the same time, just simply sleep some here
          if (migrate_wait_us_ > 0)
          {
            ::usleep(migrate_wait_us_);
          }
          ret = scan_ldb_->get_next_items(list);
          log_debug("get items %d", list.size());
        }
        return ret;
      }

      void LdbManager::set_area_quota(int area, uint64_t quota)
      {
        // we consider set area quota to cache
        if (cache_ != NULL)
        {
          cache_->set_area_quota(area, quota);
        }
      }

      void LdbManager::set_area_quota(std::map<int, uint64_t>& quota_map)
      {
        if (cache_ != NULL)
        {
          cache_->set_area_quota(quota_map);
        }
      }

      int LdbManager::op_cmd(ServerCmdType cmd, std::vector<std::string>& params)
      {
        int ret = TAIR_RETURN_FAILED;

        tbsys::CThreadGuard guard(&lock_);

        // op cmd to instance
        for (int32_t i = 0; i < db_count_; ++i)
        {
          ret = ldb_instance_[i]->op_cmd(cmd, params);
          if (ret != TAIR_RETURN_SUCCESS)
          {
            break;
          }
        }

        // op cmd in ldb_manager level shared by all instances
        if (TAIR_RETURN_SUCCESS == ret)
        {
          switch (cmd)
          {
          case TAIR_SERVER_CMD_SET_MIGRATE_WAIT_MS:
          {
            if (params.empty())
            {
              log_error("set migrate wait time but no param.");
              ret = TAIR_RETURN_FAILED;
            }
            else
            {
              ret = do_set_migrate_wait(atoi(params[0].c_str()));
            }
            break;
          }
          case TAIR_SERVER_CMD_RELEASE_MEM:
          {
            ret = do_release_mem();
            break;
          }
          case TAIR_SERVER_CMD_SET_CONFIG:
          {
            if (params.size() < 2)
            {
              log_error("set config but invalid params size: %d", params.size());
              ret = TAIR_RETURN_FAILED;
            }
            else if (params[0] == TAIR_PROFILER_THRESHOLD)
            {
              log_warn("set profiler threshold to %s", params[1].c_str());
              PROFILER_SET_THRESHOLD(atoi(params[1].c_str()));
            }
            break;
          }
          case TAIR_SERVER_CMD_PAUSE_GC:
          case TAIR_SERVER_CMD_RESUME_GC:
          case TAIR_SERVER_CMD_STAT_DB:
          {
            break;            
          }
          case TAIR_SERVER_CMD_RESET_DB:
          case TAIR_SERVER_CMD_FLUSH_MMT:
          default:
          {
            log_error("op cmd unsupported type: %d", cmd);
            ret = TAIR_RETURN_NOT_SUPPORTED;
            break;
          }
          }
        }
        return ret;
      }

      int LdbManager::do_set_migrate_wait(int32_t cmd_wait_ms)
      {
        static const int32_t MAX_MIGRATE_WAIT_MS = 60000; // 1min
        int ret = TAIR_RETURN_SUCCESS;
        if (cmd_wait_ms >= 0 && cmd_wait_ms < MAX_MIGRATE_WAIT_MS)
        {
          log_warn("set migrate wait ms %d", cmd_wait_ms);
          migrate_wait_us_ = cmd_wait_ms * 1000;
          ret = TAIR_RETURN_SUCCESS;
        }
        else
        {
          log_error("set migrate wait time but param is invalid: %d. max migrate wait ms: %d", cmd_wait_ms, MAX_MIGRATE_WAIT_MS);
          ret = TAIR_RETURN_FAILED;
        }
        return ret;
      }

      int LdbManager::do_release_mem()
      {
        last_release_time_ = time(NULL);
        return TAIR_RETURN_SUCCESS;
      }

      void LdbManager::maybe_exec_cmd()
      {
#ifdef WITH_TCMALLOC
        // force to free memory to system,
        // 'cause freed memory may be in tcmalloc's free page list.
        static const int32_t RELEASE_LDB_MEM_INTERVAL_S = 60; // 60s
        if (last_release_time_ > 0 && time(NULL) - last_release_time_ > RELEASE_LDB_MEM_INTERVAL_S)
        {
          MallocExtension::instance()->ReleaseFreeMemory();
          log_warn("tcmalloc release memory to sys now");
          malloc_stats();
          last_release_time_ = 0;
        }
#endif
      }

      void LdbManager::get_stats(tair_stat* stat)
      {
        tbsys::CThreadGuard guard(&lock_);
        log_debug("ldbmanager get stats");

        for (int32_t i = 0; i < db_count_; ++i)
        {
          ldb_instance_[i]->get_stats(stat);
        }
        // lock hold, static is ok.
        // use cache stat's 
        static cache_stat ldb_cache_stat[TAIR_MAX_AREA_COUNT];
        if (cache_ != NULL || use_bloomfilter_)
        {
          if (cache_ != NULL)
          {
            cache_->raw_get_stats(ldb_cache_stat);
          }
          if (use_bloomfilter_)
          {
            // use cache to store bloom stat now temporarily.
            // TODO ..
            get_bloom_stats(ldb_cache_stat);
          }
          cache_stat_.save(ldb_cache_stat, TAIR_MAX_AREA_COUNT);
        }
        maybe_exec_cmd();
      }

      void LdbManager::set_bucket_count(uint32_t bucket_count)
      {
        if (this->bucket_count == 0)
        {
          this->bucket_count = bucket_count;
          if (cache_ != NULL)
          {
            // set cache (mdb_manager) bucket count
            cache_->set_bucket_count(bucket_count);
          }
        }
      }

      // integer hash function
      int LdbManager::hash(int h)
      {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >> 10);
        h += (h << 3);
        h ^= (h >> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >> 16);
      }

      LdbInstance* LdbManager::get_db_instance(const int bucket_number)
      {
        LdbInstance* ret = (1 == db_count_) ? ldb_instance_[0] :
          ldb_instance_[hash(bucket_number) % db_count_];
        assert(ret != NULL);
        return ret->exist(bucket_number) ? ret : NULL;
      }

    }
  }
}
