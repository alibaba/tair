/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * storage engine interface impl
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include <assert.h>

#include "fdb_manager.hpp"

namespace tair {
  namespace storage {
    namespace fdb {

      fdb_manager::fdb_manager()
      {
        int cache_size =
          TBSYS_CONFIG.getInt(TAIRFDB_SECTION, FDB_CACHE_SIZE, 256);
        // mdb share memory engine requires 256MB memory at least
          assert(cache_size >= 256);
          memory_cache = mdb_factory::create_embedded_mdb(cache_size, 1.2);
          mreader = NULL;
          dreader = NULL;
          buckets_map = new fdb_buckets_map();
      }

      fdb_manager::~fdb_manager()
      {
        delete memory_cache;
          memory_cache = NULL;

          fdb_buckets_map::iterator it;
        for(it = buckets_map->begin(); it != buckets_map->end(); ++it)
        {
          delete it->second;
        }
        delete buckets_map;
          buckets_map = NULL;
      }

      int fdb_manager::put(int bucket_number, data_entry & key,
                           data_entry & value, bool version_care,
                           int expire_time)
      {
        fdb_bucket *bucket = get_bucket(bucket_number);
        if(bucket == NULL) {
          // bucket not exist
          log_debug("fdbBucket[%d] not exist", bucket_number);
          return TAIR_RETURN_FAILED;
        }

        int rc = TAIR_RETURN_FAILED;
        PROFILER_BEGIN("remove from cache");
        if(memory_cache->remove(bucket_number, key, false) == EXIT_FAILURE) {
          log_error("clear cache failed...");
          return rc;
        }
        PROFILER_END();
        log_debug("fdb put");
        key.data_meta.log_self();
        rc = bucket->put(key, value, version_care, expire_time);
        return rc;
      }

      int fdb_manager::get(int bucket_number, data_entry & key,
                           data_entry & value)
      {

        fdb_bucket *bucket = get_bucket(bucket_number);

        if(bucket == NULL) {
          return TAIR_RETURN_FAILED;
        }

        int rc = TAIR_RETURN_SUCCESS;
        PROFILER_BEGIN("get from cache");
        if(memory_cache->get(bucket_number, key, value) == EXIT_SUCCESS) {
          log_debug("value get from mdb, size: %d", value.get_size());
          value.decode_meta(true);
          key.data_meta = value.data_meta;
          log_debug("memcache ing");
          key.data_meta.log_self();
          log_debug("cache hit...");
          PROFILER_END();
          return rc;
        }
        PROFILER_END();

        rc = bucket->get(key, value);
        log_debug("fdb getting");
        key.data_meta.log_self();
        PROFILER_BEGIN("put into cache");
        if(rc == TAIR_RETURN_SUCCESS) {
          data_entry temp_value = value;
          temp_value.merge_meta();
          log_debug("value put into mdb, size: %d", temp_value.get_size());
          memory_cache->put(bucket_number, key, temp_value, false,
                            key.data_meta.edate);
        }
        PROFILER_END();

        return rc;
      }

      int fdb_manager::remove(int bucket_number, data_entry & key,
                              bool version_care)
      {

        fdb_bucket *bucket = get_bucket(bucket_number);

        if(bucket == NULL) {
          return TAIR_RETURN_FAILED;
        }

        int rc = TAIR_RETURN_FAILED;
        PROFILER_BEGIN("remove from cache");
        if(memory_cache->remove(bucket_number, key, false) == EXIT_FAILURE) {
          log_error("clear cache failed...");
          PROFILER_END();
          return rc;
        }
        PROFILER_END();

        rc = bucket->remove(key, version_care);
        return rc;
      }

      int fdb_manager::clear(int area)
      {
        return TAIR_RETURN_SUCCESS;
      }


      bool fdb_manager::init_buckets(const vector < int >&buckets)
      {
        tbsys::CThreadGuard guard(&stat_lock);

        fdb_buckets_map *temp_buckets_map = new fdb_buckets_map(*buckets_map);

        for(int i = 0; i < (int) buckets.size(); i++) {
          int bucket_number = buckets[i];
          fdb_bucket *bucket = get_bucket(bucket_number);
          if(bucket != NULL) {
            // already exist
            continue;
          }

          // init new FdbBucket
          fdb_bucket *new_bucket = new fdb_bucket();
          bool sr = new_bucket->start(bucket_number);
          if(!sr) {
            // init FdbBucket failed
            log_error("init bucket[%d] failed", bucket_number);
            delete new_bucket;
            return false;
          }
          (*temp_buckets_map)[bucket_number] = new_bucket;
        }

        fdb_buckets_map *old_buckets_map = buckets_map;
        buckets_map = temp_buckets_map;
        usleep(100);
        delete old_buckets_map;

#ifdef TAIR_DEBUG
        if(buckets.size() > 0) {
          for(size_t i = 0; i < buckets.size(); ++i) {
            log_debug("init bucket: [%d]", buckets[i]);
          }
        }

        log_debug("after init, local bucket:");
        fdb_buckets_map::const_iterator it;
        for(it = buckets_map->begin(); it != buckets_map->end(); ++it) {
          log_debug("bucket[%d] => [%p]", it->first, it->second);
        }

#endif
        return true;
      }

      void fdb_manager::close_buckets(const vector<int> &buckets)
      {
        tbsys::CThreadGuard guard(&stat_lock);
        fdb_buckets_map *temp_buckets_map = new fdb_buckets_map(*buckets_map);
        vector<fdb_bucket * >rm_buckets;

        for(int i = 0; i < (int) buckets.size(); i++) {
          int bucket_number = buckets[i];
          fdb_bucket *bucket = get_bucket(bucket_number);
          if(bucket == NULL) {
            // not exist
            continue;
          }

          // init new FdbBucket
          temp_buckets_map->erase(bucket_number);
          rm_buckets.push_back(bucket);
        }

        fdb_buckets_map *old_buckets_map = buckets_map;
        buckets_map = temp_buckets_map;

        sleep(1);
        for(size_t i = 0; i < rm_buckets.size(); ++i) {
          rm_buckets[i]->backup();
          delete rm_buckets[i];
        }

        delete old_buckets_map;
      }

      fdb_bucket *fdb_manager::get_bucket(int bucket_numer)
      {
        fdb_buckets_map::iterator it = buckets_map->find(bucket_numer);
        if(it == buckets_map->end()) {
          return NULL;
        }

        return it->second;
      }

      void fdb_manager::get_stats(tair_stat * stat)
      {
        tbsys::CThreadGuard guard(&stat_lock);
        fdb_buckets_map::iterator it = buckets_map->begin();
        for(; it != buckets_map->end(); ++it) {
          it->second->get_stat(stat);
        }
      }


      void fdb_manager::begin_scan(md_info & info)
      {
        const char *data_dir =
          TBSYS_CONFIG.getString(TAIRFDB_SECTION, FDB_DATA_DIR,
                                 FDB_DEFAULT_DATA_DIR);
        const char *db_name =
          TBSYS_CONFIG.getString(TAIRFDB_SECTION, FDB_NAME, FDB_DEFAULT_NAME);

        int db_id = info.db_id;
        // open data file
        char file_name[FILENAME_MAX_LENGTH];
        sprintf(file_name, "%s/%s_%06d.%s", data_dir, db_name, db_id,
                DATA_SUFFIX);
        file_operation *file = new file_operation();
        if(!file->open(file_name, O_RDONLY, 0600)) {
          TBSYS_LOG(ERROR, "database data file [%s] open failed: {%s}",
                    file_name, strerror(errno));
          delete file;
          exit(-1);
        }
        log_debug("transfer data file [%s]", file_name);
        if(info.is_migrate) {
          mreader = new data_reader();
          mreader->set_file(file);
          uint64_t file_len = file->get_size();
          mreader->set_length(file_len);
        }
        else {
          dreader = new data_reader();
          dreader->set_file(file);
          uint64_t fileLen = file->get_size();
          dreader->set_length(fileLen);
        }
      }

      bool fdb_manager::get_next_items(md_info & info,
                                       vector<item_data_info *> &list)
      {

        item_data_info *item = NULL;
        if(info.is_migrate) {
          item = mreader->get_next_item();
        }
        else {
          item = dreader->get_next_item();
        }
        if(item == NULL) {
          return false;
        }
        else {
          list.push_back(item);
          return true;
        }
      }

      void fdb_manager::end_scan(md_info & info)
      {
        if(info.is_migrate) {
          mreader->close();
          mreader = NULL;
        }
        else {
          dreader->close();
          dreader = NULL;
        }
      }

    }                                /* fdb */
  }                                /* storage */
}                                /* tair */
