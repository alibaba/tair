/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * kyotocabinet storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *
 */

#include <assert.h>

#include "kdb_manager.h"

namespace tair {
  namespace storage {
    namespace kdb {

      using namespace std;

      kdb_manager::kdb_manager()
      {
        buckets_map = new kdb_buckets_map();
      }

      kdb_manager::~kdb_manager()
      {
        kdb_buckets_map::iterator it;
        for (it = buckets_map->begin(); it != buckets_map->end(); ++it)
        {
          delete it->second;
        }

        delete buckets_map;
        buckets_map = NULL;
      }

      int kdb_manager::put(int bucket_number, data_entry & key, data_entry & value, bool version_care, int expire_time)
      {
        int rc = TAIR_RETURN_SUCCESS;

        kdb_bucket* bucket = get_bucket(bucket_number);
        if (bucket == NULL) {
          log_error("kdb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }

        if (rc == TAIR_RETURN_SUCCESS)
        {
          rc = bucket->put(key, value, version_care, expire_time);
        }

        return rc;
      }

      int kdb_manager::get(int bucket_number, data_entry & key, data_entry & value)
      {
        int rc = TAIR_RETURN_SUCCESS;

        kdb_bucket* bucket = get_bucket(bucket_number);
        if (bucket == NULL) {
          log_error("kdb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }

        if (rc == TAIR_RETURN_SUCCESS)
        {
          rc = bucket->get(key, value);
        }
        
        return rc;
      }

      int kdb_manager::remove(int bucket_number, data_entry & key, bool version_care)
      {
        int rc = TAIR_RETURN_SUCCESS;

        kdb_bucket* bucket = get_bucket(bucket_number);
        if (bucket == NULL) {
          log_error("kdb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }

        if (rc == TAIR_RETURN_SUCCESS)
        {
          rc = bucket->remove(key, version_care);
        }
        
        return rc;
      }

      int kdb_manager::clear(int area)
      {
        log_error("clear namespace not support in kdb");
        return TAIR_RETURN_FAILED;
      }

      bool kdb_manager::init_buckets(const vector <int>&buckets)
      {
        tbsys::CThreadGuard guard(&lock);

        kdb_buckets_map* temp_buckets_map = new kdb_buckets_map(*buckets_map);

        for (size_t i=0; i<buckets.size(); i++) {
          int bucket_number = buckets[i];
          kdb_bucket* bucket = get_bucket(bucket_number);
          if (bucket != NULL) {
            log_info("bucket [%d] already exist", bucket_number);
            continue;
          }

          kdb_bucket* new_bucket = new kdb_bucket();
          bool sr = new_bucket->start(bucket_number);
          if (!sr) {
            log_error("init bucket[%d] failed", bucket_number);
            delete new_bucket;
            return false;
          }
          (*temp_buckets_map)[bucket_number] = new_bucket;
        }

        kdb_buckets_map* old_bucket_map = buckets_map;
        buckets_map = temp_buckets_map;
        usleep(100);

        delete old_bucket_map;

        return true;
      }

      void kdb_manager::close_buckets(const vector <int>&buckets)
      {
        tbsys::CThreadGuard guard(&lock);
        kdb_buckets_map* temp_buckets_map = new kdb_buckets_map(*buckets_map);
        vector<kdb_bucket *> rm_buckets;

        for (size_t i=0; i<buckets.size(); ++i)
        {
          int bucket_number = buckets[i];
          kdb_bucket* bucket = get_bucket(bucket_number);
          if (bucket == NULL) {
            log_info("bucket [%d] not exist", bucket_number);
            continue;
          }

          temp_buckets_map->erase(bucket_number);
          rm_buckets.push_back(bucket);
        }

        kdb_buckets_map* old_buckets_map = buckets_map;
        buckets_map = temp_buckets_map;

        usleep(1000);
        for (size_t i=0; i<rm_buckets.size(); ++i) {
          rm_buckets[i]->destory();
          delete rm_buckets[i];
        }

        delete old_buckets_map;
      }

      kdb_bucket* kdb_manager::get_bucket(int bucket_number)
      {
        kdb_bucket* ret = NULL;

        kdb_buckets_map::iterator it = buckets_map->find(bucket_number);
        if (it != buckets_map->end()) {
          ret = it->second;
        }

        return ret;
      }

      void kdb_manager::begin_scan(md_info & info)
      {
        uint32_t bucket_id = info.db_id;

        scan_kdb = get_bucket(bucket_id);

        if (scan_kdb == NULL) {
          log_warn("scan bucket[%d] not exist", bucket_id);
        } else {
          bool bs = scan_kdb->begin_scan();
          if (!bs) {
            log_error("open bucket[%d] for scan failed", bucket_id);
          }
        }
      }

      void kdb_manager::end_scan(md_info & info)
      {
        if (scan_kdb != NULL) {
          scan_kdb->end_scan();
          scan_kdb = NULL;
        }
      }

      bool kdb_manager::get_next_items(md_info & info, vector <item_data_info *>&list)
      {
        bool ret = true;
        if (scan_kdb == NULL)
        {
          ret = false;
          log_error("scan bucket not opened");
        }

        // get more?
        if (ret) {
          while (1) {
            item_data_info* data = NULL;
            int rc = scan_kdb->get_next_item(data);
            if (rc == 0) {
              list.push_back(data);
            } else {
              ret = false;
              break;
            }
            break;
          }
        }

        return ret;
      }

      void kdb_manager::set_area_quota(int area, uint64_t quota)
      {
        // not support in kdb
      }

      void kdb_manager::set_area_quota(std::map<int, uint64_t> &quota_map)
      {
        // not support in kdb
      }

      void kdb_manager::get_stats(tair_stat * stat)
      {
        tbsys::CThreadGuard guard(&lock);
        kdb_buckets_map::iterator it = buckets_map->begin();
        for(; it != buckets_map->end(); ++it) {
          it->second->get_stat(stat);
        }
      }
    }
  }
}
