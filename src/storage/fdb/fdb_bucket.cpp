/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * fdb_bucket hold one bucket for fdb storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "fdb_bucket.hpp"

namespace tair {
  namespace storage {
    namespace fdb {
      fdb_bucket::fdb_bucket()
      {
        item_manager = NULL;
        lockers = new locker(FDB_BUCKET_NUMBER);
      }

      fdb_bucket::~fdb_bucket()
      {
        if(item_manager != NULL) {
          delete item_manager;
            item_manager = NULL;
        }
        delete lockers;
          lockers = NULL;
      }

      bool fdb_bucket::start(int bucket_number)
      {
        if(item_manager != NULL) {
          return true;
        }

        item_manager = new fdb_item_manager();
        if(item_manager->initialize(bucket_number) == INIT_FAILED) {
          log_error("init bucket[%d] failed", bucket_number);
          delete item_manager;
          item_manager = NULL;
          return false;
        }
        return true;
      }

      void fdb_bucket::stop()
      {
        if(item_manager != NULL) {
          delete item_manager;
          item_manager = NULL;
        }
      }

      int fdb_bucket::put(data_entry & key, data_entry & value,
                          bool version_care, uint32_t expire_time,
                          bool exclusive)
      {
        fdb_item item;
        memset(&item, 0, sizeof(item));
        item.is_new = false;
        item.meta.hashcode = key.get_hashcode();
        item.bucket_index = item.meta.hashcode % FDB_BUCKET_NUMBER;

        int data_size =
          key.get_size() + value.get_size() + FDB_ITEM_DATA_SIZE;
        int psize = pad_size(data_size);
        int total_size = data_size + psize;
        int cdate = 0;
        int mdate = 0;
        int edate = 0;

        if(key.data_meta.cdate == 0 || version_care) {
          cdate = time(NULL);
          mdate = cdate;
          if(expire_time > 0)
            edate = mdate + expire_time;
        }
        else {
          cdate = key.data_meta.cdate;
          mdate = key.data_meta.mdate;
          edate = key.data_meta.edate;
        }

        PROFILER_BEGIN("acquire lock");
        if(exclusive && !lockers->lock(item.bucket_index, true)) {
          log_debug("acquire lock failed");
          PROFILER_END();
          return TAIR_RETURN_FAILED;
        }
        PROFILER_END();

        int stat_data_size = 0;
        int stat_use_size = 0;

        PROFILER_BEGIN("get item from file");
        int rc = item_manager->get_item(key, item);
        PROFILER_END();
        if(rc == TAIR_RETURN_SUCCESS) {
          log_debug("item exist..., old version: %d, new version: %d",
                    item.data.version, key.data_meta.version);
          if(is_item_expired(item) == false) {
            // check version only if item is not expired
            if(version_care && key.data_meta.version != 0
               && key.data_meta.version != item.data.version) {
              // version error
              log_debug("version error, old version: %d, new version: %d",
                        item.data.version, key.data_meta.version);
              item.free_value();
              if(exclusive)
                lockers->unlock(item.bucket_index);
              return TAIR_RETURN_VERSION_ERROR;
            }
            cdate = item.data.cdate;        // reuse create time if item is not expired
          }
          if((int) item.meta.size < total_size) {
            log_debug("old size is not enough, alloc new item");
            PROFILER_BEGIN("free data");
            item_manager->free_data(item);
            PROFILER_END();
            stat_data_size = data_size - (item.meta.size - item.data.padsize);  // add new data size and sub free data size
            stat_use_size = total_size - item.meta.size;  // add new use size and sub free use size
            item.meta.size = total_size;
            PROFILER_BEGIN("new data");
            item_manager->new_data(item, true);
            PROFILER_END();
          } else {
            stat_data_size = data_size - (item.meta.size - item.data.padsize);  // add new data size and sub free data size
            stat_use_size = 0;  // use_size not change
          }
          item.free_value();
        }
        else {
          item.is_new = true;
          stat_data_size = data_size + FDB_ITEM_META_SIZE;  // add new data size
          stat_use_size = total_size + FDB_ITEM_META_SIZE;  // add new use size
          item.meta.size = total_size;
          PROFILER_BEGIN("new item");
          item_manager->new_item(item);
          PROFILER_END();
          log_debug("this is a new item, meta_offset: %llu",
                    item.meta_offset);
        }

        psize += (item.meta.size - total_size);        // when use freeblock, we need recalc padsize
        log_debug("new padsize: %d, new size: %d, total size: %d", psize,
                  item.meta.size, total_size);

        item.data.keysize = key.get_size();
        item.data.valsize = value.get_size();
        item.data.padsize = psize;
        item.data.flag = value.data_meta.flag;

        item.data.cdate = cdate;
        item.data.mdate = mdate;
        item.data.edate = edate;
        log_debug("setting item time to cdate: %d, mdate: %d, expireTime: %d",
                  cdate, mdate, edate);

        item.key = key.get_data();
        item.value = value.get_data();
        if(version_care)
          item.data.version++;
        else
          item.data.version = key.data_meta.version;

        PROFILER_BEGIN("update item");
        rc = item_manager->update_item(item);
        PROFILER_END();

        log_debug("update item return: %d", rc);

        key.data_meta = item.data;

        if(exclusive)
          lockers->unlock(item.bucket_index);

        if(rc == TAIR_RETURN_SUCCESS) {
          item_manager->stat_add(key.area, stat_data_size,
                                 stat_use_size);
        }

        return rc;
      }

      int fdb_bucket::get(data_entry & key, data_entry & value,
                          bool exclusive)
      {

        fdb_item item;
        item.meta.hashcode = key.get_hashcode();
        item.bucket_index = item.meta.hashcode % FDB_BUCKET_NUMBER;

        PROFILER_BEGIN("acquire lock");
        if(exclusive && !lockers->lock(item.bucket_index)) {
          log_debug("acquire lock failed...");
          PROFILER_END();
          return TAIR_RETURN_FAILED;
        }
        PROFILER_END();

        PROFILER_BEGIN("get items from storage");
        int rc = item_manager->get_item(key, item);
        PROFILER_END();
        if(rc == TAIR_RETURN_SUCCESS) {
          if(is_item_expired(item)) {
            item.free_value();
            PROFILER_BEGIN("remove expired data");
            int remRc = remove(key, false, false);
            PROFILER_END();
            log_debug("remove expire data return: %d", remRc);
            rc = TAIR_RETURN_DATA_EXPIRED;
          }
          else {
            key.data_meta = item.data;
            value.data_meta = item.data;
            log_debug("return value: %s, size: %d", item.value,
                      item.data.valsize);
            key.data_meta.log_self();
            value.set_alloced_data(item.value, item.data.valsize);
          }
        }

        if(exclusive)
          lockers->unlock(item.bucket_index);
        log_debug("get request return: %d", rc);
        return rc;
      }

      int fdb_bucket::remove(data_entry & key, bool version_care,
                             bool exclusive)
      {
        fdb_item item;
        item.meta.hashcode = key.get_hashcode();
        item.bucket_index = item.meta.hashcode % FDB_BUCKET_NUMBER;

        PROFILER_BEGIN("acquire lock");
        if(exclusive && !lockers->lock(item.bucket_index, true)) {
          PROFILER_END();
          return TAIR_RETURN_FAILED;
        }
        PROFILER_END();

        PROFILER_BEGIN("get item from storage");
        int rc = item_manager->get_item(key, item);
        PROFILER_END();
        if(rc == TAIR_RETURN_SUCCESS) {
          log_debug("remove item start...");
          if(version_care && key.data_meta.version != 0
             && key.data_meta.version != item.data.version) {
            log_debug("version error, old: %d, new: %d", item.data.version,
                      key.data_meta.version);
            if(exclusive)
              lockers->unlock(item.bucket_index);
            return TAIR_RETURN_VERSION_ERROR;
          }
          key.data_meta = item.data;
          PROFILER_BEGIN("free item");
          item_manager->free_item(item);
          PROFILER_END();
          item.free_value();

          int stat_data_size = item.meta.size + FDB_ITEM_META_SIZE;
          item_manager->stat_sub(key.area, stat_data_size - item.data.padsize,
                                 stat_data_size);
        }

        if(exclusive)
          lockers->unlock(item.bucket_index);

        return rc;
      }

      int fdb_bucket::pad_size(int data_size)
      {
        int psize = data_size % DATA_PADDING_SIZE;
        if(psize != 0) {
          psize = DATA_PADDING_SIZE - psize;
        }
        return psize;
      }

      bool fdb_bucket::is_item_expired(fdb_item & item)
      {
        time_t timeNow = time(NULL);
        return (item.data.edate != 0 && item.data.edate < (uint32_t) timeNow);
      }

      void fdb_bucket::get_stat(tair_stat * stat)
      {
        tair_pstat *pstat = item_manager->get_stat();
        for(int i = 0; i < TAIR_MAX_AREA_COUNT; i++) {
          stat[i].data_size_value += pstat[i].data_size();
          stat[i].use_size_value += pstat[i].use_size();
          stat[i].item_count_value += pstat[i].item_count();
        }
      }

      void fdb_bucket::backup()
      {
        item_manager->backup();
      }

    }                                /* fdb */
  }                                /* storage */
}                                /* tair */
