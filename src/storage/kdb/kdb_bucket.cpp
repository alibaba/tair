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

#include "kdb_bucket.h"

#include "common/define.hpp"
#include "common/util.hpp"

namespace tair {
  namespace storage {
    namespace kdb {

      kdb_bucket::kdb_bucket()
      {
        cursor = NULL;
        locks = new locker(LOCKER_SIZE);
      }

      kdb_bucket::~kdb_bucket()
      {
        stop();
        if (locks != NULL) {
          delete locks;
          locks = NULL;
        }
      }

      bool kdb_bucket::start(int bucket_number)
      {
        bool ret = true;
        const char* data_dir = TBSYS_CONFIG.getString(TAIRKDB_SECTION, KDB_DATA_DIR, KDB_DEFAULT_DATA_DIR);
        if (data_dir == NULL) {
          log_error("kdb data dir not config, item: %s.%s", TAIRKDB_SECTION, KDB_DATA_DIR);
          ret  = false;
        }

        if (ret) {
          snprintf(filename, PATH_MAX_LENGTH, "%s/tair_kdb_%06d.dat", data_dir, bucket_number);
          uint64_t map_size = TBSYS_CONFIG.getInt(TAIRKDB_SECTION, KDB_MAP_SIZE, KDB_MAP_SIZE_DEFAULT);
          uint64_t bucket_size = TBSYS_CONFIG.getInt(TAIRKDB_SECTION, KDB_BUCKET_SIZE, KDB_BUCKET_SIZE_DEFAULT);
          uint64_t record_align = TBSYS_CONFIG.getInt(TAIRKDB_SECTION, KDB_RECORD_ALIGN, KDB_RECORD_ALIGN_DEFAULT);

          ret = db.tune_map(map_size);
          if (!ret ) {
            print_db_error("set mmap size failed");
          }

          if (ret) {
            ret = db.tune_alignment(record_align);
            if (!ret) {
              print_db_error("set record alignment failed");
            }
          }

          if (ret) {
            ret = db.tune_options(kyotocabinet::HashDB::TLINEAR);
            if (!ret) {
              print_db_error("set option failed");
            }
          }

          if (ret) {
            ret = db.tune_buckets(bucket_size);
            if (!ret) {
              print_db_error("set bucket size failed");
            }
          }

          if (ret) {
            uint32_t mode = kyotocabinet::HashDB::OWRITER | kyotocabinet::HashDB::OCREATE;
            ret = db.open(filename, mode);
            if (!ret) {
              print_db_error("open kdb failed");
            }
          }
        }

        if (ret) {
          stat_mgr.start(bucket_number, data_dir);
          log_info("kdb [%d] opened", bucket_number);
        }

        return ret;
      }

      void kdb_bucket::stop()
      {
        if (!db.close()) {
          print_db_error("close kdb failed");
        }
      }

      int kdb_bucket::put(common::data_entry& key, common::data_entry& value, bool version_care, uint32_t expire_time)
      {
        kdb_item item;

        int cdate = 0;
        int mdate = 0;
        int edate = 0;
        int stat_data_size = 0;

        if(key.data_meta.cdate == 0 || version_care) {
          cdate = time(NULL);
          mdate = cdate;
          if(expire_time > 0)
            edate = mdate + expire_time;
        } else {
          cdate = key.data_meta.cdate;
          mdate = key.data_meta.mdate;
          edate = key.data_meta.edate;
        }

        int rc = TAIR_RETURN_SUCCESS;

        int li = util::string_util::mur_mur_hash(key.get_data(), key.get_size()) % LOCKER_SIZE;
        if(!locks->lock(li, true)) {
          log_error("acquire lock failed");
          return TAIR_RETURN_FAILED;
        }

        size_t val_size = 0;
        char* old_value = db.get(key.get_data(), key.get_size(), &val_size);
        if (old_value != NULL) {
          // key already exist
          item.full_value = old_value;
          item.full_value_size = val_size;
          item.decode();
          cdate = item.meta.cdate; // set back the create time

          if (item.is_expired()) {
            item.meta.version = 0;
          } else if (version_care) {
            // item is not expired & care version, check version
            if (key.data_meta.version != 0
                && key.data_meta.version != item.meta.version) {
              rc = TAIR_RETURN_VERSION_ERROR;
            }
          }
          item.full_value = NULL;
          item.full_value_size = 0;

          if (rc == TAIR_RETURN_SUCCESS) {
            stat_data_size -= val_size + key.get_size();
          }
        }

        if (old_value != NULL) {
          // free the memory ASAP
          delete [] old_value;
        }

        if (rc == TAIR_RETURN_SUCCESS) {
          item.meta.flag = value.data_meta.flag;
          item.meta.cdate = cdate;
          item.meta.mdate = mdate;
          item.meta.edate = edate;
          if (version_care) {
            item.meta.version++;
          } else {
            item.meta.version = key.data_meta.version;
          }

          item.value = value.get_data();
          item.value_size = value.get_size();

          item.encode();
          stat_data_size += item.full_value_size + key.get_size();
          
          int dc = db.set(key.get_data(), key.get_size(), item.full_value, item.full_value_size);
          item.free_full_value(); // free encoded value
            
          //update key's meta info
          key.data_meta.flag = item.meta.flag;
          key.data_meta.cdate = item.meta.cdate;
          key.data_meta.edate = item.meta.edate;
          key.data_meta.mdate = item.meta.mdate;
          key.data_meta.version = item.meta.version;
          key.data_meta.keysize = key.get_size();
          key.data_meta.valsize = item.value_size;
 
          if (dc < 0) {
            print_db_error("update item failed");
            rc = TAIR_RETURN_FAILED;
          }

          if (rc == TAIR_RETURN_SUCCESS) {
            stat_mgr.stat_add(key.area, stat_data_size, stat_data_size);
          }
        }

        locks->unlock(li);

        return rc;
      }

      int kdb_bucket::get(common::data_entry& key, common::data_entry& value)
      {
        int rc = TAIR_RETURN_SUCCESS;

        size_t val_size = 0;
        char* old_value = db.get(key.get_data(), key.get_size(), &val_size);
        if (old_value == NULL) {
          rc = TAIR_RETURN_DATA_NOT_EXIST;
        } else {
          kdb_item item;
          item.full_value = old_value;
          item.full_value_size = val_size;
          item.decode();

          if (item.is_expired()) {
            int remRc = remove(key, false);
            log_debug("remove expire data return: %d", remRc);
            rc = TAIR_RETURN_DATA_EXPIRED;
          } else {
            value.set_data(item.value, item.value_size);
            
            //update meta info
            value.data_meta.flag = item.meta.flag;
            value.data_meta.cdate = item.meta.cdate;
            value.data_meta.edate = item.meta.edate;
            value.data_meta.mdate = item.meta.mdate;
            value.data_meta.version = item.meta.version;
            value.data_meta.valsize = item.value_size;
            value.data_meta.keysize = key.get_size();

            key.data_meta.flag = item.meta.flag;
            key.data_meta.cdate = item.meta.cdate;
            key.data_meta.edate = item.meta.edate;
            key.data_meta.mdate = item.meta.mdate;
            key.data_meta.version = item.meta.version;
            key.data_meta.keysize = key.get_size();
            key.data_meta.valsize = item.value_size;
          }
        }

        if (old_value != NULL) {
          delete [] old_value;
        }

        return rc;
      }

      int kdb_bucket::remove(common::data_entry& key, bool version_care)
      {
        int rc = TAIR_RETURN_SUCCESS;

        int stat_data_size = 0;

        int li = util::string_util::mur_mur_hash(key.get_data(), key.get_size()) % LOCKER_SIZE;
        if(!locks->lock(li, true)) {
          log_error("acquire lock failed");
          return TAIR_RETURN_FAILED;
        }

        if (rc == TAIR_RETURN_SUCCESS) {
          size_t val_size = 0;
          char* old_value = db.get(key.get_data(), key.get_size(), &val_size);
          if (old_value == NULL) {
            rc = TAIR_RETURN_DATA_NOT_EXIST;
          } else {
            kdb_item item;
            item.full_value = old_value;
            item.full_value_size = val_size;
            stat_data_size = val_size + key.get_size();
            item.decode();

            if (version_care && key.data_meta.version != 0
                && key.data_meta.version != item.meta.version) {
              rc = TAIR_RETURN_VERSION_ERROR;
            }

            delete [] old_value;
          }
        }

        if (rc == TAIR_RETURN_SUCCESS) {
          int dc = db.remove(key.get_data(), key.get_size());
          if (dc < 0) {
            print_db_error("remove item failed");
            rc = TAIR_RETURN_FAILED;
          }

          if (rc == TAIR_RETURN_SUCCESS) {
            stat_mgr.stat_sub(key.area, stat_data_size, stat_data_size);
          }
        }

        locks->unlock(li);

        return rc;
      }

      bool kdb_bucket::begin_scan() {
        if (cursor != NULL) {
          delete cursor;
        }

        cursor = db.cursor(); // open the cursor
        bool ret = cursor->jump(); // jump to the first record
        if (ret == false) {
          const kyotocabinet::BasicDB::Error& err = db.error();
          if (err == kyotocabinet::BasicDB::Error::NOREC) {
            ret = true;
          } else {
            print_db_error("begin scan error");
          }
        }
        return ret;
      }

      bool kdb_bucket::end_scan() {
        if (cursor != NULL) {
          delete cursor;
          cursor = NULL;
        }
        return true;
      }

      int kdb_bucket::get_next_item(item_data_info* &data) {
        int ret = 0; // 0: SUCCESS; 1: END; 2: FAILED

        size_t key_size = 0;
        size_t value_size = 0;
        const char* value = NULL;
        char* key = NULL;
        while (1) {
          key = cursor->get(&key_size, &value, &value_size, true);
          if (key == NULL) {
            ret = 2;
            const kyotocabinet::BasicDB::Error& err = db.error();
            if (err == kyotocabinet::BasicDB::Error::NOREC) {
              ret = 1;
            }
          } else {
            kdb_item item;
            item.full_value = (char*)value;
            item.full_value_size = value_size;
            item.decode();
            
            if (item.is_expired()) {
              if (key != NULL) {
                delete[] key;
                key = NULL;
              }
              continue;
            }
            int total_size = ITEM_HEADER_LEN + key_size + item.value_size;
            data = (item_data_info *) malloc(total_size);
            data->header.keysize = key_size;
            data->header.version = item.meta.version;
            data->header.valsize = item.value_size;
            data->header.cdate = item.meta.cdate;
            data->header.mdate = item.meta.mdate;
            data->header.edate = item.meta.edate;

            char* p = data->m_data;
            memcpy(p, key, key_size);
            p += key_size;
            memcpy(p, item.value, item.value_size);
          }
          break;
        }

        if (key != NULL) {
          delete[] key;
        }
        
        return ret;
      }

      void kdb_bucket::print_db_error(const char* prefix) {
        const kyotocabinet::BasicDB::Error& err = db.error();
        log_error("%s %s\n", prefix, err.message());
      }

      void kdb_bucket::destory()
      { 
        if (locks != NULL) {
          delete locks;
          locks = NULL;
        }
        stop();
        stat_mgr.destory();
        if (::remove(filename) == -1) {
          log_error("remove file [%s] failed: %s", filename, strerror(errno));  
        }
      }

      void kdb_bucket::get_stat(tair_stat* stat)
      {
        tair_pstat *pstat = stat_mgr.get_stat();
        for(int i = 0; i < TAIR_MAX_AREA_COUNT; i++) {
          stat[i].data_size_value += pstat[i].data_size();
          stat[i].use_size_value += pstat[i].use_size();
          stat[i].item_count_value += pstat[i].item_count();
        }
      }
    }
  }
}
