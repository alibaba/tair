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

#include "common/log.hpp"
#include "ldb_define.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      void ldb_key_printer(const leveldb::Slice& key, std::string& output)
      {
        // we only care bucket number, area and first byte of key now
        if (key.size() < LDB_KEY_META_SIZE + LDB_KEY_AREA_SIZE + 1)
        {
          log_error("invalid ldb key. igore print");
          output.append("DiRtY");
        }
        else
        {
          char buf[32];
          int32_t skip = 0;
          // bucket number
          skip += snprintf(buf + skip, sizeof(buf) - skip, "%d",
                          LdbKey::decode_bucket_number(key.data() + LDB_EXPIRED_TIME_SIZE));
          // area
          skip += snprintf(buf + skip, sizeof(buf) - skip, "-%d", LdbKey::decode_area(key.data() + LDB_KEY_META_SIZE));
          // first byte of key
          skip += snprintf(buf + skip, sizeof(buf) - skip, "-0x%X", *(key.data() + LDB_KEY_META_SIZE + LDB_KEY_AREA_SIZE));
          output.append(buf);
        }
      }

      bool get_db_stat(leveldb::DB* db, std::string& value, const char* property)
      {
        bool ret = db != NULL && property != NULL;

        if (ret)
        {
          value.clear();

          char name[32];
          snprintf(name, sizeof(name), "leveldb.%s", property);
          std::string stat_value;

          if (!(ret = db->GetProperty(leveldb::Slice(std::string(name)), &stat_value, ldb_key_printer)))
          {
            log_error("get db stats fail");
          }
          else
          {
            value += stat_value;
          }
        }
        return ret;
      }

      bool get_db_stat(leveldb::DB* db, uint64_t& value, const char* property)
      {
        std::string str_value;
        bool ret = false;
        if ((ret = get_db_stat(db, str_value, property)))
        {
          value = atoll(str_value.c_str());
        }

        return ret;
      }

      int32_t get_level_num(leveldb::DB* db)
      {
        uint64_t level = 0;
        get_db_stat(db, level, "levelnums");
        return static_cast<int32_t>(level);
      }

      bool get_level_range(leveldb::DB* db, int32_t level, std::string* smallest, std::string* largest)
      {
        bool ret = false;
        if (db != NULL)
        {
          ret = db->GetLevelRange(level, smallest, largest);
        }
        return ret;
      }

      std::string get_back_path(const char* path)
      {
        if (path == NULL)
        {
          return std::string("");
        }

        char back_path[TAIR_MAX_PATH_LEN + 16];
        char* pos = back_path;
        pos += snprintf(back_path, TAIR_MAX_PATH_LEN, "%s.bak.", path);
        tbsys::CTimeUtil::timeToStr(time(NULL), pos);
        return std::string(back_path);
      }
    }
  }
}
