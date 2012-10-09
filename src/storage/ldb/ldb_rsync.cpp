/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * synchronize one ldb version data of specified buckets to remote cluster.
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include <signal.h>
#include <set>

#include "common/util.hpp"
#include "common/data_entry.hpp"
#include "common/record_logger_util.hpp"

// this is an ugly include now, we just need some struct
#include "dataserver/remote_sync_manager.hpp"

#include <leveldb/env.h>
#include "ldb_define.hpp"
#include "ldb_comparator.hpp"

using namespace tair::storage::ldb;
using tair::data_entry;
using tair::common::RecordLogger;
using tair::common::SequentialFileRecordLogger;
using tair::ClusterHandler;
using tair::FailRecord;

class DataStat
{
  typedef struct Stat
  {
    Stat() : count_(0), suc_count_(0), suc_size_(0), skip_count_(0), fail_count_(0) {}
    void add(Stat& stat)
    {
      count_ += stat.count_;
      suc_count_ += stat.suc_count_;
      suc_size_ += stat.suc_size_;
      skip_count_ += stat.skip_count_;
      fail_count_ += stat.fail_count_;
    }
    int64_t count_;
    int64_t suc_count_;
    int64_t suc_size_;
    // skip count indicates count of data that exists but suffers no expected operation.
    int64_t skip_count_;
    int64_t fail_count_;
  } Stat;

public:
  DataStat() {}
  ~DataStat()
  {
    do_destroy(bucket_stats_);
    do_destroy(area_stats_);
  }

  void dump(int32_t bucket, int32_t area)
  {
    if (bucket >= 0)
    {
      do_dump(bucket_stats_, bucket, "=== one bucket stats ===");
    }
    if (area >= 0)
    {
      do_dump(area_stats_, area, "=== one area stats ===");
    }
  }
  void dump_all()
  {
    do_dump(bucket_stats_, -1, "=== all bucket stats ===");
    do_dump(area_stats_, -1, "=== all area stats ===");
  }
  void update(int32_t bucket, int32_t area, int64_t size, bool skip, bool suc)
  {
    do_update(bucket_stats_, bucket, size, skip, suc);
    do_update(area_stats_, area, size, skip, suc);
  }

private:
  void do_destroy(std::map<int32_t, Stat*>& stats)
  {
    for (std::map<int32_t, Stat*>::iterator it = stats.begin(); it != stats.end(); ++it)
    {
      delete it->second;
    }
    stats.clear();
  }
  void do_dump(std::map<int32_t, Stat*>& stats, int32_t key, const char* desc)
  {
    Stat* stat = NULL;
    fprintf(stderr, "%s\n", desc);
    fprintf(stderr, "%5s%12s%12s%12s%12s%12s\n", "key", "totalCount", "sucCount", "sucSize", "failCount", "skipCount");
    // dump specified key stat
    if (key >= 0)
    {
      std::map<int32_t, Stat*>::iterator it = stats.find(key);
      if (it == stats.end())
      {
        fprintf(stderr, "NONE STATS\n");
      }
      else
      {
        stat = it->second;
        fprintf(stderr, "%5d%12ld%12ld%12ld%12ld%12ld\n", it->first, stat->count_, stat->suc_count_, stat->suc_size_,
                stat->fail_count_, stat->skip_count_);
      }
    }
    else                        // dump all stats
    {
      Stat total_stat;
      for (std::map<int32_t, Stat*>::iterator it = stats.begin(); it != stats.end(); ++it)
      {
        stat = it->second;
        total_stat.add(*stat);
        fprintf(stderr, "%5d%12ld%12ld%12ld%12ld%12ld\n", it->first, stat->count_, stat->suc_count_, stat->suc_size_,
                stat->fail_count_, stat->skip_count_);
      }
      fprintf(stderr, "%5s%12ld%12ld%12ld%12ld%12ld\n", "total", total_stat.count_, total_stat.suc_count_, total_stat.suc_size_,
              total_stat.fail_count_, total_stat.skip_count_);
    }
  }
  void do_update(std::map<int32_t, Stat*>& stats, int key, int64_t size, bool skip, bool suc)
  {
    if (key >= 0)
    {
      std::map<int32_t, Stat*>::iterator it = stats.find(key);
      Stat* stat = NULL;
      if (it == stats.end())
      {
        stat = new Stat();
        stats[key] = stat;
      }
      else
      {
        stat = it->second;
      }
      ++stat->count_;
      if (skip)                 // skip ignore success or failure
      {
        ++stat->skip_count_;
      }
      else if (suc)
      {
        ++stat->suc_count_;
        stat->suc_size_ += size;
      }
      else
      {
        ++stat->fail_count_;
      }
    }
  }

private:
  std::map<int32_t, Stat*> bucket_stats_;
  std::map<int32_t, Stat*> area_stats_;
};

class DataFilter
{
public:
  DataFilter(const char* yes_keys, const char* no_keys)
  {
    init_set(yes_keys, yes_keys_);
    init_set(no_keys, no_keys_);
  }

  void init_set(const char* keys, std::set<int32_t>& key_set)
  {
    if (keys != NULL)
    {
      std::vector<std::string> tmp_keys;
      tair::util::string_util::split_str(keys, ", ", tmp_keys);
      for (size_t i = 0; i < tmp_keys.size(); ++i)
      {
        key_set.insert(atoi(tmp_keys[i].c_str()));
      }
    }
  }

  bool ok(int32_t key)
  {
    // yes_keys is not empty and match specified key, then no_keys will be ignored
    bool empty = false;
    return ((empty = yes_keys_.empty()) || yes_keys_.find(key) != yes_keys_.end()) &&
      (!empty || no_keys_.empty() || no_keys_.find(key) == no_keys_.end());
  }

private:
  std::set<int32_t> yes_keys_;
  std::set<int32_t> no_keys_;
};


// global stop sentinel
static bool g_stop = false;

void sign_handler(int sig)
{
  switch (sig) {
  case SIGTERM:
  case SIGINT:
    log_error("catch sig %d", sig);
    g_stop = true;
    break;
  }
}

int init_cluster_handler(const char* addr, ClusterHandler& handler)
{
  int ret = TAIR_RETURN_SUCCESS;
  std::vector<std::string> confs;
  tair::util::string_util::split_str(addr, ", ", confs);
  confs.push_back("2000");      // timeout
  confs.push_back("2000");      // queuelimit
  if ((ret = handler.init_conf(confs)) == TAIR_RETURN_SUCCESS)
  {
    ret = handler.start();
  }
  return ret;
}

int get_from_local_cluster(ClusterHandler& handler, data_entry& key, data_entry*& value, bool& skip)
{
  int ret = handler.client()->get_hidden(key.get_area(), key, value);
  if (ret == TAIR_RETURN_DATA_NOT_EXIST || ret == TAIR_RETURN_DATA_EXPIRED)
  {
    log_warn("key not exist local");
    skip = true;
    ret = TAIR_RETURN_SUCCESS;
  }
  else if (ret != TAIR_RETURN_SUCCESS && ret != TAIR_RETURN_HIDDEN)
  {
    log_error("get local fail, ret: %d", ret);
  }
  else
  {
    key.data_meta.cdate = value->data_meta.cdate;
    key.data_meta.edate = value->data_meta.edate;
    key.data_meta.mdate = value->data_meta.mdate;
    key.data_meta.version = value->data_meta.version;
    key.data_meta.keysize = value->data_meta.keysize;
    key.data_meta.valsize = value->data_meta.valsize;
    ret = TAIR_RETURN_SUCCESS;
  }

  return ret;
}

int do_rsync(const char* db_path, const char* manifest_file, std::vector<int32_t>& buckets, 
             ClusterHandler* local_handler, ClusterHandler* remote_handler, bool mtime_care,
             DataFilter& filter, DataStat& stat, RecordLogger* fail_logger)
{
  // open db with specified manifest(read only)
  leveldb::DB* db = NULL;
  leveldb::Options open_options;
  open_options.error_if_exists = false; // exist is ok
  open_options.create_if_missing = true; // create if not exist
  open_options.comparator = LdbComparator(NULL); // self-defined comparator
  open_options.env = leveldb::Env::Instance();
  leveldb::Status s = leveldb::DB::Open(open_options, db_path, manifest_file, &db);

  if (!s.ok())
  {
    log_error("open db with mainfest fail: %s", s.ToString().c_str());
    delete open_options.comparator;
    delete open_options.env;
    return TAIR_RETURN_FAILED;
  }

  // get db iterator
  leveldb::ReadOptions scan_options;
  scan_options.verify_checksums = false;
  scan_options.fill_cache = false;
  leveldb::Iterator* db_it = db->NewIterator(scan_options);
  char scan_key[LDB_KEY_META_SIZE];

  bool skip_in_bucket = false;
  bool skip_in_area = false;
  uint32_t start_time = 0;
  int32_t bucket = -1;
  int32_t area = -1;

  LdbKey ldb_key;
  LdbItem ldb_item;
  data_entry* key = NULL;
  data_entry* value = NULL;

  int32_t mtime_care_flag = mtime_care ? TAIR_CLIENT_DATA_MTIME_CARE : 0;
  int ret = TAIR_RETURN_SUCCESS;

  if (db_it == NULL)
  {
    log_error("new db iterator fail.");
    ret = TAIR_RETURN_FAILED;
  }
  else
  {
    for (size_t i = 0; !g_stop && i < buckets.size(); ++i)
    {
      start_time = time(NULL);
      area = -1;
      bucket = buckets[i];

      // seek to bucket
      LdbKey::build_key_meta(scan_key, bucket);

      for (db_it->Seek(leveldb::Slice(scan_key, sizeof(scan_key))); !g_stop && db_it->Valid(); db_it->Next())
      {
        ret = TAIR_RETURN_SUCCESS;
        skip_in_bucket = false;
        skip_in_area = false;

        ldb_key.assign(const_cast<char*>(db_it->key().data()), db_it->key().size());
        ldb_item.assign(const_cast<char*>(db_it->value().data()), db_it->value().size());
        area = LdbKey::decode_area(ldb_key.key());

        // current bucket iterate over
        if (ldb_key.get_bucket_number() != bucket)
        {
          break;
        }

        // skip this data
        if (!filter.ok(area))
        {
          skip_in_bucket = true;
        }
        else
        {
          key = new data_entry(ldb_key.key(), ldb_key.key_size(), false);
          value = NULL;
          key->has_merged = true;
          key->set_prefix_size(ldb_item.prefix_size());

          // re-get from local
          if (local_handler != NULL)
          {
            ret = get_from_local_cluster(*local_handler, *key, value, skip_in_area);
          }
          else
          {
            value = new data_entry(ldb_item.value(), ldb_item.value_size(), false);
            key->data_meta.cdate = value->data_meta.cdate = ldb_item.cdate();
            key->data_meta.edate = value->data_meta.edate = ldb_item.edate();
            key->data_meta.mdate = value->data_meta.mdate = ldb_item.mdate();
            key->data_meta.version = value->data_meta.version = ldb_item.version();
            key->data_meta.keysize = value->data_meta.keysize = key->get_size();
            key->data_meta.valsize = value->data_meta.valsize = ldb_item.value_size();
            value->data_meta.flag = ldb_item.flag();
          }

          if (ret == TAIR_RETURN_SUCCESS)
          {
            log_debug("@@ k:%d %s %d %d %u %u %u.v:%s %d %d %u %u %u", key->get_area(), key->get_size() > 6 ? key->get_data()+6 : "", key->get_size(), key->get_prefix_size(),key->data_meta.cdate,key->data_meta.mdate,key->data_meta.edate, value->get_size() > 4 ? value->get_data()+4 : "", value->get_size(), value->data_meta.flag, value->data_meta.cdate, value->data_meta.mdate, value->data_meta.edate);
            // mtime care / skip cache
            key->data_meta.flag = mtime_care_flag | TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
            key->server_flag = TAIR_SERVERFLAG_RSYNC;
            // sync to remote cluster
            ret = remote_handler->client()->put(key->get_area(), *key, *value, 0, 0, false/* not fill cache */);
            if (ret == TAIR_RETURN_MTIME_EARLY)
            {
              ret = TAIR_RETURN_SUCCESS;
            }
          }

          // log failed key
          if (ret != TAIR_RETURN_SUCCESS)
          {
            log_error("fail one: %d", ret);
            FailRecord record(key, remote_handler->info(), ret);
            data_entry entry;
            FailRecord::record_to_entry(record, entry);
            int tmp_ret = fail_logger->add_record(0, TAIR_REMOTE_SYNC_TYPE_PUT, &entry, NULL);
            if (tmp_ret != TAIR_RETURN_SUCCESS)
            {
              log_error("add fail record fail, ret: %d", tmp_ret);
            }
          }
        }

        // update stat
        stat.update(bucket, skip_in_bucket ? -1 : area, // skip in bucket, then no area to update
                    ldb_key.key_size() + ldb_item.value_size(), (skip_in_bucket || skip_in_area), ret == TAIR_RETURN_SUCCESS);

        // cleanup
        if (key != NULL)
        {
          delete key;
          key = NULL;
        }
        if (value != NULL)
        {
          delete value;
          value = NULL;
        }
      }

      log_warn("sync bucket %d over, cost: %d(s), stat:\n",
               bucket, time(NULL) - start_time);
      // only dump bucket stat
      stat.dump(bucket, -1);
    }
  }

  if (db_it != NULL)
  {
    delete db_it;
  }
  if (db != NULL)
  {
    delete db;
  }
  delete open_options.comparator;
  delete open_options.env;

  return ret;
}

void print_help(const char* name)
{
  fprintf(stderr,
          "synchronize one ldb version data of specified buckets to remote cluster.\n"
          "%s -p dbpath -f manifest_file -r remote_cluster_addr -e faillogger_file -b buckets [-a yes_areas] [-A no_areas] [-l local_cluster_addr] [-n]\n"
          "NOTE:\n"
          "\tcluster_addr like: 10.0.0.1:5198,10.0.0.1:5198,group_1\n"
          "\tbuckets/areas like: 1,2,3\n"
          "\tconfig local cluster address mean that data WILL BE RE-GOT from local cluster\n"
          "\t-n : NOT mtime_care\n", name);
}

int main(int argc, char* argv[])
{
  int ret = TAIR_RETURN_SUCCESS;
  char* db_path = NULL;
  char* local_cluster_addr = NULL;
  char* remote_cluster_addr = NULL;
  char* manifest_file = NULL;
  char* fail_logger_file = NULL;
  char* buckets = NULL;
  char* yes_areas = NULL;
  char* no_areas = NULL;
  bool mtime_care = true;
  int i = 0;

  while ((i = getopt(argc, argv, "p:f:l:r:e:b:a:A:n")) != EOF)
  {
    switch (i)
    {
    case 'p':
      db_path = optarg;
      break;
    case 'f':
      manifest_file = optarg;
      break;
    case 'l':
      local_cluster_addr = optarg;
      break;
    case 'r':
      remote_cluster_addr = optarg;
      break;
    case 'e':
      fail_logger_file = optarg;
      break;
    case 'b':
      buckets = optarg;
      break;
    case 'a':
      yes_areas = optarg;
      break;
    case 'A':
      no_areas = optarg;
      break;
    case 'n':
      mtime_care = false;
      break;
    default:
      print_help(argv[0]);
      return 1;
    }
  }

  if (db_path == NULL || manifest_file == NULL || remote_cluster_addr == NULL || fail_logger_file == NULL || buckets == NULL)
  {
    print_help(argv[0]);
    return 1;
  }

  // init signals
  signal(SIGINT, sign_handler);
  signal(SIGTERM, sign_handler);

  TBSYS_LOGGER.setLogLevel("warn");

  // init local cluster handler(optional)
  ClusterHandler* local_handler = NULL;
  if (local_cluster_addr != NULL)
  {
    local_handler = new ClusterHandler();
    ret = init_cluster_handler(local_cluster_addr, *local_handler);
    if (ret != TAIR_RETURN_SUCCESS)
    {
      log_error("init local client fail, addr: %d, ret: %d", local_cluster_addr, ret);
      delete local_handler;
      return 1;
    }
  }

  // init remote cluster handler(must)
  ClusterHandler* remote_handler = new ClusterHandler();
  ret = init_cluster_handler(remote_cluster_addr, *remote_handler);
  if (ret != TAIR_RETURN_SUCCESS)
  {
    log_error("init remote client fail, addr: %s, ret: %d", remote_cluster_addr, ret);
    delete remote_handler;
    return 1;
  }

  // init buckets
  std::vector<int32_t> bucket_container;
  std::vector<std::string> bucket_strs;
  tair::util::string_util::split_str(buckets, ", ", bucket_strs);
  for (size_t i = 0; i < bucket_strs.size(); ++i)
  {
    bucket_container.push_back(atoi(bucket_strs[i].c_str()));
  }

  // init fail logger
  RecordLogger* fail_logger = new SequentialFileRecordLogger(fail_logger_file, 30<<20/*30M*/, true/*rotate*/);
  if (fail_logger->init() != TAIR_RETURN_SUCCESS)
  {
    log_error("init fail logger fail, ret: %d", ret);
  }
  else
  {
    // init data filter
    DataFilter filter(yes_areas, no_areas);
    // init data stat
    DataStat stat;

    // do data rsync
    uint32_t start_time = time(NULL);
    ret = do_rsync(db_path, manifest_file, bucket_container, local_handler, remote_handler, mtime_care, filter, stat, fail_logger);

    log_warn("rsync data over, stopped: %s, cost: %u(s), stat:", g_stop ? "yes" : "no", time(NULL) - start_time);
    stat.dump_all();
  }

  // cleanup
  delete fail_logger;
  if (local_handler != NULL)
  {
    delete local_handler;
  }
  if (remote_handler != NULL)
  {
    delete remote_handler;
  }

  return ret == TAIR_RETURN_SUCCESS ? 0 : 1;
}
