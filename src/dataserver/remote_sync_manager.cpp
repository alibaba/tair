/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "common/data_entry.hpp"
#include "common/record_logger.hpp"
#include "common/record_logger_util.hpp"

#include "tair_manager.hpp"
#include "remote_sync_manager.hpp"

namespace tair
{
  RemoteSyncManager::RemoteSyncManager(tair_manager* tair_manager)
    : tair_manager_(tair_manager), paused_(false), max_process_count_(0), processing_count_(0),
      logger_(NULL), retry_logger_(NULL), fail_logger_(NULL),
      mtime_care_(false), cluster_inited_(false)
  {
  }

  RemoteSyncManager::~RemoteSyncManager()
  {
    stop();
    wait();

    for (CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.begin();
         it != remote_cluster_handlers_.end(); ++it)
    {
      delete it->second;
    }
    remote_cluster_handlers_.clear();

    if (retry_logger_ != NULL)
    {
      delete retry_logger_;
      retry_logger_ = NULL;
    }
    if (fail_logger_ != NULL)
    {
      delete fail_logger_;
      fail_logger_ = NULL;
    }
  }

  int RemoteSyncManager::init()
  {
    int ret = TAIR_RETURN_SUCCESS;
    std::string rsync_dir = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_RSYNC_DATA_DIR, "./data/rsync");
    std::string tmp_dir = rsync_dir;
    if (!tbsys::CFileUtil::mkdirs(const_cast<char*>(tmp_dir.c_str())))
    {
      log_error("make rsync data directory fail: %s", rsync_dir.c_str());
      ret = TAIR_RETURN_FAILED;      
    }
    else
    {
      if (retry_logger_ != NULL)
      {
        delete retry_logger_;
        retry_logger_ = NULL;
      }
      if (fail_logger_ != NULL)
      {
        delete fail_logger_;
        fail_logger_ = NULL;
      }

      logger_ = tair_manager_->get_storage_manager()->get_remote_sync_logger();

      if (TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_RSYNC_DO_RETRY, 0) > 0)
      {
        retry_logger_ =
          new common::RingBufferRecordLogger((rsync_dir + "/retry_log").c_str(),
                                             TBSYS_CONFIG.getInt(TAIRSERVER_SECTION,
                                                                 TAIR_RSYNC_RETRY_LOG_MEM_SIZE, 100 << 20));
      }
      fail_logger_ =
        new common::SequentialFileRecordLogger((rsync_dir + "/fail_log").c_str(),
                                               TBSYS_CONFIG.getInt(TAIRSERVER_SECTION,
                                                                   TAIR_RSYNC_FAIL_LOG_SIZE, 30 << 20), true/*rotate*/);

      if (NULL == logger_)
      {
        log_error("current storage engine has NO its own remote sync logger");
        ret = TAIR_RETURN_FAILED;
      }
      else if ((ret = logger_->init()) != TAIR_RETURN_SUCCESS)
      {
        log_error("init remote sync logger fail: %d", ret);
      }
      else if (retry_logger_ != NULL && (ret = retry_logger_->init()) != TAIR_RETURN_SUCCESS)
      {
        log_error("init retry logger fail: %d", ret);
      }
      else if ((ret = fail_logger_->init()) != TAIR_RETURN_SUCCESS)
      {
        log_error("init fail logger fail: %d", ret);
      }
      else if ((ret = init_sync_conf()) != TAIR_RETURN_SUCCESS)
      {
        log_error("init remote sync config fail: %d", ret);
      }
      else
      {
        mtime_care_ = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_RSYNC_MTIME_CARE, 0) > 0;
        // every reader do each remote synchronization.
        setThreadCount(remote_sync_thread_count() + retry_remote_sync_thread_count());
        log_warn("remote sync manger start. sync thread count: %d, retry sync thread count: %d, retry: %s, mtime_care: %s, max_process_count: %"PRI64_PREFIX"u",
                 remote_sync_thread_count(), retry_remote_sync_thread_count(),
                 retry_logger_ != NULL ? "yes" : "no", mtime_care_ ? "yes" : "no", max_process_count_);
        start();
      }
    }
    return ret;
  }

  int RemoteSyncManager::add_record(TairRemoteSyncType type, data_entry* key, data_entry* value)
  {
    // now we only use one writer.
    return logger_->add_record(0, type, key, value);
  }

  void RemoteSyncManager::run(tbsys::CThread*, void* arg)
  {
    int32_t index = static_cast<int32_t>(reinterpret_cast<int64_t>(arg));
    int ret;

    // init cluster client until success
    while (!_stop && !cluster_inited_)
    {
      // first one is up to init client
      if (index == 0 && (ret = init_sync_client()) != TAIR_RETURN_SUCCESS)
      {
        log_warn("client init fail, ret: %d", ret);
      }
      sleep(1);
    }

    if (index < remote_sync_thread_count())
    {
      log_warn("start do remote sync, index: %d", index);
      do_remote_sync(index, logger_, (retry_logger_ != NULL), &RemoteSyncManager::filter_key);
    }
    else if (index < remote_sync_thread_count() + retry_remote_sync_thread_count())
    {
      log_warn("start do retry remote sync, index: %d", index);
      do_remote_sync(index - remote_sync_thread_count(), retry_logger_, false, &RemoteSyncManager::filter_fail_record);
    }
    else
    {
      log_error("invalid thread index: %d", index);
    }

    log_info("remote sync %d run over", index);
  }

  int RemoteSyncManager::pause(bool do_pause)
  {
    paused_ = do_pause;
    // resume rsync, logger may need some restart process
    if (!do_pause)
    {
      logger_->restart();
    }
    log_warn("%s remote sync process.", do_pause ? "pause" : "resume");
    return TAIR_RETURN_SUCCESS;
  }

  int RemoteSyncManager::set_mtime_care(bool care)
  {
    mtime_care_ = care;
    log_warn("set rsync mtime care: %s", care ? "yes" : "no");
    return TAIR_RETURN_SUCCESS;    
  }

  int RemoteSyncManager::do_remote_sync(int32_t index, RecordLogger* input_logger, bool retry, FilterKeyFunc key_filter)
  {
    static const int64_t DEFAULT_WAIT_US = 1000000; // 1s
    static const int64_t REST_WAIT_US = 10000;      // 10ms
    static const int64_t SPEED_CONTROL_WAIT_US = 2000; // 2ms

    int ret = TAIR_RETURN_SUCCESS;
    int32_t type = TAIR_REMOTE_SYNC_TYPE_NONE;
    int32_t bucket_num = -1;
    data_entry* key = NULL;
    data_entry* value = NULL;
    bool force_reget = false;
    int64_t need_wait_us = 0;
    // support mutil-remote-clusters, so we need record its cluster_info when failing,
    // and key to be processed may has cluster_info attached
    std::vector<FailRecord> fail_records;
    std::string cluster_info;

    while (!_stop)
    {
      if (need_wait_us > 0)
      {
        log_debug("@@ sleep %ld", need_wait_us);
        usleep(need_wait_us);
        need_wait_us = 0;
      }
      if (paused_)
      {
        need_wait_us = DEFAULT_WAIT_US;
        continue;
      }

      log_debug("@@ process count %"PRI64_PREFIX"u", processing_count_);
      if (processing_count_ > max_process_count_)
      {
        need_wait_us = SPEED_CONTROL_WAIT_US;
        continue;        
      }

      // get one remote sync log record.
      ret = input_logger->get_record(index, type, bucket_num, key, value, force_reget);

      // maybe storage not init yet, wait..
      if (TAIR_RETURN_SERVER_CAN_NOT_WORK == ret)
      {
        need_wait_us = DEFAULT_WAIT_US;
        continue;
      }

      if (ret != TAIR_RETURN_SUCCESS)
      {
        log_error("get one record fail: %d", ret);
        continue;
      }

      // filter key to get what key really is
      key_filter(key, cluster_info);

      // return success but key == NULL indicates no new record now..
      if (NULL == key)
      {
        log_debug("@@ no new record");
        need_wait_us = DEFAULT_WAIT_US;
        continue;
      }

      // has refed 1
      DataEntryWrapper* key_wrapper = new DataEntryWrapper(key);
      log_debug("@@ %s type: %d, %d, fg: %d, key: %s %d %d %u", cluster_info.size() > 16 ? cluster_info.c_str() + 16 : "", type, bucket_num, force_reget, key->get_size() > 6 ? key->get_data() + 6 : "", key->get_size(), key->get_prefix_size(), key->data_meta.mdate);
      // process record to do remote sync
      ret = do_process_remote_sync_record(static_cast<TairRemoteSyncType>(type), bucket_num,
                                          key_wrapper, value, force_reget, retry, cluster_info, fail_records);

      if (ret != TAIR_RETURN_SUCCESS)
      {
        // TAIR_RETURN_SEND_FAILED, async queue may be full,
        // just rest for a while
        if (ret == TAIR_RETURN_SEND_FAILED)
        {
          need_wait_us = REST_WAIT_US;
        }
        for (size_t i = 0; i < fail_records.size(); ++i)
        {
          if (retry)
          {
            ret = log_fail_record(retry_logger_, static_cast<TairRemoteSyncType>(type), fail_records[i]);
          }

          // not retry or log retry logger fail
          if (!retry || ret != TAIR_RETURN_SUCCESS)
          {
            ret = log_fail_record(fail_logger_, static_cast<TairRemoteSyncType>(type), fail_records[i]);
            if (ret != TAIR_RETURN_SUCCESS)
            {
              log_error("add record to fail logger fail: %d", ret);
            }
          }
        }
      }

      // cleanup
      key_wrapper->unref();
      key = NULL;
      if (value != NULL)
      {
        delete value;
        value = NULL;
      }
      fail_records.clear();
      cluster_info.clear();
      bucket_num = -1;
      force_reget = true;
      log_debug("do one sync suc.");
    }

    return ret;
  }

#ifdef DO_REMOTE_SYNC_OP
#undef DO_REMOTE_SYNC_OP
#endif
// brown sugar...
// how come you taste so good...
#define DO_REMOTE_SYNC_OP(type, op_func)                                \
  do {                                                                  \
    fail_records.clear();                                               \
    int last_ret = ret;                                                 \
    RecordLogger* callback_logger = retry ? retry_logger_ : fail_logger_; \
    if (!cluster_info.empty())                                          \
    {                                                                   \
      CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.find(cluster_info); \
      if (it != remote_cluster_handlers_.end())                         \
      {                                                                 \
        log_debug("@@ one cluster");                                    \
        ClusterHandler* handler = it->second;                           \
        RemoteSyncManager::CallbackArg* arg =                           \
          new RemoteSyncManager::CallbackArg(callback_logger, type, key_wrapper, handler, &processing_count_); \
        ret = handler->client()->op_func;                               \
        if (ret != TAIR_RETURN_SUCCESS)                                 \
        {                                                               \
          delete arg;                                                   \
          fail_records.push_back(FailRecord(key, handler->info(), ret)); \
          last_ret = ret;                                               \
        }                                                               \
      }                                                                 \
      else                                                              \
      {                                                                 \
        log_error("invalid cluster info");                              \
      }                                                                 \
    }                                                                   \
    else                                                                \
    {                                                                   \
      for (CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.begin(); \
           it != remote_cluster_handlers_.end(); ++it)                  \
      {                                                                 \
        ClusterHandler* handler = it->second;                           \
        RemoteSyncManager::CallbackArg* arg =                           \
          new RemoteSyncManager::CallbackArg(callback_logger, type, key_wrapper, handler, &processing_count_); \
        ret = handler->client()->op_func;                               \
        if (ret != TAIR_RETURN_SUCCESS)                                 \
        {                                                               \
          delete arg;                                                   \
          fail_records.push_back(FailRecord(key, handler->info(), ret)); \
          last_ret = ret;                                               \
        }                                                               \
      }                                                                 \
    }                                                                   \
    ret = fail_records.empty() ? TAIR_RETURN_SUCCESS : last_ret;        \
  } while (0)

  int RemoteSyncManager::do_process_remote_sync_record(TairRemoteSyncType type, int32_t bucket_num,
                                                       DataEntryWrapper* key_wrapper, data_entry*& value,
                                                       bool force_reget, bool retry, std::string& cluster_info,
                                                       std::vector<FailRecord>& fail_records)
  {
    int ret = TAIR_RETURN_SUCCESS;
    bool is_master = false, need_reget = false;
    data_entry *key = key_wrapper->entry();

    if (!is_valid_record(type))
    {
      log_warn("invalid record type: %d, ignore", type);
      // treat as success
    }
    else
    {
      // force reget or this record need synchronize value but value is null
      if (force_reget || (NULL == value && is_remote_sync_need_value(type)))
      {
        log_debug("@@ fr 1 %d %x %d", force_reget, value, is_remote_sync_need_value(type));
        need_reget = true;
        // need is_master to determines whether record can get from local_storage
        is_master = is_master_node(bucket_num, *key);
      }
      else if (!(is_master = is_master_node(bucket_num, *key)))
      {
        // current node is not master one of this key
        log_debug("@@ fr 2");
        need_reget = true;
      }

      // re-get current data status in local cluster
      if (need_reget)
      {
        log_debug("@@ get local %d", is_master);
        ret = do_get_from_local_cluster(is_master, bucket_num, key, value);
      }

      if (TAIR_RETURN_SUCCESS == ret || TAIR_RETURN_DATA_NOT_EXIST == ret)
      {
        // we need attach some specified info to deliver to remote server.
        attach_info_to_key(*key, value);

        log_debug("@@ v %s %d %d %d %d", (value != NULL && value->get_size() > 2) ? value->get_data()+2 : "n", value != NULL ? value->get_size() : 0, key->has_merged, key->get_prefix_size(), value != NULL ? value->data_meta.flag : -1);
        // do remote synchronization based on type
        switch (type)
        {
        case TAIR_REMOTE_SYNC_TYPE_DELETE:
        {
          // only sync delete type record when data does NOT exist in local cluster.
          if (!need_reget || TAIR_RETURN_DATA_NOT_EXIST == ret)
          {
            log_debug("@@ del op");
            DO_REMOTE_SYNC_OP(TAIR_REMOTE_SYNC_TYPE_DELETE, remove(key->get_area(), *key, &RemoteSyncManager::callback, arg));
          }
          else
          {
            log_debug("sync DELETE but data exist in local cluster");
            ret = TAIR_RETURN_SUCCESS;
          }
          break;
        }
        case TAIR_REMOTE_SYNC_TYPE_PUT:
        {
          // only sync put type record when data really exists in local cluster
          if (TAIR_RETURN_SUCCESS == ret)
          {
            log_debug("@@ put op");
            DO_REMOTE_SYNC_OP(TAIR_REMOTE_SYNC_TYPE_PUT, put(key->get_area(), *key, *value, 0, 0, false,
                                                             &RemoteSyncManager::callback, arg));
          }
          else
          {
            log_debug("sync PUT but data not exist in local cluster");
            ret = TAIR_RETURN_SUCCESS;
          }
          break;
        }
        default:
        {
          // after is_valid_record(), no invalid record type, ignore here.
          log_debug("ignore record type: %d", type);
          ret = TAIR_RETURN_SUCCESS;
        }
        }
      }
      else
      {
        // log failed record
        for (CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.begin();
             it != remote_cluster_handlers_.end(); ++it)
        {
          fail_records.push_back(FailRecord(key, it->second->info(), ret));
        }
      }
    }

    return ret;
  }

  int RemoteSyncManager::do_get_from_local_cluster(bool local_storage, int32_t bucket_num,
                                                   data_entry* key, data_entry*& value)
  {
    int ret = TAIR_RETURN_SUCCESS;
    if (local_storage)
    {
      value = new data_entry();
      // get from local server storage directly
      ret = tair_manager_->get_storage_manager()->get(bucket_num, *key, *value);
      if (ret != TAIR_RETURN_SUCCESS)
      {
        delete value;
        value = NULL;
      }
    }
    else
    {
      // key has merged area,
      // consider hidden data as normal.
      ret = local_cluster_handler_.client()->get_hidden(key->get_area(), *key, value);
    }
    
    // wrap some return code
    if (ret == TAIR_RETURN_HIDDEN)
    {
      ret = TAIR_RETURN_SUCCESS;
    }
    else if (ret == TAIR_RETURN_DATA_EXPIRED)
    {
      ret = TAIR_RETURN_DATA_NOT_EXIST;
    }

    return ret;
  }

  void RemoteSyncManager::attach_info_to_key(data_entry& key, data_entry* value)
  {
    // I have a dream that one day server would only check value's meta info,
    // then key will be free.
    if (value != NULL)
    {
      key.data_meta.cdate = value->data_meta.cdate;
      key.data_meta.edate = value->data_meta.edate;
      key.data_meta.mdate = value->data_meta.mdate;
      key.data_meta.version = value->data_meta.version;
    }
    key.server_flag = TAIR_SERVERFLAG_RSYNC;
    key.data_meta.flag = 0;

    if (mtime_care_)
    {
      // not |=, use = to clear other dummy flag
      key.data_meta.flag = TAIR_CLIENT_DATA_MTIME_CARE;
    }
  }

  int32_t RemoteSyncManager::remote_sync_thread_count()
  {
    // one thread service one remote sync log reader
    return logger_ != NULL ? logger_->get_reader_count() : 0;
  }

  int32_t RemoteSyncManager::retry_remote_sync_thread_count()
  {
    return retry_logger_ != NULL ? retry_logger_->get_reader_count() : 0;
  }

  bool RemoteSyncManager::is_master_node(int32_t& bucket_num, const data_entry& key)
  {
    if (bucket_num < 0)
    {
      bucket_num = tair_manager_->get_bucket_number(key);
    }

    // TODO. we consider all data is due to this node now, all will be ok when emitting do_proxy()
    return tair_manager_->get_table_manager()->is_master(bucket_num, TAIR_SERVERFLAG_CLIENT) ||
      tair_manager_->get_table_manager()->is_master(bucket_num, TAIR_SERVERFLAG_PROXY);
  }

  bool RemoteSyncManager::is_valid_record(TairRemoteSyncType type)
  {
    return (type > TAIR_REMOTE_SYNC_TYPE_NONE || type < TAIR_REMOTE_SYNC_TYPE_MAX);
  }

  bool RemoteSyncManager::is_remote_sync_need_value(TairRemoteSyncType type)
  {
    return (type > TAIR_REMOTE_SYNC_TYPE_WITH_VALUE_START);
  }

  // much resemble json format.
  // one local cluster config and one or multi remote cluster config.
  // {local:[master_cs_addr,slave_cs_addr,group_name,timeout_ms,queue_limit],remote:[...],remote:[...]}
  int RemoteSyncManager::init_sync_conf()
  {
    static const char* LOCAL_CLUSTER_CONF_KEY = "local";
    static const char* REMOTE_CLUSTER_CONF_KEY = "remote";

    int ret = TAIR_RETURN_SUCCESS;
    int32_t min_queue_limit = 0;
    std::string conf_str = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_RSYNC_CONF, "");
    std::vector<std::string> clusters;
    tair::common::string_util::split_str(conf_str.c_str(), "]}", clusters);

    if (clusters.size() < 2)
    {
      ret = TAIR_RETURN_FAILED;
      log_error("invalid clusters count in conf");
    }
    else
    {
      for (size_t i = 0; i < clusters.size(); ++i)
      {
        ret = TAIR_RETURN_FAILED;
        std::vector<std::string> one_conf;
        tair::common::string_util::split_str(clusters[i].c_str(), "{[", one_conf);

        if (one_conf.size() != 2)
        {
          log_error("invalid one cluster key/value conf format: %s", clusters[i].c_str());
          break;
        }

        std::string& conf_key = one_conf[0];
        std::string& conf_value = one_conf[1];
        std::vector<std::string> client_conf;
        tair::common::string_util::split_str(conf_key.c_str(), ",: ", client_conf);
        if (client_conf.size() != 1)
        {
          log_error("invalid cluster key: %s", conf_key.c_str());
          break;
        }
        conf_key = client_conf[0];
        client_conf.clear();
        tair::common::string_util::split_str(conf_value.c_str(), ", ", client_conf);

        if (conf_key == LOCAL_CLUSTER_CONF_KEY)
        {
          if (local_cluster_handler_.conf_inited())
          {
            log_error("multi local cluster config.");
            break;
          }
          if (local_cluster_handler_.init_conf(client_conf) != TAIR_RETURN_SUCCESS)
          {
            log_error("invalid local cluster conf value: %s", conf_value.c_str());
            break;
          }
        }
        else if (conf_key == REMOTE_CLUSTER_CONF_KEY)
        {
          ClusterHandler* handler = new ClusterHandler();
          if (handler->init_conf(client_conf) != TAIR_RETURN_SUCCESS)
          {
            log_error("invalid remote cluster conf value: %s", conf_value.c_str());
            delete handler;
            break;
          }
          else
          {
            remote_cluster_handlers_[handler->info()] = handler;
            if (min_queue_limit <= 0 || handler->get_queue_limit() < min_queue_limit)
            {
              min_queue_limit = handler->get_queue_limit();
            }
          }
        }
        else
        {
          log_error("invalid cluster conf type: %s", conf_key.c_str());
          break;
        }
        ret = TAIR_RETURN_SUCCESS;
      }
    }

    if (ret != TAIR_RETURN_SUCCESS)
    {
      log_error("invalid remote sync conf: %s", conf_str.c_str());
    }
    else if (!local_cluster_handler_.conf_inited())
    {
      log_error("local cluster not inited");
      ret = TAIR_RETURN_FAILED;
    }
    else if (remote_cluster_handlers_.empty())
    {
      log_error("remote cluster not config");
      ret = TAIR_RETURN_FAILED;
    }
    else
    {
      // we consider min_queue_limit's 70%(queue_limit includes channel size besides request queue size now)
      // as max_process_count
      max_process_count_ = static_cast<uint64_t>(min_queue_limit * 0.70);
      std::string debug_str = std::string(LOCAL_CLUSTER_CONF_KEY);
      debug_str.append(":");
      debug_str.append(local_cluster_handler_.debug_string());
      for (CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.begin();
           it != remote_cluster_handlers_.end(); ++it)
      {
        debug_str.append(REMOTE_CLUSTER_CONF_KEY);
        debug_str.append(":");
        debug_str.append(it->second->debug_string());
      }
      log_warn("init remote sync conf: %s", debug_str.c_str());
    }

    return ret;
  }

  // init_sync_conf() should return success
  int RemoteSyncManager::init_sync_client()
  {
    int ret = TAIR_RETURN_SUCCESS;

    if (!cluster_inited_)
    {
      if ((ret = local_cluster_handler_.start()) != TAIR_RETURN_SUCCESS)
      {
        log_error("start local cluster fail: %d, %s", ret, local_cluster_handler_.debug_string().c_str());
      }
      else
      {
        for (CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.begin();
             it != remote_cluster_handlers_.end(); ++it)
        {
          if ((ret = it->second->start()) != TAIR_RETURN_SUCCESS)
          {
            log_error("start remote cluster fail: %d, %s", ret, it->second->debug_string().c_str());
            break;
          }
        }

        if (ret == TAIR_RETURN_SUCCESS)
        {
          log_warn("start all cluster client.");
          cluster_inited_ = true;
        }
      }
    }

    return ret;
  }

  void RemoteSyncManager::callback(int ret, void* arg)
  {
    RemoteSyncManager::CallbackArg* callback_arg = (RemoteSyncManager::CallbackArg*)arg;

    bool failed = false;
    switch (callback_arg->type_)
    {
    case TAIR_REMOTE_SYNC_TYPE_PUT:
      failed = (ret != TAIR_RETURN_SUCCESS && ret != TAIR_RETURN_MTIME_EARLY);
      break;
    case TAIR_REMOTE_SYNC_TYPE_DELETE:
      // not exist or expired also OK
      failed = (ret != TAIR_RETURN_SUCCESS && ret != TAIR_RETURN_DATA_NOT_EXIST &&
                ret != TAIR_RETURN_DATA_EXPIRED && ret != TAIR_RETURN_MTIME_EARLY);
      break;
    default:
      log_error("unknown callback type: %d, ret: %d", callback_arg->type_, ret);
      break;
    }

    log_debug("@@ cb %d", ret);

    if (failed)
    {
      FailRecord record(callback_arg->key_->entry(), callback_arg->handler_->info(), ret);
      ret = RemoteSyncManager::log_fail_record(callback_arg->fail_logger_, callback_arg->type_, record);
      if (ret != TAIR_RETURN_SUCCESS)
      {
        // this is all I can do...
        log_error("log fail record fail: %d", ret);
      }
    }

    delete callback_arg;
  }

  int RemoteSyncManager::log_fail_record(RecordLogger* logger, TairRemoteSyncType type, FailRecord& record)
  {
    data_entry entry;
    FailRecord::record_to_entry(record, entry);
    // only one writer here
    return logger->add_record(0, type, &entry, NULL);
  }

  void RemoteSyncManager::filter_key(data_entry*& key, std::string& cluster_info)
  {
    // key is all I want, do nothing
    UNUSED(key);
    UNUSED(cluster_info);
  }

  void RemoteSyncManager::filter_fail_record(data_entry*& key, std::string& cluster_info)
  {
    if (key != NULL)
    {
      // key data is FailRecord actually
      FailRecord record;
      FailRecord::entry_to_record(*key, record);
      // key is of no need
      delete key;
      cluster_info.assign(record.cluster_info_);
      key = record.key_;
    }
  }

}
