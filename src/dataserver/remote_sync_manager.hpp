/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * RemoteSyncManager:
 *   Manage synchronization between local and remote cluster.
 *   Data synchronization is based on record logger interface, so different
 *   user should implement its own record logger(reader/writer).
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_DATASERVER_REMOTE_SYNC_MANAGER_H
#define TAIR_DATASERVER_REMOTE_SYNC_MANAGER_H

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)
#include <unordered_map>
#else
#include <tr1/unordered_map>
namespace std {
  using tr1::hash;
  using tr1::unordered_map;
}
#endif

#include <tbsys.h>

#include "common/tair_atomic.hpp"
#include "client/tair_client_api_impl.hpp"

class tair::common::RecordLogger;

namespace tair
{
  class tair_client_impl;
  class tair_manager;
  class ClusterHandler;

  typedef std::unordered_map<std::string, ClusterHandler*> CLUSTER_HANDLER_MAP;

  // cluster conf and client handler
  class ClusterHandler
  {
  public:
    ClusterHandler() : client_(NULL)
    {}
    ~ClusterHandler()
    {
      if (client_ != NULL)
      {
        delete client_;
        client_ = NULL;
      }
    }

    inline bool conf_inited() 
    {
      return !master_cs_addr_.empty();
    }
    inline int init_conf(const std::string& info, int32_t timeout_ms, int32_t queue_limit)
    {
      info_ = info;
      timeout_ms_ = timeout_ms;
      queue_limit_ = queue_limit;
      return decode_info();
    }
    inline int init_conf(std::vector<std::string>& conf)
    {
      int ret = TAIR_RETURN_FAILED;
      if (conf.size() == 5)
      {
        master_cs_addr_ = conf[0];
        slave_cs_addr_ = conf[1];
        group_name_ = conf[2];
        timeout_ms_ = atoi(conf[3].c_str());
        queue_limit_ = atoi(conf[4].c_str());

        encode_info();
        ret = TAIR_RETURN_SUCCESS;
      }
      return ret;
    }
    inline int start()
    {
      if (client_ == NULL)
      {
        client_ = new tair_client_impl();
        client_->set_timeout(timeout_ms_);
        client_->set_queue_limit(queue_limit_);
      }

      return client_->startup(master_cs_addr_.c_str(), slave_cs_addr_.c_str(), group_name_.c_str()) ?
        TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
    }
    inline tair_client_impl* client()
    {
      return client_;
    }
    inline std::string& info()
    {
      return info_;
    }
    inline int32_t get_queue_limit()
    {
      return queue_limit_;        
    }
    inline void encode_info()
    {
      char buf[sizeof(uint64_t)*2];
      tair::util::coding_util::
        encode_fixed64(buf,
                       tbsys::CNetUtil::strToAddr(master_cs_addr_.c_str(), TAIR_CONFIG_SERVER_DEFAULT_PORT));
      tair::util::coding_util::
        encode_fixed64(buf+sizeof(uint64_t),
                       tbsys::CNetUtil::strToAddr(slave_cs_addr_.c_str(), TAIR_CONFIG_SERVER_DEFAULT_PORT));
      info_.clear();
      info_.reserve(sizeof(buf) + group_name_.size());
      info_.append(buf, sizeof(buf));
      info_.append(group_name_);
    }
    inline int decode_info()
    {
      int ret = TAIR_RETURN_FAILED;
      if (info_.size() > sizeof(uint64_t) * 2)
      {
        master_cs_addr_ = tbsys::CNetUtil::addrToString(tair::util::coding_util::decode_fixed64(info_.c_str()));
        slave_cs_addr_ = tbsys::CNetUtil::addrToString(tair::util::coding_util::decode_fixed64(info_.c_str() + sizeof(uint64_t)));
        group_name_ = info_.substr(sizeof(uint64_t)*2);
        ret = TAIR_RETURN_SUCCESS;
      }
      return ret;
    }
    inline std::string debug_string()
    {
      char tmp[32];
      snprintf(tmp, sizeof(tmp), "%d,%d", timeout_ms_, queue_limit_);
      return std::string("[") + master_cs_addr_ + "," + slave_cs_addr_ + "," + group_name_ + "," + tmp + "]";
    }

  private:
    tair_client_impl* client_;
    std::string master_cs_addr_;
    std::string slave_cs_addr_;
    std::string group_name_;
    int32_t timeout_ms_;
    int32_t queue_limit_;
    std::string info_;
  };

  // failed record stuff
  typedef struct FailRecord
  {
    FailRecord() : key_(NULL), scratch_(NULL), scratch_size_(0) {}
    FailRecord(common::data_entry* key, std::string& cluster_info, int code)
      : key_(key), cluster_info_(cluster_info), code_(code), scratch_(NULL), scratch_size_(0) {}
    FailRecord(const char* data, int32_t size) : scratch_(NULL), scratch_size_(0)
    {
      decode(data, size);
    }
    ~FailRecord()
    {
      if (scratch_ != NULL)
      {
        delete [] scratch_;
      }
    }

    void encode()
    {
      if (scratch_ == NULL)
      {
        scratch_size_ = key_->get_size() + cluster_info_.size() + 3 * sizeof(int32_t);
        scratch_ = new char[scratch_size_];
        char* pos = scratch_;
        tair::util::coding_util::encode_fixed32(pos, cluster_info_.size());
        pos += sizeof(int32_t);
        memcpy(pos, cluster_info_.data(), cluster_info_.size());
        pos += cluster_info_.size();
        tair::util::coding_util::encode_fixed32(pos, key_->get_size());
        pos += sizeof(int32_t);
        memcpy(pos, key_->get_data(), key_->get_size());
        pos += key_->get_size();
        tair::util::coding_util::encode_fixed32(pos, code_);
      }
    }
    void decode(const char* data, int32_t size)
    {
      uint32_t per_size = tair::util::coding_util::decode_fixed32(data);
      cluster_info_.assign(data + sizeof(int32_t), per_size);
      data += per_size + sizeof(int32_t);
      per_size = tair::util::coding_util::decode_fixed32(data);
      // a little weird, but outer MUST delete key_
      key_ = new data_entry(data + sizeof(int32_t), per_size, true);
      data += per_size + sizeof(int32_t);
      code_ = tair::util::coding_util::decode_fixed32(data);
    }
    const char* data()
    {
      return scratch_;
    }
    int32_t size()
    {
      return scratch_size_;
    }

    static void entry_to_record(const common::data_entry& entry, FailRecord& record)
    {
      record.decode(entry.get_data(), entry.get_size());
      record.key_->data_meta.cdate = entry.data_meta.cdate;
      record.key_->data_meta.mdate = entry.data_meta.mdate;
      record.key_->set_prefix_size(entry.get_prefix_size());
      record.key_->has_merged = true;
    }
    static void record_to_entry(FailRecord& record, common::data_entry& entry, bool alloc = false)
    {
      record.encode();
      entry.set_data(record.data(), record.size(), alloc);
      entry.set_prefix_size(record.key_->get_prefix_size());
      entry.data_meta.cdate = record.key_->data_meta.cdate;
      entry.data_meta.mdate = record.key_->data_meta.mdate;
    }

    common::data_entry* key_;
    std::string cluster_info_;
    int code_;
    char* scratch_;
    int32_t scratch_size_;
  } FailRecord;

  // avoid dummy data_entry copy, use ref for aync concurrent use.
  typedef struct DataEntryWrapper
  {
  public:
    DataEntryWrapper(data_entry* entry)
      : entry_(entry), ref_(1) {}
    inline data_entry* entry()
    {
      return entry_;
    }
    inline uint32_t ref()
    {
      return tair::common::atomic_inc(&ref_);
    }
    inline uint32_t unref()
    {
      uint32_t ret = tair::common::atomic_dec(&ref_);
      if (ret <= 0)
      {
        delete this;
      }
      return ret;
    }

  private:
    ~DataEntryWrapper()
    {
      if (entry_ != NULL)
      {
        delete entry_;
      }
    }
    common::data_entry* entry_;
    volatile uint32_t ref_;
  } DataEntryWrapper;

  class RemoteSyncManager : public tbsys::CDefaultRunnable
  {
    // remote synchronization callback arg
    typedef struct CallbackArg
    {
      CallbackArg(RecordLogger* fail_logger, TairRemoteSyncType type,
                  DataEntryWrapper* key, ClusterHandler* handler, volatile uint64_t* count)
        : fail_logger_(fail_logger), type_(type), key_(key), handler_(handler), count_(count)
      {
        key_->ref();
        tair::common::atomic_inc(count_);
      }
      ~CallbackArg()
      {
        key_->unref();
        tair::common::atomic_dec(count_);
      }

      RecordLogger* fail_logger_;
      TairRemoteSyncType type_;
      DataEntryWrapper* key_;
      ClusterHandler* handler_;
      volatile uint64_t* count_;
    } CallbackArg;

  public:
    explicit RemoteSyncManager(tair_manager* tair_manager);
    ~RemoteSyncManager();

    int init();
    int add_record(TairRemoteSyncType type, data_entry* key, data_entry* value);
    void run(tbsys::CThread*, void* arg);

    int pause(bool do_pause);
    int set_mtime_care(bool care);

    static void callback(int ret, void* arg);
    static int log_fail_record(RecordLogger* logger, TairRemoteSyncType type, FailRecord& record);

    typedef void (*FilterKeyFunc)(common::data_entry*& key, std::string& cluster_info);
    static void filter_key(common::data_entry*& key, std::string& cluster_info);
    static void filter_fail_record(common::data_entry*& key, std::string& cluster_info);

  private:
    int32_t remote_sync_thread_count();
    int32_t retry_remote_sync_thread_count();

    // check whether current node is the master one of key
    bool is_master_node(int32_t& bucket_num, const data_entry& key);
    // check whether this type record is valid
    bool is_valid_record(TairRemoteSyncType type);
    // check whether this type record need value to do remote synchronization
    bool is_remote_sync_need_value(TairRemoteSyncType type);

    int init_sync_conf();
    int init_sync_client();

    int do_remote_sync(int32_t index, RecordLogger* input_logger, bool retry, FilterKeyFunc key_filter);

    // Process one record.
    // Returning TAIR_RETURN_SUCCESS means this record is out of remote synchronization
    // purpose(maybe synchronization succeed, or invalid record, or out-of-date
    // record of no synchronization need, etc.)
    int do_process_remote_sync_record(TairRemoteSyncType type, int32_t bucket_num,
                                      DataEntryWrapper* key_wrapper, data_entry*& value,
                                      bool force_reget, bool retry, std::string& cluster_info,
                                      std::vector<FailRecord>& fail_records);
    int do_get_from_local_cluster(bool local_storage, int32_t bucket_num, data_entry* key, data_entry*& value);

    // attach some specified information to deliver to remote server.
    void attach_info_to_key(data_entry& key, data_entry* value);

  private:
    tair_manager* tair_manager_;

    // rsync process has paused
    bool paused_;

    // rsync max process count allowed
    uint64_t max_process_count_;
    // rsync processing count
    volatile uint64_t processing_count_;

    // logger of records to be remote synchronized,
    // this logger is specific with storage engine.
    tair::common::RecordLogger* logger_;
    // logger of records failed at first time.(optional)
    tair::common::RecordLogger* retry_logger_;
    // logger of failed records at last.
    tair::common::RecordLogger* fail_logger_;

    // whether care remote modify time when rsync.
    // maybe data are updated both in local and remote clusters in some condition,
    // and we can't determine final status just based on local cluster,
    // so we do care data modify time of remote cluster, and remote cluster
    // will check modify time to reserve latest update when processing rsync request.
    bool mtime_care_;

    // cluster inited flag
    bool cluster_inited_;
    // one local cluster handler
    ClusterHandler local_cluster_handler_;
    // multi remote cluster handler
    CLUSTER_HANDLER_MAP remote_cluster_handlers_;
  };

}

#endif
