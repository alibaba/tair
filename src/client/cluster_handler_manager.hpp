/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_CLIENT_CLUSTER_HANDLER_MANAGER_H
#define TAIR_CLIENT_CLUSTER_HANDLER_MANAGER_H

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
#include "tair_client_api_impl.hpp"

namespace tair
{
  struct cluster_info
  {
    cluster_info() {}
    cluster_info(const std::string& m_cs_addr, const std::string& s_cs_addr, const std::string& g_name)
      : master_cs_addr_(m_cs_addr), slave_cs_addr_(s_cs_addr), group_name_(g_name) {}

    inline bool operator==(const cluster_info& info) const
    {
      // cluster are separated by group_name now.
      return group_name_ == info.group_name_;
    }
    inline std::string debug_string() const
    {
      return std::string("[ ") + master_cs_addr_ + " | " + slave_cs_addr_ + " | " + group_name_ + " ]";
    }

    std::string master_cs_addr_;
    std::string slave_cs_addr_;
    std::string group_name_;
  };

  struct hash_cluster_info
  {
    size_t operator() (const cluster_info& info) const
    {
      // cluster are separated by group_name now.
      return std::hash<std::string>()(info.group_name_);
    }
  };

  class cluster_handler;
  class cluster_info_updater;

  typedef std::vector<cluster_info> CLUSTER_INFO_LIST;
  typedef std::unordered_map<cluster_info, cluster_handler*, hash_cluster_info> CLUSTER_HANDLER_MAP;
  typedef std::vector<cluster_handler*> CLUSTER_HANDLER_LIST;
  typedef std::vector< CLUSTER_HANDLER_MAP::iterator > CLUSTER_HANDLER_MAP_ITER_LIST;
  typedef std::vector<uint64_t> DOWN_SERVER_LIST;
  typedef std::set<int32_t> DOWN_BUCKET_LIST;
  typedef std::unordered_map<int32_t, int32_t> BUCKET_INDEX_MAP;
  typedef tbsys::STR_STR_MAP CONFIG_MAP;

  static const int32_t DEFAULT_CLUSTER_HANDLER_TIMEOUT_MS = 2000;

  // cluster handler
  class cluster_handler
  {
  public:
    cluster_handler(bool force_service = false);
    ~cluster_handler();

    void init(const char* master_cs_addr, const char* slave_cs_addr, const char* group_name);
    void init(const cluster_info& info);
    bool start(int32_t timeout_ms = DEFAULT_CLUSTER_HANDLER_TIMEOUT_MS);

    std::string debug_string();

    inline bool ok(int32_t bucket) const
    {
      return down_buckets_.empty() || (down_buckets_.find(bucket) == down_buckets_.end());
    }
    inline tair_client_impl* get_client() const
    {
      return client_;
    }
    inline const cluster_info& get_cluster_info() const
    {
      return info_;
    }
    inline void set_index(int32_t index)
    {
      index_ = index;
    }
    inline int32_t get_index() const
    {
      return index_;
    }
    inline DOWN_SERVER_LIST& get_down_servers()
    {
      return down_servers_;
    }
    inline DOWN_BUCKET_LIST& get_down_buckets()
    {
      return down_buckets_;
    }
    inline void reset()
    {
      // reuse client_/info_
      index_ = -1;
      down_servers_.clear();
      down_buckets_.clear();
    }

    inline int32_t get_bucket_count()
    {
      return client_->get_bucket_count();
    }
    inline uint32_t get_version()
    {
      return client_->get_config_version();
    }
    inline int retrieve_server_config(bool update_server_table, CONFIG_MAP& config_map, uint32_t& version)
    {
      return client_->retrieve_server_config(update_server_table, config_map, version);
    }
    inline void get_buckets_by_server(uint64_t server_id, DOWN_BUCKET_LIST& buckets)
    {
      client_->get_buckets_by_server(server_id, buckets);
    }

  private:
    // use one tair client implementation tair_client_impl 
    tair_client_impl* client_;
    // cluster information
    cluster_info info_;
    // handler service index
    int32_t index_;
    // tmp down servers
    DOWN_SERVER_LIST down_servers_;
    // tmp down buckets of down servers
    DOWN_BUCKET_LIST down_buckets_;
  };

  // cluster handler manager interface
  class cluster_handler_manager
  {
  public:
    cluster_handler_manager() : timeout_ms_(DEFAULT_CLUSTER_HANDLER_TIMEOUT_MS) {};
    virtual ~cluster_handler_manager() {};

    // update cluster handlers based on new cluster information
    virtual bool update(const CLUSTER_INFO_LIST& cluster_infos) = 0;
    // force update cluster handlers
    virtual bool update() = 0;
    // current servicing cluster handler manager
    virtual void* current() = 0;
    // current servicing cluster handlers count
    virtual int32_t get_handler_count() = 0;
    // pick handler based on key
    virtual cluster_handler* pick_handler(const tair::common::data_entry& key) = 0;
    // pick handler based on cluster info
    virtual cluster_handler* pick_handler(const cluster_info& info) = 0;
    // pick handler based on index and key
    virtual cluster_handler* pick_handler(int32_t index, const tair::common::data_entry& key) = 0;
    // close all cluster handler
    virtual void close() = 0;
    // debug string
    virtual std::string debug_string() = 0;
    // set timeout
    virtual void set_timeout(int32_t timeout_ms) = 0;

  protected:
    int32_t timeout_ms_;
  private:
    cluster_handler_manager(const cluster_handler_manager&) {};
  };

  // cluster handler manager delegate interface
  class cluster_handler_manager_delegate
  {
  public:
    explicit cluster_handler_manager_delegate(cluster_handler_manager* manager)
      : manager_(manager) {}
    virtual ~cluster_handler_manager_delegate() {};

    virtual cluster_handler* pick_handler(const tair::common::data_entry& key) = 0;
    virtual cluster_handler* pick_handler(const cluster_info& info) = 0;
    virtual cluster_handler* pick_handler(int32_t index, const tair::common::data_entry& key) = 0;

    inline cluster_handler_manager* get_delegate()
    {
      return manager_;
    }

  protected:
    cluster_handler_manager_delegate() {};
    cluster_handler_manager_delegate(const cluster_handler_manager_delegate&) {};

  protected:
    cluster_handler_manager* manager_;
  };


  ////////////////////////////////////////////////////////
  // specified cluster handler manger by bucket sharding
  ////////////////////////////////////////////////////////
  class handlers_node
  {
  public:
    handlers_node();
    explicit handlers_node(CLUSTER_HANDLER_MAP* handler_map,
                           CLUSTER_HANDLER_LIST* handlers,
                           BUCKET_INDEX_MAP* extra_bucket_map);
    ~handlers_node();

    void update(const CLUSTER_INFO_LIST& cluster_infos, const handlers_node& diff_handlers_node);
    cluster_handler* pick_handler(const tair::common::data_entry& key);
    cluster_handler* pick_handler(const cluster_info& info);
    cluster_handler* pick_handler(int32_t index, const tair::common::data_entry& key);
    // we reuse cluster handler, so cluster handler should be marked whether it should be cleared later.
    void mark_clear(const handlers_node& diff_handlers_node);
    // clear all. do clear cluster handler based on force flag or out-of-service flag
    void clear(bool force);

    std::string debug_string();

    inline int32_t get_handler_count()
    {
      return handlers_->size();
    }
    inline void ref()
    {
      atomic_inc(&ref_);
    }
    inline void unref()
    {
      atomic_dec(&ref_);
      // we collect handlers_node by list,
      // delete() will occur updating list concurrently,
      // so do nothing here. node will be deleted when do list op(cleanup_using_node_list())
    }
    inline int32_t get_ref()
    {
      return atomic_read(&ref_);
    }
    inline void set_timeout(int32_t timeout_ms)
    {
      if (timeout_ms > 0)
      {
        timeout_ms_ = timeout_ms;
        if (handler_map_ != NULL)
        {
          for (CLUSTER_HANDLER_MAP::iterator it = handler_map_->begin(); it != handler_map_->end(); ++it)
          {
            it->second->get_client()->set_timeout(timeout_ms_);
          }
        }
      }
    }

  private:
    void construct_handler_map(const CLUSTER_INFO_LIST& cluster_infos,
                               const handlers_node& diff_handlers_node);
    void construct_handlers(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers);
    void construct_extra_bucket_map(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers);
    void collect_down_bucket(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers,
                             CLUSTER_HANDLER_MAP_ITER_LIST& has_down_bucket_handlers);
    void shard_down_bucket(const CLUSTER_HANDLER_MAP_ITER_LIST& has_down_bucket_handlers);
    void get_handler_index_of_bucket(int32_t bucket, const cluster_info& exclude, std::vector<int32_t>& indexs);

    int32_t key_to_bucket(const data_entry& key);
    int32_t bucket_to_handler_index(int32_t bucket);

  private:
    handlers_node(const handlers_node&);

  public:
    // Avoid wild cluster_handler crashing when concurrent using and updating,
    // we use reference count to protect cluster_handler from unexpected destructing.
    atomic_t ref_;
    handlers_node* next_;

    // cluster handler timeout
    int32_t timeout_ms_;
    // multi-cluster must have same bucket count
    int32_t bucket_count_;

    // to lookup handler by cluster_info when update
    CLUSTER_HANDLER_MAP* handler_map_;
    // to lookup handler by index of sharding when pick
    CLUSTER_HANDLER_LIST* handlers_;
    // to lookup handler index in handlers_ by bucket when pick
    BUCKET_INDEX_MAP* extra_bucket_map_;
  };

  // shard bucket to clusters
  class bucket_shard_cluster_handler_manager : public cluster_handler_manager
  {
  public:
    friend class bucket_shard_cluster_handler_manager_delegate;
    bucket_shard_cluster_handler_manager(cluster_info_updater* updater);
    virtual ~bucket_shard_cluster_handler_manager();

    void* current()
    {
      return current_;
    }

    bool update(const CLUSTER_INFO_LIST& cluster_infos);
    bool update();
    int32_t get_handler_count();
    cluster_handler* pick_handler(const tair::common::data_entry& key);
    cluster_handler* pick_handler(const cluster_info& info);
    cluster_handler* pick_handler(int32_t index, const tair::common::data_entry& key);
    void close();
    std::string debug_string();
    void set_timeout(int32_t timeout_ms)
    {
      if (timeout_ms > 0)
      {
        timeout_ms_ = timeout_ms;
        if (current_ != NULL)
        {
          current_->set_timeout(timeout_ms_);
        }
      }
    }

  private:
    void add_to_using_node_list(handlers_node* node);
    void cleanup_using_node_list(bool force = false);

    bucket_shard_cluster_handler_manager();
    bucket_shard_cluster_handler_manager(const bucket_shard_cluster_handler_manager&);

  private:
    cluster_info_updater* updater_;
    // mutex for update
    tbsys::CThreadMutex lock_;
    handlers_node* current_;
    // avoid wild handler crash when concurrent using and updating(delete, etc),
    // handlers_node has reference count to be self-responsible to destruction.
    // We insert new using node to list tail and clean up from list head, and
    // make sure to clean up nodes by time sequence, so re-used cluster_handler
    // can be sure to be out of former reference.
    handlers_node* using_head_;
    handlers_node* using_tail_;
  };

  class bucket_shard_cluster_handler_manager_delegate : public cluster_handler_manager_delegate
  {
  public:
    explicit bucket_shard_cluster_handler_manager_delegate(cluster_handler_manager* manager);
    bucket_shard_cluster_handler_manager_delegate(const bucket_shard_cluster_handler_manager_delegate& delegate);

    virtual ~bucket_shard_cluster_handler_manager_delegate();

    virtual cluster_handler* pick_handler(const tair::common::data_entry& key);
    virtual cluster_handler* pick_handler(const cluster_info& info);
    virtual cluster_handler* pick_handler(int32_t index, const tair::common::data_entry& key);

  private:
    cluster_handler* handler_;
    handlers_node* node_;
    uint32_t version_;
  };

  // utility function
  extern int32_t hash_bucket(int32_t bucket);
  extern void parse_config(const CONFIG_MAP& config_map, const char* key, std::string& value);
  extern void parse_config(const CONFIG_MAP& config_map,
                           const char* key, const char* delim, std::vector<std::string>& values);
  extern void split_str(const char* str, const char* delim, std::vector<std::string>& values);
}
#endif
