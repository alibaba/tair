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

#include "cluster_info_updater.hpp"
#include "cluster_handler_manager.hpp"

namespace tair
{
  using namespace tair::common;
  //////////////////////////// cluster_handler ////////////////////////////
  cluster_handler::cluster_handler(bool force_service) : index_(-1)
  {
    client_ = new tair_client_impl();
    client_->set_force_service(force_service);
  }

  cluster_handler::~cluster_handler()
  {
    if (client_ != NULL)
    {
      client_->close();
      delete client_;
    }
  }

  void cluster_handler::init(const char* master_cs_addr, const char* slave_cs_addr, const char* group_name)
  {
    info_.master_cs_addr_.assign(master_cs_addr);
    info_.slave_cs_addr_.assign(NULL == slave_cs_addr ? master_cs_addr : slave_cs_addr);
    info_.group_name_.assign(group_name);
  }

  void cluster_handler::init(const cluster_info& info)
  {
    info_ = info;
  }

  bool cluster_handler::start(int32_t timeout_ms)
  {
    if (client_ != NULL)
    {
      client_->close();
    }
    client_->set_timeout(timeout_ms);
    return client_->startup(info_.master_cs_addr_.c_str(), info_.slave_cs_addr_.c_str(), info_.group_name_.c_str());
  }

  std::string cluster_handler::debug_string()
  {
    char tmp_buf[64];
    std::string result("cluster: ");
    result.append(info_.debug_string());
    snprintf(tmp_buf, sizeof(tmp_buf), ", version: %u, bucket count: %d, index: %d. ",
             get_version(), get_bucket_count(), get_index());
    result.append(tmp_buf);
    snprintf(tmp_buf, sizeof(tmp_buf), "\ndown servers: %zd [ ", down_servers_.size());
    result.append(tmp_buf);
    for (DOWN_SERVER_LIST::const_iterator it = down_servers_.begin(); it != down_servers_.end(); ++it)
    {
      result.append(tbsys::CNetUtil::addrToString(*it));
      result.append(" ");
    }
    snprintf(tmp_buf, sizeof(tmp_buf), "]\ndown buckets: %zd [ ", down_buckets_.size());
    result.append(tmp_buf);
    for (DOWN_BUCKET_LIST::const_iterator it = down_buckets_.begin(); it != down_buckets_.end(); ++it)
    {
      snprintf(tmp_buf, sizeof(tmp_buf), "%d ", *it);
      result.append(tmp_buf);
    }
    result.append("]\n");
    return result;
  }

  //////////////////////////// handlers_node ////////////////////////////
  handlers_node::handlers_node()
    : next_(NULL), timeout_ms_(DEFAULT_CLUSTER_HANDLER_TIMEOUT_MS), bucket_count_(0), sharding_type_(HASH_SHARDING_TYPE),
      handler_map_(NULL), handlers_(NULL), extra_bucket_map_(NULL)
  {
    atomic_set(&ref_, 0);
  }

  handlers_node::handlers_node(CLUSTER_HANDLER_MAP* handler_map, CLUSTER_HANDLER_LIST* handlers,
                               BUCKET_INDEX_MAP* extra_bucket_map, sharding_type type)
    : next_(NULL), timeout_ms_(DEFAULT_CLUSTER_HANDLER_TIMEOUT_MS), bucket_count_(0), sharding_type_(type),
      handler_map_(handler_map), handlers_(handlers), extra_bucket_map_(extra_bucket_map)
  {
    atomic_set(&ref_, 0);
  }

  handlers_node::~handlers_node()
  {
    log_debug("handlers node delete");
    clear(false);
  }

  void handlers_node::update(const CLUSTER_INFO_LIST& cluster_infos, const handlers_node& diff_handlers_node)
  {
    CLUSTER_HANDLER_MAP_ITER_LIST has_down_server_handlers;

    // construct new handler_map
    construct_handler_map(cluster_infos, diff_handlers_node);
    // construct new handlers
    construct_handlers(has_down_server_handlers);
    // construct new extra_bucket_map
    construct_extra_bucket_map(has_down_server_handlers);
    // check sharding type
    if (sharding_type_ == MAP_SHARDING_TYPE)
    {
      map_sharding_bucket(diff_handlers_node);
    }
  }

  cluster_handler* handlers_node::pick_handler(const data_entry& key)
  {
    if (bucket_count_ <= 0)
    {
      return NULL;
    }

    int32_t bucket = key_to_bucket(key);
    int32_t index = bucket_to_handler_index(bucket);
    log_debug("pick: %s => %d => %d => %s", key.get_data(), bucket, index,
              index >= 0 ? (*handlers_)[index]->get_cluster_info().debug_string().c_str() : "none");
    return index >= 0 ? (*handlers_)[index] : NULL;
  }

  cluster_handler* handlers_node::pick_handler(const cluster_info& info)
  {
    // lock should hold `cause handler_map may be updated when mark_clear
    cluster_handler* handler = NULL;
    CLUSTER_HANDLER_MAP::iterator it = handler_map_->find(info);
    if (it != handler_map_->end())
    {
      handler = it->second;
    }
    return handler;
  }

  cluster_handler* handlers_node::pick_handler(int32_t index, const tair::common::data_entry& key)
  {
    if (bucket_count_ <= 0 || index < 0 || index >= (int32_t)handlers_->size())
    {
      return NULL;
    }

    int32_t bucket = key_to_bucket(key);
    return (*handlers_)[index]->ok(bucket) ? (*handlers_)[index] : NULL;
  }

  void handlers_node::clear(bool force)
  {
    // not remove from list here for simplicity
    if (handler_map_ != NULL)
    {
      for (CLUSTER_HANDLER_MAP::iterator it = handler_map_->begin(); it != handler_map_->end(); ++it)
      {
        cluster_handler* handler = it->second;
        if (force || handler->get_index() < 0)
        {
          log_debug("delete cluster handler: %s, service index: %d",
                    handler->get_cluster_info().debug_string().c_str(), handler->get_index());
          delete handler;
        }
      }
      delete handler_map_;
      handler_map_ = NULL;
    }
    if (handlers_ != NULL)
    {
      delete handlers_;
      handlers_ = NULL;
    }
    if (extra_bucket_map_ != NULL)
    {
      delete extra_bucket_map_;
      extra_bucket_map_ = NULL;
    }
  }

  void handlers_node::mark_clear(const handlers_node& diff_handlers_node)
  {
    CLUSTER_HANDLER_MAP* diff_handler_map = diff_handlers_node.handler_map_;
    for (CLUSTER_HANDLER_MAP::iterator it = handler_map_->begin(); it != handler_map_->end();)
    {
      // handler not reused
      if (diff_handler_map->find(it->first) == diff_handler_map->end())
      {
        cluster_handler* handler = it->second;
        // this handler has not been at service
        log_debug("mark cluster out-of-service: %s", handler->get_cluster_info().debug_string().c_str());
        handler->set_index(-1);
        ++it;
      }
      else
      {
        // handler has been reused by diff_handlers_node,
        // erase here to avoid multi referenced.
        handler_map_->erase(it++);
      }
    }
  }

  void handlers_node::construct_handler_map(const CLUSTER_INFO_LIST& cluster_infos,
                                            const handlers_node& diff_handlers_node)
  {
    handler_map_->clear();
    if (cluster_infos.empty())
    {
      return;
    }

    int ret = TAIR_RETURN_SUCCESS;
    cluster_handler* handler = NULL;
    bool new_handler = false;

    for (CLUSTER_INFO_LIST::const_iterator info_it = cluster_infos.begin(); info_it != cluster_infos.end(); ++info_it)
    {
      // already operate
      if (handler_map_->find(*info_it) != handler_map_->end())
      {
        continue;
      }

      CLUSTER_HANDLER_MAP::const_iterator handler_it = diff_handlers_node.handler_map_->find(*info_it);
      if (handler_it != diff_handlers_node.handler_map_->end())
      {
        new_handler = false;
        // reuse handler
        handler = handler_it->second;
      }
      else
      {
        new_handler = true;
        handler = new cluster_handler();
        handler->init(*info_it);

        // be tolerable to one cluster's down
        if (!handler->start(timeout_ms_))
        {
          log_error("start cluster handler fail. info: %s", info_it->debug_string().c_str());
          delete handler;
          continue;
        }
      }

      int32_t new_bucket_count = handler->get_bucket_count();
      // multi-cluster must have same bucket count
      if (bucket_count_ > 0 && bucket_count_ != new_bucket_count)
      {
        log_error("bucket count conflict, ignore this cluster. expect %d but %d, info: %s",
                  bucket_count_, new_bucket_count, info_it->debug_string().c_str());
        // must be a new handler
        assert(new_handler);
        delete handler;
      }

      // update bucket count
      if (bucket_count_ <= 0)
      {
        bucket_count_ = new_bucket_count;
      }

      CONFIG_MAP config_map;
      uint32_t new_version;
      // update server table
      ret = handler->retrieve_server_config(true, config_map, new_version);
      if (ret != TAIR_RETURN_SUCCESS)
      {
        log_error("retrieve cluster config fail. ret: %d, info: %s", ret, info_it->debug_string().c_str());
        // be tolerable to one cluster's down
        if (new_handler)
        {
          delete handler;
        }
        continue;
      }

      // check get cluster status
      std::string status;
      parse_config(config_map, TAIR_GROUP_STATUS, status);
      // cluster status not ON.
      // no status config means OFF.
      if (strcasecmp(status.c_str(), TAIR_GROUP_STATUS_ON) != 0)
      {
        log_info("cluster OFF: %s", info_it->debug_string().c_str());

        if (new_handler)
        {
          delete handler;
        }
        continue;
      }


      handler->reset();
      // this handler will at service.
      (*handler_map_)[*info_it] = handler;

      // check cluster servers
      std::vector<std::string> down_servers;
      parse_config(config_map, TAIR_TMP_DOWN_SERVER, TAIR_CONFIG_VALUE_DELIMITERS, down_servers);

      if (!down_servers.empty())
      {
        DOWN_SERVER_LIST& down_server_ids = handler->get_down_servers();
        for (std::vector<std::string>::const_iterator it = down_servers.begin(); it != down_servers.end(); ++it)
        {
          uint64_t server_id = tbsys::CNetUtil::strToAddr(it->c_str(), 0);
          if (server_id > 0)
          {
            down_server_ids.push_back(server_id);
          }
          else
          {
            log_warn("get invalid down server address: %s", it->c_str());
          }
        }
      }
    }
  }

  void handlers_node::construct_handlers(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers)
  {
    handlers_->clear();
    if (handler_map_->empty())
    {
      return;
    }

    int32_t i = 0;
    for (CLUSTER_HANDLER_MAP::iterator it = handler_map_->begin(); it != handler_map_->end(); ++it)
    {
      cluster_handler* handler = it->second;
      handler->set_index(i++);
      handlers_->push_back(handler);

      if (!handler->get_down_servers().empty())
      {
        // handler_map_ is constructd over here, won't change any more,
        // iterator is safe to use.
        has_down_server_handlers.push_back(it);
      }
    }
  }

  void handlers_node::construct_extra_bucket_map(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers)
  {
    extra_bucket_map_->clear();
    // no down servers, no down buckets, no extra special bucket sharding
    if (has_down_server_handlers.empty())
    {
      return;
    }

    CLUSTER_HANDLER_MAP_ITER_LIST has_down_bucket_handlers;
    // collect all down buckets by down servers
    collect_down_bucket(has_down_server_handlers, has_down_bucket_handlers);
    // sharding all down buckets
    shard_down_bucket(has_down_bucket_handlers);
  }

  void handlers_node::collect_down_bucket(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers,
                                          CLUSTER_HANDLER_MAP_ITER_LIST& has_down_bucket_handlers)
  {
    has_down_bucket_handlers.clear();
    if (has_down_server_handlers.empty())
    {
      return;
    }

    for (CLUSTER_HANDLER_MAP_ITER_LIST::iterator handler_it_it = has_down_server_handlers.begin();
         handler_it_it != has_down_server_handlers.end(); ++handler_it_it)
    {
      cluster_handler* handler = (*handler_it_it)->second;
      DOWN_SERVER_LIST& down_servers = handler->get_down_servers();
      if (down_servers.empty())
      {
        continue;
      }

      DOWN_BUCKET_LIST& down_buckets = handler->get_down_buckets();
      for (DOWN_SERVER_LIST::const_iterator server_it = down_servers.begin(); server_it != down_servers.end(); ++server_it)
      {
        handler->get_buckets_by_server(*server_it, down_buckets);
      }

      if (!down_buckets.empty())
      {
        has_down_bucket_handlers.push_back(*handler_it_it);
      }
    }
  }

  void handlers_node::shard_down_bucket(const CLUSTER_HANDLER_MAP_ITER_LIST& has_down_bucket_handlers)
  {
    extra_bucket_map_->clear();
    if (has_down_bucket_handlers.empty())
    {
      return;
    }

    // one bucket has at most handler_map_.size() shard choice.
    std::vector<int32_t> indexs(handler_map_->size());

    // iterator all down buckets to find handler that can service it.
    // we prefer skiping bucket's orignal cluster handler(exclude) to
    // sorting all down buckets then finding from all cluster handlers.
    for (CLUSTER_HANDLER_MAP_ITER_LIST::const_iterator handler_it_it = has_down_bucket_handlers.begin();
         handler_it_it != has_down_bucket_handlers.end(); ++handler_it_it)
    {
      DOWN_BUCKET_LIST& down_buckets = (*handler_it_it)->second->get_down_buckets();
      if (down_buckets.empty())
      {
        continue;
      }

      for (DOWN_BUCKET_LIST::const_iterator bucket_it = down_buckets.begin(); bucket_it != down_buckets.end(); ++bucket_it)
      {
        // already operate
        if (extra_bucket_map_->find(*bucket_it) != extra_bucket_map_->end())
        {
          continue;
        }

        get_handler_index_of_bucket(*bucket_it, (*handler_it_it)->first, indexs);

        (*extra_bucket_map_)[*bucket_it] = indexs.empty() ? -1 : // this bucket has no handler service
          indexs[hash_bucket(*bucket_it) % indexs.size()]; // hash sharding to one handler
      }
    }
  }

  void handlers_node::map_sharding_bucket(const handlers_node& diff_handlers_node)
  {
    if (handlers_->empty())
    {
      return;
    }

    // last buckets sharding map, we want to do least resharding when update
    BUCKET_INDEX_MAP* last_bucket_map = diff_handlers_node.extra_bucket_map_;
    // total cluster count now
    int32_t total = handlers_->size();

    // live buckets, we don't care dead bucket
    std::vector<int32_t> buckets;
    // buckets sharded to each cluster
    std::vector<int32_t>* sharding_buckets = new std::vector<int32_t>[total];
    // buckets that need resharding
    std::vector<int32_t>* reshard_buckets = NULL;

    get_live_buckets(buckets);

    if (last_bucket_map->empty())
    {
      reshard_buckets = new std::vector<int32_t>(buckets);
    }
    else
    {
      reshard_buckets = new std::vector<int32_t>();
      for (std::vector<int32_t>::const_iterator bucket_it = buckets.begin(); bucket_it != buckets.end(); ++bucket_it)
      {
        BUCKET_INDEX_MAP::const_iterator it = last_bucket_map->find(*bucket_it);
        if (it != last_bucket_map->end())
        {
          // last sharding is not invalid now
          if (it->second >= total || it->second < 0)
          {
            reshard_buckets->push_back(*bucket_it);
          }
          else
          {
            // TODO: need check cluster_info actually
            sharding_buckets[it->second].push_back(*bucket_it);
          }
        }
        else
        {
          reshard_buckets->push_back(*bucket_it);
        }
      }
    }

    int32_t average_count = buckets.size() / total, remainder = buckets.size() % total, diff_count = 0;

    for (int i = 0; i < total; ++i)
    {
      std::vector<int32_t>* tmp_buckets = &sharding_buckets[i];
      std::vector<int32_t>::iterator bucket_it = tmp_buckets->end();

      if (static_cast<int32_t>(tmp_buckets->size()) > average_count)
      {
        diff_count = tmp_buckets->size() - average_count;
        // we prefer reserving old bucket index
        if (remainder > 0)
        {
          --diff_count;
          --remainder;
        }
        for (int c = 0; c < diff_count; ++c)
        {
          --bucket_it;
          reshard_buckets->push_back(*bucket_it);
        }
        tmp_buckets->erase(bucket_it, tmp_buckets->end());
      }
      else if (static_cast<int32_t>(tmp_buckets->size()) < average_count)
      {
        diff_count = average_count - tmp_buckets->size();
        std::vector<int32_t>::iterator reshard_it = reshard_buckets->end();
        for (int c = 0; c < diff_count; ++c)
        {
          --reshard_it;
          tmp_buckets->push_back(*reshard_it);
        }
        reshard_buckets->erase(reshard_it, reshard_buckets->end());
      }
    }

    if (remainder > 0)
    {
      std::vector<int32_t>::iterator reshard_it = reshard_buckets->end();
      for (int i = 0; i < total && remainder > 0; ++i, --remainder)
      {
        if (static_cast<int32_t>(sharding_buckets[i].size()) <= average_count)
        {
          --reshard_it;
          sharding_buckets[i].push_back(*reshard_it);
        }
      }
      assert(reshard_buckets->begin() == reshard_it);
    }

    for (int32_t index = 0; index < total; ++index)
    {
      for (std::vector<int32_t>::const_iterator it = sharding_buckets[index].begin();
           it != sharding_buckets[index].end(); ++it)
      {
        (*extra_bucket_map_)[*it] = index;
      }
    }

    delete [] sharding_buckets;
    delete reshard_buckets;
  }

  void handlers_node::get_live_buckets(std::vector<int32_t>& buckets)
  {
    for (int32_t i = 0; i < bucket_count_; ++i)
    {
      // dead buckets are already sharded, we ignore them now..
      if (extra_bucket_map_->empty() || extra_bucket_map_->find(i) == extra_bucket_map_->end())
      {
        buckets.push_back(i);
      }
    }
  }

  void handlers_node::get_handler_index_of_bucket(int32_t bucket, const cluster_info& exclude, std::vector<int32_t>& indexs)
  {
    indexs.clear();
    for (CLUSTER_HANDLER_MAP::const_iterator handler_it = handler_map_->begin();
         handler_it != handler_map_->end(); ++handler_it)
    {
      // bucket is from exclude, no need check, skip directly
      if (handler_it->first == exclude)
      {
        continue;
      }

      DOWN_BUCKET_LIST& down_buckets = handler_it->second->get_down_buckets();
      // bucket is not down in this cluster handler
      if (down_buckets.empty() || down_buckets.find(bucket) == down_buckets.end())
      {
        indexs.push_back(handler_it->second->get_index());
      }
    }
  }

  std::string handlers_node::debug_string()
  {
    if (handlers_->empty())
    {
      return std::string("[ NO alive cluster servicing ]\n");
    }

    std::vector<std::string> shard_buckets(handlers_->size());
    std::vector<int32_t> shard_bucket_counts(shard_buckets.size(), 0);

    std::string dead_buckets;
    int32_t dead_bucket_count = 0;

    int32_t index = 0;
    char tmp_buf[64];

    for (int32_t i = 0; i < bucket_count_; ++i)
    {
      index = bucket_to_handler_index(i);
      snprintf(tmp_buf, sizeof(tmp_buf), "%d ", i);
      if (index < 0)
      {
        dead_buckets.append(tmp_buf);
        ++dead_bucket_count;
      }
      else
      {
        shard_buckets[index].append(tmp_buf);
        shard_bucket_counts[index]++;
      }
    }

    std::string result;
    snprintf(tmp_buf, sizeof(tmp_buf), "[ buckets: %d, clusters on service: %zd ]\n", bucket_count_, handlers_->size());
    result.append(tmp_buf);
    snprintf(tmp_buf, sizeof(tmp_buf), "{\ndead buckets: %d [ ", dead_bucket_count);
    result.append(tmp_buf);
    result.append(dead_buckets);
    result.append("]\n}\n");
    for (size_t i = 0; i < handlers_->size(); ++i)
    {
      result.append("{\n");
      result.append((*handlers_)[i]->debug_string());
      snprintf(tmp_buf, sizeof(tmp_buf), "sharded buckets: %d [ ", shard_bucket_counts[i]);
      result.append(tmp_buf);
      result.append(shard_buckets[(*handlers_)[i]->get_index()]);
      result.append("]\n}\n");
    }
    return result;
  }

  int32_t handlers_node::key_to_bucket(const data_entry& key)
  {
    return util::string_util::mur_mur_hash(key.get_data(), key.get_size()) % bucket_count_;
  }

  int32_t handlers_node::bucket_to_handler_index(int32_t bucket)
  {
    // no alive cluster
    if (handlers_->empty())
    {
      return -1;
    }

    // check whether this bucket is in special condition
    // (eg. one cluster's ds is up after down, but before reset)
    if (!extra_bucket_map_->empty())
    {
      BUCKET_INDEX_MAP::const_iterator it = extra_bucket_map_->find(bucket);
      if (it != extra_bucket_map_->end())
      {
        return it->second;
      }
    }

    // hash sharding bucket to cluster
    return hash_bucket(bucket) % handlers_->size();
  }


  //////////////////////////// bucket_shard_cluster_handler_manager ////////////////////////////
  bucket_shard_cluster_handler_manager::bucket_shard_cluster_handler_manager(cluster_info_updater* updater, sharding_type type)
    : updater_(updater)
  {
    current_ = new handlers_node(new CLUSTER_HANDLER_MAP(), new CLUSTER_HANDLER_LIST(), new BUCKET_INDEX_MAP(), type);
    current_->ref();
    using_tail_ = using_head_ = NULL;
  }

  bucket_shard_cluster_handler_manager::~bucket_shard_cluster_handler_manager()
  {
    // clear all cluster handler
    current_->clear(true);
    delete current_;

    // force clear all node
    cleanup_using_node_list(true);
  }

  bool bucket_shard_cluster_handler_manager::update(const CLUSTER_INFO_LIST& cluster_infos)
  {
    // mutex update
    tbsys::CThreadGuard guard(&lock_);
    bool urgent = false;
    handlers_node* new_handlers_node =
      new handlers_node(new CLUSTER_HANDLER_MAP(),
                        new CLUSTER_HANDLER_LIST(),
                        new BUCKET_INDEX_MAP(),
                        current_->get_sharding_type());
    new_handlers_node->set_timeout(timeout_ms_);
    new_handlers_node->update(cluster_infos, *current_);

    if (new_handlers_node->handler_map_->empty())
    {
      log_warn("NO alive cluster on service.");
      urgent = true;
    }

    handlers_node* old_handlers_node = current_;
    // ref
    new_handlers_node->ref();
    // update
    current_ = new_handlers_node;

    // length of debug_string() is over tblog's buffer.
    if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
    {
      ::fprintf(stderr, "current cluster handler manager status:\n%s", debug_string().c_str());
      ::fflush(stderr);
    }

    // mark clear out-of-service cluster handler
    old_handlers_node->mark_clear(*current_);
    // unref now
    old_handlers_node->unref();
    // add to using node list to cleanup later
    add_to_using_node_list(old_handlers_node);
    // cleanup using node list, clear trash node
    cleanup_using_node_list();

    return urgent;
  }

  bool bucket_shard_cluster_handler_manager::update()
  {
    updater_->signal_update();
    return true;
  }

  int32_t bucket_shard_cluster_handler_manager::get_handler_count()
  {
    return current_->get_handler_count();
  }

  cluster_handler* bucket_shard_cluster_handler_manager::pick_handler(const data_entry& key)
  {
    return current_->pick_handler(key);
  }

  cluster_handler* bucket_shard_cluster_handler_manager::pick_handler(const cluster_info& info)
  {
    tbsys::CThreadGuard guard(&lock_);
    return current_->pick_handler(info);      
  }

  cluster_handler* bucket_shard_cluster_handler_manager::pick_handler(int32_t index, const tair::common::data_entry& key)
  {
    return current_->pick_handler(index, key);
  }

  void bucket_shard_cluster_handler_manager::close()
  {

  }

  std::string bucket_shard_cluster_handler_manager::debug_string()
  {
    char tmp_buf[32];
    std::string result;
    handlers_node* node = current_;
    node->ref();
    snprintf(tmp_buf, sizeof(tmp_buf), "[ ref: %d ]\n", current_->get_ref());
    result.append(updater_->debug_string());
    result.append("\n");
    result.append(tmp_buf);
    result.append(node->debug_string());
    node->unref();
    return result;
  }

  // lock_ hold
  void bucket_shard_cluster_handler_manager::add_to_using_node_list(handlers_node* node)
  {
    if (NULL == node)
    {
      return;
    }

    if (using_tail_ == NULL)
    {
      using_tail_ = using_head_ = node;
    }
    else
    {
      using_tail_->next_ = node;
      using_tail_ = node;
    }
  }

  // lock hold
  void bucket_shard_cluster_handler_manager::cleanup_using_node_list(bool force)
  {
    if (using_head_ != NULL)
    {
      handlers_node* next_node = NULL;
      handlers_node* node = using_head_;
      while (node != NULL)
      {
        next_node = node->next_;
        if (force || node->get_ref() <= 0)
        {
          delete node;
          node = next_node;
        }
        else
        {
          // make sure nodes are cleared by time sequence,
          // so re-used cluster_handler can be sure to be out of former reference.
          break;
        }
      }

      using_head_ = node;
      if (NULL == using_head_)  // clear up all node
      {
        using_tail_ = NULL;
      }
    }
  }

  //////////////////////////// cluster_handler_manager_delegate ////////////////////////////
  bucket_shard_cluster_handler_manager_delegate::bucket_shard_cluster_handler_manager_delegate(cluster_handler_manager* manager)
  {
    manager_ = manager;
    handler_ = NULL;
    node_ = static_cast<handlers_node*>(manager_->current());
    node_->ref();
    version_ = 0;
  }

  bucket_shard_cluster_handler_manager_delegate::bucket_shard_cluster_handler_manager_delegate(
    const bucket_shard_cluster_handler_manager_delegate& delegate)
  {
    manager_ = delegate.manager_;
    handler_ = NULL;
    // this is what we really want
    node_ = delegate.node_;
    node_->ref();
    version_ = 0;
  }

  bucket_shard_cluster_handler_manager_delegate::~bucket_shard_cluster_handler_manager_delegate()
  {
    if (handler_ != NULL && handler_->get_version() != version_)
    {
      // one cluster handler version changes then clusters' version MUST be changed.
      manager_->update();
    }
    node_->unref();
  }

  cluster_handler* bucket_shard_cluster_handler_manager_delegate::pick_handler(const data_entry& key)
  {
    handler_ = node_->pick_handler(key);
    if (handler_ != NULL)
    {
      version_ = handler_->get_version();
    }
    return handler_;
  }

  cluster_handler* bucket_shard_cluster_handler_manager_delegate::pick_handler(const cluster_info& info)
  {
    tbsys::CThreadGuard guard(&(dynamic_cast<bucket_shard_cluster_handler_manager*>(manager_)->lock_));
    handler_ = node_->pick_handler(info);
    if (handler_ != NULL)
    {
      version_ = handler_->get_version();
    }
    return handler_;
  }

  cluster_handler* bucket_shard_cluster_handler_manager_delegate::pick_handler(int32_t index, const tair::common::data_entry& key)
  {
    handler_ = node_->pick_handler(index, key);
    if (handler_ != NULL)
    {
      version_ = handler_->get_version();
    }
    return handler_;
  }

  //////////////////////////// utility function ////////////////////////////

  // use integer hash to optimize uniform condition compared to silly mod sharding
  int32_t hash_bucket(int32_t h)
  {
    // Spread bits to regularize both segment and index locations,
    // using variant of single-word Wang/Jenkins hash.
    h += (h << 15) ^ 0xffffcd7d;
    h ^= (h >> 10);
    h += (h << 3);
    h ^= (h >> 6);
    h += (h << 2) + (h << 14);
    return h ^ (h >> 16);
    // return h;
  }

  void parse_config(const CONFIG_MAP& config_map, const char* key, std::string& value)
  {
    CONFIG_MAP::const_iterator it = config_map.find(key);
    if (it == config_map.end())
    {
      return;
    }
    value.assign(it->second);
  }

  void parse_config(const CONFIG_MAP& config_map,
                    const char* key, const char* delim, std::vector<std::string>& values)
  {
    CONFIG_MAP::const_iterator it = config_map.find(key);
    if (it == config_map.end())
    {
      return;
    }
    else
    {
      split_str(it->second.c_str(), delim, values);
    }
  }

  void split_str(const char* str, const char* delim, std::vector<std::string>& values)
  {
    if (NULL == str)
    {
      return;
    }
    if (NULL == delim)
    {
      values.push_back(str);
    }
    else
    {
      const char* pos = str;
      size_t len = 0;
      while ((len = strcspn(pos, delim)) > 0)
      {
        values.push_back(std::string(pos, len));
        pos += len;
        pos += strspn(pos, delim);
      }
    }
  }

}
