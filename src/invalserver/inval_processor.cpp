#include "inval_processor.hpp"
namespace tair {
  int RequestProcessor::process(request_invalid *req,
      request_invalid *&post_req) {
    int ret = TAIR_RETURN_SUCCESS;
    std::vector<tair_client_api*> *clients = invalid_loader->get_client_list(req->group_name);
    if (clients != NULL) {
      //~ 'set' to collect keys that failed
      tair_dataentry_set key_set;
      for (size_t i = 0; i < clients->size(); ++i) {
        tair_client_api *client = (*clients)[i];
        //~ single key
        if (req->key_count == 1) {
          if ((ret = client->remove(req->area, *(req->key))) != TAIR_RETURN_SUCCESS) {
            if (ret != TAIR_RETURN_DATA_NOT_EXIST && ret != TAIR_RETURN_DATA_EXPIRED) {
              std::vector<std::string> servers;
              client->get_server_with_key(*(req->key), servers);
              if (servers.size() > 0) {
              log_error("[FATAL ERROR] RemoveFailure: Group %s, DataServer %s.",
                  req->group_name, servers[0].c_str());
              }
              data_entry *key = new data_entry(*req->key);
              if (key_set.insert(key).second == false) {
                delete key;
              }
            }
          }
          //error
          else{
            log_info("client[%d], ok, removed.'");
          }
        } else if (req->key_count > 1) {
          //~ multiple keys
          for (tair_dataentry_set::iterator it = req->key_list->begin();
              it != req->key_list->end(); ++it) {
            if ((ret = client->remove(req->area, **it)) != TAIR_RETURN_SUCCESS) {
              if (ret != TAIR_RETURN_DATA_NOT_EXIST && ret != TAIR_RETURN_DATA_EXPIRED) {
                std::vector<std::string> servers;
                client->get_server_with_key(**it, servers);
                log_error("[FATAL ERROR] RemoveFailure: Group %s, DataServer %s.",
                    req->group_name, servers[0].c_str());
                data_entry *key = new data_entry(**it);
                if (key_set.insert(key).second == false) {
                  delete key;
                }
              }
            }
          }
        } //~ end if
      } //~ end for
      if (!key_set.empty()) {
        post_req = new request_invalid();
        post_req->set_group_name(req->group_name);
        post_req->area = req->area;
        for (tair_dataentry_set::iterator it = key_set.begin();
            it != key_set.end(); ++it) {
          post_req->add_key(*it);
        }
        key_set.clear(); //~ no need to free
      }
      ret = TAIR_RETURN_SUCCESS;
    } else {
      log_error("[FATAL ERROR] cannot find clients, maybe Group %s not added, or invalid_loader still loading.",
          req->group_name);
      ret = TAIR_RETURN_INVAL_CONN_ERROR;
    }
    return ret;
  }

  int RequestProcessor::process(request_hide_by_proxy *req,
      request_hide_by_proxy *&post_req) {
    int ret = TAIR_RETURN_SUCCESS;

    std::vector<tair_client_api*> *clients = invalid_loader->get_client_list(req->group_name);
    if (clients != NULL) {
      //~ 'set' to collect keys that failed
      tair_dataentry_set key_set;

      for (size_t i = 0; i < clients->size(); ++i) {
        tair_client_api *client = clients->at(i);
        //~ single key
        if (req->key_count == 1) {
          if ((ret = client->hide(req->area, *(req->key))) != TAIR_RETURN_SUCCESS) {
            if (ret != TAIR_RETURN_DATA_NOT_EXIST && ret != TAIR_RETURN_DATA_EXPIRED) {
              std::vector<std::string> servers;
              client->get_server_with_key(*(req->key), servers);
              log_error("[FATAL ERROR] HideFailure: Group %s, DataServer %s.",
                  req->group_name, servers[0].c_str());

              data_entry *key = new data_entry(*req->key);
              if (key_set.insert(key).second == false) {
                delete key;
              }
            }
          }
        } else if (req->key_count > 1) {
          //~ multiple keys
          for (tair_dataentry_set::iterator it = (*req->key_list).begin();
              it != (*req->key_list).end(); ++it) {
            if ((ret = client->hide(req->area, **it)) != TAIR_RETURN_SUCCESS) {
              if (ret != TAIR_RETURN_DATA_NOT_EXIST && ret != TAIR_RETURN_DATA_EXPIRED) {
                std::vector<std::string> servers;
                client->get_server_with_key(**it, servers);
                log_error("[FATAL ERROR] HideFailure: Group %s, DataServer %s.",
                    req->group_name, servers[0].c_str());
                data_entry *key = new data_entry(**it);
                if (key_set.insert(key).second == false) {
                  delete key;
                }
              }
            }
          }
        } //~ end if
      } //~ end for

      if (!key_set.empty()) {
        post_req = new request_hide_by_proxy();
        post_req->set_group_name(req->group_name);
        post_req->area = req->area;
        for (tair_dataentry_set::iterator it = key_set.begin();
            it != key_set.end(); ++it) {
          post_req->add_key(*it);
        }
        key_set.clear();
      }
      ret = TAIR_RETURN_SUCCESS;
    } else {
      log_error("[FATAL ERROR] cannot find clients, maybe Group %s not added, or invalid_loader still loading.",
          req->group_name);
      ret = TAIR_RETURN_INVAL_CONN_ERROR;
    }
    return ret;
  }

  int RequestProcessor::process(request_prefix_hides_by_proxy *req,
      request_prefix_hides_by_proxy *&post_req) {
    int ret = TAIR_RETURN_SUCCESS;
    vector<tair_client_api*> *clients = invalid_loader->get_client_list(req->group_name);
    if (clients != NULL) {
      data_entry pkey;
      tair_dataentry_set mkey_set;
      tair_dataentry_set failed_key_set;
      if (req->key != NULL) {
        mkey_set.insert(req->key);
        split_key(req->key, &pkey, NULL);
      } else if (req->key_list != NULL) {
        tair_dataentry_set::iterator itr = req->key_list->begin();
        split_key(*itr, &pkey, NULL);
        while (itr != req->key_list->end()) {
          mkey_set.insert(*itr);
          ++itr;
        }
      }

      for (size_t i = 0; i < clients->size(); ++i) {
        tair_client_api *client = clients->at(i);
        key_code_map_t key_code_map;
        if ((ret = client->hides(req->area, mkey_set, key_code_map)) != TAIR_RETURN_SUCCESS) {
          key_code_map_t::iterator itr = key_code_map.begin();
          while (itr != key_code_map.end()) {
            if (itr->second != TAIR_RETURN_DATA_NOT_EXIST && itr->second != TAIR_RETURN_DATA_EXPIRED) {
              data_entry *mkey = new data_entry();
              merge_key(pkey, *itr->first, *mkey);
              std::vector<std::string> servers;
              client->get_server_with_key(*mkey, servers);
              log_error("[FATAL ERROR] PrefixHidesFailure: Group %s, DataServer %s.",
                  req->group_name, servers[0].c_str());

              if (failed_key_set.insert(mkey).second == false) {
                delete mkey;
              }
            }
            ++itr;
          }
          defree(key_code_map);
        }
      }
      if (!failed_key_set.empty()) {
        post_req = new request_prefix_hides_by_proxy();
        post_req->set_group_name(req->group_name);
        post_req->area = req->area;
        tair_dataentry_set::iterator itr = failed_key_set.begin();
        while (itr != failed_key_set.end()) {
          post_req->add_key(*itr++);
        }
      }
      ret = TAIR_RETURN_SUCCESS;
      mkey_set.clear();
    } else {
      log_error("[FATAL ERROR] cannot find clients, group %s not added, or still loading", req->group_name);
      ret = TAIR_RETURN_INVAL_CONN_ERROR;
    }
    return ret;
  }

  int RequestProcessor::process(request_prefix_invalids *req,
      request_prefix_invalids *&post_req) {
    int ret = TAIR_RETURN_SUCCESS;
    vector<tair_client_api*> *clients = invalid_loader->get_client_list(req->group_name);
    if (clients != NULL) {
      data_entry pkey;
      tair_dataentry_set mkey_set;
      tair_dataentry_set failed_key_set;
      if (req->key != NULL) {
        mkey_set.insert(req->key);
        split_key(req->key, &pkey, NULL);
      } else if (req->key_list != NULL) {
        tair_dataentry_set::iterator itr = req->key_list->begin();
        split_key(*itr, &pkey, NULL);
        while (itr != req->key_list->end()) {
          mkey_set.insert(*itr);
          ++itr;
        }
      }

      for (size_t i = 0; i < clients->size(); ++i) {
        tair_client_api *client = clients->at(i);
        key_code_map_t key_code_map;
        if ((ret = client->removes(req->area, mkey_set, key_code_map)) != TAIR_RETURN_SUCCESS) {
          key_code_map_t::iterator itr = key_code_map.begin();
          while (itr != key_code_map.end()) {
            if (itr->second != TAIR_RETURN_DATA_NOT_EXIST && itr->second != TAIR_RETURN_DATA_EXPIRED) {
              data_entry *mkey = new data_entry();
              merge_key(pkey, *itr->first, *mkey);
              std::vector<std::string> servers;
              client->get_server_with_key(*mkey, servers);
              log_error("[FATAL ERROR] PrefixInvalidsFailure: Group %s, DataServer %s.",
                  req->group_name, servers[0].c_str());
              if (failed_key_set.insert(mkey).second == false) {
                delete mkey;
              }
            }
            ++itr;
          }
          defree(key_code_map);
        }
      }
      if (!failed_key_set.empty()) {
        post_req = new request_prefix_invalids();
        post_req->set_group_name(req->group_name);
        post_req->area = req->area;
        tair_dataentry_set::iterator itr = failed_key_set.begin();
        while (itr != failed_key_set.end()) {
          post_req->add_key(*itr++);
        }
      }
      ret = TAIR_RETURN_SUCCESS;
      mkey_set.clear();
    } else {
      log_error("[FATAL ERROR] cannot find clients, group %s not added, or still loading", req->group_name);
      ret = TAIR_RETURN_INVAL_CONN_ERROR;
    }
    return ret;
  }
  void RequestProcessor::dump_key(const tair_dataentry_set &keyset, const char *msg) {
    if (msg == NULL) {
      msg = "error";
    }
    tair_dataentry_set::const_iterator it = keyset.begin();
    while (it != keyset.end()) {
      dump_key(**it, msg);
      ++it;
    }
  }

  void RequestProcessor::dump_key(const data_entry &key, const char *msg) {
    if (msg == NULL) {
      msg = "error";
    }
    char *d_str = util::string_util::bin2ascii(key.get_data(), key.get_size(), NULL, 0);
    log_error("%s, key: %s", msg, d_str);
    free(d_str);
  }
}
