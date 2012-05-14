/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * request dispatcher impl
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "request_processor.hpp"

namespace tair {
   request_processor::request_processor(tair_manager *tair_mgr, heartbeat_thread *heart_beat, tbnet::ConnectionManager *connection_mgr)
   {
      this->tair_mgr = tair_mgr;
      this->heart_beat = heart_beat;
      this->connection_mgr = connection_mgr;
   }

   request_processor::~request_processor()
   {
   }

   int request_processor::process(request_put *request, bool &send_return)
   {
      int rc = 0;

      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      if (request->key.get_size() == 0 || request->data.get_size() == 0) {
         log_debug("put value is null, return failed");
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      uint64_t target_server_id = 0;
      if (tair_mgr->should_proxy(request->key, target_server_id))
      {
         base_packet *proxy_packet = new request_put(*(request_put*)request);
         rc = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
         return rc;
      }

      request->key.server_flag = request->server_flag;
      request->key.data_meta.version = request->version;
      log_debug("receive put request, serverFlag: %d", request->key.server_flag);
      PROFILER_START("put operation start");
      plugin::plugins_root* plugin_root = NULL;
      PROFILER_BEGIN("do request plugin");
      int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                    TAIR_REQ_PUT_PACKET, request->area, &(request->key), &(request->data), plugin_root);
      PROFILER_END();
      send_return = true;
      if (plugin_ret < 0) {
         log_debug("plugin return %d, skip excute", plugin_ret);
         rc = TAIR_RETURN_PLUGIN_ERROR;
      }
      else {
         PROFILER_BEGIN("do put");
         rc = tair_mgr->put(request->area, request->key, request->data, request->expired, request, heart_beat->get_client_version());
         PROFILER_END();

         PROFILER_BEGIN("do response plugin");
         tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                       TAIR_REQ_PUT_PACKET, request->area, &(request->key), &(request->data), plugin_root);
         PROFILER_END();
      }
      log_debug("put request return: %d", rc);
      PROFILER_DUMP();
      PROFILER_STOP();
      return rc;
   }

   int request_processor::process(request_get *request, bool &send_return)
   {
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      set<data_entry*, data_entry_comparator>::iterator it;
      data_entry *data = NULL;
      response_get *resp = new response_get();

      uint64_t target_server_id = 0;
      int rc = TAIR_RETURN_FAILED;

      if (request->key_list != NULL) {
         uint32_t count = 0;
         PROFILER_START("batch get operation start");
         for (it = request->key_list->begin(); it != request->key_list->end(); ++it) {
            PROFILER_BEGIN("sub get operation in batch begin");
            if (data == NULL) {
               data = new data_entry();
            }
            data_entry *key = (*it);

            if (tair_mgr->should_proxy(*key, target_server_id))
            {
               resp->add_proxyed_key(key);
               continue;
            }

            plugin::plugins_root* plugin_root = NULL;
            PROFILER_BEGIN("do request plugin");
            int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                          TAIR_REQ_GET_PACKET, request->area, key, NULL, plugin_root);
            PROFILER_END();
            if (plugin_ret < 0) {
               log_debug("plugin return %d, skip excute", plugin_ret);
               continue;
            }
            PROFILER_BEGIN("do get");
            int rev = tair_mgr->get(request->area, *key, *data);
            PROFILER_END();

            PROFILER_BEGIN("do response plugin");
            tair_mgr->plugins_manager.do_response_plugins(rev, plugin::PLUGIN_TYPE_SYSTEM,
                                                          TAIR_REQ_GET_PACKET, request->area, key, data, plugin_root);
            PROFILER_END();
            if (rev != TAIR_RETURN_SUCCESS) {
               continue;
            }
            ++count;
            resp->add_key_data(new data_entry(*key), data);
            data = NULL;
            PROFILER_END();
         }
         if (data != NULL) {
            delete data;
            data = NULL;
         }
         if(count == request->key_count){
            rc = TAIR_RETURN_SUCCESS;
         }else if(count > 0){
            rc = TAIR_RETURN_PARTIAL_SUCCESS;
         }else {
            rc = TAIR_RETURN_FAILED;
         }
      } else if (request->key != NULL) {
         PROFILER_START("get operation start");

         if (tair_mgr->should_proxy(*request->key, target_server_id))
         {
            base_packet *proxy_packet = new request_get(*(request_get*)request);
            rc = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
            delete resp; resp = 0;
            return rc;
         }

         plugin::plugins_root* plugin_root = NULL;
         PROFILER_BEGIN("do request plugin");
         int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                       TAIR_REQ_GET_PACKET,request->area, (request->key), NULL, plugin_root);
         PROFILER_END();

         if (plugin_ret < 0) {
            log_debug("plugin return %d, skip excute", plugin_ret);
            rc = TAIR_RETURN_PLUGIN_ERROR;
         }
         else {
            data = new data_entry();
            PROFILER_BEGIN("do get");
            rc = tair_mgr->get(request->area, *(request->key), *data);
            PROFILER_END();

            PROFILER_BEGIN("do response plugin");
            tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                          TAIR_REQ_GET_PACKET,request->area, (request->key), data, plugin_root);
            PROFILER_END();
            if (rc == TAIR_RETURN_SUCCESS) {
               resp->add_key_data(request->key, data);
               request->key = NULL;
               log_debug("get return %s", resp->data->get_data());
            } else {
               delete data;
            }
         }
      }
      resp->config_version = heart_beat->get_client_version();
      resp->setChannelId(request->getChannelId());
      resp->set_code(rc);
      if(request->get_connection()->postPacket(resp) == false) {
         delete resp;
         resp = 0;
      }
      PROFILER_DUMP();
      PROFILER_STOP();

      send_return = false;
      return rc;
   }

    int request_processor::process(request_hide *request, bool &send_return)
    {
      send_return = true;
      if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      int rc = TAIR_RETURN_FAILED;

      uint64_t target_server_id = 0;
      if (tair_mgr->should_proxy(*request->key, target_server_id)) {
        base_packet *proxy_packet = new request_hide(*(request_hide*)request);
        return do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
      }

      PROFILER_START("local hide operation start");
      plugin::plugins_root* plugin_root = NULL;
      PROFILER_BEGIN("do request plugin");
      int plugin_ret = tair_mgr->plugins_manager.
        do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
            TAIR_REQ_HIDE_PACKET, request->area, request->key, NULL, plugin_root);
      PROFILER_END();
      if (plugin_ret < 0) {
        log_debug("plugin return %d, skip excute", plugin_ret);
        return rc;
      }

      PROFILER_BEGIN("do hide");
      rc = tair_mgr->hide(request->area, *request->key, request, heart_beat->get_client_version());
      PROFILER_END();

      PROFILER_BEGIN("do response plugin");
      tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
          TAIR_REQ_HIDE_PACKET, request->area, request->key, NULL, plugin_root);
      PROFILER_END();
      PROFILER_DUMP();
      PROFILER_STOP();

      return rc;
    }

   int request_processor::process(request_prefix_hides *request, bool &send_return)
   {
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      int rc = 0;
      plugin::plugins_root* plugin_root = NULL;
      uint64_t target_server_id = 0;
      send_return = false;
      response_mreturn *resp = new response_mreturn();

      if (request->key_list != NULL) {
        tair_dataentry_set *key_list = request->key_list;
        tair_dataentry_set::iterator it = key_list->begin();
        data_entry *mkey = *it;
        plocker.lock(*mkey);
        uint32_t ndone = 0;
        while (it != request->key_list->end()) {
          PROFILER_BEGIN("do request plugin");
          int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
              TAIR_REQ_PREFIX_HIDES_PACKET, request->area, *it, NULL, plugin_root);
          PROFILER_END();
          if (tair_mgr->should_proxy(**it, target_server_id)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
            break;
          }
          if (plugin_ret < 0) {
            log_error("plugin return %d, skip excute", plugin_ret);
            rc = TAIR_RETURN_PLUGIN_ERROR;
          } else {
            PROFILER_BEGIN("do prefix hides");
            rc = tair_mgr->hide(request->area, **it, request, heart_beat->get_client_version());
            PROFILER_END();
            PROFILER_BEGIN("do response plugin");
            tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                TAIR_REQ_PREFIX_HIDES_PACKET, request->area, *it, NULL, plugin_root);
            PROFILER_END();
          }
          if (rc == TAIR_RETURN_SUCCESS) {
            ++ndone;
          } else {
            data_entry *skey = new data_entry();
            int prefix_size = (*it)->get_prefix_size();
            skey->set_data((*it)->get_data() + prefix_size, (*it)->get_size() - prefix_size);
            resp->add_key_code(skey, rc);
          }
          ++it;
        }
        plocker.unlock(*mkey);
        if (ndone > 0 && ndone < request->key_count) {
          rc = TAIR_RETURN_PARTIAL_SUCCESS;
        } else if (ndone == 0) {
          //rc = rc;
        }
      } else if (request->key != NULL) {
        PROFILER_START("prefix hides operation start");
        request->key->server_flag = request->server_flag;
        data_entry *mkey = request->key;
        plocker.lock(*mkey);
        do {
          if (tair_mgr->should_proxy(*request->key, target_server_id)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
            break;
          } else {
            PROFILER_BEGIN("do request plugin");
            int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                TAIR_REQ_PREFIX_HIDES_PACKET, request->area, request->key, NULL, plugin_root);
            PROFILER_END();
            if (plugin_ret < 0) {
              log_error("plugin return %d, skip excute", plugin_ret);
              rc = TAIR_RETURN_PLUGIN_ERROR;
            }else {
              PROFILER_BEGIN("do prefix hides");
              rc = tair_mgr->hide(request->area, *(request->key),request,heart_beat->get_client_version());
              PROFILER_END();

              PROFILER_BEGIN("do response plugin");
              tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                  TAIR_REQ_PREFIX_HIDES_PACKET, request->area, (request->key), NULL, plugin_root);
              PROFILER_END();
            }
            PROFILER_END();
          }
          if (rc != TAIR_RETURN_SUCCESS) {
            data_entry *skey = new data_entry();
            int prefix_size = request->key->get_prefix_size();
            skey->set_data(request->key->get_data() + prefix_size, request->key->get_size() - prefix_size);
            resp->add_key_code(skey, rc);
          }
        } while (false);
        plocker.unlock(*mkey);
      }
      PROFILER_DUMP();
      PROFILER_STOP();
      resp->config_version = heart_beat->get_client_version();
      resp->setChannelId(request->getChannelId());
      resp->set_code(rc);
      if (request->get_connection()->postPacket(resp) == false) {
        delete resp;
      }
      return rc;
   }

    int request_processor::process(request_get_hidden *request, bool &send_return)
    {
      if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      set<data_entry*, data_entry_comparator>::iterator it;
      data_entry *data = NULL;
      response_get *resp = new response_get();

      uint64_t target_server_id = 0;
      int rc = TAIR_RETURN_FAILED;

      if (request->key_list != NULL) {
        uint32_t count = 0;
        PROFILER_START("batch get_hidden operation start");
        for (it = request->key_list->begin(); it != request->key_list->end(); ++it) {
          PROFILER_BEGIN("sub get_hidden operation in batch begin");
          if (data == NULL) {
            data = new data_entry();
          }
          data_entry *key = (*it);

          if (tair_mgr->should_proxy(*key, target_server_id))
          {
            resp->add_proxyed_key(key); //? when to do_proxy
            continue;
          }

          plugin::plugins_root* plugin_root = NULL;
          PROFILER_BEGIN("do request plugin");
          int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
              TAIR_REQ_GET_HIDDEN_PACKET, request->area, key, NULL, plugin_root);
          PROFILER_END();
          if (plugin_ret < 0) {
            log_debug("plugin return %d, skip excute", plugin_ret);
            continue;
          }
          PROFILER_BEGIN("do get");
          int rev = tair_mgr->get_hidden(request->area, *key, *data);
          PROFILER_END();

          PROFILER_BEGIN("do response plugin");
          tair_mgr->plugins_manager.do_response_plugins(rev, plugin::PLUGIN_TYPE_SYSTEM,
              TAIR_REQ_GET_HIDDEN_PACKET, request->area, key, data, plugin_root);
          PROFILER_END();
          if (rev != TAIR_RETURN_SUCCESS && rev != TAIR_RETURN_HIDDEN) {
            continue;
          }
          rev = TAIR_RETURN_SUCCESS;
          ++count;
          resp->add_key_data(new data_entry(*key), data);
          data = NULL;
          PROFILER_END();
        }
        if (data != NULL) {
          delete data;
          data = NULL;
        }
        if(count == request->key_count){
          rc = TAIR_RETURN_SUCCESS;
        }else if(count > 0){
          rc = TAIR_RETURN_PARTIAL_SUCCESS;
        }else {
          rc = TAIR_RETURN_FAILED;
        }
      } else if (request->key != NULL) {
        PROFILER_START("get_hidden operation start");

        if (tair_mgr->should_proxy(*request->key, target_server_id))
        {
          base_packet *proxy_packet = new request_get_hidden(*(request_get_hidden*)request);
          rc = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
          delete resp; resp = 0;
          return rc;
        }

        plugin::plugins_root* plugin_root = NULL;
        PROFILER_BEGIN("do request plugin");
        int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
            TAIR_REQ_GET_HIDDEN_PACKET,request->area, (request->key), NULL, plugin_root);
        PROFILER_END();

        if (plugin_ret < 0) {
          log_debug("plugin return %d, skip excute", plugin_ret);
          rc = TAIR_RETURN_PLUGIN_ERROR;
        }
        else {
          data = new data_entry();
          PROFILER_BEGIN("do get_hidden");
          rc = tair_mgr->get_hidden(request->area, *(request->key), *data);
          PROFILER_END();

          PROFILER_BEGIN("do response plugin");
          tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
              TAIR_REQ_GET_HIDDEN_PACKET,request->area, (request->key), data, plugin_root);
          PROFILER_END();
          if (rc == TAIR_RETURN_SUCCESS || rc == TAIR_RETURN_HIDDEN) {
            rc = TAIR_RETURN_SUCCESS;
            resp->add_key_data(request->key, data);
            request->key = NULL;
            log_debug("get return %s", resp->data->get_data());
          } else {
            delete data;
          }
        }
      }
      resp->config_version = heart_beat->get_client_version();
      resp->setChannelId(request->getChannelId());
      resp->set_code(rc);
      if(request->get_connection()->postPacket(resp) == false) {
        delete resp;
        resp = 0;
      }
      PROFILER_DUMP();
      PROFILER_STOP();

      send_return = false;
      return rc;
    }

   int request_processor::process(request_remove *request, bool &send_return)
   {
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      int rc = 0;
      plugin::plugins_root* plugin_root = NULL;
      uint64_t target_server_id = 0;
      send_return = true;

      if (request->key_list != NULL) {

         log_info("bo batch remove,area=%d,size=%d",request->area,request->key_list->size());
         return tair_mgr->batch_remove(request->area,request->key_list,request,heart_beat->get_client_version());

      } else if (request->key != NULL) {
         PROFILER_START("remove operation start");
         request->key->server_flag = request->server_flag;

         if (tair_mgr->should_proxy(*request->key, target_server_id))
         {
            base_packet *proxy_packet = new request_remove(*(request_remove*)request);
            rc = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
            return rc;
         }

         PROFILER_BEGIN("do request plugin");
         int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                       TAIR_REQ_REMOVE_PACKET, request->area, request->key, NULL, plugin_root);
         PROFILER_END();
         if (plugin_ret < 0) {
            log_error("plugin return %d, skip excute", plugin_ret);
            rc = TAIR_RETURN_PLUGIN_ERROR;
         }else {
            PROFILER_BEGIN("do remove");
            rc = tair_mgr->remove(request->area, *(request->key),request,heart_beat->get_client_version());
            PROFILER_END();

            PROFILER_BEGIN("do response plugin");
            tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                          TAIR_REQ_REMOVE_PACKET, request->area, (request->key), NULL, plugin_root);
            PROFILER_END();
         }
         PROFILER_END();
      }
      PROFILER_DUMP();
      PROFILER_STOP();
      return rc;
   }

   int request_processor::process(request_inc_dec *request, bool &send_return)
   {
      int rc = 0;
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      uint64_t target_server_id = 0;
      if (tair_mgr->should_proxy(request->key, target_server_id))
      {
         base_packet *proxy_packet = new request_inc_dec(*(request_inc_dec*)request);
         rc = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
         return rc;
      }

      request->key.server_flag = request->server_flag;

      int ret_value = 0;
      PROFILER_START("addcounter operation start");
      plugin::plugins_root* plugin_root = NULL;
      PROFILER_BEGIN("do request plugin");
      int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                    TAIR_REQ_INCDEC_PACKET, request->area, &(request->key), NULL, plugin_root);
      PROFILER_END();
      if (plugin_ret < 0) {
         log_debug("plugin return %d, skip excute", plugin_ret);
         rc = TAIR_RETURN_PLUGIN_ERROR;
         send_return = true;
      } else {
         PROFILER_BEGIN("do addCount");
         rc = tair_mgr->add_count(request->area, request->key,
                                  request->add_count, request->init_value, &ret_value,request->expired,request,heart_beat->get_client_version());
         PROFILER_END();

         PROFILER_BEGIN("do response plugins");
         tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                       TAIR_REQ_INCDEC_PACKET, request->area, &(request->key), NULL, plugin_root);
         PROFILER_END();

         if (rc != TAIR_RETURN_SUCCESS) {
           //if copy_count>1 ,it will return TAIR_DUP_WAIT_RSP;
            send_return = true;
         } else {
            response_inc_dec *resp = new response_inc_dec();
            resp->value = ret_value;
            resp->config_version = heart_beat->get_client_version();
            resp->setChannelId(request->getChannelId());
            if (request->get_connection()->postPacket(resp) == false) {
               delete resp;
            }
            send_return = false;
         }
      }
      PROFILER_DUMP();
      PROFILER_STOP();

      return rc;
   }

   int request_processor::process(request_duplicate *request, bool &sendReturn)
   {
      int ret = 0;

      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      PROFILER_START("start duplicate operation");
      log_debug("request key serverflag = %d", request->key.server_flag);
      if (request->data.data_meta.flag == TAIR_ITEM_FLAG_DELETED) {
         // remove
         log_debug("duplicate remove operate");
         request->key.data_meta.version = 0; // force remove
         PROFILER_BEGIN("do remove operation");
         ret = tair_mgr->remove(request->area, request->key);
         PROFILER_END();
      } else {
         // put
         log_debug("duplicate put operate");
         PROFILER_BEGIN("do put operation");
         ret = tair_mgr->put(request->area, request->key, request->data, request->key.data_meta.edate);
         PROFILER_END();
      }
      log_debug("duplicate return code: %d", ret);
      if (ret == TAIR_RETURN_SUCCESS || ret == TAIR_RETURN_DATA_NOT_EXIST) {
         log_debug("send return packet to duplicate source");
         response_duplicate *dresp = new response_duplicate();
         dresp->setChannelId(request->getChannelId());
         dresp->server_id = local_server_ip::ip;
         dresp->packet_id = request->packet_id;
         dresp->bucket_id = request->bucket_number;
         if (request->get_connection()->postPacket(dresp) == false) {
            delete dresp;
         }
      } else {
         log_debug("XXX do duplicate operation failed");
      }
      PROFILER_DUMP();
      PROFILER_STOP();
      sendReturn = false;
      return ret;
   }

   int request_processor::process(request_add_items *request, bool &send_return)
   {
      int ret = 0;

      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      PROFILER_START("add items operation start");
      uint64_t target_server_id = 0;
      if (tair_mgr->should_proxy(request->key, target_server_id))
      {
         base_packet *proxy_packet = new request_add_items(*(request_add_items*)request);
         ret = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
         return ret;
      }

      request->key.server_flag = request->server_flag;
      request->key.data_meta.version = request->version;

      plugin::plugins_root* plugin_root = NULL;
      PROFILER_BEGIN("do request plugins");
      int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                    TAIR_REQ_ADDITEMS_PACKET, request->area, &(request->key), &(request->data), plugin_root);
      PROFILER_END();
      if (plugin_ret < 0) {
         log_debug("plugin return %d, skip excute", plugin_ret);
         ret = TAIR_RETURN_PLUGIN_ERROR;
      }else {
         PROFILER_BEGIN("do add items");
         ret = tair_mgr->add_items(request->area,request->key,request->data,request->max_count,request->expired);
         PROFILER_END();

         PROFILER_BEGIN("do response plugins");
         tair_mgr->plugins_manager.do_response_plugins(ret, plugin::PLUGIN_TYPE_SYSTEM,
                                                       TAIR_REQ_ADDITEMS_PACKET, request->area, &(request->key), &(request->data), plugin_root);
         PROFILER_END();

      }
      PROFILER_DUMP();
      PROFILER_STOP();
      send_return = true;
      return ret;
   }

   int request_processor::process(request_get_items *request, bool &send_return)
   {
      int ret = 0;
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      PROFILER_START("get items operation start");
      uint64_t target_server_id = 0;
      if (tair_mgr->should_proxy(*(request->key), target_server_id))
      {
         base_packet *proxy_packet = new request_get_items(*(request_get_items*)request);
         ret = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
         return ret;
      }

      plugin::plugins_root* plugin_root = NULL;
      PROFILER_BEGIN("do request plugins");
      int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                    TAIR_REQ_GETITEMS_PACKET, request->area, (request->key), NULL, plugin_root);
      PROFILER_END();
      if (plugin_ret < 0) {
         log_debug("plugin return %d, skip excute", plugin_ret);
         ret = TAIR_RETURN_PLUGIN_ERROR;
         PROFILER_DUMP();
         PROFILER_STOP();
         return ret; //let caller respond
      }

      data_entry *data = new data_entry();
      response_get_items *resp = new response_get_items();
      PROFILER_BEGIN("do get items");
      ret = tair_mgr->get_items(request->area,*(request->key),request->offset,request->count,*data,request->type);
      PROFILER_END();

      PROFILER_BEGIN("do response plugins");
      tair_mgr->plugins_manager.do_response_plugins(ret, plugin::PLUGIN_TYPE_SYSTEM,
                                                    TAIR_REQ_GETITEMS_PACKET,request->area, (request->key), data, plugin_root);
      PROFILER_END();

      resp->set_code(ret);

      if( ret == TAIR_RETURN_SUCCESS){
         log_debug("getItems success,data:%s",data->get_data()+sizeof(uint32_t));
         resp->add_key_data(request->key, data);
         request->key = 0;
      }else{
         delete data;
         data = 0;
      }
      resp->config_version = heart_beat->get_client_version();
      resp->setChannelId(request->getChannelId());
      if(request->get_connection()->postPacket(resp) == false) {
         delete resp;
      }
      send_return = false;
      PROFILER_DUMP();
      PROFILER_STOP();
      return ret;
   }

   int request_processor::process(request_remove_items *request, bool &send_return)
   {
      int ret = 0;
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      uint64_t target_server_id = 0;

      PROFILER_START("remove items operation start");
      if (tair_mgr->should_proxy(*(request->key), target_server_id))
      {
         base_packet *proxy_packet = new request_remove_items(*(request_remove_items*)request);
         ret = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
         return ret;
      }

      request->key->server_flag = request->server_flag;

      plugin::plugins_root* plugin_root = NULL;
      PROFILER_BEGIN("do request plugins");
      int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                    TAIR_REQ_REMOVEITEMS_PACKET, request->area, (request->key), NULL, plugin_root);
      PROFILER_END();
      if (plugin_ret < 0) {
         log_debug("plugin return %d, skip excute", plugin_ret);
         ret = TAIR_RETURN_PLUGIN_ERROR;
      }else {
         PROFILER_BEGIN("do remove items");
         ret = tair_mgr->remove_items(request->area,*(request->key),request->offset,request->count);
         PROFILER_END();

         PROFILER_BEGIN("do response plugins");
         tair_mgr->plugins_manager.do_response_plugins(ret, plugin::PLUGIN_TYPE_SYSTEM,
                                                       TAIR_REQ_REMOVEITEMS_PACKET, request->area, (request->key), NULL, plugin_root);
         PROFILER_END();

      }
      PROFILER_DUMP();
      PROFILER_STOP();
      send_return = true;
      return ret;
   }

   int request_processor::process(request_get_and_remove_items *request, bool &send_return)
   {
      int ret = 0;
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      PROFILER_START("getAndRemove items start");
      uint64_t target_server_id = 0;
      if (tair_mgr->should_proxy(*(request->key), target_server_id))
      {
         base_packet *proxy_packet = new request_get_and_remove_items(*(request_get_and_remove_items*)request);
         ret = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
         return ret;
      }

      request->key->server_flag = request->server_flag;

      plugin::plugins_root* plugin_root = NULL;
      PROFILER_BEGIN("do response plugins");
      int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                    TAIR_REQ_GETANDREMOVEITEMS_PACKET, request->area, (request->key), NULL, plugin_root);
      PROFILER_END();
      if (plugin_ret < 0) {
         log_debug("plugin return %d, skip excute", plugin_ret);
         ret = TAIR_RETURN_PLUGIN_ERROR;
         return ret; //let caller respond
      }

      data_entry *data = new data_entry();
      response_get_items *resp = new response_get_items();
      PROFILER_BEGIN("do getAndRemove");
      ret = tair_mgr->get_and_remove(request->area,*(request->key),request->offset,request->count,*data,request->type);
      PROFILER_END();

      PROFILER_BEGIN("do response plugins");
      tair_mgr->plugins_manager.do_response_plugins(ret, plugin::PLUGIN_TYPE_SYSTEM,
                                                    TAIR_REQ_GETANDREMOVEITEMS_PACKET,request->area, (request->key), data, plugin_root);
      PROFILER_END();

      resp->set_code(ret);
      if(ret == TAIR_RETURN_SUCCESS){
         resp->add_key_data(request->key, data);
         request->key = 0;
      }else{
         delete data;
         data = 0;
      }
      resp->config_version = heart_beat->get_client_version();
      resp->setChannelId(request->getChannelId());
      if(request->get_connection()->postPacket(resp) == false) {
         delete resp;
      }
      PROFILER_DUMP();
      PROFILER_STOP();
      send_return = false;
      return ret;
   }

   int request_processor::process(request_get_items_count *request,bool& send_return)
   {
      int ret = 0;
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }
      uint64_t target_server_id = 0;
      PROFILER_START("getitems operation start");
      if (tair_mgr->should_proxy(*(request->key), target_server_id))
      {
         base_packet *proxy_packet = new request_get_items(*(request_get_items*)request);
         ret = do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
         return ret;
      }

      request->key->server_flag = request->server_flag;

      plugin::plugins_root* plugin_root = NULL;
      PROFILER_BEGIN("do request plugin");
      int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,TAIR_REQ_GETITEMSCOUNT_PACKET, request->area, (request->key), NULL, plugin_root);
      PROFILER_END();
      if (plugin_ret < 0) {
         log_debug("plugin return %d, skip excute", plugin_ret);
         ret = TAIR_RETURN_PLUGIN_ERROR;
      }else {

         PROFILER_BEGIN("do getItemCount");
         ret = tair_mgr->get_item_count(request->area,*(request->key));
         PROFILER_END();

         PROFILER_BEGIN("do response plugins");
         tair_mgr->plugins_manager.do_response_plugins(ret, plugin::PLUGIN_TYPE_SYSTEM, TAIR_REQ_GETITEMSCOUNT_PACKET,request->area, (request->key), NULL, plugin_root);
         PROFILER_END();
      }
      send_return = true;
      PROFILER_DUMP();
      PROFILER_STOP();
      return ret;

   }

   int request_processor::process(request_mupdate *request,bool &send_return)
   {
     if (tair_mgr->is_working() == false) {
       return TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }

     tair_operc_vector::iterator it;

     if (request->server_flag != TAIR_SERVERFLAG_MIGRATE) {
       log_warn("requestMUpdatePacket have no MIGRATE flag");
       return TAIR_RETURN_INVALID_ARGUMENT;
     }

     int rc = TAIR_RETURN_SUCCESS;
     if (request->key_and_values != NULL) {
       for (it = request->key_and_values->begin(); it != request->key_and_values->end(); ++it) {
         operation_record *oprec = (*it);
         if (oprec->operation_type == 1) {
           // put
           rc = tair_mgr->direct_put(*oprec->key, *oprec->value);
         } else if (oprec->operation_type == 2) {
           // remove
           rc = tair_mgr->direct_remove(*oprec->key);
         } else {
           rc = TAIR_RETURN_INVALID_ARGUMENT;
           break;
         }

         if (rc != TAIR_RETURN_SUCCESS) {
           log_debug("do migrate operation failed, rc: %d", rc);
           break;
         }
       }
     }

     return rc;
   }

  int request_processor::process(request_lock *request, bool &send_return)
  {
    send_return = true;

    if (tair_mgr->is_working() == false) {
      return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = TAIR_RETURN_FAILED;

    uint64_t target_server_id = 0;
    if (tair_mgr->should_proxy(request->key, target_server_id)) {
      base_packet *proxy_packet = new request_lock(*(request_lock*)request);
      return do_proxy(target_server_id, proxy_packet, request) ? TAIR_RETURN_PROXYED : TAIR_RETURN_FAILED;
    }

    PROFILER_START("local lock operation start");

    plugin::plugins_root* plugin_root = NULL;
    PROFILER_BEGIN("do request plugin");
    int plugin_ret = tair_mgr->plugins_manager.
      do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                         TAIR_REQ_LOCK_PACKET, request->area, &request->key, NULL, plugin_root);
    PROFILER_END();
    if (plugin_ret < 0) {
      log_debug("plugin return %d, skip excute", plugin_ret);
      return rc;
    }

    PROFILER_BEGIN("do lock");
    rc = tair_mgr->lock(request->area, request->lock_type, request->key,
                        request, heart_beat->get_client_version());
    PROFILER_END();

    PROFILER_BEGIN("do response plugin");
    tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                  TAIR_REQ_LOCK_PACKET, request->area, &request->key, NULL, plugin_root);
    PROFILER_END();

    PROFILER_DUMP();
    PROFILER_STOP();

    return rc;
  }

  int request_processor::process(request_prefix_puts *request, bool &send_return)
  {
    int rc = 0;
    if (tair_mgr->is_working() == false) {
      return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }
    if (request->key_count <= 0) {
      log_error("prefix mput key count not greater than zero");
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    tair_keyvalue_map *kvmap = request->kvmap;
    tair_keyvalue_map::iterator it = kvmap->begin();
    data_entry *mkey = it->first;
    plocker.lock(*mkey);
    uint32_t ndone = 0;
    response_mreturn *resp = new response_mreturn();
    while (it != kvmap->end()) {
      data_entry *key = it->first;
      data_entry value;
      int version = key->data_meta.version;
      int expire = key->data_meta.edate;
      tair_mgr->get(request->area, *key, value); // any method to get meta info only?
      if (version != 0 && version != key->data_meta.version) {
        rc = TAIR_RETURN_VERSION_ERROR;
        data_entry *skey = new data_entry();
        int prefix_size = key->get_prefix_size();
        skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
        resp->add_key_code(skey, rc);
      }
      key->data_meta.edate = expire;
      ++it;
    }
    it = kvmap->begin();
    if (rc != TAIR_RETURN_VERSION_ERROR) {
      while (it != kvmap->end()) {
        data_entry *key = it->first;
        data_entry *value = it->second;

        uint64_t target_server_id = 0;
        if (tair_mgr->should_proxy(*key, target_server_id))
        {
          rc = TAIR_RETURN_SHOULD_PROXY;
          break;
        }

        rc = tair_mgr->put(request->area, *key, *value, key->data_meta.edate, request, heart_beat->get_client_version());
        if (rc == TAIR_RETURN_SUCCESS) {
          ++ndone;
        } else {
          data_entry *skey = new data_entry();
          int prefix_size = key->get_prefix_size();
          skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
          resp->add_key_code(skey, rc);
        }
        ++it;
      }
    }
    plocker.unlock(*mkey);
    if (ndone > 0 && ndone < request->key_count) {
      rc = TAIR_RETURN_PARTIAL_SUCCESS;
    }
    resp->set_code(rc);
    resp->config_version = heart_beat->get_client_version();
    resp->setChannelId(request->getChannelId());
    if (request->get_connection()->postPacket(resp) == false) {
      delete resp;
    }
    send_return = false;
    return rc;
  }

   int request_processor::process(request_prefix_removes *request, bool &send_return)
   {
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      int rc = 0;
      plugin::plugins_root* plugin_root = NULL;
      uint64_t target_server_id = 0;
      send_return = false;
      response_mreturn *resp = new response_mreturn();

      if (request->key_list != NULL) {
        tair_dataentry_set *key_list = request->key_list;
        tair_dataentry_set::iterator it = key_list->begin();
        uint32_t ndone = 0;
        data_entry *mkey = *it;
        plocker.lock(*mkey);
        while (it != request->key_list->end()) {
          PROFILER_BEGIN("do request plugin");
          int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
              TAIR_REQ_PREFIX_REMOVES_PACKET, request->area, *it, NULL, plugin_root);
          PROFILER_END();
          if (tair_mgr->should_proxy(**it, target_server_id)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
            break;
          }
          if (plugin_ret < 0) {
            log_error("plugin return %d, skip excute", plugin_ret);
            rc = TAIR_RETURN_PLUGIN_ERROR;
          } else {
            PROFILER_BEGIN("do prefix removes");
            rc = tair_mgr->remove(request->area, **it, request, heart_beat->get_client_version());
            PROFILER_END();
            PROFILER_BEGIN("do response plugin");
            tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                TAIR_REQ_PREFIX_REMOVES_PACKET, request->area, *it, NULL, plugin_root);
            PROFILER_END();
          }
          if (rc == TAIR_RETURN_SUCCESS) {
            ++ndone;
          } else {
            data_entry *skey = new data_entry();
            int prefix_size = (*it)->get_prefix_size();
            skey->set_data((*it)->get_data() + prefix_size, (*it)->get_size() - prefix_size);
            resp->add_key_code(skey, rc);
          }
          ++it;
        }
        plocker.unlock(*mkey);
        if (ndone > 0 && ndone < request->key_count) {
          rc = TAIR_RETURN_PARTIAL_SUCCESS;
        } else if (ndone == 0) {
          //rc = rc;
        }
      } else if (request->key != NULL) {
        PROFILER_START("prefix removes operation start");
        request->key->server_flag = request->server_flag;
        data_entry *mkey = request->key;
        plocker.lock(*mkey);
        do {
          if (tair_mgr->should_proxy(*request->key, target_server_id)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
            break;
          } else {
            PROFILER_BEGIN("do request plugin");
            int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                TAIR_REQ_REMOVE_PACKET, request->area, request->key, NULL, plugin_root);
            PROFILER_END();
            if (plugin_ret < 0) {
              log_error("plugin return %d, skip excute", plugin_ret);
              rc = TAIR_RETURN_PLUGIN_ERROR;
            }else {
              PROFILER_BEGIN("do prefix removes");
              rc = tair_mgr->remove(request->area, *(request->key),request,heart_beat->get_client_version());
              PROFILER_END();

              PROFILER_BEGIN("do response plugin");
              tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                  TAIR_REQ_REMOVE_PACKET, request->area, (request->key), NULL, plugin_root);
              PROFILER_END();
            }
            PROFILER_END();
          }
          if (rc != TAIR_RETURN_SUCCESS) {
            data_entry *skey = new data_entry();
            int prefix_size = request->key->get_prefix_size();
            skey->set_data(request->key->get_data() + prefix_size, request->key->get_size() - prefix_size);
            resp->add_key_code(skey, rc);
          }
        } while (false);
        plocker.unlock(*mkey);
      }
      PROFILER_DUMP();
      PROFILER_STOP();
      resp->config_version = heart_beat->get_client_version();
      resp->setChannelId(request->getChannelId());
      resp->set_code(rc);
      if (request->get_connection()->postPacket(resp) == false) {
        delete resp;
      }
      return rc;
   }

   int request_processor::process(request_prefix_incdec *request, bool &send_return)
   {
     int rc = 0;
     if (tair_mgr->is_working() == false) {
       rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }
     plugin::plugins_root *plugin_root = NULL;
     send_return = false;
     response_prefix_incdec *resp = new response_prefix_incdec();
     uint32_t ndone = 0;
     if (request->key_count > 0) {
       request_prefix_incdec::key_counter_map_t *key_counter_map = request->key_counter_map;
       request_prefix_incdec::key_counter_map_t::iterator it = key_counter_map->begin();
       data_entry *mkey = it->first;
       plocker.lock(*mkey);
       while (it != key_counter_map->end()) {
         data_entry *key = it->first;
         counter_wrapper *wrapper = it->second;
         int ret_value;
         uint64_t target_server_id;
         if (tair_mgr->should_proxy(*key, target_server_id)) {
           rc = TAIR_RETURN_SHOULD_PROXY;
           break;
         }
         PROFILER_BEGIN("do request plugin");
         int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
             TAIR_REQ_PREFIX_INCDEC_PACKET, request->area, key, NULL, plugin_root);
         PROFILER_END();
         if (plugin_ret < 0) {
           log_error("plugin return %d, skip execute", plugin_ret);
           rc = TAIR_RETURN_PLUGIN_ERROR;
         } else {
           PROFILER_BEGIN("do prefix incdec");
           rc = tair_mgr->add_count(request->area, *key, wrapper->count, wrapper->init_value, &ret_value, wrapper->expire, request, heart_beat->get_client_version());
           PROFILER_END();
           PROFILER_BEGIN("do response plugin");
           tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
               TAIR_REQ_PREFIX_INCDEC_PACKET, request->area, key, NULL, plugin_root);
           PROFILER_END();
         }
         data_entry *skey = new data_entry();
         int prefix_size = key->get_prefix_size();
         skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
         if (rc == TAIR_RETURN_SUCCESS) {
           resp->add_key_value(skey, ret_value);
           ++ndone;
         } else {
           resp->add_key_code(skey, rc);
         }
         ++it;
       }
       plocker.unlock(*mkey);
       if (ndone == request->key_count) {
         rc = TAIR_RETURN_SUCCESS;
       } else if (ndone > 0) {
         rc = TAIR_RETURN_PARTIAL_SUCCESS;
       } else {
         //rc = rc;
       }
     }
     PROFILER_DUMP();
     PROFILER_STOP();
     resp->config_version = heart_beat->get_client_version();
     resp->setChannelId(request->getChannelId());
     resp->set_code(rc);
     if (request->get_connection()->postPacket(resp) == false) {
       delete resp;
     }

     return rc;
   }

   int request_processor::process(request_prefix_gets *request, bool &send_return)
   {
     if (tair_mgr->is_working() == false) {
       return TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }

     set<data_entry*, data_entry_comparator>::iterator it;
     data_entry *data = NULL;
     response_prefix_gets *resp = new response_prefix_gets();

     uint64_t target_server_id = 0;
     int rc = TAIR_RETURN_FAILED;

     if (request->key_list != NULL) {
       uint32_t count = 0;
       PROFILER_START("batch get operation start");
       it = request->key_list->begin();
       data_entry *mkey = *it;
       plocker.lock(*mkey);
       for (; it != request->key_list->end(); ++it) {
         data_entry *key = (*it);

         if (tair_mgr->should_proxy(*key, target_server_id))
         {
           rc = TAIR_RETURN_SHOULD_PROXY;
           break;
         }

         plugin::plugins_root* plugin_root = NULL;
         PROFILER_BEGIN("do request plugin");
         int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
             TAIR_REQ_PREFIX_GETS_PACKET, request->area, key, NULL, plugin_root);
         PROFILER_END();
         if (plugin_ret < 0) {
           log_debug("plugin return %d, skip excute", plugin_ret);
           rc = TAIR_RETURN_PLUGIN_ERROR;
         } else {
           PROFILER_BEGIN("do get");
           if (data == NULL) {
             data = new data_entry();
           }
           rc = tair_mgr->get(request->area, *key, *data);
           PROFILER_END();

           PROFILER_BEGIN("do response plugin");
           tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
               TAIR_REQ_PREFIX_GETS_PACKET, request->area, key, data, plugin_root);
           PROFILER_END();
         }
         data_entry *skey = new data_entry();
         int prefix_size = key->get_prefix_size();
         skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
         skey->data_meta.version = key->data_meta.version;
         skey->data_meta.edate = key->data_meta.edate;
         //skey->data_meta = key->data_meta;
         if (resp->pkey == NULL) {
           resp->set_pkey(key->get_data(), key->get_prefix_size());
         }
         if (rc == TAIR_RETURN_SUCCESS) {
           ++count;
           resp->add_key_value(skey, data);
           data = NULL;
         } else {
           resp->add_key_code(skey, rc);
         }
       }
       plocker.unlock(*mkey);
       if (data != NULL) {
         delete data;
         data = NULL;
       }
       if (count == request->key_count) {
         rc = TAIR_RETURN_SUCCESS;
       } else if (count > 0) {
         rc = TAIR_RETURN_PARTIAL_SUCCESS;
       } else {
         //~ rc = rc;
       }
     } else if (request->key != NULL) {
       data_entry *mkey = request->key;
       plocker.lock(*mkey);
       do {
         if (tair_mgr->should_proxy(*request->key, target_server_id))
         {
           rc = TAIR_RETURN_SHOULD_PROXY;
           break;
         }

         plugin::plugins_root* plugin_root = NULL;
         PROFILER_BEGIN("do request plugin");
         int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
             TAIR_REQ_PREFIX_GETS_PACKET,request->area, (request->key), NULL, plugin_root);
         PROFILER_END();

         if (plugin_ret < 0) {
           log_debug("plugin return %d, skip excute", plugin_ret);
           rc = TAIR_RETURN_PLUGIN_ERROR;
         } else {
           data = new data_entry();
           data_entry *key = request->key;
           PROFILER_BEGIN("do get");
           rc = tair_mgr->get(request->area, *(request->key), *data);
           PROFILER_END();

           PROFILER_BEGIN("do response plugin");
           tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
               TAIR_REQ_GET_PACKET,request->area, (request->key), data, plugin_root);
           PROFILER_END();

           data_entry *skey = new data_entry();
           int prefix_size = key->get_prefix_size();
           skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
           skey->data_meta.version = key->data_meta.version;
           skey->data_meta.edate = key->data_meta.edate;

           if (resp->pkey == NULL) {
             resp->set_pkey(key->get_data(), key->get_prefix_size());
           }
           if (rc == TAIR_RETURN_SUCCESS) {
             resp->add_key_value(skey, data);
             data = NULL;
           } else {
             delete data;
             data = NULL;
           }
         }
       } while (false);
       plocker.unlock(*mkey);
     }
     resp->config_version = heart_beat->get_client_version();
     resp->setChannelId(request->getChannelId());
     resp->set_code(rc);
     if(request->get_connection()->postPacket(resp) == false) {
       delete resp;
       resp = 0;
     }
     PROFILER_DUMP();
     PROFILER_STOP();

     send_return = false;
     return rc;
   }

   int request_processor::process(request_prefix_get_hiddens *request, bool &send_return)
   {
     if (tair_mgr->is_working() == false) {
       return TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }

     set<data_entry*, data_entry_comparator>::iterator it;
     data_entry *data = NULL;
     response_prefix_gets *resp = new response_prefix_gets();

     uint64_t target_server_id = 0;
     int rc = TAIR_RETURN_FAILED;

     if (request->key_list != NULL) {
       uint32_t count = 0;
       PROFILER_START("batch get operation start");
       it = request->key_list->begin();
       data_entry *mkey = *it;
       plocker.lock(*mkey);
       for (; it != request->key_list->end(); ++it) {
         data_entry *key = (*it);

         if (tair_mgr->should_proxy(*key, target_server_id))
         {
           rc = TAIR_RETURN_SHOULD_PROXY;
           break;
         }

         plugin::plugins_root* plugin_root = NULL;
         PROFILER_BEGIN("do request plugin");
         int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
             TAIR_REQ_PREFIX_GET_HIDDENS_PACKET, request->area, key, NULL, plugin_root);
         PROFILER_END();
         if (plugin_ret < 0) {
           log_debug("plugin return %d, skip excute", plugin_ret);
           rc = TAIR_RETURN_PLUGIN_ERROR;
         } else {
           PROFILER_BEGIN("do prefix get hiddens");
           if (data == NULL) {
             data = new data_entry();
           }
           rc = tair_mgr->get_hidden(request->area, *key, *data);
           PROFILER_END();

           PROFILER_BEGIN("do response plugin");
           tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
               TAIR_REQ_PREFIX_GET_HIDDENS_PACKET, request->area, key, data, plugin_root);
           PROFILER_END();
         }
         data_entry *skey = new data_entry();
         int prefix_size = key->get_prefix_size();
         skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
         skey->data_meta.version = key->data_meta.version;
         skey->data_meta.edate = key->data_meta.edate;
         //skey->data_meta = key->data_meta;
         if (resp->pkey == NULL) {
           resp->set_pkey(key->get_data(), key->get_prefix_size());
         }
         if (rc == TAIR_RETURN_SUCCESS || rc == TAIR_RETURN_HIDDEN) {
           ++count;
           resp->add_key_value(skey, data, false, rc);
           data = NULL;
         } else {
           resp->add_key_code(skey, rc);
         }
       }
       plocker.unlock(*mkey);
       if (data != NULL) {
         delete data;
         data = NULL;
       }
       if (count == request->key_count) {
         rc = TAIR_RETURN_SUCCESS;
       } else if (count > 0) {
         rc = TAIR_RETURN_PARTIAL_SUCCESS;
       } else {
         //~ rc = rc;
       }
     } else if (request->key != NULL) {
       data_entry *mkey = request->key;
       plocker.lock(*mkey);
       do {
         if (tair_mgr->should_proxy(*request->key, target_server_id))
         {
           rc = TAIR_RETURN_SHOULD_PROXY;
           break;
         }

         plugin::plugins_root* plugin_root = NULL;
         PROFILER_BEGIN("do request plugin");
         int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
             TAIR_REQ_PREFIX_GET_HIDDENS_PACKET,request->area, (request->key), NULL, plugin_root);
         PROFILER_END();

         if (plugin_ret < 0) {
           log_debug("plugin return %d, skip excute", plugin_ret);
           rc = TAIR_RETURN_PLUGIN_ERROR;
         } else {
           data = new data_entry();
           data_entry *key = request->key;
           PROFILER_BEGIN("do get");
           rc = tair_mgr->get_hidden(request->area, *(request->key), *data);
           PROFILER_END();

           PROFILER_BEGIN("do response plugin");
           tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
               TAIR_REQ_PREFIX_GET_HIDDENS_PACKET,request->area, (request->key), data, plugin_root);
           PROFILER_END();

           data_entry *skey = new data_entry();
           int prefix_size = key->get_prefix_size();
           skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
           skey->data_meta.version = key->data_meta.version;
           skey->data_meta.edate = key->data_meta.edate;

           if (resp->pkey == NULL) {
             resp->set_pkey(key->get_data(), key->get_prefix_size());
           }
           if (rc == TAIR_RETURN_SUCCESS || rc == TAIR_RETURN_HIDDEN) {
             resp->add_key_value(skey, data, false, rc);
             data = NULL;
           } else {
             delete data;
             data = NULL;
           }
         }
       } while (false);
       plocker.unlock(*mkey);
     }
     resp->config_version = heart_beat->get_client_version();
     resp->setChannelId(request->getChannelId());
     resp->set_code(rc);
     if(request->get_connection()->postPacket(resp) == false) {
       delete resp;
       resp = 0;
     }
     PROFILER_DUMP();
     PROFILER_STOP();

     send_return = false;
     return rc;
   }

   bool request_processor::do_proxy(uint64_t target_server_id, base_packet *proxy_packet, base_packet *packet)
   {
      proxy_packet->server_flag = TAIR_SERVERFLAG_PROXY;
      bool rc = connection_mgr->sendPacket(target_server_id, proxy_packet, NULL, packet);
      if (rc == false) {
         delete proxy_packet;
      }
      return rc;
   }


}
