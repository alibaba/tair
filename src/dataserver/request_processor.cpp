/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#include "request_processor.hpp"
#include "tair_server.hpp"
#include "tair_manager.hpp"
#include "heartbeat_thread.hpp"
#include <limits>
#include "common/tair_stat.hpp"

namespace tair {
request_processor::request_processor(tair_manager *tair_mgr, heartbeat_thread *heart_beat, tair_server *server) {
    this->tair_mgr = tair_mgr;
    this->heart_beat = heart_beat;
    this->server = server;
}

request_processor::~request_processor() {
}

int request_processor::process(request_put *request, bool &send_return, uint32_t &resp_size) {
    resp_size = 0;
    int rc = 0;
    send_return = true;
    UNUSED(resp_size);

    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (request->key.get_size() == 0 || request->data.get_size() == 0) {
        log_debug("put value is null, return failed");
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    uint64_t target_server_id = 0;
    if (tair_mgr->should_proxy(request->key, target_server_id)) {
        rc = TAIR_RETURN_SHOULD_PROXY;
        return rc;
    }

    request->key.server_flag = request->server_flag;
    request->key.data_meta.version = request->version;
    log_debug("receive put request, serverFlag: %d", request->key.server_flag);
    PROFILER_START("put operation start");
    plugin::plugins_root *plugin_root = NULL;
    PROFILER_BEGIN("do request plugin");
    int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                  TAIR_REQ_PUT_PACKET, request->area, &(request->key),
                                                                  &(request->data), plugin_root);
    PROFILER_END();
    if (plugin_ret < 0) {
        log_debug("plugin return %d, skip excute", plugin_ret);
        rc = TAIR_RETURN_PLUGIN_ERROR;
    } else {
        PROFILER_BEGIN("do put");
        rc = tair_mgr->put(request->area, request->key, request->data, request->expired, request,
                           heart_beat->get_client_version());
        PROFILER_END();

        PROFILER_BEGIN("do response plugin");
        tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                      TAIR_REQ_PUT_PACKET, request->area, &(request->key),
                                                      &(request->data), plugin_root);
        PROFILER_END();
    }
    log_debug("put request return: %d", rc);
    PROFILER_DUMP();
    PROFILER_STOP();
    return rc;
}

int request_processor::process(request_get *request, bool &send_return, uint32_t &resp_size, uint32_t &weight) {
    easy_request_t *r = request->r;
    int rc = TAIR_RETURN_FAILED;
    resp_size = 0;
    response_get *resp = new response_get();

    if (tair_mgr->is_working() == false) {
        rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else {
        weight = request->key_count;
        set<data_entry *, data_entry_comparator>::iterator it;
        data_entry *data = NULL;
        uint64_t target_server_id = 0;
        if (request->key_list != NULL) {
            tair_mgr->get_stat_manager()->update(request->area, OP_GET);
            uint32_t count = 0;
            PROFILER_START("batch get operation start");
            for (it = request->key_list->begin(); it != request->key_list->end(); ++it) {
                PROFILER_BEGIN("sub get operation in batch begin");
                if (data == NULL) {
                    data = new data_entry();
                }
                data_entry *key = (*it);

                if (tair_mgr->should_proxy(*key, target_server_id)) {
                    resp->add_proxyed_key(key);
                    continue;
                }

                plugin::plugins_root *plugin_root = NULL;
                PROFILER_BEGIN("do request plugin");
                int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                              TAIR_REQ_GET_PACKET, request->area, key,
                                                                              NULL, plugin_root);
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
                                                              TAIR_REQ_GET_PACKET, request->area, key, data,
                                                              plugin_root);
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
            if (count == request->key_count) {
                rc = TAIR_RETURN_SUCCESS;
            } else {
                rc = TAIR_RETURN_PARTIAL_SUCCESS;
            }
        } else if (request->key != NULL) {
            if (request->key->get_prefix_size() == 0)
                tair_mgr->get_stat_manager()->update(request->area, OP_GET);
            else
                tair_mgr->get_stat_manager()->update(request->area, OP_PF_GET);

            PROFILER_START("get operation start");

            if (tair_mgr->should_proxy(*request->key, target_server_id)) {
                rc = TAIR_RETURN_SHOULD_PROXY;
            } else {
                plugin::plugins_root *plugin_root = NULL;
                PROFILER_BEGIN("do request plugin");
                int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                              TAIR_REQ_GET_PACKET, request->area,
                                                                              (request->key), NULL, plugin_root);
                PROFILER_END();

                if (plugin_ret < 0) {
                    log_debug("plugin return %d, skip excute", plugin_ret);
                    rc = TAIR_RETURN_PLUGIN_ERROR;
                } else {
                    data = new data_entry();
                    PROFILER_BEGIN("do get");
                    rc = tair_mgr->get(request->area, *(request->key), *data);
                    PROFILER_END();

                    PROFILER_BEGIN("do response plugin");
                    tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                                  TAIR_REQ_GET_PACKET, request->area, (request->key),
                                                                  data, plugin_root);
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
        }
    }
    resp->config_version = heart_beat->get_client_version();
    resp->setChannelId(request->getChannelId());
    resp->set_code(rc);
    resp_size = resp->size();

    r->opacket = resp;
    PROFILER_DUMP();
    PROFILER_STOP();

    send_return = false;
    return rc;
}

int request_processor::process(request_statistics *request, bool &send_return) {
    easy_request_t *r = request->r;
    send_return = false;
    int rc = TAIR_RETURN_SUCCESS;
    if (tair_mgr->is_working() == false)
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;

    response_statistics *resp = new response_statistics();
    tair_mgr->get_stat_manager()->get_stats(resp->data_, resp->data_len_);
    if (request->get_with_schema())
        tair_mgr->get_stat_manager()->get_schema_str(resp->schema_, resp->schema_len_);
    resp->schema_version_ = tair_mgr->get_stat_manager()->get_version();
    resp->setChannelId(request->getChannelId());
    resp->config_version_ = heart_beat->get_client_version();
    r->opacket = resp;

    return rc;
}

int request_processor::process(request_hide *request, bool &send_return, uint32_t &resp_size) {
    send_return = true;
    UNUSED(resp_size);
    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = TAIR_RETURN_FAILED;

    uint64_t target_server_id = 0;
    if (tair_mgr->should_proxy(*request->key, target_server_id)) {
        rc = TAIR_RETURN_SHOULD_PROXY;
        return rc;
    }

    PROFILER_START("local hide operation start");
    plugin::plugins_root *plugin_root = NULL;
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
    request->key->server_flag = request->server_flag;
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

int
request_processor::process(request_prefix_hides *request, bool &send_return, uint32_t &resp_size, uint32_t &weight) {
    easy_request_t *r = request->r;
    int rc = TAIR_RETURN_SUCCESS;
    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else {
        send_return = false;
        weight = request->key_count;
        data_entry key = request->key == NULL ?
                         **(request->key_list->begin()) : *request->key;
        plocker.lock(key);
        rc = tair_mgr->prefix_hides(request, heart_beat->get_client_version(), resp_size);
        plocker.unlock(key);
        if (rc == TAIR_RETURN_DUPLICATE_BUSY) {
            log_warn("prefix hides duplicate busy");
        } else {
            return rc;
        }
    }
    response_mreturn *resp = new response_mreturn();
    resp->config_version = heart_beat->get_client_version();
    resp->setChannelId(request->getChannelId());
    resp->set_code(rc);
    resp_size = resp->size();
    r->opacket = resp;
    return rc;
}

int request_processor::process(request_get_hidden *request, bool &send_return, uint32_t &resp_size) {
    easy_request_t *r = request->r;
    int rc = TAIR_RETURN_FAILED;
    response_get *resp = new response_get();

    if (tair_mgr->is_working() == false) {
        rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else {
        set<data_entry *, data_entry_comparator>::iterator it;
        data_entry *data = NULL;
        uint64_t target_server_id = 0;

        if (request->key_list != NULL) {
            tair_mgr->get_stat_manager()->update(request->area, OP_GET);
            uint32_t count = 0;
            PROFILER_START("batch get_hidden operation start");
            for (it = request->key_list->begin(); it != request->key_list->end(); ++it) {
                PROFILER_BEGIN("sub get_hidden operation in batch begin");
                if (data == NULL) {
                    data = new data_entry();
                }
                data_entry *key = (*it);

                if (tair_mgr->should_proxy(*key, target_server_id)) {
                    resp->add_proxyed_key(key); //? when to do_proxy
                    continue;
                }

                plugin::plugins_root *plugin_root = NULL;
                PROFILER_BEGIN("do request plugin");
                int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                              TAIR_REQ_GET_HIDDEN_PACKET, request->area,
                                                                              key, NULL, plugin_root);
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
                                                              TAIR_REQ_GET_HIDDEN_PACKET, request->area, key, data,
                                                              plugin_root);
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
            if (count == request->key_count) {
                rc = TAIR_RETURN_SUCCESS;
            } else if (count > 0) {
                rc = TAIR_RETURN_PARTIAL_SUCCESS;
            } else {
                rc = TAIR_RETURN_FAILED;
            }
        } else if (request->key != NULL) {
            if (request->key->get_prefix_size() == 0)
                tair_mgr->get_stat_manager()->update(request->area, OP_GET);
            else
                tair_mgr->get_stat_manager()->update(request->area, OP_PF_GET);
            PROFILER_START("get_hidden operation start");

            if (tair_mgr->should_proxy(*request->key, target_server_id)) {
                rc = TAIR_RETURN_SHOULD_PROXY;
            } else {
                plugin::plugins_root *plugin_root = NULL;
                PROFILER_BEGIN("do request plugin");
                int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                              TAIR_REQ_GET_HIDDEN_PACKET, request->area,
                                                                              (request->key), NULL, plugin_root);
                PROFILER_END();

                if (plugin_ret < 0) {
                    log_debug("plugin return %d, skip excute", plugin_ret);
                    rc = TAIR_RETURN_PLUGIN_ERROR;
                } else {
                    data = new data_entry();
                    PROFILER_BEGIN("do get_hidden");
                    rc = tair_mgr->get_hidden(request->area, *(request->key), *data);
                    PROFILER_END();

                    PROFILER_BEGIN("do response plugin");
                    tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                                  TAIR_REQ_GET_HIDDEN_PACKET, request->area,
                                                                  (request->key), data, plugin_root);
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
        }
    }
    resp->config_version = heart_beat->get_client_version();
    resp->setChannelId(request->getChannelId());
    resp->set_code(rc);
    resp_size = resp->size();
    r->opacket = resp;
    PROFILER_DUMP();
    PROFILER_STOP();

    send_return = false;
    return rc;
}

// both get_range and del_range use this packet
int request_processor::process(request_get_range *request, bool &send_return, uint32_t &resp_size, uint32_t &weight) {
    easy_request_t *r = request->r;
    int rc = TAIR_RETURN_FAILED;
    response_get_range *resp = new response_get_range();

    if (tair_mgr->is_working() == false) {
        rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else {
        int size = 0;
        plugin::plugins_root *plugin_root = NULL;
        uint64_t target_server_id = 0;
        bool has_next;

        PROFILER_START("range operation start");
        request->key_start.server_flag = request->server_flag;
        request->key_end.server_flag = request->server_flag;

        if (tair_mgr->should_proxy(request->key_start, target_server_id)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
        } else {
            PROFILER_BEGIN("do request plugin");
            int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                          TAIR_REQ_GET_RANGE_PACKET, request->area,
                                                                          &request->key_start, &request->key_end,
                                                                          plugin_root);
            PROFILER_END();
            if (plugin_ret < 0) {
                log_error("plugin return %d, skip excute", plugin_ret);
                rc = TAIR_RETURN_PLUGIN_ERROR;
            } else {
                tair_dataentry_vector *result = new tair_dataentry_vector();

                if (request->cmd < CMD_DEL_RANGE) {
                    PROFILER_BEGIN("do get_range");
                    rc = tair_mgr->get_range(request->area, request->key_start, request->key_end, request->offset,
                                             request->limit, request->cmd, *result, has_next);
                    PROFILER_END();
                } else {
                    PROFILER_BEGIN("del get_range");
                    // key_start may be changed after del_range, save this key for plocker
                    data_entry saved_key = request->key_start;
                    plocker.lock(saved_key);
                    rc = tair_mgr->del_range(request->area, request->key_start, request->key_end, request->offset,
                                             request->limit, request->cmd, *result, has_next);
                    plocker.unlock(saved_key);
                    PROFILER_END();
                }

                resp->set_cmd(request->cmd);
                size = result->size();
                // weight of get_range is 10
                weight = (size == 0) ? 1 : 10;
                if (size == 0 || TAIR_RETURN_SUCCESS != rc) {
                    if (TAIR_RETURN_SUCCESS != rc && 0 != size) {
                        for (uint32_t i = 0; i < result->size(); i++) {
                            delete (*result)[i];
                            (*result)[i] = NULL;
                        }
                    }
                    delete result;
                    result = NULL;
                    log_info("no data return from get_range or err happen, rc:%d, key: %s", rc,
                             request->key_start.get_data() + 4);
                } else {
                    resp->set_hasnext(has_next);
                    resp->set_key_count(size);
                    resp->set_key_data_vector(result);
                }
                resp_size = resp->size();
            }
        }
    }
    PROFILER_END();

    resp->config_version = heart_beat->get_client_version();
    resp->setChannelId(request->getChannelId());
    resp->set_code(rc);
    r->opacket = resp;
    send_return = false;

    return rc;
}

int request_processor::process(request_remove *request, bool &send_return, uint32_t &resp_size) {
    send_return = true;
    UNUSED(resp_size);

    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = 0;
    plugin::plugins_root *plugin_root = NULL;
    uint64_t target_server_id = 0;

    if (request->key_list != NULL) {
        // mdelete
        log_info("batch remove,area=%u, size=%lu", request->area, request->key_list->size());
        return tair_mgr->batch_remove(request->area, request->key_list, request, heart_beat->get_client_version());

    } else if (request->key != NULL) {
        PROFILER_START("remove operation start");
        request->key->server_flag = request->server_flag;

        if (tair_mgr->should_proxy(*request->key, target_server_id)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
            return rc;
        }

        PROFILER_BEGIN("do request plugin");
        int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                      TAIR_REQ_REMOVE_PACKET, request->area,
                                                                      request->key, NULL, plugin_root);
        PROFILER_END();
        if (plugin_ret < 0) {
            log_error("plugin return %d, skip excute", plugin_ret);
            rc = TAIR_RETURN_PLUGIN_ERROR;
        } else {
            PROFILER_BEGIN("do remove");
            rc = tair_mgr->remove(request->area, *(request->key), request, heart_beat->get_client_version());
            PROFILER_END();

            PROFILER_BEGIN("do response plugin");
            tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                          TAIR_REQ_REMOVE_PACKET, request->area, (request->key), NULL,
                                                          plugin_root);
            PROFILER_END();
        }
        PROFILER_END();
    }
    PROFILER_DUMP();
    PROFILER_STOP();
    return rc;
}

int request_processor::process(request_uniq_remove *request, bool &send_return) {
    send_return = true;

    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = 0;
    uint64_t target_server_id = 0;

    if (request->key != NULL) {
        PROFILER_START("remove operation start");
        request->key->server_flag = request->server_flag;

        if (tair_mgr->should_proxy(*request->key, target_server_id)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
            return rc;
        }

        rc = tair_mgr->uniq_remove(request->area, request->bucket, *(request->key));
    }

    PROFILER_DUMP();
    PROFILER_STOP();
    return rc;
}

int request_processor::process(request_mput *request, bool &send_return, uint32_t &resp_size) {
    UNUSED(resp_size);
    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = 0;
    if (request->record_vec != NULL) {
        log_debug("batch put, area: %u, size: %lu", request->area, request->record_vec->size());
        rc = tair_mgr->batch_put(request->area, request->record_vec, request, heart_beat->get_client_version());
    }
    send_return = request->server_flag != TAIR_SERVERFLAG_DUPLICATE ?
                  (TAIR_DUP_WAIT_RSP != rc) : (TAIR_RETURN_SUCCESS != rc);
    return rc;
}

int request_processor::process(request_inc_dec *request, bool &send_return, uint32_t &resp_size) {
    easy_request_t *r = request->r;
    send_return = true;
    int rc = 0;
    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    uint64_t target_server_id = 0;
    if (tair_mgr->should_proxy(request->key, target_server_id)) {
        rc = TAIR_RETURN_SHOULD_PROXY;
        return rc;
    }

    request->key.server_flag = request->server_flag;

    int ret_value = 0;
    PROFILER_START("addcounter operation start");
    plugin::plugins_root *plugin_root = NULL;
    PROFILER_BEGIN("do request plugin");
    int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                  TAIR_REQ_INCDEC_PACKET, request->area,
                                                                  &(request->key), NULL, plugin_root);
    PROFILER_END();
    if (plugin_ret < 0) {
        log_debug("plugin return %d, skip excute", plugin_ret);
        rc = TAIR_RETURN_PLUGIN_ERROR;
    } else {
        int low_bound = 0;
        int upper_bound = 0;
        request_inc_dec_bounded *req = dynamic_cast<request_inc_dec_bounded *>(request);
        if (req != NULL) {
            //bounded counter
            low_bound = req->get_low_bound();
            upper_bound = req->get_upper_bound();
        } else {
            //in fact, every counter now was restricted within a boundary,
            //which low bound indicated by `low_bound, upper bound indicated by `upper_bound.
            //the counter has the default boundary, that is in the range of minimum number and the
            //maximum number stored in 32-bit integer, if the old protocol does not specify the boundary.
            low_bound = std::numeric_limits<int32_t>::min();
            upper_bound = std::numeric_limits<int32_t>::max();
        }

        PROFILER_BEGIN("do addCount");
        rc = tair_mgr->add_count(request->area, request->key,
                                 request->add_count,
                                 request->init_value,
                                 low_bound, upper_bound,
                                 &ret_value,
                                 request->expired, request, heart_beat->get_client_version());
        PROFILER_END();

        PROFILER_BEGIN("do response plugins");
        tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                      TAIR_REQ_INCDEC_PACKET, request->area, &(request->key), NULL,
                                                      plugin_root);
        PROFILER_END();

        if (rc != TAIR_RETURN_SUCCESS) {
            //if copy_count>1 ,it will return TAIR_DUP_WAIT_RSP;
            send_return = true;
        } else {
            response_inc_dec *resp = NULL;
            if (req != NULL) {
                resp = new response_inc_dec_bounded();
            } else {
                resp = new response_inc_dec();
            }
            resp->value = ret_value;
            resp->config_version = heart_beat->get_client_version();
            resp->setChannelId(request->getChannelId());
            resp_size = resp->size();
            r->opacket = resp;
            send_return = false;
        }
    }
    PROFILER_DUMP();
    PROFILER_STOP();

    return rc;
}

int request_processor::process(request_batch_duplicate *request, bool &send_return) {
    easy_request_t *r = request->r;
    int ret = TAIR_RETURN_SUCCESS;

    if (tair_mgr->is_working() == false) {
        ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
        log_warn("recv dup request while not work, return noting...");
    } else {
        PROFILER_START("start batch duplicate operation");
        std::vector<common::Record *>::iterator iter;
        for (iter = request->records->begin();
             iter != request->records->end(); iter++) {
            common::KVRecord *rh = static_cast<common::KVRecord *>(*iter);
            if (rh->operator_type_ == TAIR_REMOTE_SYNC_TYPE_PUT) {
                PROFILER_BEGIN("do put operation");
                ret = tair_mgr->put(rh->key_->get_area(), *(rh->key_), *(rh->value_), rh->key_->data_meta.edate);
                PROFILER_END();
            } else if (rh->operator_type_ == TAIR_REMOTE_SYNC_TYPE_DELETE) {
                // rh->key.data_meta.version = 0 // force remove // not needed,
                // duplicate never care version, cover data's everything
                PROFILER_BEGIN("do remove operation");
                ret = tair_mgr->remove(rh->key_->get_area(), *(rh->key_));
                PROFILER_END();
                if (ret == TAIR_RETURN_DATA_NOT_EXIST) {
                    ret = TAIR_RETURN_SUCCESS;
                }
            }
            if (ret != TAIR_RETURN_SUCCESS) {
                break;
            }
        }
        if (ret == TAIR_RETURN_SUCCESS) {
            response_duplicate *dresp = new response_duplicate();
            dresp->setChannelId(request->getChannelId());
            dresp->server_id = local_server_ip::ip;
            dresp->packet_id = request->packet_id;
            dresp->bucket_id = request->bucket;
            r->opacket = dresp;
            send_return = false;
        } else {
            log_warn("do duplicate operation failed %d", ret);
        }
        PROFILER_DUMP();
        PROFILER_STOP();
    }
    return ret;
}

int request_processor::process(request_duplicate *request, bool &send_return, uint32_t &resp_size) {
    easy_request_t *r = request->r;
    int ret = 0;

    if (tair_mgr->is_working() == false) {
        ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
        log_warn("recv dup request while not work, return nothing...");
    } else {
        PROFILER_START("start duplicate operation");
        log_debug("request key serverflag = %d", request->key.server_flag);
        if (request->data.data_meta.flag == TAIR_ITEM_FLAG_DELETED && request->data.get_size() == 0) {
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
            resp_size = dresp->size();
            r->opacket = dresp;
        } else {
            log_debug("XXX do duplicate operation failed");
        }
        PROFILER_DUMP();
        PROFILER_STOP();
    }
    send_return = false;
    return ret;
}

int request_processor::process(request_mupdate *request, bool &send_return) {
    send_return = true;

    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    if (request->server_flag != TAIR_SERVERFLAG_MIGRATE) {
        log_warn("requestMUpdatePacket have no MIGRATE flag");
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int rc = TAIR_RETURN_SUCCESS;
    if (request->key_and_values != NULL && !request->key_and_values->empty()) {
        //update migrate stat, add it key number to area 0 statistics
        tair_mgr->get_stat_manager()->update(0, OP_MIGRATE_KEY, request->key_and_values->size());
        tair_mgr->get_stat_manager()->update(0, OP_MIGRATE_SIZE, request->size());

        // NOTE: now we consider all kvs are belong to one same bucket
        rc = tair_mgr->get_storage_manager()->
                direct_mupdate(tair_mgr->get_bucket_number(*(*request->key_and_values)[0]->key),
                               *request->key_and_values);
        if (rc == TAIR_RETURN_SUCCESS) {
            // update stat
            for (tair_operc_vector::iterator it = request->key_and_values->begin();
                 it != request->key_and_values->end(); ++it) {
                int area = (*it)->key->get_area();
                if ((*it)->operation_type == 1) {
                    TAIR_STAT.stat_put(area);
                } else if ((*it)->operation_type == 2) {
                    TAIR_STAT.stat_remove(area);
                }
            }
        } else if (rc == TAIR_RETURN_NOT_SUPPORTED) {
            for (tair_operc_vector::iterator it = request->key_and_values->begin();
                 it != request->key_and_values->end(); ++it) {
                operation_record *oprec = (*it);
                if (oprec->operation_type == 1) {
                    // put
                    rc = tair_mgr->direct_put(*oprec->key, *oprec->value, false);
                } else if (oprec->operation_type == 2) {
                    // remove
                    rc = tair_mgr->direct_remove(*oprec->key, false);
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
    }

    return rc;
}

int request_processor::process(request_lock *request, bool &send_return, uint32_t &resp_size) {
    send_return = true;
    UNUSED(resp_size);

    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    int rc = TAIR_RETURN_FAILED;

    uint64_t target_server_id = 0;
    if (tair_mgr->should_proxy(request->key, target_server_id)) {
        rc = TAIR_RETURN_SHOULD_PROXY;
        return rc;
    }

    PROFILER_START("local lock operation start");

    plugin::plugins_root *plugin_root = NULL;
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
                                                  TAIR_REQ_LOCK_PACKET, request->area, &request->key, NULL,
                                                  plugin_root);
    PROFILER_END();

    PROFILER_DUMP();
    PROFILER_STOP();

    return rc;
}

int request_processor::process(request_prefix_puts *request, bool &send_return, uint32_t &resp_size, uint32_t &weight) {
    easy_request_t *r = request->r;
    send_return = false;
    int rc = TAIR_RETURN_SUCCESS;

    if (tair_mgr->is_working() == false) {
        rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else {
        if (request->key_count <= 0) {
            log_error("prefix puts key count not greater than zero");
            rc = TAIR_RETURN_ITEMSIZE_ERROR;
        } else {
            uint64_t target_server_id = 0;
            if (tair_mgr->should_proxy(*request->pkey, target_server_id)) {
                rc = TAIR_RETURN_SHOULD_PROXY;
            } else {
                tair_keyvalue_map::iterator it = request->kvmap->begin();
                data_entry mkey = *it->first;
                weight = request->key_count;
                plocker.lock(mkey);
                rc = tair_mgr->prefix_puts(request, heart_beat->get_client_version(), resp_size);
                plocker.unlock(mkey);
                if (rc == TAIR_RETURN_DUPLICATE_BUSY) {
                    log_warn("prefix puts duplicate busy");
                } else {
                    return rc;
                }
            }
        }
    }
    response_mreturn *resp = new response_mreturn();
    resp->set_code(rc);
    resp->setChannelId(request->getChannelId());
    resp->config_version = heart_beat->get_client_version();
    resp_size = resp->size();
    r->opacket = resp;
    return rc;
}

int
request_processor::process(request_prefix_removes *request, bool &send_return, uint32_t &resp_size, uint32_t &weight) {
    easy_request_t *r = request->r;
    send_return = false;
    int rc = TAIR_RETURN_SUCCESS;
    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else {
        data_entry key = request->key == NULL ?
                         **(request->key_list->begin()) : *request->key;
        weight = request->key_count;
        plocker.lock(key);
        rc = tair_mgr->prefix_removes(request, heart_beat->get_client_version(), resp_size);
        plocker.unlock(key);
        if (rc == TAIR_RETURN_DUPLICATE_BUSY) {
            log_warn("prefix removes duplicate busy");
        } else {
            return rc;
        }
    }
    response_mreturn *resp = new response_mreturn();
    resp->config_version = heart_beat->get_client_version();
    resp->setChannelId(request->getChannelId());
    resp->set_code(rc);
    resp_size = resp->size();
    r->opacket = resp;
    return rc;
}

int
request_processor::process(request_prefix_incdec *request, bool &send_return, uint32_t &resp_size, uint32_t &weight) {
    easy_request_t *r = request->r;
    send_return = false;
    int rc = TAIR_RETURN_SUCCESS;
    int32_t low_bound = 0;
    int32_t upper_bound = 0;
    request_prefix_incdec_bounded *req = dynamic_cast<request_prefix_incdec_bounded *>(request);
    if (req != NULL) {
        low_bound = req->get_low_bound();
        upper_bound = req->get_upper_bound();
    } else {
        low_bound = std::numeric_limits<int32_t>::min();
        upper_bound = std::numeric_limits<int32_t>::max();
    }

    response_prefix_incdec *resp = NULL;
    if (req != NULL) {
        resp = new response_prefix_incdec_bounded();
    } else {
        resp = new response_prefix_incdec();
    }

    if (tair_mgr->is_working() == false) {
        rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else if (request->key_counter_map == NULL || request->key_counter_map->size() == 0) {
        rc = TAIR_RETURN_FAILED;
    } else {
        uint64_t target_server_id = 0;
        if (tair_mgr->should_proxy(*request->key_counter_map->begin()->first, target_server_id)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
        } else {
            weight = request->key_count;
            data_entry key = *request->key_counter_map->begin()->first;
            plocker.lock(key);
            rc = tair_mgr->prefix_incdec(request, heart_beat->get_client_version(), low_bound, upper_bound,
                                         (req != NULL), resp);
            plocker.unlock(key);
        }
    }

    if (TAIR_DUP_WAIT_RSP != rc) {
        if (request->server_flag & TAIR_SERVERFLAG_DUPLICATE) {
            resp->server_flag = TAIR_SERVERFLAG_DUPLICATE;
            if (request->key_counter_map == NULL || request->key_counter_map->size() == 0) {
                resp->bucket_id = 0;
                log_warn("key_counter_map is null or empty in request_prefix_incdec");
            } else {
                resp->bucket_id = tair_mgr->get_bucket_number(*request->key_counter_map->begin()->first);
            }
            resp->server_id = local_server_ip::ip;
            resp->packet_id = request->packet_id;
        }
        resp->set_code(rc);
        resp->config_version = heart_beat->get_client_version();
        resp->setChannelId(request->getChannelId());
        resp_size = resp->size();
        r->opacket = resp;
    } else {
        delete resp;
        resp = NULL;
    }
    return rc;
}

int request_processor::process(request_simple_get *request, uint32_t &resp_size, uint32_t &weight) {
    PROFILER_START("simple get operation start");
    response_simple_get *response = new response_simple_get();
    response->set_config_version(heart_beat->get_client_version());
    response->set_error_code(TAIR_RETURN_SUCCESS);
    response->setChannelId(request->getChannelId());
    request->r->opacket = response;

    if (tair_mgr->is_working() == false) {
        response->set_error_code(TAIR_RETURN_SERVER_CAN_NOT_WORK);
    } else {
        uint16_t area = request->ns();
        vector <key_element> &request_keys = request->mutable_keyset();
        vector <kv_element> &kv_result = response->mutable_kvset();
        kv_result.resize(request_keys.size());
        for (size_t i = 0; i < request_keys.size(); ++i) {
            PROFILER_BEGIN("do simple get batch prime key");
            slice &prime = request_keys[i].primekey;
            // TODO: check should proxy code
            kv_element &elem_result = kv_result[i];
            if (request_keys[i].subkeys.size() == 0) {
                // TODO: without prefix key
                response->set_error_code(TAIR_RETURN_NOT_SUPPORTED);
            } else {
                weight += request_keys[i].subkeys.size();
                // with prefix keys
                elem_result.subresize(request_keys[i].subkeys.size());
                for (size_t j = 0; j < request_keys[i].subkeys.size(); ++j) {
                    PROFILER_BEGIN("do simple get sub key");
                    slice &sub = request_keys[i].subkeys[j];
                    // build entry key, merge [area, prime, sub]
                    data_entry keyentry;
                    // set_data params: buf, blen, not alloc new memory, merged area
                    keyentry.set_data((char *) (sub.buf), sub.len, false, true);
                    // set prefix length
                    keyentry.set_prefix_size(prime.len);
                    data_entry valentry;
                    // query to val dataentry
                    int status = tair_mgr->get(area, keyentry, valentry, true);
                    // zero copy, move the buffer to response, free in response
                    // result to response
                    elem_result.substatus[j] = status;
                    elem_result.subkeys[j] = sub;
                    sub.buf = NULL;
                    // assign value buffer
                    if (status == TAIR_RETURN_SUCCESS || status == TAIR_ITEM_FLAG_DELETED) {
                        if (valentry.get_size() > 1024 * 16) {
                            elem_result.substatus[j] = TAIR_HAS_MORE_DATA;
                        } else {
                            // TODO: check whether ldb return 2 byte flag
                            // mark, with flag, avoid copy memory,
                            // so client decode should skip 2 bytes.
                            // data entry mv memory address to response
                            elem_result.subvals[j].buf = valentry.drop_true_data();
                            elem_result.subvals[j].len = valentry.drop_true_size();
                        }
                    }
                    PROFILER_END();
                }
            }
            // drop request buffer
            elem_result.primekey.buf = prime.buf;
            elem_result.primekey.len = prime.len;
            prime.buf = NULL;

            PROFILER_END();
        }
    }
    PROFILER_DUMP();
    PROFILER_STOP();
    resp_size = response->size();
    return response->error_code();
}

int request_processor::process(request_prefix_gets *request, bool &send_return, uint32_t &resp_size, uint32_t &weight) {
    easy_request_t *r = request->r;
    int rc = TAIR_RETURN_FAILED;
    response_prefix_gets *resp = new response_prefix_gets();
    tair_mgr->get_stat_manager()->update(request->area, OP_PF_GET);

    if (tair_mgr->is_working() == false) {
        rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else {
        set<data_entry *, data_entry_comparator>::iterator it;
        data_entry *data = NULL;
        uint64_t target_server_id = 0;

        if (request->key_list != NULL) {
            weight = request->key_count;
            uint32_t count = 0;
            PROFILER_START("batch get operation start");
            it = request->key_list->begin();
            data_entry *mkey = *it;
            plocker.lock(*mkey);
            for (; it != request->key_list->end(); ++it) {
                data_entry *key = (*it);
                //set pkey first
                if (resp->pkey == NULL) {
                    resp->set_pkey(key->get_data(), key->get_prefix_size());
                }
                if (tair_mgr->should_proxy(*key, target_server_id)) {
                    rc = TAIR_RETURN_SHOULD_PROXY;
                    break;
                } else {
                    plugin::plugins_root *plugin_root = NULL;
                    PROFILER_BEGIN("do request plugin");
                    int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                                  TAIR_REQ_PREFIX_GETS_PACKET,
                                                                                  request->area, key, NULL,
                                                                                  plugin_root);
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
                                                                      TAIR_REQ_PREFIX_GETS_PACKET, request->area, key,
                                                                      data, plugin_root);
                        PROFILER_END();
                    }
                }
                data_entry *skey = new data_entry();
                int prefix_size = key->get_prefix_size();
                skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                skey->data_meta = key->data_meta;
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
            if (resp->pkey == NULL) {
                resp->set_pkey(mkey->get_data(), mkey->get_prefix_size());
            }
            if (tair_mgr->should_proxy(*mkey, target_server_id)) {
                rc = TAIR_RETURN_SHOULD_PROXY;
            } else {
                plocker.lock(*mkey);
                do {
                    plugin::plugins_root *plugin_root = NULL;
                    PROFILER_BEGIN("do request plugin");
                    int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                                  TAIR_REQ_PREFIX_GETS_PACKET,
                                                                                  request->area, (request->key), NULL,
                                                                                  plugin_root);
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
                                                                      TAIR_REQ_GET_PACKET, request->area,
                                                                      (request->key), data, plugin_root);
                        PROFILER_END();

                        data_entry *skey = new data_entry();
                        int prefix_size = key->get_prefix_size();
                        skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                        skey->data_meta = key->data_meta;

                        if (rc == TAIR_RETURN_SUCCESS) {
                            resp->add_key_value(skey, data);
                            data = NULL;
                        } else {
                            delete skey;
                            skey = NULL;
                            delete data;
                            data = NULL;
                        }
                    }
                } while (false);
                plocker.unlock(*mkey);
            }
        }
    }
    resp->config_version = heart_beat->get_client_version();
    resp->setChannelId(request->getChannelId());
    resp->set_code(rc);
    resp_size = resp->size();
    r->opacket = resp;
    PROFILER_DUMP();
    PROFILER_STOP();

    send_return = false;
    return rc;
}

int request_processor::process(request_prefix_get_hiddens *request, bool &send_return, uint32_t &resp_size,
                               uint32_t &weight) {
    easy_request_t *r = request->r;
    int rc = TAIR_RETURN_FAILED;
    response_prefix_gets *resp = new response_prefix_gets();
    tair_mgr->get_stat_manager()->update(request->area, OP_PF_GET);

    if (tair_mgr->is_working() == false) {
        rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else {
        set<data_entry *, data_entry_comparator>::iterator it;
        data_entry *data = NULL;
        uint64_t target_server_id = 0;

        if (request->key_list != NULL) {
            weight = request->key_count;
            uint32_t count = 0;
            PROFILER_START("batch get operation start");
            it = request->key_list->begin();
            data_entry *mkey = *it;
            plocker.lock(*mkey);
            for (; it != request->key_list->end(); ++it) {
                data_entry *key = (*it);
                if (resp->pkey == NULL) {
                    resp->set_pkey(key->get_data(), key->get_prefix_size());
                }
                if (tair_mgr->should_proxy(*key, target_server_id)) {
                    rc = TAIR_RETURN_SHOULD_PROXY;
                    break;
                } else {
                    plugin::plugins_root *plugin_root = NULL;
                    PROFILER_BEGIN("do request plugin");
                    int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                                  TAIR_REQ_PREFIX_GET_HIDDENS_PACKET,
                                                                                  request->area, key, NULL,
                                                                                  plugin_root);
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
                                                                      TAIR_REQ_PREFIX_GET_HIDDENS_PACKET, request->area,
                                                                      key, data, plugin_root);
                        PROFILER_END();
                    }
                }
                data_entry *skey = new data_entry();
                int prefix_size = key->get_prefix_size();
                skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                skey->data_meta = key->data_meta;
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
            // set pkey
            if (resp->pkey == NULL) {
                resp->set_pkey(mkey->get_data(), mkey->get_prefix_size());
            }
            if (tair_mgr->should_proxy(*mkey, target_server_id)) {
                rc = TAIR_RETURN_SHOULD_PROXY;
            } else {
                plocker.lock(*mkey);
                do {
                    plugin::plugins_root *plugin_root = NULL;
                    PROFILER_BEGIN("do request plugin");
                    int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                                  TAIR_REQ_PREFIX_GET_HIDDENS_PACKET,
                                                                                  request->area, (request->key), NULL,
                                                                                  plugin_root);
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
                                                                      TAIR_REQ_PREFIX_GET_HIDDENS_PACKET, request->area,
                                                                      (request->key), data, plugin_root);
                        PROFILER_END();

                        data_entry *skey = new data_entry();
                        int prefix_size = key->get_prefix_size();
                        skey->set_data(key->get_data() + prefix_size, key->get_size() - prefix_size);
                        skey->data_meta = key->data_meta;

                        if (rc == TAIR_RETURN_SUCCESS || rc == TAIR_RETURN_HIDDEN) {
                            resp->add_key_value(skey, data, false, rc);
                            data = NULL;
                        } else {
                            delete skey;
                            skey = NULL;
                            delete data;
                            data = NULL;
                        }
                    }
                } while (false);
                plocker.unlock(*mkey);
            }
        }
    }
    resp->config_version = heart_beat->get_client_version();
    resp->setChannelId(request->getChannelId());
    resp->set_code(rc);
    resp_size = resp->size();
    r->opacket = resp;
    PROFILER_DUMP();
    PROFILER_STOP();

    send_return = false;
    return rc;
}

int request_processor::process(request_op_cmd *request, bool &send_return) {
    easy_request_t *r = request->r;

    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    send_return = true;

    int ret = TAIR_RETURN_SUCCESS;
    if (request->cmd == TAIR_SERVER_CMD_GET_CONFIG ||
        request->cmd == TAIR_SERVER_CMD_GET_RSYNC_STAT ||
        request->cmd == TAIR_SERVER_CMD_GET_CLUSTER_INFO ||
        request->cmd == TAIR_SERVER_CMD_RSYNC_CONNECTION_STAT ||
        (request->cmd == TAIR_SERVER_CMD_OPTIONS_RSYNC && request->params.size() == 1) ||
        request->cmd == TAIR_SERVER_CMD_START_REVISE_STAT ||
        request->cmd == TAIR_SERVER_CMD_STOP_REVISE_STAT ||
        request->cmd == TAIR_SERVER_CMD_GET_REVISE_STATUS ||
        request->cmd == TAIR_SERVER_CMD_MODIFY_CHKEXPIR_HOUR_RANGE ||
        request->cmd == TAIR_SERVER_CMD_MODIFY_MDB_CHECK_GRANULARITY ||
        request->cmd == TAIR_SERVER_CMD_MODIFY_SLAB_MERGE_HOUR_RANGE ||
        request->cmd == TAIR_SERVER_CMD_MODIFY_PUT_REMOVE_EXPIRED ||
        request->cmd == TAIR_ADMIN_SERVER_CMD_MODIFY_STAT_HIGN_OPS) {
        send_return = false;
        response_op_cmd *resp = new response_op_cmd();
        ret = tair_mgr->op_cmd(request->cmd, request->params, resp->infos);
        resp->setChannelId(request->getChannelId());
        resp->set_code(ret);
        r->opacket = resp;
    } else {
        std::vector<std::string> infos;
        ret = tair_mgr->op_cmd(request->cmd, request->params, infos);
    }
    return ret;
}

int request_processor::process(request_expire *request, bool &send_return, uint32_t &resp_size) {
    send_return = true;
    UNUSED(resp_size);
    int rc = TAIR_RETURN_FAILED;

    if (tair_mgr->is_working() == false) {
        rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    } else {
        uint64_t target_server_id = 0;
        if (tair_mgr->should_proxy(request->key, target_server_id)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
        } else {
            request->key.server_flag = request->server_flag;
            log_debug("receive expire request, serverFlag: %d", request->key.server_flag);
            PROFILER_START("expire operation start");
            plugin::plugins_root *plugin_root = NULL;
            PROFILER_BEGIN("do request plugin");
            int plugin_ret = tair_mgr->plugins_manager.do_request_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                                                                          TAIR_REQ_EXPIRE_PACKET, request->area,
                                                                          &(request->key), NULL, plugin_root);
            PROFILER_END();
            if (plugin_ret < 0) {
                log_debug("plugin return %d, skip excute", plugin_ret);
                rc = TAIR_RETURN_PLUGIN_ERROR;
            } else {
                PROFILER_BEGIN("do expire");
                rc = tair_mgr->expire(request->area, request->key, request->expired, request,
                                      heart_beat->get_client_version());
                PROFILER_END();

                PROFILER_BEGIN("do response plugin");
                tair_mgr->plugins_manager.do_response_plugins(rc, plugin::PLUGIN_TYPE_SYSTEM,
                                                              TAIR_REQ_EXPIRE_PACKET, request->area, &(request->key),
                                                              NULL, plugin_root);
                PROFILER_END();
            }
            log_debug("expire reques return: %d", rc);
            PROFILER_DUMP();
            PROFILER_STOP();
        }
    }

    return rc;
}

int request_processor::process(request_sync *request, bool &send_return) {
    send_return = true;

    if (tair_mgr->is_working() == false) {
        return TAIR_RETURN_SERVER_CAN_NOT_WORK;
    }

    PROFILER_START("sync operation start");
    int ret = TAIR_RETURN_SUCCESS;
    if (request->records == NULL || request->records->size() == 0) {
        log_warn("get a sync packet have no record");
        return ret;
    }

    uint64_t target_server_id = 0;
    if (tair_mgr->should_proxy(*(request->records->front()->key_), target_server_id)) {
        return TAIR_RETURN_SHOULD_PROXY;
    }

    PROFILER_BEGIN("do sync");
    ret = tair_mgr->sync(request->bucket, request->records, request, heart_beat->get_client_version());
    PROFILER_END();

    PROFILER_DUMP();
    PROFILER_STOP();
    return ret;
}

int request_processor::process(request_mc_ops *request, bool &send_return, uint32_t &resp_size) {
    send_return = false;
    int rc = TAIR_RETURN_SUCCESS;
    response_mc_ops *resp = NULL;
    do {
        if (!tair_mgr->is_working()) {
            rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
            break;
        }
        uint64_t target_ds = 0;
        if (tair_mgr->should_proxy(request->key, target_ds)) {
            rc = TAIR_RETURN_SHOULD_PROXY;
            break;
        }

        rc = tair_mgr->do_mc_ops(request, resp, heart_beat->get_client_version());
    } while (false);

    if (resp != NULL) { /* not TAIR_DUP_WAIT_RSP */
        resp->code = rc;
        resp_size = resp->size();
        request->r->opacket = resp;
    }
    return rc;
}

bool request_processor::do_proxy(uint64_t target_server_id, base_packet *proxy_packet, easy_request_t *r) {
    proxy_packet->server_flag = TAIR_SERVERFLAG_PROXY;
    easy_addr_t addr = easy_helper::convert_addr(target_server_id);
    addr.cidx = r->ms->c->ioth->idx;
    r->ms->c->pool->ref++;
    easy_atomic_inc(&r->ms->pool->ref);

    if (server->send_proxy(addr, proxy_packet, r) == EASY_ERROR) {
        log_error("easy_async_send error with null packet or handler");
        delete proxy_packet;
        return false;
    }
    return true;
}
}
