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

#include "heartbeat_thread.hpp"
#include "easy_helper.hpp"
#include "configuration_manager.h"
#include "ping_packet.hpp"
#include "response_return_packet.hpp"

namespace tair {
heartbeat_thread::heartbeat_thread() {
    tair_mgr = NULL;
    client_version = 0U;
    server_version = 0U;
    plugins_version = 0U;
    transition_version = 0U;
    recovery_version = 0U;
    heartbeat_packet.set_no_free();
    heartbeat_packet.server_flag = 1; // heartbeat will always with server flag
    request_count = 0;
    have_config_server_down = false;
    update = true;

    easy_helper::init_handler(&handler, this);
    handler.process = packet_handler_cb;
    memset(&eio, 0, sizeof(eio));
    easy_io_create(&eio, 1);
    eio.do_signal = 0;
    eio.no_redispatch = 1;
    eio.tcp_nodelay = 1;
    eio.tcp_cork = 0;
    //easy_eio_set_uthread_start(&eio, easy_helper::easy_set_thread_name<heartbeat_thread>, const_cast<char*>("hb_io"));
    easy_io_start(&eio);
}

heartbeat_thread::~heartbeat_thread() {
    easy_io_stop(&eio);
    easy_io_wait(&eio);
    easy_io_destroy(&eio);
}

void heartbeat_thread::set_thread_parameter(tair_manager *tair_mgr, configuration_manager *a_configuration_mgr) {
    heartbeat_packet.set_server_id(local_server_ip::ip);
    this->tair_mgr = tair_mgr;
    this->configuration_mgr = a_configuration_mgr;
}

void heartbeat_thread::set_health_check(bool flag) {
    log_warn("set health_check to %d", flag);
    this->health_check = flag;
}

vector <uint64_t> heartbeat_thread::get_config_servers() {
    return config_server_list;
}

void heartbeat_thread::run(tbsys::CThread *thread, void *arg) {
    easy_helper::set_thread_name("heartbeat");
    vector<const char *> str_list = TBSYS_CONFIG.getStringList(TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
    if (str_list.size() == 0U) {
        log_warn("miss config item %s:%s", TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
        return;
    }
    int port = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
    uint64_t id;
    for (uint32_t i = 0; i < str_list.size(); i++) {
        id = tbsys::CNetUtil::strToAddr(str_list[i], port);
        if (id == 0) continue;
        config_server_list.push_back(id);
        if (config_server_list.size() == 2U) break;
    }
    if (config_server_list.size() == 0U) {
        return;
    }

    string str_msg;
    double d;

    tzset();
    int log_rotate_time = (time(NULL) - timezone) % 86400;
    while (!_stop) {
        if (getloadavg(&d, 1) == 1) curr_load = (int) (d * 100);

        bool alive = true;
        if (health_check) {
            // throw a req into work-thread-pool and wait it to return
            // make sure dataserver is able to process req
            // timeout use default 2000ms
            base_packet *bp = (base_packet *) easy_helper::easy_sync_send(&eio, local_server_ip::ip,
                                                                          (void *) (new request_ping()), NULL,
                                                                          &handler);
            if (bp == NULL || bp->getPCode() != TAIR_RESP_RETURN_PACKET) {
                log_error("send ping packet to %s err, health check failed, skip heartbeat...",
                          tbsys::CNetUtil::addrToString(local_server_ip::ip).c_str());
                alive = false;
            } else {
                delete bp;
                bp = NULL;
            }
        }

        if (alive) {
            if (tair_mgr->if_support_admin() == true)
                heartbeat_packet.set_is_server_ready(configuration_mgr->if_all_configuration_inited());
            else
                heartbeat_packet.set_is_server_ready(true);

            heartbeat_packet.transition_info.reset(transition_version);
            heartbeat_packet.recovery_version = recovery_version;
            heartbeat_packet.config_version = server_version;
            heartbeat_packet.plugins_version = plugins_version;
            // set the proxying buckets to heartbeat packet
            heartbeat_packet.vec_bucket_no.clear();
            tair_mgr->get_proxying_buckets(heartbeat_packet.vec_bucket_no);
            heartbeat_packet.pull_migrated_info = tair_mgr->is_working() ? 0 : 1;

            int stat_size = TAIR_STAT.get_size();
            const char *stat_data = TAIR_STAT.get_and_reset();
            heartbeat_packet.set_stat_info(stat_data, stat_size);

            for (uint32_t i = 0; i < config_server_list.size(); i++) {
                request_heartbeat *new_packet = new request_heartbeat(heartbeat_packet);
                new_packet->transition_info.reset(transition_version);
                if (easy_helper::easy_async_send(&eio,
                                                 config_server_list[i],
                                                 new_packet,
                                                 (void *) ((long) i),
                                                 &handler) == EASY_ERROR) {
                    delete new_packet;
                }
            }
        }

        // rotate log
        log_rotate_time++;
        if (log_rotate_time % 86400 == 86340) {
            log_info("rotateLog End");
            TBSYS_LOGGER.rotateLog(NULL, "%d");
            log_info("rotateLog start");
        }
        if (log_rotate_time % 3600 == 3000) {
            log_rotate_time = (time(NULL) - timezone) % 86400;
        }

        TAIR_SLEEP(_stop, 1);
    }
}


int heartbeat_thread::packet_handler(easy_request_t *r) {
    base_packet *packet = (base_packet *) r->ipacket;
    if (packet == NULL) {
        log_error("heartbeat to %s timeout", easy_helper::easy_connection_to_str(r->ms->c).c_str());
        easy_session_destroy(r->ms);
        return EASY_ERROR; //~ destroy this connection
    }

    if (_stop) {
        log_warn("thread is stop, but receive packet. pcode :%d", packet->getPCode());
    } else {
        int pcode = packet->getPCode();
        int server_index = (int) ((long) r->args);

        switch (pcode) {
            case TAIR_RESP_HEARTBEAT_PACKET: {
                if (!update) {
                    log_warn("heartbeat update is down, ignore hearbeat return packet");
                    break;
                }
                response_heartbeat *resp = (response_heartbeat *) packet;
                bool need_set = true;
                if (server_index == 1 && config_server_list.size() == 2U &&
                    (config_server_list[0] & TAIR_FLAG_CFG_DOWN) == 0) {
                    need_set = false;
                }

                if (need_set && resp->client_version > 0 && resp->client_version != client_version) {
                    log_info("set _clientVersion to %d from %d", resp->client_version, client_version);
                    client_version = resp->client_version;
                }

                if (need_set && resp->server_version == 1) {
                    tair_mgr->set_solitary();
                    server_version = 1;
                }

                // log_debug("%d <> %d, %d <> %d, failover: %d, mig: %d, data_need: %d", resp->transition_info.transition_version, transition_version, resp->server_version, server_version, resp->tra

                bool need_table_updater = false;
                bool version_changed = false;
                if (need_set && resp->server_version >= TAIR_CONFIG_MIN_VERSION &&
                    resp->server_version != server_version) {
                    need_table_updater = (server_version == 0) || // first run, force update table
                                         !resp->transition_info.failovering; // no updater anyway when failovering
                    // reset server_version
                    server_version = resp->server_version;
                    version_changed = true;
                }

                if (need_set) {
                    if (need_table_updater) {
                        // reset transition_version
                        transition_version = 0;
                        recovery_version = 0;

                        uint64_t *server_list = resp->get_server_list(resp->bucket_count, resp->copy_count);
                        // update the serverTable async
                        server_table_updater *stu =
                                new server_table_updater(tair_mgr, server_list, resp->server_list_count, server_version,
                                                         resp->transition_info.data_need_move ? 1 : 0,
                                                         resp->migrated_info,
                                                         resp->copy_count, resp->bucket_count,
                                                         !resp->transition_info.failovering/*no migrate when failovering*/);
                        stu->start();
                        stu->stop();

                    } else if (version_changed && resp->transition_info.failovering) {
                        // version changed but still failovering, such as slave cs restarted
                        transition_version = 0;
                        recovery_version = 0;

                        // update the server table directly.
                        log_debug("server_version diff, but still in failovering, to read server_list");

                        uint64_t *server_list = resp->get_server_list(resp->bucket_count, resp->copy_count);
                        if (NULL != server_list) {
                            log_debug("direct update %d %d %d", resp->server_list_count, resp->copy_count,
                                      resp->bucket_count);
                            tair_mgr->direct_update_table(server_list, resp->server_list_count, resp->copy_count,
                                                          resp->bucket_count,
                                                          resp->transition_info.failovering, true, server_version);
                        } else {
                            log_error("serverTable is NULL");
                        }

                        // if group is in failovering, check the recovery_ds
                        if (resp->transition_info.failovering) {
                            recovery_bucket_updater *rbu = new recovery_bucket_updater(tair_mgr, resp->recovery_ds,
                                                                                       server_version,
                                                                                       transition_version);
                            rbu->start();
                            rbu->stop();
                        }

                    } else if (resp->transition_info.transition_version != transition_version) {
                        // reset transition_version
                        transition_version = resp->transition_info.transition_version;
                        // reset recovery_version
                        recovery_version = 0;

                        // update the server table directly.
                        log_debug("transition_version diff, to read server_list");

                        uint64_t *server_list = resp->get_server_list(resp->bucket_count, resp->copy_count);
                        if (NULL != server_list) {
                            log_debug("direct update %d %d %d", resp->server_list_count, resp->copy_count,
                                      resp->bucket_count);
                            tair_mgr->direct_update_table(server_list, resp->server_list_count, resp->copy_count,
                                                          resp->bucket_count, resp->transition_info.failovering);
                        } else {
                            log_error("serverTable is NULL");
                        }

                        // if group is in failovering, check the recovery_ds
                        if (resp->transition_info.failovering) {
                            recovery_bucket_updater *rbu = new recovery_bucket_updater(tair_mgr, resp->recovery_ds,
                                                                                       server_version,
                                                                                       transition_version);
                            rbu->start();
                            rbu->stop();
                        }

                    } else if (resp->transition_info.failovering && resp->recovery_version != recovery_version) {
                        // direct update table when one bucket recoveryed
                        // server_version and transition version remain
                        recovery_version = resp->recovery_version;

                        uint64_t *server_list = resp->get_server_list(resp->bucket_count, resp->copy_count);
                        if (NULL != server_list) {
                            log_debug("direct update %d %d %d", resp->server_list_count, resp->copy_count,
                                      resp->bucket_count);
                            tair_mgr->direct_update_table(server_list, resp->server_list_count, resp->copy_count,
                                                          resp->bucket_count, resp->transition_info.failovering);
                        } else {
                            log_debug("serverTable is NULL");
                        }
                    }
                }

                if (resp->down_slave_config_server && have_config_server_down == false) {
                    for (uint32_t i = 0; i < config_server_list.size(); i++) {
                        if ((config_server_list[i] & TAIR_FLAG_SERVER) == resp->down_slave_config_server) {
                            config_server_list[i] |= TAIR_FLAG_CFG_DOWN;
                            log_warn("config server DOWN: %s",
                                     tbsys::CNetUtil::addrToString(resp->down_slave_config_server).c_str());
                        }
                    }
                    have_config_server_down = true;
                } else if (resp->down_slave_config_server == 0 && have_config_server_down == true) {
                    for (uint32_t i = 0; i < config_server_list.size(); i++) {
                        config_server_list[i] &= TAIR_FLAG_SERVER;
                        log_warn("config server HOST UP: %s",
                                 tbsys::CNetUtil::addrToString(config_server_list[i]).c_str());
                    }
                    have_config_server_down = false;
                }

                if (need_set && resp->plugins_version != plugins_version) {
                    log_debug("get plugIns needset=%d, plugInsVersion =%d _plugInsVersion =%d", need_set,
                              resp->plugins_version, plugins_version);
                    plugins_version = resp->plugins_version;
                    plugins_updater *pud = new plugins_updater(tair_mgr, resp->plugins_dll_names);
                    pud->start(); //this will delete itself
                    pud->stop();
                }
                if (need_set && !resp->vec_area_capacity_info.empty() && !tair_mgr->is_localmode()) {
                    char area_arry[TAIR_MAX_AREA_COUNT];
                    memset(area_arry, 0, sizeof(area_arry));
                    log_error("_areaCapacityVersion = %u", resp->area_capacity_version);
                    for (vector < pair < uint32_t, uint64_t > > ::iterator it = resp->vec_area_capacity_info.begin();
                            it != resp->vec_area_capacity_info.end();
                    it++) {
                        if (it->first >= 0 && it->first < TAIR_MAX_AREA_COUNT) {
                            area_arry[it->first] = 1;
                            log_info("area %u, capacity %"
                            PRI64_PREFIX
                            "u", it->first, it->second);
                            tair_mgr->set_area_quota(static_cast<int>(it->first), it->second);
                        }
                    }
                    for (uint32_t i = 0; i < TAIR_MAX_AREA_COUNT; i++) {
                        if (area_arry[i] != 1) {
                            tair_mgr->set_area_quota(i, 0);
                        }
                    }
                    heartbeat_packet.area_capacity_version = resp->area_capacity_version;
                }
                break;
            }
            default:
                log_warn("unknow packet, pcode: %d", pcode);
        }
    }
    easy_session_destroy(r->ms);
    return EASY_OK;
}

// client_version
uint32_t heartbeat_thread::get_client_version() {
    return client_version;
}

// server_version
uint32_t heartbeat_thread::get_server_version() {
    return server_version;
}

uint32_t heartbeat_thread::get_plugins_version() {
    return plugins_version;
}
}

//////////////////
