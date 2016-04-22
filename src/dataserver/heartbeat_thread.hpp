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

#ifndef TAIR_HEART_BEAT_THREAD_H
#define TAIR_HEART_BEAT_THREAD_H

#include <pthread.h>
#include <tbsys.h>
#include <easy_io.h>
#include <easy_io_struct.h>
#include "packet_factory.hpp"
#include "tair_manager.hpp"
#include "stat_helper.hpp"
#include "heartbeat_packet.hpp"

namespace tair {
class configuration_manager;

class heartbeat_thread : public tbsys::CDefaultRunnable {
public:
    heartbeat_thread();

    ~heartbeat_thread();

    void run(tbsys::CThread *thread, void *arg);

    void set_thread_parameter(tair_manager *tair_mgr, configuration_manager *a_configuration_mgr);

    void set_health_check(bool flag);

    uint32_t get_client_version();

    uint32_t get_server_version();

    uint32_t get_plugins_version();

    vector <uint64_t> get_config_servers();

    void set_update(bool flag) { update = flag; }

public:
    uint64_t request_count;
    uint32_t curr_load;

private:
    int packet_handler(easy_request_t *r);

    static int packet_handler_cb(easy_request_t *r) {
        if (r->ms->c == NULL) {
            easy_session_destroy(r->ms);
            return EASY_ERROR;
        }
        heartbeat_thread *_this = (heartbeat_thread *) r->ms->c->handler->user_data;
        return _this->packet_handler(r);
    }

    easy_io_t eio;
    easy_io_handler_pt handler;
private:
    request_heartbeat heartbeat_packet;
    tair_manager *tair_mgr;
    configuration_manager *configuration_mgr;
    vector <uint64_t> config_server_list;
    bool have_config_server_down;
    uint32_t client_version;
    uint32_t server_version;
    uint32_t plugins_version;
    // transition status version
    uint32_t transition_version;
    // add for recovery
    uint32_t recovery_version;
    bool update;
    bool health_check;
};

class server_table_updater : public tbsys::CDefaultRunnable {
public:
    server_table_updater(tair_manager *tair_mgr, uint64_t *server_list, int server_list_count, uint32_t server_version,
                         int32_t data_need_move, vector <uint64_t> current_state_table, uint32_t copy_count,
                         uint32_t bucket_count, bool calc_migrate = true) {
        this->tair_mgr = tair_mgr;
        this->calc_migrate = calc_migrate;
        this->server_list = NULL;

        this->valid = (NULL != server_list);
        if (valid) {
            this->server_list_count = server_list_count;
            this->server_list = new uint64_t[server_list_count];
            this->server_version = server_version;
            this->data_need_move = data_need_move;
            this->current_state_table = current_state_table;
            memcpy(this->server_list, server_list, server_list_count * sizeof(uint64_t));
            this->copy_count = copy_count;
            this->bucket_count = bucket_count;
        }
    }

    ~server_table_updater() {
        if (server_list != NULL)
            delete[] server_list;
    }

    void run(tbsys::CThread *thread, void *arg) {
        pthread_detach(pthread_self());

        if (valid) {
            if (server_list_count > 0 && tair_mgr != NULL) {
                tair_mgr->update_server_table(server_list, server_list_count, server_version,
                                              data_need_move, current_state_table,
                                              copy_count, bucket_count, calc_migrate);
            }
        } else if (tair_mgr != NULL) { // deal with null server_list

            // stop the recovery_manager
            tair_mgr->reset_recovery_task();
            // delete fail log if needed
            if (calc_migrate) {
                usleep(50000);
                tair_mgr->clear_dup_depot();
            }
        }

        // wait for main thread
        while (!_stop) {
            sleep(1);
        }
        delete this;
    }

private:
    easy_io_t eio;
    tair_manager *tair_mgr;
    int server_list_count;
    uint32_t server_version;
    uint64_t *server_list;
    int32_t data_need_move;
    vector <uint64_t> current_state_table;
    uint32_t copy_count;
    uint32_t bucket_count;
    bool calc_migrate;
    bool valid;
};

class plugins_updater : public tbsys::CDefaultRunnable {
public:
    plugins_updater(tair_manager *tair_mgr, const std::vector<std::string> &plugins_dll_names_list)
            : tair_mgr(tair_mgr) {
        for (std::vector<std::string>::const_iterator it = plugins_dll_names_list.begin();
             it != plugins_dll_names_list.end(); ++it) {
            this->plugins_dll_names_list.insert(*it);
        }
    }

    ~plugins_updater() {
    }

    void run(tbsys::CThread *thread, void *arg) {
        pthread_detach(pthread_self());
        if (tair_mgr->plugins_manager.chang_plugins_to(plugins_dll_names_list)) {
            log_info("change plugins ok");
        } else {
            log_info("change plugins error");
        }
        // wait for main thread
        while (!_stop) {
            sleep(1);
        }
        delete this;

    }

private:
    tair_manager *tair_mgr;
    std::set<std::string> plugins_dll_names_list;
};

// recovery_bcuket_updater init recovery_task
class recovery_bucket_updater : public tbsys::CDefaultRunnable {
public:
    recovery_bucket_updater(tair_manager *tair_mgr, std::vector<uint64_t> recovery_ds, uint32_t server_version,
                            uint32_t transition_version) {
        this->tair_mgr = tair_mgr;
        this->recovery_ds = recovery_ds;
        this->server_version = server_version;
        this->transition_version = transition_version;
    }

    ~recovery_bucket_updater() {
    }

    void run(tbsys::CThread *thread, void *arg) {
        pthread_detach(pthread_self());
        if (tair_mgr != NULL) {
            if (recovery_ds.size() > 0) {
                log_warn("start to init work recovery_bucket_updater, server_version is %d, transition_version is %d",
                         this->server_version, this->transition_version);
                tair_mgr->update_recovery_bucket(recovery_ds, transition_version);
            } else {
                log_warn(
                        "server_version or transition_version changed in failover status, but recovery ds is empty, stop current recovery process");
                tair_mgr->reset_recovery_task();
            }
        }

        // wait for main thread
        while (!_stop) {
            sleep(1);
        }
        delete this;
    }

private:
    tair_manager *tair_mgr;
    // ds that will be recovered
    std::vector<uint64_t> recovery_ds;
    // when finish recover one bucket, send packet to cs with server_version
    // and transition_version
    uint32_t server_version;
    uint32_t transition_version;
};

}

#endif
