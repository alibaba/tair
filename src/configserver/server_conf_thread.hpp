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

#ifndef TAIR_SERVER_CONF_THREAD_H
#define TAIR_SERVER_CONF_THREAD_H

#include <algorithm>
#include <tbsys.h>
#include <easy_io.h>
#include <utime.h>
#include "mmap_file.hpp"
#include "server_info.hpp"
#include "group_info.hpp"
#include "get_group_packet.hpp"
#include "heartbeat_packet.hpp"
#include "query_info_packet.hpp"
#include "get_group_non_down_dataserver_packet.hpp"
#include "get_server_table_packet.hpp"
#include "set_master_packet.hpp"
#include "conf_heartbeat_packet.hpp"
#include "migrate_finish_packet.hpp"
#include "group_names_packet.hpp"
#include "data_server_ctrl_packet.hpp"
#include "get_migrate_machine.hpp"
#include "wait_object.hpp"
#include "op_cmd_packet.hpp"

#include "recovery_finish_packet.hpp"

namespace tair {
namespace config_server {
class server_conf_thread : public tbsys::CDefaultRunnable {
public:
    server_conf_thread(easy_io_t *eio);

    ~server_conf_thread();

    // Runnable interface
    void run(tbsys::CThread *thread, void *arg);

    void load_config_server();

    void find_group_host(request_get_group *req,
                         response_get_group *resp);

    void do_heartbeat_packet(request_heartbeat *req,
                             response_heartbeat *resp);

    void do_query_info_packet(request_query_info *, response_query_info *);

    void do_get_group_non_down_dataserver_packet(request_get_group_not_down_dataserver *req,
                                                 response_get_group_non_down_dataserver *resp);

    void do_get_server_table_packet(request_get_server_table *req,
                                    response_get_server_table *resp);

    bool do_set_master_packet(request_set_master *req, uint64_t server_id);

    uint64_t get_slave_server_id();

    void do_conf_heartbeat_packet(request_conf_heartbeart *req);

    bool do_set_server_table_packet(response_get_server_table *packet);

    //return vaule -1 response it as error 0 response it ok 1 noresponse
    int do_finish_migrate_packet(request_migrate_finish *packet);

    // add for recovery
    int do_finish_recovery_packet(request_recovery_finish *packet);

    void do_group_names_packet(response_group_names *resp);

    group_info_map *get_group_info_map();

    server_info_map *get_server_info_map();

    void set_stat_interval_time(int stat_interval_time);

    void change_server_list_to_group_file(request_data_server_ctrl *req, response_data_server_ctrl *resp);

    void get_migrating_machines(request_get_migrate_machine *req,
                                response_get_migrate_machine *resp);

    void do_op_cmd(request_op_cmd *req, response_op_cmd *resp);

private:
    enum {
        GROUP_DATA = 0,
        GROUP_CONF
    };

    server_conf_thread(const server_conf_thread &);

    server_conf_thread &operator=(const server_conf_thread &);

    void load_group_file(const char *file_name, uint32_t version,
                         uint64_t sync_server_id);

    uint32_t get_file_time(const char *file_name);

    void check_server_status(uint32_t loop_count);

    void check_config_server_status(uint32_t loop_count);

    uint64_t get_master_config_server(uint64_t id, int value);

    void get_server_table(uint64_t sync_server_id, const char *group_name,
                          int type);

    bool backup_and_write_file(const char *file_name, const char *data,
                               int size, int modified_time);

    void read_group_file_to_packet(response_get_server_table *resp);

    void send_group_file_packet();

    int load_conf(const char *group_file_name, tbsys::CConfig &config);

    int get_group_config_value(response_op_cmd *resp,
                               const vector<string> &params, const char *group_file_name,
                               const char *config_key, const char *default_value);

    int get_group_config_value_list(response_op_cmd *resp,
                                    const vector<string> &params, const char *group_file_name,
                                    const char *config_key, const char *default_value);

    int set_group_status(response_op_cmd *resp,
                         const vector<string> &params, const char *group_file_name);

    int set_area_status(response_op_cmd *resp,
                        const vector<string> &params, const char *group_file_name);

    int do_reset_ds_packet(response_op_cmd *resp, const std::vector<std::string> &params);

    int do_allocate_area(response_op_cmd *resp, const std::vector<std::string> &params, const char *group_file_name);

    int do_set_quota(response_op_cmd *resp, const std::vector<std::string> &params, const char *group_file_name);

    int set_area_quota(const char *area_key, const char *quota, const char *group_name, const char *group_file_name,
                       bool new_area, uint32_t *new_area_num);

    int do_force_migrate_bucket(response_op_cmd *resp, const std::vector<std::string> &params);

    int do_force_switch_bucket(response_op_cmd *resp, const std::vector<std::string> &params);

    int do_active_failover_or_recover(response_op_cmd *resp, const std::vector<std::string> &params);

    int do_force_replace_ds(response_op_cmd *resp, const std::vector<std::string> &params);

    bool
    check_replace_ds_params(std::map<uint64_t, uint64_t> &replace_ds_pairs, const std::vector<std::string> &params);

    int do_urgent_offline(response_op_cmd *resp, const std::vector<std::string> &params);

    void set_server_standby(server_info *p_server);

    void set_server_alive(server_info *p_server);

private:
    int packet_handler(easy_request_t *r);

    static int packet_handler_cb(easy_request_t *r) {
        server_conf_thread *_this = (server_conf_thread *) r->args;
        return _this->packet_handler(r);
    }

    easy_io_t *eio;
    easy_io_handler_pt handler;
private:
    group_info_map group_info_map_data;
    server_info_map data_server_info_map;
    server_info_map config_server_info_map;
    vector<server_info *> config_server_info_list;
    tbsys::CThreadMutex mutex_grp_need_build;
    tbsys::CRWSimpleLock group_info_rw_locker;
    tbsys::CRWSimpleLock server_info_rw_locker;
    int stat_interval_time;
    uint64_t master_config_server_id;
    uint64_t down_slave_config_server;
    tbsys::CThreadMutex group_file_mutex;

    int heartbeat_curr_time;
    bool is_ready;

    static const uint32_t server_up_inc_step = 100;
private:
    class table_builder_thread : public tbsys::CDefaultRunnable {
    public:
        explicit table_builder_thread(server_conf_thread *);

        void run(tbsys::CThread *thread, void *arg);

        ~table_builder_thread();

    public:
        vector<group_info *> group_need_build;
    private:
        server_conf_thread *p_server_conf;

        void build_table(vector<group_info *> &groupe_will_builded);
    };

    friend class table_builder_thread;

    table_builder_thread builder_thread;
};
}
}

#endif
