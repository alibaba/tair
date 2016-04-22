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

#ifndef TAIR_CLIENT_H
#define TAIR_CLIENT_H

#include <string>
#include <vector>
#include <map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>

#include <tbsys.h>

#ifdef HAVE_LIBREADLINE
#include <readline/readline.h>
#include <readline/history.h>
#endif

#include "tair_client_api_impl.hpp"
#include "flowrate.h"

using namespace std;

namespace tair {

#define CMD_MAX_LEN 4096

typedef vector<char *> VSTRING;

class tair_client;

typedef void (tair_client::*cmd_call)(VSTRING &param);

typedef map<string, cmd_call> str_cmdcall_map;
typedef str_cmdcall_map::iterator str_cmdcall_map_iter;

class tair_client {
public:
    tair_client();

    ~tair_client();

    bool parse_cmd_line(int argc, char *const argv[]);

    bool start();

    void cancel();

    class flow_sort_item {
    public:
        int ns;
        tair::stat::Flowrate rate;

        flow_sort_item(int ns, tair::stat::Flowrate rate) {
            this->ns = ns;
            this->rate = rate;
        }

        static bool sort_ops(flow_sort_item i, flow_sort_item j) { return (i.rate.ops > j.rate.ops); }

        static bool sort_in(flow_sort_item i, flow_sort_item j) { return (i.rate.in > j.rate.in); }

        static bool sort_out(flow_sort_item i, flow_sort_item j) { return (i.rate.out > j.rate.out); }

        static bool sort_total(flow_sort_item i, flow_sort_item j) {
            return ((i.rate.in + i.rate.out) > (j.rate.in + j.rate.out));
        }
    };

private:
    void print_usage(char *prog_name);

    uint64_t get_ip_address(char *str, int port);

    cmd_call parse_cmd(char *key, VSTRING &param);

    void print_help(const char *cmd);

    char *canonical_key(char *key, char **akey, int *size, int format = 0);

    void set_flow_limit_bound(int ns, int lower, int upper, tair::stat::FlowType type);

    void print_stat(int32_t size_unit, const map<string, vector<int64_t> > &stat);

    void default_print_op_cmd_result_map(const op_cmd_result_map &result);

    int64_t ping(uint64_t server_id);

    void do_cmd_get_flow_rate(VSTRING &param);

    void do_cmd_get_flow_top(VSTRING &param);

    void do_cmd_set_flow_limit_bound(VSTRING &param);

    void do_cmd_set_flow_policy(VSTRING &param);

    void do_cmd_quit(VSTRING &param);

    void do_cmd_help(VSTRING &param);

    void do_cmd_put(VSTRING &param);

    void do_cmd_addcount(VSTRING &param);

    void do_cmd_get(VSTRING &param);

    void do_cmd_aget(VSTRING &param);

    void do_cmd_mget(VSTRING &param);

    void do_cmd_remove(VSTRING &param);

    void do_cmd_mremove(VSTRING &param);

    void do_cmd_stat(VSTRING &param);

    void do_cmd_health(VSTRING &param);

    //void  doCmdAddItems(VSTRING &param);
    void do_cmd_remove_area(VSTRING &param);

    void do_cmd_dump_area(VSTRING &param);

    void do_cmd_hide(VSTRING &params);

    void do_cmd_get_hidden(VSTRING &params);

    void do_cmd_prefix_get(VSTRING &params);

    void do_cmd_prefix_gets(VSTRING &params);

    void do_cmd_prefix_put(VSTRING &params);

    void do_cmd_prefix_puts(VSTRING &params);

    void do_cmd_prefix_remove(VSTRING &params);

    void do_cmd_prefix_removes(VSTRING &params);

    void do_cmd_prefix_get_hidden(VSTRING &params);

    void do_cmd_prefix_hide(VSTRING &params);

    void do_cmd_prefix_hides(VSTRING &params);

    void do_cmd_setstatus(VSTRING &params);

    void do_cmd_getstatus(VSTRING &params);

    void do_cmd_gettmpdownsvr(VSTRING &params);

    void do_cmd_resetserver(VSTRING &params);

    void do_cmd_migrate_bucket(VSTRING &params);

    void do_cmd_switch_bucket(VSTRING &params);

    void do_cmd_active_failover(VSTRING &params);

    void do_cmd_active_recover(VSTRING &params);

    void do_cmd_replace_ds(VSTRING &params);

    void do_cmd_urgent_offline(VSTRING &params);

    void do_cmd_flushmmt(VSTRING &params);

    void do_cmd_resetdb(VSTRING &params);

    void do_cmd_set_migrate_wait_ms(VSTRING &param);

    void do_cmd_stat_db(VSTRING &param);

    void do_cmd_release_mem(VSTRING &param);

    void do_cmd_backup_db(VSTRING &param);

    void do_cmd_unload_backuped_db(VSTRING &param);

    void do_cmd_pause_gc(VSTRING &param);

    void do_cmd_resume_gc(VSTRING &param);

    void do_cmd_pause_rsync(VSTRING &param);

    void do_cmd_resume_rsync(VSTRING &param);

    void do_cmd_watch_rsync(VSTRING &param);

    void do_cmd_stat_rsync(VSTRING &param);

    void do_cmd_options_rsync(VSTRING &param);

    void do_cmd_get_rsync_stat(VSTRING &param);

    void do_cmd_get_cluster_info(VSTRING &param);

    void do_cmd_rsync_connection_stat(VSTRING &param);

    void do_cmd_start_balance(VSTRING &param);

    void do_cmd_stop_balance(VSTRING &param);

    void do_cmd_set_balance_wait_ms(VSTRING &param);

    void do_cmd_start_revise_stat(VSTRING &param);

    void do_cmd_stop_revise_stat(VSTRING &param);

    void do_cmd_get_revise_status(VSTRING &param);

    void do_cmd_set_config(VSTRING &param);

    void do_cmd_get_config(VSTRING &param);

    void do_cmd_readonly_on(VSTRING &param);

    void do_cmd_readonly_off(VSTRING &param);

    void do_cmd_readonly_status(VSTRING &param);

    void do_cmd_set_rsync_config_service(VSTRING &param);

    void do_cmd_set_rsync_config_update_interval(VSTRING &param);

    void do_cmd_ldb_instance_config(VSTRING &param);

    void do_cmd_new_namespace(VSTRING &param);

    void do_cmd_add_namespace(VSTRING &param);

    void do_cmd_delete_namespace(VSTRING &param);

    void do_cmd_delete_namespaceall(VSTRING &param);

    void do_cmd_link_namespace(VSTRING &param);

    void do_cmd_reset_namespace(VSTRING &param);

    void do_cmd_switch_namespace(VSTRING &param);

    void do_cmd_op_ds_or_not(VSTRING &param, const char *cmd_str, ServerCmdType cmd_type, int base_param_size = 0);

    void do_cmd_op_ds(const vector<string> &param, const char *cmd_str, ServerCmdType cmd_type,
                      int base_param_size, op_cmd_result_map &result);

    void do_cmd_mc_set(VSTRING &param);

    void do_cmd_mc_add(VSTRING &param);

    void do_cmd_mc_append(VSTRING &param);

    void do_cmd_mc_prepend(VSTRING &param);

    void do_cmd_to_inval(VSTRING &param);

    void do_cmd_alloc_area(VSTRING &param);

    void do_cmd_modify_area(VSTRING &param);

    void do_rt(VSTRING &param);

    void do_rt_enable(VSTRING &param);

    void do_rt_disable(VSTRING &param);

    void do_rt_reload(VSTRING &param);

    void do_rt_reload_light(VSTRING &param);

    void do_cmd_opkill(VSTRING &param);

    void do_cmd_opkill_disable(VSTRING &param);

    void do_cmd_modify_high_ops_upper(VSTRING &params);

    void do_cmd_modify_chkexpir_hour_range(VSTRING &params);

    void do_cmd_modify_mdb_check_granularity(VSTRING &params);

    void do_cmd_modify_slab_merge_hour_range(VSTRING &params);

    void do_cmd_modify_put_remove_expired(VSTRING &params);

private:
    void active_failover_or_recover(VSTRING &params, bool is_failover);

    static void async_get_cb(int retcode, const data_entry *, const data_entry *, void *);

    static void async_mc_set_cb(int retcode, response_mc_ops *resp, void *args);

    int do_cmd_inval_retryall(VSTRING &param);

    int do_cmd_inval_retrieve(VSTRING &param);

    int do_cmd_inval_info(VSTRING &param);

#ifdef HAVE_LIBREADLINE
    char *input(char *buffer, size_t size);
    void update_history(const char *line);
#endif
    str_cmdcall_map cmd_map;

    tair_client_impl client_helper;
    const char *server_addr;
    const char *slave_server_addr;
    char *group_name;
    char *cmd_line;
    char *cmd_file_name;
    bool is_config_server;
    bool is_data_server;
    bool is_cancel;
    int key_format;
    int default_area;
};
}

#endif
///////////////////////END
