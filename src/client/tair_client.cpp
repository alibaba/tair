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

#include "tair_client.hpp"
#include "define.hpp"
#include "util.hpp"
#include "dump_data_info.hpp"
#include "query_info_packet.hpp"
#include "key_value_pack.hpp"
#include "mc_ops_packet.hpp"

#include <byteswap.h>

namespace tair {

tair_client::tair_client() {
    is_config_server = false;
    is_data_server = false;
    //server_id = 0;
    //m_slaveServerId = 0;
    group_name = NULL;
    cmd_line = NULL;
    cmd_file_name = NULL;
    is_cancel = false;
    server_addr = NULL;
    slave_server_addr = NULL;

    key_format = 0;
    default_area = 0;

    cmd_map["help"] = &tair_client::do_cmd_help;
    cmd_map["quit"] = &tair_client::do_cmd_quit;
    cmd_map["exit"] = &tair_client::do_cmd_quit;
    cmd_map["put"] = &tair_client::do_cmd_put;
    cmd_map["incr"] = &tair_client::do_cmd_addcount;
    cmd_map["get"] = &tair_client::do_cmd_get;
    cmd_map["aget"] = &tair_client::do_cmd_aget;
    cmd_map["mget"] = &tair_client::do_cmd_mget;
    cmd_map["remove"] = &tair_client::do_cmd_remove;
    cmd_map["mremove"] = &tair_client::do_cmd_mremove;
    cmd_map["delall"] = &tair_client::do_cmd_remove_area;
    cmd_map["dump"] = &tair_client::do_cmd_dump_area;
    cmd_map["stat"] = &tair_client::do_cmd_stat;
    cmd_map["ls"] = &tair_client::do_cmd_stat;
    cmd_map["health"] = &tair_client::do_cmd_health;
    cmd_map["hide"] = &tair_client::do_cmd_hide;
    cmd_map["gethidden"] = &tair_client::do_cmd_get_hidden;
    cmd_map["pput"] = &tair_client::do_cmd_prefix_put;
    cmd_map["pputs"] = &tair_client::do_cmd_prefix_puts;
    cmd_map["pget"] = &tair_client::do_cmd_prefix_get;
    cmd_map["pgets"] = &tair_client::do_cmd_prefix_gets;
    cmd_map["premove"] = &tair_client::do_cmd_prefix_remove;
    cmd_map["premoves"] = &tair_client::do_cmd_prefix_removes;
    cmd_map["pgethidden"] = &tair_client::do_cmd_prefix_get_hidden;
    cmd_map["phide"] = &tair_client::do_cmd_prefix_hide;
    cmd_map["phides"] = &tair_client::do_cmd_prefix_hides;
    cmd_map["flowlimit"] = &tair_client::do_cmd_set_flow_limit_bound;
    cmd_map["flowrate"] = &tair_client::do_cmd_get_flow_rate;
    cmd_map["flowtop"] = &tair_client::do_cmd_get_flow_top;
    cmd_map["flow_policy"] = &tair_client::do_cmd_set_flow_policy;
    cmd_map["setstatus"] = &tair_client::do_cmd_setstatus;
    cmd_map["getstatus"] = &tair_client::do_cmd_getstatus;
    cmd_map["gettmpdownsvr"] = &tair_client::do_cmd_gettmpdownsvr;
    cmd_map["resetserver"] = &tair_client::do_cmd_resetserver;
    cmd_map["migrate_bucket"] = &tair_client::do_cmd_migrate_bucket;
    cmd_map["switch_bucket"] = &tair_client::do_cmd_switch_bucket;
    cmd_map["active_failover"] = &tair_client::do_cmd_active_failover;
    cmd_map["active_recover"] = &tair_client::do_cmd_active_recover;
    cmd_map["replace_ds"] = &tair_client::do_cmd_replace_ds;
    cmd_map["urgent_offline"] = &tair_client::do_cmd_urgent_offline;
    cmd_map["flushmmt"] = &tair_client::do_cmd_flushmmt;
    cmd_map["resetdb"] = &tair_client::do_cmd_resetdb;
    cmd_map["set_migrate_wait"] = &tair_client::do_cmd_set_migrate_wait_ms;
    cmd_map["stat_db"] = &tair_client::do_cmd_stat_db;
    cmd_map["release_mem"] = &tair_client::do_cmd_release_mem;
    cmd_map["backup_db"] = &tair_client::do_cmd_backup_db;
    cmd_map["unload_backuped_db"] = &tair_client::do_cmd_unload_backuped_db;
    cmd_map["pause_gc"] = &tair_client::do_cmd_pause_gc;
    cmd_map["resume_gc"] = &tair_client::do_cmd_resume_gc;
    cmd_map["start_balance"] = &tair_client::do_cmd_start_balance;
    cmd_map["stop_balance"] = &tair_client::do_cmd_stop_balance;
    cmd_map["set_balance_wait"] = &tair_client::do_cmd_set_balance_wait_ms;
    cmd_map["pause_rsync"] = &tair_client::do_cmd_pause_rsync;
    cmd_map["resume_rsync"] = &tair_client::do_cmd_resume_rsync;
    cmd_map["get_rsync_stat"] = &tair_client::do_cmd_get_rsync_stat;
    cmd_map["watch_rsync"] = &tair_client::do_cmd_watch_rsync;
    cmd_map["stat_rsync"] = &tair_client::do_cmd_stat_rsync;
    cmd_map["options_rsync"] = &tair_client::do_cmd_options_rsync;
    cmd_map["get_cluster_info"] = &tair_client::do_cmd_get_cluster_info;
    cmd_map["rsync_connection_stat"] = &tair_client::do_cmd_rsync_connection_stat;
    cmd_map["set_rsync_config_service"] = &tair_client::do_cmd_set_rsync_config_service;
    cmd_map["set_rsync_config_update_interval"] = &tair_client::do_cmd_set_rsync_config_update_interval;
    cmd_map["start_revise_stat"] = &tair_client::do_cmd_start_revise_stat;
    cmd_map["stop_revise_stat"] = &tair_client::do_cmd_stop_revise_stat;
    cmd_map["set_config"] = &tair_client::do_cmd_set_config;
    cmd_map["get_config"] = &tair_client::do_cmd_get_config;
    cmd_map["ldb_instance_config"] = &tair_client::do_cmd_ldb_instance_config;
    cmd_map["new_nsu"] = &tair_client::do_cmd_new_namespace;
    cmd_map["add_ns"] = &tair_client::do_cmd_add_namespace;
    cmd_map["link_ns"] = &tair_client::do_cmd_link_namespace;
    cmd_map["delete_ns"] = &tair_client::do_cmd_delete_namespaceall;
    cmd_map["delete_nsu"] = &tair_client::do_cmd_delete_namespace;
    cmd_map["reset_ns"] = &tair_client::do_cmd_reset_namespace;
    cmd_map["switch_ns"] = &tair_client::do_cmd_switch_namespace;
    cmd_map["readonly_on"] = &tair_client::do_cmd_readonly_on;
    cmd_map["readonly_off"] = &tair_client::do_cmd_readonly_off;
    cmd_map["readonly_status"] = &tair_client::do_cmd_readonly_status;
    //cmd_map["mcput"] = &tair_client::do_cmd_mcput;
    cmd_map["mc_set"] = &tair_client::do_cmd_mc_set;
    cmd_map["mc_add"] = &tair_client::do_cmd_mc_add;
    cmd_map["mc_append"] = &tair_client::do_cmd_mc_append;
    cmd_map["mc_prepend"] = &tair_client::do_cmd_mc_prepend;
    cmd_map["invalcmd"] = &tair_client::do_cmd_to_inval;
    // cmd_map["additems"] = &tair_client::doCmdAddItems;
    cmd_map["alloc_area"] = &tair_client::do_cmd_alloc_area;
    cmd_map["modify_area"] = &tair_client::do_cmd_modify_area;
    cmd_map["rt"] = &tair_client::do_rt;
    cmd_map["rt_enable"] = &tair_client::do_rt_enable;
    cmd_map["rt_disable"] = &tair_client::do_rt_disable;
    cmd_map["rt_reload"] = &tair_client::do_rt_reload;
    cmd_map["rt_reload_light"] = &tair_client::do_rt_reload_light;
    cmd_map["opkill"] = &tair_client::do_cmd_opkill;
    cmd_map["opkill_disable"] = &tair_client::do_cmd_opkill_disable;
    // revise stat
    cmd_map["start_revise_stat"] = &tair_client::do_cmd_start_revise_stat;
    cmd_map["stop_revise_stat"] = &tair_client::do_cmd_stop_revise_stat;
    cmd_map["get_revise_status"] = &tair_client::do_cmd_get_revise_status;
    cmd_map["modify_high_ops_upper"] = &tair_client::do_cmd_modify_high_ops_upper;
    cmd_map["modify_chkexpir_hour_range"] = &tair_client::do_cmd_modify_chkexpir_hour_range;
    cmd_map["modify_mdb_check_granularity"] = &tair_client::do_cmd_modify_mdb_check_granularity;
    cmd_map["modify_slab_merge_hour_range"] = &tair_client::do_cmd_modify_slab_merge_hour_range;
    cmd_map["modify_put_remove_expired"] = &tair_client::do_cmd_modify_put_remove_expired;
}

tair_client::~tair_client() {
    if (group_name) {
        free(group_name);
        group_name = NULL;
    }
    if (cmd_line) {
        free(cmd_line);
        cmd_line = NULL;
    }
    if (cmd_file_name) {
        free(cmd_file_name);
        cmd_file_name = NULL;
    }
}

void tair_client::print_usage(char *prog_name) {
    fprintf(stderr, "%s -s server:port\n OR\n%s -c configserver:port -g groupname\n\n"
                    "    -s, --server           data server,default port:%d\n"
                    "    -c, --configserver     default port: %d\n"
                    "    -g, --groupname        group name\n"
                    "    -l, --cmd_line         exec cmd\n"
                    "    -q, --cmd_file         script file\n"
                    "    -h, --help             print this message\n"
                    "    -v, --verbose          print debug info\n"
                    "    -V, --version          print version\n\n",
            prog_name, prog_name, TAIR_SERVER_DEFAULT_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
}

bool tair_client::parse_cmd_line(int argc, char *const argv[]) {
    int opt;
    const char *optstring = "s:c:g:hVvl:q:";
    struct option longopts[] = {
            {"server",       1, NULL, 's'},
            {"configserver", 1, NULL, 'c'},
            {"groupname",    1, NULL, 'g'},
            {"cmd_line",     1, NULL, 'l'},
            {"cmd_file",     1, NULL, 'q'},
            {"help",         0, NULL, 'h'},
            {"verbose",      0, NULL, 'v'},
            {"version",      0, NULL, 'V'},
            {0,              0, 0,    0}
    };

    opterr = 0;
    while ((opt = getopt_long(argc, argv, optstring, longopts, NULL)) != -1) {
        switch (opt) {
            case 'c': {
                if (server_addr != NULL && is_config_server == false) {
                    return false;
                }
                //uint64_t id = tbsys::CNetUtil::strToAddr(optarg, TAIR_CONFIG_SERVER_DEFAULT_PORT);

                if (server_addr == NULL) {
                    server_addr = optarg;
                } else {
                    slave_server_addr = optarg;
                }
                is_config_server = true;
            }
                break;
            case 's': {
                if (server_addr != NULL && is_config_server == true) {
                    return false;
                }
                server_addr = optarg;
                is_data_server = true;
                //server_id = tbsys::CNetUtil::strToAddr(optarg, TAIR_SERVER_DEFAULT_PORT);
            }
                break;
            case 'g':
                group_name = strdup(optarg);
                break;
            case 'l':
                cmd_line = strdup(optarg);
                break;
            case 'q':
                cmd_file_name = strdup(optarg);
                break;
            case 'v':
                TBSYS_LOGGER.setLogLevel("DEBUG");
                break;
            case 'V':
                fprintf(stderr, "BUILD_TIME: %s %s\n", __DATE__, __TIME__);
                return false;
            case 'h':
                print_usage(argv[0]);
                return false;
        }
    }
    if (server_addr == NULL || (is_config_server == true && group_name == NULL)) {
        print_usage(argv[0]);
        return false;
    }
    return true;
}

cmd_call tair_client::parse_cmd(char *key, VSTRING &param) {
    cmd_call cmdCall = NULL;
    char *token;
    while (*key == ' ') key++;
    token = key + strlen(key);
    while (*(token - 1) == ' ' || *(token - 1) == '\n' || *(token - 1) == '\r') token--;
    *token = '\0';
    if (key[0] == '\0') {
        return NULL;
    }
    token = strchr(key, ' ');
    if (token != NULL) {
        *token = '\0';
    }
    str_cmdcall_map_iter it = cmd_map.find(tbsys::CStringUtil::strToLower(key));
    if (it == cmd_map.end()) {
        return NULL;
    } else {
        cmdCall = it->second;
    }
    if (token != NULL) {
        token++;
        key = token;
    } else {
        key = NULL;
    }
    param.clear();
    while ((token = strsep(&key, " ")) != NULL) {
        if (token[0] == '\0') continue;
        param.push_back(token);
    }
    return cmdCall;
}

void tair_client::cancel() {
    is_cancel = true;
}

bool tair_client::start() {
    bool done = true;
    client_helper.set_timeout(5000);
    client_helper.set_force_service(true);
    if (is_config_server) {
        done = client_helper.startup(server_addr, slave_server_addr, group_name);
    } else {
        if (is_data_server) {
            done = client_helper.directup(server_addr);
        } else {
            //done = client_helper.startup(server_id);
            done = false;
        }
    }
    if (done == false) {
        fprintf(stderr, "%s cann't connect.\n", server_addr);
        return false;
    }

    char buffer[CMD_MAX_LEN];
    VSTRING param;

    if (cmd_line != NULL) {
        strcpy(buffer, cmd_line);
        vector<char *> cmd_list;
        tbsys::CStringUtil::split(buffer, ";", cmd_list);
        for (uint32_t i = 0; i < cmd_list.size(); i++) {
            cmd_call this_cmd_call = parse_cmd(cmd_list[i], param);
            if (this_cmd_call == NULL) {
                continue;
            }
            (this->*this_cmd_call)(param);
        }
    } else if (cmd_file_name != NULL) {
        FILE *fp = fopen(cmd_file_name, "rb");
        if (fp != NULL) {
            while (fgets(buffer, CMD_MAX_LEN, fp)) {
                cmd_call this_cmd_call = parse_cmd(buffer, param);
                if (this_cmd_call == NULL) {
                    fprintf(stderr, "unknown command.\n\n");
                    continue;
                }
                (this->*this_cmd_call)(param);
            }
            fclose(fp);
        } else {
            fprintf(stderr, "open failure: %s\n\n", cmd_file_name);
        }
    } else {
        while (done) {
#ifdef HAVE_LIBREADLINE
                                                                                                                                    char *line = input(buffer, CMD_MAX_LEN);
           if (line == NULL) break;
#else
            if (fprintf(stderr, "TAIR> ") < 0) break;
            if (fgets(buffer, CMD_MAX_LEN, stdin) == NULL) {
                break;
            }
#endif
            cmd_call this_cmd_call = parse_cmd(buffer, param);
            if (this_cmd_call == NULL) {
                fprintf(stderr, "unknown command.\n\n");
                continue;
            }
            if (this_cmd_call == &tair_client::do_cmd_quit) {
                break;
            }
            (this->*this_cmd_call)(param);
            is_cancel = false;
        }
    }

    // stop
    client_helper.close();

    return true;
}

//! you should duplicate the result
#ifdef HAVE_LIBREADLINE
                                                                                                                        char* tair_client::input(char *buffer, size_t size) {
     static char prompt[64];
     static int color = 30;
     color = ++color == 37 ? 31 : color;

     snprintf(prompt, sizeof(prompt), "\033[1;%dmTAIR>\033[0m ", color);
     char *line = NULL;
     line = readline(prompt);
     if (line == NULL) return NULL;
     if (*line == '\0') {
       strncpy(buffer, "stat", size);
     } else {
       update_history(line);
       strncpy(buffer, line, size);
     }
     free(line);
     return buffer;
   }
   void tair_client::update_history(const char *line) {
     int i = 0;
     HIST_ENTRY **the_history = history_list();
     for (; i < history_length; ++i) {
       HIST_ENTRY *hist = the_history[i];
       if (strcmp(hist->line, line) == 0) {
         break;
       }
     }
     if (i == history_length) {
       add_history(line);
       return ;
     }
     HIST_ENTRY *hist = the_history[i];
     for (; i < history_length - 1; ++i) {
       the_history[i] = the_history[i+1];
     }
     the_history[i] = hist;
   }
#endif

///////////////////////////////////////////////////////////////////////////////////////////////////
int64_t tair_client::ping(uint64_t server_id) {
    if (server_id == 0ul) {
        return 0L;
    }
    int ping_count = 10;
    int64_t total = 0;
    for (int i = 0; i < ping_count; ++i) {
        total += client_helper.ping(server_id);
    }
    return total / ping_count;
}

void tair_client::do_cmd_quit(VSTRING &param) {
    return;
}

void tair_client::print_help(const char *cmd) {
    if (cmd == NULL || strcmp(cmd, "put") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : put key data [area] [expired]\n"
                        "DESCRIPTION: area   - namespace , default: 0\n"
                        "             expired- in seconds, default: 0,never expired\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "incr") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : incr key [count] [initValue] [area]\n"
                        "DESCRIPTION: initValue , default: 0\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "get") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : get key [area]\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "mget") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : mget key1 ... keyn area\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "remove") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : remove key [area]\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "mremove") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : mremove key1 ... keyn area\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "delall") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : delall area [all | -s ip:port]\n"
                        "DESCRIPTION: delete all data of area\n"
        );

    }
    if (cmd == NULL || strcmp(cmd, "stat") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : stat [-a|--all -e|--area num -d|--dataserver -c|--ds-area -b -k -g -m]\n"
                        "DESCRIPTION: get stat info\n"
                        "             -a|--all  display area, dataserver, ds-area all info, default\n"
                        "             -e|--area num,  only display area info, num specify which area to display,"
                        " if -1, display all area\n"
                        "             -d|--dataserver only display dataserver info\n"
                        "             -c|--ds-area only display dataserver-area info\n"
                        "             -b  display number in bytes or one_unit, this is default\n"
                        "             -k  display number in kilo_unit, 1K=1024B(or one_unit)\n"
                        "             -m  display number in mega_unit, 1M=1024K\n"
                        "             -g  display number in giga_unit, 1G=1024M\n"
                        "             short options can be put together, e.g. -e 23 -d -m = -dme23\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "health") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : health\n"
                        "DESCRIPTION: get cluster health info\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "dump") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : dump dumpinfo.txt\n"
                        "DESCRIPTION: dumpinfo.txt is a config file of dump,syntax:\n"
                        "area start_time end_time,eg:"
                        "10 2008-12-09 12:08:07 2008-12-10 12:10:00\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "setstatus") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : setstatus group status\n"
                        "DESCRIPTION: set group to on or off\n"
                        "\tgroup: groupname to set, status: on/off\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "getstatus") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : getstatus group1 group2...\n"
                        "DESCRIPTION: get status of group(s)\n"
                        "\tgroup[n]: groupnames of which to get status\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "flushmmt") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : flushmmt [ds_addr]\n"
                        "DESCRIPTION: flush memtable of all tairserver or specified `ds_addr. WARNING: use this cmd carefully\n"
                        "\tds_addr: address of tairserver\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "resetdb") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : resetdb [ds_addr]\n"
                        "DESCRIPTION: reset db of all tairserver or specified `ds_addr. WARNING: use this cmd carefully\n"
                        "\tds_addr: address of tairserver\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "hide") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: hide [area] key\n"
                        "DESCRIPTION: to hide one item\n");
    }
    if (cmd == NULL || strcmp(cmd, "gethidden") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: gethidden [area] key\n"
                        "DESCRIPTION: to get one hidden item\n");
    }
    if (cmd == NULL || strcmp(cmd, "pput") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: pput [area] pkey skey value\n"
                        "DESCRIPTION: to put one item with prefix\n");
    }
    if (cmd == NULL || strcmp(cmd, "pget") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: pget [area] pkey skey\n"
                        "DESCRIPTION: to get one item with prefix\n");
    }
    if (cmd == NULL || strcmp(cmd, "pgets") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: pgets area pkey skey1 skey2...skeyn\n"
                        "DESCRIPTION: to get multiple items with prefix\n");
    }
    if (cmd == NULL || strcmp(cmd, "pputs") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: pputs area pkey skey1 value1 skey2 value2...skeyn valuen\n"
                        "DESCRIPTION: to put multiple items with prefix\n");
    }
    if (cmd == NULL || strcmp(cmd, "premove") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: premove [area] pkey skey\n"
                        "DESCRIPTION: to remove one item with prefix\n");
    }
    if (cmd == NULL || strcmp(cmd, "premoves") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: premoves area pkey skey1...skeyn\n"
                        "DESCRIPTION: to remove multiple items with prefix\n");
    }
    if (cmd == NULL || strcmp(cmd, "pgethidden") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: pgethidden [area] pkey skey\n"
                        "DESCRIPTION: to get one hidden item with prefix\n");
    }
    if (cmd == NULL || strcmp(cmd, "invalcmd") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS(0)   : invalcmd [retryall [ip:port] | retrieve | info ip::iport]\n"
                        "DESCRIPTION: retry all request, retrieve all inval servers, get the inval server's info.\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "phide") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: phide [area] pkey skey\n"
                        "DESCRIPTION: to hide one item with prefix\n");
    }
    if (cmd == NULL || strcmp(cmd, "phides") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: phides area pkey skey1...skeyn\n"
                        "DESCRIPTION: to hide multiple items with prefix\n");
    }
    if (cmd == NULL || strcmp(cmd, "flowlimit") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: flowlimit ns lower upper type, example flowlimit 123 30000 40000 ops\n"
                        "DESCRIPTION: set or view flow limit bound\n");
    }
    if (cmd == NULL || strcmp(cmd, "flowrate") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: flowrate ns \n"
                        "DESCRIPTION: view flow rate\n");
    }
    if (cmd == NULL || strcmp(cmd, "flowtop") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: flowtop ds_addr [top_num max_area]\n"
                        "DESCRIPTION: get top flow area\n");
    }
    if (cmd == NULL || strcmp(cmd, "flow_policy") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS: flow_policy ns policy_type[0 or 1]\n"
                        "DESCRIPTION: set ns flow policy\n"
                        "eg: flow_policy 100 0");
    }

    if (cmd == NULL || strcmp(cmd, "gettmpdownsvr") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : gettmpdownsvr group1 group2...\n"
                        "DESCRIPTION: get tmp down servers of group(s)\n"
                        "\tgroup[n]: groupnames of which to get status\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "resetserver") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : resetserver group [ds_addr ds_addr]\n"
                        "DESCRIPTION: clear the all or some specified by `ds_addr down server in group, namely 'tmp_down_server' in group.conf\n"
                        "\tgroup: groupname to reset\n"
                        "\tds_addr: dataserver to reset\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "migrate_bucket") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : migrate_bucket group bucket_no copy_no src_ds_addr(ip:port) dest_ds_addr(ip:port)\n"
                        "DESCRIPTION: force migrate bucket bucket_no from src_ds_addr to dest_ds_addr\n"
                        "\tgroup: groupname to migrate\n"
                        "\tbucket_no: bucket_no to migrate\n"
                        "\tcopy_no: which copy of bucket_no to migrate\n"
                        "\tsrc_ds_addr: who hold this copy of bucket_no, for check\n"
                        "\tdest_ds_addr: migrate destnation\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "switch_bucket") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : switch_bucket group bucket_nos\n"
                        "DESCRIPTION: force siwtch master and slave of input bucket_nos\n"
                        "\tgroup: groupname to switch\n"
                        "\tbucket_nos: bucket_nos to switch, eg: 1,2,3,4,101\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "active_failover") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : active_failover group ds_addr\n"
                        "DESCRIPTION: active failover one ds when it is still alive, for the sake of update and so on...\n"
                        "\tgroup: groupname to do active_failover\n"
                        "\tds_addr: dataserver to do active failover, eg: xxx.xxx.xxx.xxx:5191\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "active_recover") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : active_recover group ds_addr\n"
                        "DESCRIPTION: active recover one ds as it may be active failovered before\n"
                        "\tgroup: groupname to do active_recover\n"
                        "\tds_addr: dataserver to do active recover, eg: xxx.xxx.xxx.xxx:5191\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "replace_ds") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : replace_ds group ds_cnt src_ds dst_ds\n"
                        "DESCRIPTION: replace src_ds by dst_ds\n"
                        "\tgroup: groupname to do replace_ds\n"
                        "\tsrc_ds: dataservers to be replaced, eg: xxx.xxx.xxx.xxx:5191,xxx.xxx.xxx.xxx:5191\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "urgent_offline") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : urgent_offline group src_ds dst_ds\n"
                        "DESCRIPTION: urgent offline src_ds by dst_ds\n"
                        "\tgroup: groupname to do urgent offline\n"
                        "\tsrc_ds: dataserver which is already down, eg: xxx.xxx.xxx.xxx:5191\n"
                        "\tdst_ds: dataservers which to replace down server, eg: xxx.xxx.xxx.xxx:5191,xxx.xxx.xxx.xxx:5191\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "set_migrate_wait") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : set_migrate_wait time_ms [ds_addr]\n"
                        "DESCRIPTION: set migrate wait  time ms to all or one specified by `ds_addr\n"
                        "\ttime_ms : time in ms unit\n"
                        "\tds_addr : specified dataserver address\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "new_nsu") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : new_nsu namepsace\n"
                        "DESCRIPTION: new namespace unit\n"
                        "\tnamespace : new namespace unit\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "add_ns") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : add_ns namepsace new_namespace\n"
                        "DESCRIPTION: add namespace to another unit\n"
                        "\tnamespace : existed namespace unit\n"
                        "\tnew_namespace : namespace to add\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "link_ns") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : link_ns target_namespace link_namespace\n"
                        "DESCRIPTION: link namespace to another\n"
                        "\ttarget_namespace : target namespace\n"
                        "\tlink_namespace : new link namespace\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "delete_ns") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : delete_ns namepsace\n"
                        "DESCRIPTION: remove a namespace, if no more namespace in this unit, delete dir.\n"
                        "\tnamespace : namespace to be deleting\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "delete_ns_all") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : delete_ns_all namepsace\n"
                        "DESCRIPTION: remove a namespace recusive\n"
                        "\tnamespace : namespace to be deleting\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "reset_ns") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : reset_ns namepsace\n"
                        "DESCRIPTION: reset namespace for dump\n"
                        "\tnamespace : namespace to be resetting\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "reset_ns") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : reset_ns namepsace\n"
                        "DESCRIPTION: reset namespace for dump\n"
                        "\tnamespace : namespace to be resetting\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "switch_ns") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : switch_ns namepsace\n"
                        "DESCRIPTION: switch namespace to reset namespace\n"
                        "\tnamespace : namespace to be swithing\n"
        );
    }
    if (cmd == NULL || strcmp(cmd, "ldb_instance_config") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : ldb_instance_config param1 param2 instance_list(0,1,2) ds_list(ip:port)\n"
                        "DESCRIPTION: ldb_instance_config to one or multi db instance\n"
                        "EXAMPLE: ldb_instance_config add_gc_bucket 100,200 0,2,6 xxx.xxx.xxx.xxx:5191\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "set_config") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : set_config name value [ds_ip_port]\n"
                        "DESCRIPTION: change the property value in leveldb::config\n"
                        "\tname : name about property in leveldb::config\n"
                        "\tvalue : value about property in leveldb::config\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "get_config") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : get_config [ds_ip_port]\n"
                        "DESCRIPTION: get the properties in leveldb::config\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "readonly_on") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : readonly_on area\n"
                        "DESCRIPTION: set area read-only on\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "readonly_off") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : readonly_off area\n"
                        "DESCRIPTION: set area read-only off\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "readonly_status") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : readonly_status area|all\n"
                        "DESCRIPTION: show area's read-only status, all mean show all areas\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "pause_rsync") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : pause_rsync [group@unit]|all\n"
                        "DESCRIPTION: pause rsync process, if argument is 'all' mean pause all rsync process\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "resume_rsync") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : resume_rsync [group@unit]|all\n"
                        "DESCRIPTION: resume rsync process, if argument is 'all' mean resume all rsync process\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "watch_rsync") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : watch_rsync [group@unit]|all\n"
                        "DESCRIPTION: reset a watch begin point, if argument is 'all', mean reset all rsync process\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "stat_rsync") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : stat_rsync [group@unit]|all\n"
                        "DESCRIPTION: print rsync send packet info into log, if argument is 'all', mean print all rsync process\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "options_rsync") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : options_rsync [group@unit]|all option value\n"
                        "DESCRIPTION: change rsync process's options\noptions:\n"
                        "\t[RecordSizeLimit]: change batch record's size limit\n"
                        "\t[RecordCountLimit]: change limit a batch record can contain how many records\n"
                        "\t[FailWaitTime]: change when bucket queue send fail, how long need wait\n"
                        "\t[QueueMaxSize}: change bucket queue's max size\n"
                        "\t[LogBufferSize]: change log buffer's size\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "get_rsync_stat") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : get_rsync_stat\n"
                        "DESCRIPTION: get rsync's status\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "get_cluster_info") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : get_cluster_info\n"
                        "DESCRIPTION: get cluster config info\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "rsync_connection_stat") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : rsync_connection_stat\n"
                        "DESCRIPTION: get rsync connection stat\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "set_rsync_config_service") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : set_rsync_config_service [http|file]://webservicehost/[unit]/[group]\n"
                        "DESCRIPTION: set rsync config service address, unit is current cluster's unit, group is current cluster's group\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "set_rsync_config_update_interval") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : set_rsync_config_update_interval [interval]\n"
                        "DESCRIPTION: set rsync config update interval to be [interval]\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "alloc_area") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : alloc_area quota\n"
                        "DESCRIPTION: alloc_area from specify group, return unused area\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "modify_area") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : modify_area area quota\n"
                        "DESCRIPTION: modify the quota of specify area in delicate group\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "start_revise_stat") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : start_revise_stat [options]\n"
                        "DESCRIPTION : revise user statistics or physical statistics\n"
                        "       -p|--revise-type type     type is user or physical, if not specified, default is user\n"
                        "       -f|--force-revise         if not specified, and if the statistics is already revised, the revise not take place\n"
                        "       -i|--sleep-interval num   every num key/value is revised, revise thread sleeps for sleep-time(us), if set to 0, never sleep, default 0.\n"
                        "       -t|--sleep-time num       every sleep-interval key/value is revised, revise thread sleeps for num(us), range [0, 1000000), if sleep-interval is 0, this is of no use, default 1000\n"
                        "       --instance instance-str   specify which instance to revise, e.g. instance-str 0,4,6, only revise instance 0,4,6, if not specified, revise all instance\n"
                        "       -d|dataserver ds          ds is [ip:port], only revise this ds, if not specified, revise all datasever\n"
                        "       short options can be put together, e.g. -t 1 -f = -ft1\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "stop_revise_stat") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : stop_revise_stat [ip:port]\n"
                        "DESCRIPTION : if revising ldb statistics is running, stop it, no use to mdb\n"
                        "              if ip:port is specified, only stop revise on this dataserver\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "get_revise_status") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : get_revise_status [ip:port]\n"
                        "DESCRIPTION : get revise status from all dataserver\n"
                        "              if ip:port is specified, only get status on this dataserver\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "modify_high_ops_upper") == 0) {
        fprintf(stderr,
                "-------------------------------------------------\n"
                        "SYNOPSIS    : modify_high_ops_upper ops_upper(-1 means query)  [dsip:port]\n"
                        "DESCRIPTION : modify high_ops_upper\n"
                        "              if get/put count more than this value, dataserver will output log\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "modify_chkexpir_hour_range") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : modify_chkexpir_hour_range hour_range\n"
                        "DESCRIPTION: modify check_expired_hour_range, such as modify_chkexpir_hour_range 4-6\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "modify_mdb_check_granularity") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : modify_mdb_check_granularity check_granularity(ms)\n"
                        "DESCRIPTION: modify mdb_check_granularity, such as modify_mdb_check_granularity 10\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "modify_slab_merge_hour_range") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : modify_slab_merge_hour_range hour_range\n"
                        "DESCRIPTION: modify mdb slab_merge_hour_range, such as modify_slab_merge_hour_range 4-6\n"
        );
    }

    if (cmd == NULL || strcmp(cmd, "modify_put_remove_expired") == 0) {
        fprintf(stderr,
                "------------------------------------------------\n"
                        "SYNOPSIS   : modify_put_remove_expired 1 or 0\n"
                        "DESCRIPTION: enable or disable put with remove expired keys, 1 means enable, 0 means disable\n"
        );
    }

    fprintf(stderr, "\n");
}

void tair_client::do_cmd_help(VSTRING &param) {
    if (param.size() == 0U) {
        print_help(NULL);
    } else {
        print_help(param[0]);
    }
    return;
}

char *tair_client::canonical_key(char *key, char **akey, int *size, int formart) {
    char *pdata = key;
    if (key_format == 1) { // java
        *size = strlen(key) + 2;
        pdata = (char *) malloc(*size);
        pdata[0] = '\0';
        pdata[1] = '\4';
        memcpy(pdata + 2, key, strlen(key));
        *akey = pdata;
    } else if (key_format == 2) { // raw
        pdata = (char *) malloc(strlen(key) + 1);
        util::string_util::conv_raw_string(key, pdata, size);
        *akey = pdata;
    } else if (key_format == 3) { // mc
        pdata = (char *) malloc(strlen(key) + sizeof(int32_t));
        memset(pdata, 0, sizeof(int32_t));
        memcpy(pdata + sizeof(int32_t), key, strlen(key));
        *akey = pdata;
    } else {
        *size = strlen(key) + 1;
    }

    return pdata;
}

void tair_client::do_cmd_put(VSTRING &param) {
    if (param.size() < 2U || param.size() > 4U) {
        print_help("put");
        return;
    }
    int area = default_area;
    int expired = 0;
    if (param.size() > 2U) area = atoi(param[2]);
    if (param.size() > 3U) expired = atoi(param[3]);

    char *akey = NULL;
    int pkeysize = 0;
    char *pkey = canonical_key(param[0], &akey, &pkeysize);
    data_entry key(pkey, pkeysize, false);
    data_entry data(param[1], false);

    int ret = client_helper.put(area, key, data, expired, 0);
    fprintf(stderr, "put: %s\n", client_helper.get_error_msg(ret));

    if (akey) free(akey);
}

void tair_client::do_cmd_addcount(VSTRING &param) {
    if (param.size() < 1U) {
        print_help("incr");
        return;
    }
    int count = 1;
    int initValue = 0;
    int area = default_area;
    if (param.size() > 1U) count = atoi(param[1]);
    if (param.size() > 2U) initValue = atoi(param[2]);
    if (param.size() > 3U) area = atoi(param[3]);

    char *akey = NULL;
    int pkeysize = 0;
    char *pkey = canonical_key(param[0], &akey, &pkeysize);
    data_entry key(pkey, pkeysize, false);

    // put
    int retCount = 0;
    int ret = client_helper.add_count(area, key, count, &retCount, initValue);

    if (ret != TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "add failed:%d,%s.\n", ret, client_helper.get_error_msg(ret));
    } else {
        fprintf(stderr, "retCount: %d\n", retCount);
    }
    if (akey) free(akey);

    return;
}

void tair_client::do_cmd_get(VSTRING &param) {
    if (param.size() < 1U || param.size() > 2U) {
        print_help("get");
        return;
    }
    int area = default_area;
    if (param.size() >= 2U) {
        char *p = param[1];
        if (*p == 'n') {
            area |= TAIR_FLAG_NOEXP;
            p++;
        }
        area |= atoi(p);
    }
    char *akey = NULL;
    int pkeysize = 0;
    char *pkey = canonical_key(param[0], &akey, &pkeysize);
    data_entry key(pkey, pkeysize, false);
    data_entry *data = NULL;

    // get
    int ret = client_helper.get(area, key, data);
    if (ret != TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "get failed: %s.\n", client_helper.get_error_msg(ret));
    } else if (data != NULL) {
        char *p = util::string_util::conv_show_string(data->get_data(), data->get_size());
        fprintf(stderr, "KEY: %s, LEN: %d\n raw data: %s, %s\n", param[0], data->get_size(), data->get_data(), p);
        free(p);
        delete data;
    }
    if (akey) free(akey);
    return;
}

void tair_client::do_cmd_aget(VSTRING &param) {
    if (param.size() < 1U || param.size() > 2U) {
        print_help("get");
        return;
    }
    int area = default_area;
    if (param.size() >= 2U) {
        char *p = param[1];
        if (*p == 'n') {
            area |= TAIR_FLAG_NOEXP;
            p++;
        }
        area |= atoi(p);
    }
    char *akey = NULL;
    int pkeysize = 0;
    char *pkey = canonical_key(param[0], &akey, &pkeysize);
    data_entry key(pkey, pkeysize, false);

    // get
    int ret = client_helper.get(area, key, async_get_cb, NULL);
    if (ret != TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "get failed: %s.\n", client_helper.get_error_msg(ret));
    } else {
        fprintf(stderr, "asynchronous request sent...\n");
    }
    if (akey) free(akey);
    return;
}

void tair_client::async_get_cb(int retcode, const data_entry *key, const data_entry *data, void *pargs) {
    fprintf(stderr, "ret: %d, ", retcode);
    if (key != NULL) {
        fprintf(stderr, "key: %s, ", key->get_data());
    }
    if (data != NULL) {
        fprintf(stderr, "data: %s\n", data->get_data());
    } else {
        fprintf(stderr, "\n");
    }
}

void tair_client::async_mc_set_cb(int retcode, response_mc_ops *resp, void *args) {
    fprintf(stderr, "ret: %d, version: %d, config_version: %d\n", retcode, resp->version, resp->config_version);
    if (retcode == TAIR_RETURN_SUCCESS)
        fprintf(stderr, "get key: %s, value: %s\n", resp->key.get_data(), resp->value.get_data());
}

void tair_client::do_cmd_mget(VSTRING &param) {
    if (param.size() < 2U) {
        print_help("mget");
        return;
    }
    int area = default_area;
    char *p = param[param.size() - 1];
    if (*p == 'n') {
        area |= TAIR_FLAG_NOEXP;
        p++;
    }
    area |= atoi(p);
    fprintf(stderr, "mget area: %d\n", area);

    vector<data_entry *> keys;
    for (int i = 0; i < static_cast<int>(param.size() - 1); ++i) {
        char *akey = NULL;
        int pkeysize = 0;
        fprintf(stderr, "mget key index: %u, key: %s\n", i, param[i]);
        char *pkey = canonical_key(param[i], &akey, &pkeysize);
        data_entry *key = new data_entry(pkey, pkeysize, false);
        keys.push_back(key);
        if (akey) free(akey);
    }
    tair_keyvalue_map datas;

    // mget
    int ret = client_helper.mget(area, keys, datas);
    if (ret == TAIR_RETURN_SUCCESS || ret == TAIR_RETURN_PARTIAL_SUCCESS) {
        tair_keyvalue_map::iterator mit = datas.begin();
        for (; mit != datas.end(); ++mit) {
            char *key = util::string_util::conv_show_string(mit->first->get_data(), mit->first->get_size());
            char *data = util::string_util::conv_show_string(mit->second->get_data(), mit->second->get_size());
            fprintf(stderr, "KEY: %s, RAW VALUE: %s, VALUE: %s, LEN: %d\n",
                    key, mit->second->get_data(), data, mit->second->get_size());
            free(key);
            free(data);
        }
        fprintf(stderr, "get success, ret: %d.\n", ret);
    } else {
        fprintf(stderr, "get failed: %s, ret: %d.\n", client_helper.get_error_msg(ret), ret);
    }

    vector<data_entry *>::iterator vit = keys.begin();
    for (; vit != keys.end(); ++vit) {
        delete *vit;
        (*vit) = NULL;
    }
    tair_keyvalue_map::iterator kv_mit = datas.begin();
    for (; kv_mit != datas.end();) {
        data_entry *key = kv_mit->first;
        data_entry *value = kv_mit->second;
        datas.erase(kv_mit++);
        delete key;
        key = NULL;
        delete value;
        value = NULL;
    }
    return;
}

// remove
void tair_client::do_cmd_remove(VSTRING &param) {
    if (param.size() < 1U || param.size() > 2U) {
        print_help("remove");
        return;
    }

    int area = default_area;
    if (param.size() >= 2U) area = atoi(param[1]);

    char *akey = NULL;
    int pkeysize = 0;
    char *pkey = canonical_key(param[0], &akey, &pkeysize);
    data_entry key(pkey, pkeysize, false);

    int ret = client_helper.remove(area, key);
    fprintf(stderr, "remove: %s.\n", client_helper.get_error_msg(ret));
    if (akey) free(akey);
    return;
}

const char *FlowStatusStr(tair::stat::FlowStatus status) {
    switch (status) {
        case tair::stat::UP:
            return "UP";
        case tair::stat::KEEP:
            return "KEEP";
        case tair::stat::DOWN:
            return "DOWN";
    }
    return "UNKNOW";
}

void tair_client::do_cmd_get_flow_rate(VSTRING &param) {
    if (param.size() < 1U) {
        print_help("flowrate");
        return;
    }
    int ns = atoi(param[0]);

    set <uint64_t> servers;
    client_helper.get_servers(servers);
    size_t success = 0;
    for (set<uint64_t>::iterator iter = servers.begin(); iter != servers.end(); ++iter) {
        uint64_t addr = *iter;
        tair::stat::Flowrate rate;
        int ret = client_helper.get_flow(addr, ns, rate);

        if (ret == 0) {
            fprintf(stderr, "success %s \tns:%d \tin:%ld,%s \tout:%ld,%s \tops:%ld,%s \tstatus:%s\n",
                    tbsys::CNetUtil::addrToString(addr).c_str(), ns,
                    rate.in, FlowStatusStr(rate.in_status),
                    rate.out, FlowStatusStr(rate.out_status),
                    rate.ops, FlowStatusStr(rate.ops_status),
                    FlowStatusStr(rate.status));
            success++;
        } else {
            fprintf(stderr, "fail %s \terr:%d\n", tbsys::CNetUtil::addrToString(addr).c_str(), ret);
        }
    }
    if (success == servers.size()) {
        fprintf(stderr, "all success\n");
    } else if (success == 0) {
        fprintf(stderr, "all fail\n");
    } else {
        fprintf(stderr, "part success\n");
    }
}

void tair_client::do_cmd_get_flow_top(VSTRING &param) {
    if (param.size() < 1U) {
        print_help("flowtop");
        return;
    }

    // flowtop ds_addr [top_num max_area]

    uint64_t addr = 0;

    std::string ipport = param[0];
    size_t pos = ipport.find_first_of(':');
    if (pos == std::string::npos || pos == ipport.size()) {
        fprintf(stderr, "should be ip:port, %s\n", ipport.c_str());
        print_help("flowtop");
        return;
    }

    std::string ipstr = ipport.substr(0, pos);
    std::string portstr = ipport.substr(pos + 1, ipport.size());
    addr = tbsys::CNetUtil::strToAddr(ipstr.c_str(), atoi(portstr.c_str()));

    int top_num = 3;
    if (param.size() >= 2U) {
        top_num = atoi(param[1]);
        if (top_num < 1) {
            fprintf(stderr, "top_num should be integer bigger than 1 : %s\n", param[1]);
            print_help("flowtop");
            return;
        }
    }

    int max_ns = TAIR_MAX_AREA_COUNT;
    if (param.size() >= 3U) {
        max_ns = atoi(param[2]);
        if (max_ns < 1 || max_ns > TAIR_MAX_AREA_COUNT) {
            fprintf(stderr, "max_ns should be integer within [1:%d) : %s\n", TAIR_MAX_AREA_COUNT, param[2]);
            print_help("flowtop");
            return;
        }
    }

    int success = 0;
    vector<flow_sort_item> stats;

    for (int ns = 0; ns < max_ns; ns++) {
        tair::stat::Flowrate rate;
        int ret = client_helper.get_flow(addr, ns, rate);

        if (ret == 0) {
            success++;
            // add to sth..
            stats.push_back(flow_sort_item(ns, rate));
        } else {
            fprintf(stderr, "fail %s \tns:%d  \terr:%d\n", tbsys::CNetUtil::addrToString(addr).c_str(), ns, ret);
        }
    }

    if (success == max_ns) {
        fprintf(stderr, "all success\n");
    } else if (success == 0) {
        fprintf(stderr, "all fail\n");
    } else {
        fprintf(stderr, "part success\n");
    }

    // sort
    // top ops
    int cnt = 0;
    fprintf(stderr,
            "------------------top %4d ops--------------------\n", top_num);
    sort(stats.begin(), stats.end(), flow_sort_item::sort_ops);
    cnt = 0;
    for (vector<flow_sort_item>::iterator it = stats.begin(); it != stats.end() && cnt < top_num; ++it, ++cnt) {
        fprintf(stderr, "ns : %4d \t   ops : %ld\n", it->ns, it->rate.ops);
    }
    // top in
    fprintf(stderr,
            "------------------top %4d in---------------------\n", top_num);
    sort(stats.begin(), stats.end(), flow_sort_item::sort_in);
    cnt = 0;
    for (vector<flow_sort_item>::iterator it = stats.begin(); it != stats.end() && cnt < top_num; ++it, ++cnt) {
        fprintf(stderr, "ns : %4d \t    in : %ld\n", it->ns, it->rate.in);
    }
    // top out
    fprintf(stderr,
            "------------------top %4d out--------------------\n", top_num);
    sort(stats.begin(), stats.end(), flow_sort_item::sort_out);
    cnt = 0;
    for (vector<flow_sort_item>::iterator it = stats.begin(); it != stats.end() && cnt < top_num; ++it, ++cnt) {
        fprintf(stderr, "ns : %4d \t   out : %ld\n", it->ns, it->rate.out);
    }
    // top max
    fprintf(stderr,
            "------------------top %4d total------------------\n", top_num);
    sort(stats.begin(), stats.end(), flow_sort_item::sort_total);
    cnt = 0;
    for (vector<flow_sort_item>::iterator it = stats.begin(); it != stats.end() && cnt < top_num; ++it, ++cnt) {
        fprintf(stderr, "ns : %4d \t total : %ld\n", it->ns, it->rate.in + it->rate.out);
    }
}

void tair_client::do_cmd_set_flow_limit_bound(VSTRING &param) {
    if (param.size() < 4U) {
        print_help("flowlimit");
        return;
    }
    int ns = atoi(param[0]);
    int lower = atoi(param[1]);
    int upper = atoi(param[2]);

    char *strtemp = NULL;
    int typestr_len = 0;
    char *typestr = canonical_key(param[3], &typestr, &typestr_len);
    tair::stat::FlowType type = tair::stat::IN;
    if (strncmp("in", typestr, 2) == 0)
        type = tair::stat::IN;
    else if (strncmp("out", typestr, 3) == 0)
        type = tair::stat::OUT;
    else if (strncmp("ops", typestr, 3) == 0)
        type = tair::stat::OPS;
    else {
        fprintf(stderr, "unknow type:%s just support [in, out, ops]\n", typestr);
        return;
    }
    if (type != tair::stat::OPS && (lower > 1024 * 1024 || upper > 1024 * 1024)) {
        fprintf(stderr, "flowcontrol unit is MB, not Byte\n");
        return;
    }

    set_flow_limit_bound(ns, lower, upper, type);
    if (strtemp != NULL)
        free(strtemp);
}

void tair_client::do_cmd_set_flow_policy(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "flow_policy", TAIR_SERVER_CMD_SET_FLOW_POLICY, 2);
}

const char *FlowTypeStr(tair::stat::FlowType type) {
    switch (type) {
        case tair::stat::IN:
            return "in";
        case tair::stat::OUT:
            return "out";
        case tair::stat::OPS:
            return "ops";
    }
    return "unknow";
}

void tair_client::set_flow_limit_bound(int ns, int lower, int upper, tair::stat::FlowType type) {
    set <uint64_t> servers;
    client_helper.get_servers(servers);
    size_t success = 0;
    for (set<uint64_t>::iterator iter = servers.begin(); iter != servers.end(); ++iter) {
        uint64_t addr = *iter;
        int local_ns = ns;
        int local_lower = lower;
        int local_upper = upper;
        tair::stat::FlowType local_type = type;
        int ret = client_helper.set_flow_limit_bound(addr, local_ns,
                                                     local_lower,
                                                     local_upper,
                                                     local_type);

        if (ret == 0) {
            fprintf(stderr, "success %s \tns:%d \tlower:%d \tupper:%d \ttype:%s\n",
                    tbsys::CNetUtil::addrToString(addr).c_str(),
                    local_ns, local_lower, local_upper, FlowTypeStr(local_type));
            success++;
        } else {
            fprintf(stderr, "fail %s \terr:%d\n", tbsys::CNetUtil::addrToString(addr).c_str(), ret);
        }
    }
    if (success == servers.size()) {
        fprintf(stderr, "all success\n");
    } else if (success == 0) {
        fprintf(stderr, "all fail\n");
    } else {
        fprintf(stderr, "part success\n");
    }
}

void tair_client::do_cmd_mremove(VSTRING &param) {
    if (param.size() < 2U) {
        print_help("mremove");
        return;
    }
    int area = default_area;
    char *p = param[param.size() - 1];
    if (*p == 'n') {
        area |= TAIR_FLAG_NOEXP;
        p++;
    }
    area |= atoi(p);
    fprintf(stderr, "mremove area: %d\n", area);

    vector<data_entry *> keys;
    for (int i = 0; i < static_cast<int>(param.size() - 1); ++i) {
        char *akey = NULL;
        int pkeysize = 0;
        fprintf(stderr, "mremove key index: %u, key: %s\n", i, param[i]);
        char *pkey = canonical_key(param[i], &akey, &pkeysize);
        data_entry *key = new data_entry(pkey, pkeysize, false);
        keys.push_back(key);
        if (akey) free(akey);
        //todo delete key
    }

    int ret = client_helper.mdelete(area, keys);
    fprintf(stderr, "mremove: %s, ret: %d\n", client_helper.get_error_msg(ret), ret);
    vector<data_entry *>::iterator vit = keys.begin();
    for (; vit != keys.end(); ++vit) {
        delete *vit;
        (*vit) = NULL;
    }
    return;
}

void tair_client::do_cmd_stat(VSTRING &param) {
    // option -b -k -m -g
    int argc = param.size() + 1;
    if (argc > 50) {   // 50 would be enough
        fprintf(stderr, "the number of options should be less than 50\n");
        return;
    }
    static char *argv[50];
    memcpy(argv + 1, &param[0], sizeof(char *) * (argc - 1));

    int32_t size_unit = 1;
    int32_t area_num = -1;
    bool show_area = false, show_server = false, show_server_area = false;
    string desc_suffix("");
    int ch;
    static struct option long_option[] =
            {
                    {"all",        no_argument,       NULL, 'a'},
                    {"area",       required_argument, NULL, 'e'},
                    {"dataserver", no_argument,       NULL, 'd'},
                    {"ds-area",    no_argument,       NULL, 'c'},
                    {NULL, 0,                         NULL, 0}
            };
    // very important, it has been modified by function parse_cmd_line().
    // and optind = 0 would make getopt execute a initialization.
    optind = 0;
    while ((ch = getopt_long(argc, argv, "bkmgae:dc", long_option, NULL)) != -1) {
        switch (ch) {
            case 'b':
                size_unit = 1;
                desc_suffix = "";
                break;
            case 'k':
                size_unit = 1024;
                desc_suffix = "/K";
                break;
            case 'm':
                size_unit = 1024 * 1024;
                desc_suffix = "/M";
                break;
            case 'g':
                size_unit = 1024 * 1024 * 1024;
                desc_suffix = "/G";
                break;
            case 'a':
                show_area = true;
                show_server = true;
                show_server_area = true;
                break;
                // case '?':
            case 'e':
                show_area = true;
                area_num = atoi(optarg);
                break;
            case 'd':
                show_server = true;
                break;
            case 'c':
                show_server_area = true;
                break;
            default:
                print_help("stat");
                return;
        }
    }
    if (optind != argc) {
        print_help("stat");
        return;
    }

    tair_statistics stat_info;
    if (client_helper.query_from_dataserver(0, stat_info) == TAIR_RETURN_FAILED) {
        fprintf(stderr, "get tair stat from dataserver failed\n");
        return;
    }

    if ((show_area | show_server | show_server_area) == false)
        show_area = show_server = show_server_area = true;

    //print desc line;
    const vector<string> &desc = stat_info.get_stat_desc();
    fprintf(stderr, "%-20s ", "area/ds/ds-area");
    for (vector<string>::const_iterator it = desc.begin(); it != desc.end(); ++it)
        fprintf(stderr, "%-10s ", ((*it) + desc_suffix).c_str());
    fprintf(stderr, "\n");

    if (show_area) {
        const map<string, vector<int64_t> > &area_stat = stat_info.get_area_stat();
        if (area_num < 0)
            print_stat(size_unit, area_stat);
        else {
            char area_str[8];
            snprintf(area_str, sizeof(area_str), "%d", area_num);
            map<string, vector<int64_t> >::const_iterator it = area_stat.find(area_str);
            if (it != area_stat.end()) {
                map<string, vector<int64_t> > one_area_stat;
                one_area_stat[area_str] = it->second;
                print_stat(size_unit, one_area_stat);
            } else
                fprintf(stderr, "%-20d area not exist", area_num);
        }
        fprintf(stderr, "\n");
    }// end of if (show_area)

    if (show_server) {
        const map<string, vector<int64_t> > &server_stat = stat_info.get_server_stat();
        print_stat(size_unit, server_stat);
        fprintf(stderr, "\n");
    }

    if (show_server_area) {
        const map<string, vector<int64_t> > &server_area_stat = stat_info.get_server_area_stat();
        print_stat(size_unit, server_area_stat);
        fprintf(stderr, "\n");
    }//  if (show_server_area)
}

void tair_client::print_stat(int32_t size_unit, const map<string, vector<int64_t> > &stat) {
    //print
    for (map<string, vector<int64_t> >::const_iterator it = stat.begin(); it != stat.end(); ++it) {
        fprintf(stderr, "%-20s ", it->first.c_str());
        for (vector<int64_t>::const_iterator row_it = it->second.begin(); row_it != it->second.end(); ++row_it)
            fprintf(stderr, "%-10ld ", *row_it / size_unit);
        fprintf(stderr, "\n");
    }
}

void tair_client::do_cmd_health(VSTRING &param) {
    if (is_data_server) {
        fprintf(stderr, "direct connect to ds, can not use health\n");
        return;
    }

    if (param.size() != 0) {
        print_help("health");
        return;
    }

    map<string, string> out_info;
    map<string, string>::iterator it;
    string group = group_name;

    //************************************************************
    // print the DataServer status.
    client_helper.query_from_configserver(request_query_info::Q_DATA_SERVER_INFO, group, out_info);
    fprintf(stderr, "%20s %20s\n", "server", "status");
    for (it = out_info.begin(); it != out_info.end(); it++)
        fprintf(stderr, "%20s %20s\n", it->first.c_str(), it->second.c_str());
    fprintf(stderr, "\n");

    //************************************************************
    //print the migrate info.
    client_helper.query_from_configserver(request_query_info::Q_MIG_INFO, group, out_info);
    fprintf(stderr, "%20s %20s\n", "migrate", "ongoing");

    for (it = out_info.begin(); it != out_info.end(); it++)
        fprintf(stderr, "%20s %20s\n", it->first.c_str(), it->second.c_str());
    fprintf(stderr, "\n");

    //************************************************************
    //print the ping time to each DataServer
    set <uint64_t> servers;
    int64_t ping_time;
    client_helper.get_servers(servers);
    fprintf(stderr, "%20s %20s\n", "server", "ping");
    for (set<uint64_t>::iterator it = servers.begin(); it != servers.end(); ++it) {
        ping_time = ping(*it);
        fprintf(stderr, "%20s %20lf\n", tbsys::CNetUtil::addrToString(*it).c_str(), ping_time / 1000.);
    }
    fprintf(stderr, "\n");
}

void tair_client::do_cmd_remove_area(VSTRING &param) {
    if (param.size() < 2U) {
        print_help("delall");
        return;
    }
    int area = atoi(param[0]);
    if (area < 0) {
        return;
    }
    // remove
    int ret = TAIR_RETURN_SUCCESS;
    if (strncmp("all", param[1], 3) == 0) {
        std::set<uint64_t> ds_set;
        //remove all ds in the cluster.
        client_helper.get_servers(ds_set);
        for (std::set<uint64_t>::iterator it = ds_set.begin(); it != ds_set.end(); it++) {
            ret = client_helper.remove_area(area, *it);
            fprintf(stderr, "removeArea: area:%d, ds: %s, %s\n", area,
                    tbsys::CNetUtil::addrToString(*it).c_str(),
                    client_helper.get_error_msg(ret));
        }
    } else if (strncmp("-s", param[1], 2) == 0) {
        //remove area on special ds.
        if (param.size() != 3U) {
            print_help("delall");
        } else {
            //if id is not avaliable, `remove_area returns `invalid_args code.
            std::string ipport = param[2];
            size_t pos = ipport.find_first_of(':');
            if (pos == std::string::npos || pos == ipport.size()) {
                fprintf(stderr, "should be ip:port, %s", ipport.c_str());
                print_help("delall");
            } else {
                std::string ipstr = ipport.substr(0, pos);
                std::string portstr = ipport.substr(pos + 1, ipport.size());
                uint64_t id = tbsys::CNetUtil::strToAddr(ipstr.c_str(), atoi(portstr.c_str()));
                ret = client_helper.remove_area(area, id);
                fprintf(stderr, "removeArea: area:%d,ds: %s, %s\n", area, param[2], client_helper.get_error_msg(ret));
            }
        }
    } else {
        print_help("delall");
    }
}

void tair_client::do_cmd_dump_area(VSTRING &param) {
    if (param.size() != 1U) {
        print_help("dump");
        return;
    }

    FILE *fp = fopen(param[0], "r");
    if (fp == NULL) {
        fprintf(stderr, "open file %s failed", param[0]);
        return;
    }

    char buf[1024];
    struct tm start_time;
    struct tm end_time;
    set <dump_meta_info> dump_set;
    while (fgets(buf, sizeof(buf), fp) != NULL) {
        dump_meta_info info;
        if (sscanf(buf, "%d %4d-%2d-%2d %2d:%2d:%2d %4d-%2d-%2d %2d:%2d:%2d", \
                    &info.area, \
                    &start_time.tm_year, &start_time.tm_mon, \
                    &start_time.tm_mday, &start_time.tm_hour, \
                    &start_time.tm_min, &start_time.tm_sec,
                   &end_time.tm_year, &end_time.tm_mon, \
                    &end_time.tm_mday, &end_time.tm_hour, \
                    &end_time.tm_min, &end_time.tm_sec) != 13) {
            fprintf(stderr, "syntax error : %s", buf);
            continue;
        }

        start_time.tm_year -= 1900;
        end_time.tm_year -= 1900;
        start_time.tm_mon -= 1;
        end_time.tm_mon -= 1;

        if (info.area < -1 || info.area >= TAIR_MAX_AREA_COUNT) {
            fprintf(stderr, "ilegal area");
            continue;
        }

        time_t tmp_time = -1;
        if ((tmp_time = mktime(&start_time)) == static_cast<time_t>(-1)) {
            fprintf(stderr, "incorrect time");
            continue;
        }
        info.start_time = static_cast<uint32_t>(tmp_time);

        if ((tmp_time = mktime(&end_time)) == static_cast<time_t>(-1)) {
            fprintf(stderr, "incorrect time");
            continue;
        }
        info.end_time = static_cast<uint32_t>(tmp_time);

        if (info.start_time < info.end_time) {
            std::swap(info.start_time, info.end_time);
        }
        dump_set.insert(info);
    }
    fclose(fp);

    if (dump_set.empty()) {
        fprintf(stderr, "what do you want to dump?");
        return;
    }

    // dump
    int ret = client_helper.dump_area(dump_set);
    fprintf(stderr, "dump : %s\n", client_helper.get_error_msg(ret));
    return;
}

void tair_client::do_cmd_hide(VSTRING &params) {
    if (params.size() != 2) {
        print_help("hide");
        return;
    }
    int area = atoi(params[0]);
    data_entry key;
    key.set_data(params[1], strlen(params[1]) + 1);
    fprintf(stderr, "area: %d, key: %s\n", area, params[1]);
    int ret = client_helper.hide(area, key);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
    }
}

void tair_client::do_cmd_set_migrate_wait_ms(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "set_migrate_wait", TAIR_SERVER_CMD_SET_MIGRATE_WAIT_MS, 1);
}

void tair_client::do_cmd_get_hidden(VSTRING &params) {
    int area = 0;
    const char *kstr = NULL;
    if (params.size() == 2) {
        area = atoi(params[0]);
        kstr = params[1];
    } else if (params.size() == 1) {
        area = 0;
        kstr = params[0];
    } else {
        print_help("gethidden");
        return;
    }
    data_entry key;
    key.set_data(kstr, strlen(kstr) + 1);
    data_entry *value = NULL;
    int ret = client_helper.get_hidden(area, key, value);
    if (value != NULL) {
        char *p = util::string_util::conv_show_string(value->get_data(), value->get_size());
        fprintf(stderr, "key: %s, len: %d\n", key.get_data(), key.get_size());
        fprintf(stderr, "value: %s|%s, len: %d\n", value->get_data(), p, value->get_size());
        free(p);
        delete value;
    } else {
        fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
    }
}

void tair_client::do_cmd_prefix_put(VSTRING &params) {
    int area = 0;
    const char *pstr = NULL,
            *sstr = NULL,
            *vstr = NULL;
    if (params.size() == 4) {
        area = atoi(params[0]);
        pstr = params[1];
        sstr = params[2];
        vstr = params[3];
    } else if (params.size() == 3) {
        area = 0;
        pstr = params[0];
        sstr = params[1];
        vstr = params[2];
    } else {
        print_help("pput");
        return;
    }
    data_entry pkey,
            skey,
            value;
    pkey.set_data(pstr, strlen(pstr) + 1);
    skey.set_data(sstr, strlen(sstr) + 1);
    value.set_data(vstr, strlen(vstr) + 1);

    int ret = client_helper.prefix_put(area, pkey, skey, value, 0, 0);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
    }
}

void tair_client::do_cmd_prefix_puts(VSTRING &params) {
    if (params.size() < 4 || (params.size() & 0x1) != 0) {
        print_help("pputs");
        return;
    }

    int area = 0;
    area = atoi(params[0]);
    data_entry pkey(params[1], strlen(params[1]) + 1, false);
    vector<key_value_pack_t *> packs;
    packs.reserve((params.size() - 2) >> 1);
    for (size_t i = 2; i < params.size(); i += 2) {
        key_value_pack_t *pack = new key_value_pack_t;
        data_entry *skey = new data_entry(params[i], strlen(params[i]) + 1, false);
        data_entry *value = new data_entry(params[i + 1], strlen(params[i + 1]) + 1, false);
        pack->key = skey;
        pack->value = value;
        packs.push_back(pack);
    }

    key_code_map_t failed_map;
    int ret = client_helper.prefix_puts(area, pkey, packs, failed_map);
    fprintf(stderr, "%d: %s\n", ret, client_helper.get_error_msg(ret));
    if (!failed_map.empty()) {
        key_code_map_t::iterator itr = failed_map.begin();
        while (itr != failed_map.end()) {
            fprintf(stderr, "skey: %s, code: %d, msg: %s\n",
                    itr->first->get_data(), itr->second, client_helper.get_error_msg(itr->second));
            ++itr;
        }
    }
}

void tair_client::do_cmd_prefix_get(VSTRING &params) {
    int area = 0;
    const char *pstr = NULL,
            *sstr = NULL;
    if (params.size() == 3) {
        area = atoi(params[0]);
        pstr = params[1];
        sstr = params[2];
    } else if (params.size() == 2) {
        area = 0;
        pstr = params[0];
        sstr = params[1];
    } else {
        print_help("pget");
        return;
    }

    data_entry pkey,
            skey;
    pkey.set_data(pstr, strlen(pstr) + 1);
    skey.set_data(sstr, strlen(sstr) + 1);
    data_entry *value = NULL;

    int ret = client_helper.prefix_get(area, pkey, skey, value);
    if (value != NULL) {
        char *p = util::string_util::conv_show_string(value->get_data(), value->get_size());
        fprintf(stderr, "pkey: %s, len: %d\n", pkey.get_data(), pkey.get_size());
        fprintf(stderr, "skey: %s, len: %d\n", skey.get_data(), skey.get_size());
        fprintf(stderr, "value: %s|%s, len %d\n", value->get_data(), p, value->get_size());
        free(p);
        delete value;
    } else {
        fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
    }
}

void tair_client::do_cmd_prefix_gets(VSTRING &params) {
    if (params.size() < 3) {
        print_help("pgets");
        return;
    }
    int area = 0;
    data_entry pkey(params[1], strlen(params[1]) + 1, false);
    tair_dataentry_set skeys;

    area = atoi(params[0]);
    for (size_t i = 2; i < params.size(); ++i) {
        data_entry *skey = new data_entry(params[i], strlen(params[i]) + 1, false);
        if (!skeys.insert(skey).second) {
            delete skey;
        }
    }

    tair_keyvalue_map result_map;
    key_code_map_t failed_map;

    int ret = client_helper.prefix_gets(area, pkey, skeys, result_map, failed_map);
    fprintf(stderr, "%d: %s\n", ret, client_helper.get_error_msg(ret));

    if (!result_map.empty()) {
        tair_keyvalue_map::iterator itr = result_map.begin();
        while (itr != result_map.end()) {
            data_entry *skey = itr->first;
            data_entry *value = itr->second;
            fprintf(stderr, "skey: %s, value: %s\n", skey->get_data(), value->get_data());
            ++itr;
        }
        defree(result_map);
    }
    if (!failed_map.empty()) {
        key_code_map_t::iterator itr = failed_map.begin();
        while (itr != failed_map.end()) {
            fprintf(stderr, "skey: %s, err_code: %d, err_msg: %s\n",
                    itr->first->get_data(), itr->second, client_helper.get_error_msg(itr->second));
            ++itr;
        }
        defree(failed_map);
    }
}

void tair_client::do_cmd_prefix_remove(VSTRING &params) {
    int area = 0;
    const char *pstr = NULL,
            *sstr = NULL;
    if (params.size() == 3) {
        area = atoi(params[0]);
        pstr = params[1];
        sstr = params[2];
    } else if (params.size() == 2) {
        area = 0;
        pstr = params[0];
        sstr = params[1];
    }
    data_entry pkey,
            skey;
    pkey.set_data(pstr, strlen(pstr) + 1);
    skey.set_data(sstr, strlen(sstr) + 1);

    int ret = client_helper.prefix_remove(area, pkey, skey);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
    }
}

void tair_client::do_cmd_prefix_removes(VSTRING &params) {
    if (params.size() < 3) {
        print_help("premoves");
        return;
    }
    int area = atoi(params[0]);
    data_entry pkey;
    pkey.set_data(params[1], strlen(params[1]) + 1);
    tair_dataentry_set skey_set;
    for (size_t i = 2; i < params.size(); ++i) {
        data_entry *skey = new data_entry();
        skey->set_data(params[i], strlen(params[i]) + 1);
        skey_set.insert(skey);
    }
    key_code_map_t key_code_map;
    int ret = client_helper.prefix_removes(area, pkey, skey_set, key_code_map);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        key_code_map_t::iterator itr = key_code_map.begin();
        while (itr != key_code_map.end()) {
            data_entry *skey = itr->first;
            int ret = itr->second;
            fprintf(stderr, "failed key: %s, len: %d, code: %d, msg: %s\n",
                    skey->get_data(), skey->get_size(), ret, client_helper.get_error_msg(ret));
            ++itr;
            delete skey;
        }
    }
}

void tair_client::do_cmd_prefix_get_hidden(VSTRING &params) {
    int area = 0;
    const char *pstr = NULL,
            *sstr = NULL;
    if (params.size() == 3) {
        area = atoi(params[0]);
        pstr = params[1];
        sstr = params[2];
    } else if (params.size() == 2) {
        area = 0;
        pstr = params[0];
        sstr = params[1];
    } else {
        print_help("pget");
        return;
    }

    data_entry pkey,
            skey;
    pkey.set_data(pstr, strlen(pstr) + 1);
    skey.set_data(sstr, strlen(sstr) + 1);
    data_entry *value = NULL;

    int ret = client_helper.prefix_get_hidden(area, pkey, skey, value);
    if (value != NULL) {
        char *p = util::string_util::conv_show_string(value->get_data(), value->get_size());
        fprintf(stderr, "code: %d, msg: %s\n", ret, client_helper.get_error_msg(ret));
        fprintf(stderr, "pkey: %s, len: %d\n", pkey.get_data(), pkey.get_size());
        fprintf(stderr, "skey: %s, len: %d\n", skey.get_data(), skey.get_size());
        fprintf(stderr, "value: %s|%s, len %d\n", value->get_data(), p, value->get_size());
        free(p);
        delete value;
    } else {
        fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
    }
}

void tair_client::do_cmd_prefix_hide(VSTRING &params) {
    int area = 0;
    const char *pstr = NULL,
            *sstr = NULL;
    if (params.size() == 3) {
        area = atoi(params[0]);
        pstr = params[1];
        sstr = params[2];
    } else if (params.size() == 2) {
        area = 0;
        pstr = params[0];
        sstr = params[1];
    }
    data_entry pkey,
            skey;
    pkey.set_data(pstr, strlen(pstr) + 1);
    skey.set_data(sstr, strlen(sstr) + 1);

    int ret = client_helper.prefix_hide(area, pkey, skey);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d: %s\n", ret, client_helper.get_error_msg(ret));
    }
}

void tair_client::do_cmd_prefix_hides(VSTRING &params) {
    if (params.size() < 3) {
        print_help("phides");
        return;
    }
    int area = atoi(params[0]);
    data_entry pkey;
    pkey.set_data(params[1], strlen(params[1]) + 1);
    tair_dataentry_set skey_set;
    for (size_t i = 2; i < params.size(); ++i) {
        data_entry *skey = new data_entry();
        skey->set_data(params[i], strlen(params[i]) + 1);
        skey_set.insert(skey);
    }
    key_code_map_t key_code_map;
    int ret = client_helper.prefix_hides(area, pkey, skey_set, key_code_map);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        key_code_map_t::iterator itr = key_code_map.begin();
        while (itr != key_code_map.end()) {
            data_entry *skey = itr->first;
            int ret = itr->second;
            fprintf(stderr, "failed key: %s, len: %d, code: %d, msg: %s\n",
                    skey->get_data(), skey->get_size(), ret, client_helper.get_error_msg(ret));
            ++itr;
            delete skey;
        }
    }
}

void tair_client::do_cmd_setstatus(VSTRING &params) {
    if (params.size() != 2) {
        print_help("setstatus");
        return;
    }
    std::vector<std::string> cmd_params(params.begin(), params.end());
    int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_SET_GROUP_STATUS, &cmd_params, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_getstatus(VSTRING &params) {
    if (params.empty()) {
        print_help("getstatus");
        return;
    }
    vector<string> status;
    std::vector<std::string> cmd_params(params.begin(), params.end());
    int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_GET_GROUP_STATUS, &cmd_params, &status);
    if (TAIR_RETURN_SUCCESS == ret) {
        for (size_t i = 0; i < status.size(); ++i) {
            fprintf(stderr, "\t%s\n", status[i].c_str());
        }
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_gettmpdownsvr(VSTRING &params) {
    if (params.empty()) {
        print_help("gettmpdownsvr");
        return;
    }
    vector<string> down_servers;
    std::vector<std::string> cmd_params(params.begin(), params.end());
    int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER, &cmd_params, &down_servers);
    if (TAIR_RETURN_SUCCESS == ret) {
        for (size_t i = 0; i < down_servers.size(); ++i) {
            fprintf(stderr, "\t%s\n", down_servers[i].c_str());
        }
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_resetserver(VSTRING &params) {
    if (params.size() < 1) {
        print_help("resetserver");
        return;
    }
    std::vector<std::string> cmd_params(params.begin(), params.end());
    int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_RESET_DS, &cmd_params, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_migrate_bucket(VSTRING &params) {
    if (params.size() < 1) {
        print_help("migrate_bucket");
        return;
    }
    std::vector<std::string> cmd_params(params.begin(), params.end());
    int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_MIGRATE_BUCKET, &cmd_params, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_switch_bucket(VSTRING &params) {
    if (params.size() != 2) {
        print_help("switch_bucket");
        return;
    }
    std::vector<std::string> cmd_params(params.begin(), params.end());
    int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_SWITCH_BUCKET, &cmd_params, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_active_failover(VSTRING &params) {
    if (params.size() != 2) {
        print_help("active_failover");
        return;
    }
    active_failover_or_recover(params, true);
}

void tair_client::do_cmd_active_recover(VSTRING &params) {
    if (params.size() != 2) {
        print_help("active_recover");
        return;
    }
    active_failover_or_recover(params, false);
}

void tair_client::active_failover_or_recover(VSTRING &params, bool is_failover) {
    std::vector<std::string> cmd_params(params.begin(), params.end());
    char buf[4096];
    if (is_failover) {
        sprintf(buf, "%d", ACTIVE_FAILOVER);
    } else {
        sprintf(buf, "%d", ACTIVE_RECOVER);
    }
    cmd_params.push_back(buf);
    int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_ACTIVE_FAILOVER_OR_RECOVER, &cmd_params, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_replace_ds(VSTRING &params) {
    if (params.size() != 4) {
        print_help("replace_ds");
        return;
    }
    std::vector<std::string> cmd_params(params.begin(), params.end());
    int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_REPLACE_DS, &cmd_params, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_urgent_offline(VSTRING &params) {
    if (params.size() != 3) {
        print_help("urgent_offline");
        return;
    }
    std::vector<std::string> cmd_params(params.begin(), params.end());
    int ret = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_URGENT_OFFLINE, &cmd_params, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "successful\n");
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_flushmmt(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "flushmmt", TAIR_SERVER_CMD_FLUSH_MMT);
}

void tair_client::do_cmd_resetdb(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "resetdb", TAIR_SERVER_CMD_RESET_DB);
}

void tair_client::do_cmd_stat_db(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "stat_db", TAIR_SERVER_CMD_STAT_DB);
}

void tair_client::do_cmd_release_mem(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "release_mem", TAIR_SERVER_CMD_RELEASE_MEM);
}

void tair_client::do_cmd_backup_db(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "backup_db", TAIR_SERVER_CMD_BACKUP_DB);
}

void tair_client::do_cmd_unload_backuped_db(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "unload_backuped_db", TAIR_SERVER_CMD_UNLOAD_BACKUPED_DB);
}

void tair_client::do_cmd_pause_gc(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "pause_gc", TAIR_SERVER_CMD_PAUSE_GC);
}

void tair_client::do_cmd_resume_gc(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "resume_gc", TAIR_SERVER_CMD_RESUME_GC);
}

void tair_client::do_cmd_pause_rsync(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "pause_rsync", TAIR_SERVER_CMD_PAUSE_RSYNC, 1);
}

void tair_client::do_cmd_resume_rsync(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "resume_rsync", TAIR_SERVER_CMD_RESUME_RSYNC, 1);
}

void tair_client::do_cmd_watch_rsync(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "watch_rsync", TAIR_SERVER_CMD_WATCH_RSYNC, 1);
}

void tair_client::do_cmd_stat_rsync(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "stat_rsync", TAIR_SERVER_CMD_STAT_RSYNC, 1);
}

void tair_client::do_cmd_options_rsync(VSTRING &param) {
    int args = param.size() == 1 ? 1 : 3;
    do_cmd_op_ds_or_not(param, "options_rsync", TAIR_SERVER_CMD_OPTIONS_RSYNC, args);
}

void tair_client::do_cmd_get_rsync_stat(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "get_rsync_stat", TAIR_SERVER_CMD_GET_RSYNC_STAT);
}

void tair_client::do_cmd_get_cluster_info(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "get_cluster_info", TAIR_SERVER_CMD_GET_CLUSTER_INFO);
}

void tair_client::do_cmd_rsync_connection_stat(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "rsync_connection_stat", TAIR_SERVER_CMD_RSYNC_CONNECTION_STAT, 0);
}

void tair_client::do_cmd_set_rsync_config_service(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "set_rsync_config_service", TAIR_SERVER_CMD_SET_RSYNC_CONFIG_SERVICE, 1);
}

void tair_client::do_cmd_set_rsync_config_update_interval(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "set_rsync_config_update_interval", TAIR_SERVER_CMD_SET_RSYNC_CONFIG_INTERVAL, 1);
}

void tair_client::do_cmd_start_balance(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "start_balance", TAIR_SERVER_CMD_START_BALANCE);
}

void tair_client::do_cmd_stop_balance(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "stop_balance", TAIR_SERVER_CMD_STOP_BALANCE);
}

void tair_client::do_cmd_set_balance_wait_ms(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "set_balance_wait", TAIR_SERVER_CMD_SET_BALANCE_WAIT_MS, 1);
}

void tair_client::do_cmd_get_config(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "get_config", TAIR_SERVER_CMD_GET_CONFIG, 0);
}

void tair_client::do_cmd_start_revise_stat(VSTRING &param) {
    bool is_revise_user_stat = true;
    bool force_revise = false;
    string sleep_interval = "0";
    string sleep_time = "1000";
    string instance_str;
    string dataserver;

    // option
    vector<char *> params_for_parse;
    params_for_parse.push_back(NULL);
    params_for_parse.insert(params_for_parse.begin() + 1, param.begin(), param.end());
    int argc = (int32_t) params_for_parse.size();
    char **argv = params_for_parse.data();

    static struct option long_option[] =
            {
                    {"revise-type",    required_argument, NULL, 'p'},
                    {"force-revise",   no_argument,       NULL, 'f'},
                    {"sleep-interval", required_argument, NULL, 'i'},
                    {"sleep-time",     required_argument, NULL, 't'},
                    {"instance",       required_argument, NULL, 0},
                    {"dataserver",     required_argument, NULL, 'd'},
                    {NULL, 0,                             NULL, 0}
            };
    // very important, it has been modified by function parse_cmd_line().
    // and optind = 0 would make getopt execute a initialization.
    optind = 0;
    int ch;
    while ((ch = getopt_long(argc, argv, "p:fi:t:d:", long_option, NULL)) != -1) {
        switch (ch) {
            case 'p':
                if (strcmp(optarg, "user") == 0)
                    is_revise_user_stat = true;
                else if (strcmp(optarg, "physical") == 0)
                    is_revise_user_stat = false;
                else {
                    print_help("start_revise_stat");
                    return;
                }
                break;
            case 'f':
                force_revise = true;
                break;
            case 'i':
                sleep_interval = optarg;
                break;
            case 't':
                sleep_time = optarg;
                break;
            case 0:
                instance_str = optarg;
                break;
            case 'd':
                dataserver = optarg;
                break;
            default:
                print_help("start_revise_stat");
                return;
        }
    }
    if (optind != argc) {
        print_help("start_revise_stat");
        return;
    }
    int sleep_time_int = atoi(sleep_time.c_str());
    if (!(0 <= sleep_time_int && sleep_time_int < 1000000)) {
        fprintf(stderr, "sleep time out of range\n");
        print_help("start_revise_stat");
        return;
    }

    vector<string> all_instance;
    if (!instance_str.empty()) {
        string::size_type pos1, pos2;
        pos1 = 0;
        pos2 = instance_str.find_first_of(',', pos1);
        while (pos2 != string::npos) {
            all_instance.push_back(string(instance_str, pos1, pos2 - pos1));
            pos1 = pos2 + 1;
            pos2 = instance_str.find_first_of(',', pos1);
        }
        all_instance.push_back(string(instance_str, pos1, string::npos));
    }

    vector<string> op_cmd_params;
    op_cmd_params.push_back(is_revise_user_stat ? "1" : "0");
    op_cmd_params.push_back(force_revise ? "1" : "0");
    op_cmd_params.push_back(sleep_interval);
    op_cmd_params.push_back(sleep_time);
    // insert instance
    char instance_num[16];
    snprintf(instance_num, sizeof(instance_num), "%lu", all_instance.size());
    op_cmd_params.push_back(instance_num);
    op_cmd_params.insert(op_cmd_params.end(), all_instance.begin(), all_instance.end());
    int32_t base_param_size = (int32_t) op_cmd_params.size();
    // insert dataserver
    if (!dataserver.empty())
        op_cmd_params.push_back(dataserver);
    op_cmd_result_map result;
    do_cmd_op_ds(op_cmd_params, "start_revise_stat", TAIR_SERVER_CMD_START_REVISE_STAT, base_param_size, result);
    default_print_op_cmd_result_map(result);
}

void tair_client::do_cmd_stop_revise_stat(VSTRING &param) {
    op_cmd_result_map result;
    vector<string> op_cmd_params;
    op_cmd_params.insert(op_cmd_params.end(), param.begin(), param.end());
    do_cmd_op_ds(op_cmd_params, "stop_revise_stat", TAIR_SERVER_CMD_STOP_REVISE_STAT, 0, result);
    default_print_op_cmd_result_map(result);
}

void tair_client::do_cmd_get_revise_status(VSTRING &param) {
    op_cmd_result_map result;
    vector<string> op_cmd_params;
    op_cmd_params.insert(op_cmd_params.end(), param.begin(), param.end());
    do_cmd_op_ds(op_cmd_params, "get_revise_status", TAIR_SERVER_CMD_GET_REVISE_STATUS, 0, result);
    for (op_cmd_result_map::const_iterator it = result.begin(); it != result.end(); ++it) {
        fprintf(stderr, "dataserver: %s, ", tbsys::CNetUtil::addrToString(it->first).c_str());
        fprintf(stderr, "ret code: %d\n", it->second.retcode);
        vector<string>::const_iterator info_it = it->second.infos.begin();
        for (; info_it != it->second.infos.end(); ++info_it)
            fprintf(stderr, "    %s\n", info_it->c_str());
        fprintf(stderr, "\n");
    }
}

void tair_client::do_cmd_set_config(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "set_config", TAIR_SERVER_CMD_SET_CONFIG, 2);
}

void tair_client::do_cmd_ldb_instance_config(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "ldb_instance_config", TAIR_SERVER_CMD_LDB_INSTANCE_CONFIG, 3);
}

void tair_client::do_cmd_new_namespace(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "new_nsu", TAIR_SERVER_CMD_NEW_AREAUNIT, 1);
}

void tair_client::do_cmd_add_namespace(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "add_ns", TAIR_SERVER_CMD_ADD_AREA, 2);
}

void tair_client::do_cmd_link_namespace(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "link_ns", TAIR_SERVER_CMD_LINK_AREA, 2);
}

void tair_client::do_cmd_delete_namespace(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "delete_ns", TAIR_SERVER_CMD_DELETE_AREA, 1);
}

void tair_client::do_cmd_delete_namespaceall(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "delete_ns_all", TAIR_SERVER_CMD_DELETE_AREAUNIT, 1);
}

void tair_client::do_cmd_reset_namespace(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "reset_ns", TAIR_SERVER_CMD_RESET_AREAUNIT, 1);
}

void tair_client::do_cmd_switch_namespace(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "switch_ns", TAIR_SERVER_CMD_SWITCH_AREAUNIT, 1);
}

void tair_client::do_cmd_readonly_on(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "readonly_on", TAIR_SERVER_CMD_NS_READ_ONLY_ON, 1);
}

void tair_client::do_cmd_readonly_off(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "readonly_off", TAIR_SERVER_CMD_NS_READ_ONLY_OFF, 1);
}

void tair_client::do_cmd_readonly_status(VSTRING &param) {
    do_cmd_op_ds_or_not(param, "readonly_status", TAIR_SERVER_CMD_GET_NS_READ_ONLY_STATUS, 1);
}

void
tair_client::do_cmd_op_ds_or_not(VSTRING &param, const char *cmd_str, ServerCmdType cmd_type, int base_param_size) {
    if (static_cast<int>(param.size()) < base_param_size || static_cast<int>(param.size()) > base_param_size + 1) {
        print_help(cmd_str);
        return;
    }
    std::vector<std::string> cmd_params;
    for (int i = 0; i < base_param_size; ++i) {
        cmd_params.push_back(param[i]);
    }

    std::vector<std::string> infos;
    int ret = client_helper.op_cmd_to_ds(cmd_type, &cmd_params, &infos,
                                         static_cast<int>(param.size()) > base_param_size ? param[base_param_size]
                                                                                          : NULL);
    if (ret != TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "%s fail, ret: %d\n", cmd_str, ret);
    } else {
        fprintf(stderr, "%s success.\n", cmd_str);
        std::vector<std::string>::iterator iter;
        for (iter = infos.begin(); iter != infos.end(); iter++) {
            fprintf(stderr, "%s\n", iter->c_str());
        }
    }
}

void tair_client::do_cmd_opkill(VSTRING &param) {
    const char *addr = NULL;
    if (param.size() < 2) {
        fprintf(stderr, "opkill <area> <opcode> [IP:Port]\n");
        return;
    }
    if (param.size() == 3) {
        addr = param[2];
    }
    std::vector<std::string> params;
    params.push_back(param[0]);
    params.push_back(param[1]);
    std::vector<std::string> infos;
    int ret = client_helper.op_cmd_to_ds(TAIR_SERVER_CMD_OPKILL, &params, &infos, addr);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "success\n");
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_opkill_disable(VSTRING &param) {
    const char *addr = NULL;
    if (param.size() > 1) {
        fprintf(stderr, "opkill [IP:Port]\n");
        return;
    }
    if (param.size() == 1) {
        addr = param[0];
    }
    std::vector<std::string> params;
    std::vector<std::string> infos;
    int ret = client_helper.op_cmd_to_ds(TAIR_SERVER_CMD_OPKILL_DISABLE, &params, &infos, addr);
    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "success\n");
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::default_print_op_cmd_result_map(const op_cmd_result_map &result) {
    for (op_cmd_result_map::const_iterator it = result.begin(); it != result.end(); ++it) {
        fprintf(stderr, "dataserver: %s, ", tbsys::CNetUtil::addrToString(it->first).c_str());
        fprintf(stderr, "ret code: %d; ", it->second.retcode);
        vector<string>::const_iterator info_it = it->second.infos.begin();
        for (; info_it != it->second.infos.end(); ++info_it)
            fprintf(stderr, "%s;", info_it->c_str());
        fprintf(stderr, "\n");
    }
}

void
tair_client::do_cmd_op_ds(const vector<string> &param, const char *cmd_str, ServerCmdType cmd_type, int base_param_size,
                          op_cmd_result_map &result) {
    if (static_cast<int>(param.size()) < base_param_size || static_cast<int>(param.size()) > base_param_size + 1) {
        print_help(cmd_str);
        return;
    }
    std::vector<std::string> cmd_params;
    cmd_params.assign(param.begin(), param.begin() + base_param_size);

    const char *dataserver = static_cast<int>(param.size()) > base_param_size ? param[base_param_size].c_str() : NULL;
    client_helper.op_cmd_to_ds_new(cmd_type, cmd_params, dataserver, result);
}

void tair_client::do_cmd_mc_set(VSTRING &param) {
    if (param.size() < 2U || param.size() > 3U) {
        print_help("put");
        return;
    }
    int area = default_area;
    if (param.size() > 2U) area = atoi(param[2]);

    char *akey = NULL;
    int pkeysize = 0, valuesize = 0;
    char *pkey = canonical_key(param[0], &akey, &pkeysize);
    data_entry key(pkey, pkeysize, false);
    data_entry data(param[1], valuesize, false);

    int ret = client_helper.mc_ops(0x01, area, &key, &data, 0, 0, async_mc_set_cb, this);
    fprintf(stderr, "put: %s\n", client_helper.get_error_msg(ret));

    if (ret != TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "set failed: %s.\n", client_helper.get_error_msg(ret));
    } else {
        fprintf(stderr, "asynchronous request sent...\n");
    }
    if (akey) free(akey);
}

void tair_client::do_cmd_mc_add(VSTRING &param) {
    if (param.size() < 2U || param.size() > 3U) {
        print_help("put");
        return;
    }
    int area = default_area;
    if (param.size() > 2U) area = atoi(param[2]);

    char *akey = NULL;
    int pkeysize = 0, valuesize = 0;
    char *pkey = canonical_key(param[0], &akey, &pkeysize);
    data_entry key(pkey, pkeysize, false);
    data_entry data(param[1], valuesize, false);

    int ret = client_helper.mc_ops(0x02, area, &key, &data, 0, 0, async_mc_set_cb, this);
    fprintf(stderr, "put: %s\n", client_helper.get_error_msg(ret));

    if (ret != TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "set failed: %s.\n", client_helper.get_error_msg(ret));
    } else {
        fprintf(stderr, "asynchronous request sent...\n");
    }
    if (akey) free(akey);
}

void tair_client::do_cmd_mc_append(VSTRING &param) {
    if (param.size() < 2U || param.size() > 3U) {
        print_help("put");
        return;
    }
    int area = default_area;
    if (param.size() > 2U) area = atoi(param[2]);

    char *akey = NULL;
    int pkeysize = 0, valuesize = 0;
    char *pkey = canonical_key(param[0], &akey, &pkeysize);
    data_entry key(pkey, pkeysize, false);
    data_entry data(param[1], valuesize, false);

    int ret = client_helper.mc_ops(0x0e, area, &key, &data, 0, 0, async_mc_set_cb, this);
    fprintf(stderr, "put: %s\n", client_helper.get_error_msg(ret));

    if (ret != TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "set failed: %s.\n", client_helper.get_error_msg(ret));
    } else {
        fprintf(stderr, "asynchronous request sent...\n");
    }
    if (akey) free(akey);
}

void tair_client::do_cmd_mc_prepend(VSTRING &param) {
    if (param.size() < 2U || param.size() > 3U) {
        print_help("put");
        return;
    }
    int area = default_area;
    if (param.size() > 2U) area = atoi(param[2]);

    char *akey = NULL;
    int pkeysize = 0, valuesize = 0;
    char *pkey = canonical_key(param[0], &akey, &pkeysize);
    data_entry key(pkey, pkeysize, false);
    data_entry data(param[1], valuesize, false);

    int ret = client_helper.mc_ops(0x0f, area, &key, &data, 0, 0, async_mc_set_cb, this);
    fprintf(stderr, "put: %s\n", client_helper.get_error_msg(ret));

    if (ret != TAIR_RETURN_SUCCESS) {
        fprintf(stderr, "set failed: %s.\n", client_helper.get_error_msg(ret));
    } else {
        fprintf(stderr, "asynchronous request sent...\n");
    }
    if (akey) free(akey);
}

int tair_client::do_cmd_inval_retryall(VSTRING &params) {
    int ret = TAIR_RETURN_SUCCESS;
    if (params.size() == 1) {
        ret = client_helper.retry_all();
    } else if (params.size() == 2) {
        std::string ipport = params[1];
        size_t pos = ipport.find_first_of(':');
        if (pos == std::string::npos || pos == ipport.size()) {
            fprintf(stderr, "should be ip:port, %s", ipport.c_str());
            ret = TAIR_RETURN_FAILED;
        } else {
            std::string ipstr = ipport.substr(0, pos);
            std::string portstr = ipport.substr(pos + 1, ipport.size());
            uint64_t inval_server = tbsys::CNetUtil::strToAddr(ipstr.c_str(), atoi(portstr.c_str()));

            std::vector<uint64_t> inval_server_set;
            int ret_retrieve = client_helper.retrieve_invalidserver(inval_server_set);
            if (ret_retrieve == TAIR_RETURN_SUCCESS) {
                std::vector<uint64_t>::iterator it = std::find(inval_server_set.begin(), inval_server_set.end(),
                                                               inval_server);
                if (it == inval_server_set.end()) {
                    ret = TAIR_RETURN_FAILED;
                    fprintf(stderr, "inval server: %s is not in the list.\n", params[1]);
                } else {
                    ret = client_helper.retry_all(inval_server);
                }
            } else {
                ret = TAIR_RETURN_FAILED;
                fprintf(stderr, "can't got the inval server list.");
            }
        }
    } else {
        print_help("invalcmd");
        ret = TAIR_RETURN_FAILED;
    }
    return ret;
}

int tair_client::do_cmd_inval_info(VSTRING &params) {
    int ret = TAIR_RETURN_SUCCESS;
    if (params.size() == 2) {
        std::string ipport = params[1];
        size_t pos = ipport.find_first_of(':');
        if (pos == std::string::npos || pos == ipport.size()) {
            fprintf(stderr, "should be ip:port, %s", ipport.c_str());
            ret = TAIR_RETURN_FAILED;
        } else {
            std::string ipstr = ipport.substr(0, pos);
            std::string portstr = ipport.substr(pos + 1, ipport.size());
            uint64_t inval_server = tbsys::CNetUtil::strToAddr(ipstr.c_str(), atoi(portstr.c_str()));

            std::vector<uint64_t> inval_server_set;
            int ret_retrieve = client_helper.retrieve_invalidserver(inval_server_set);
            if (ret_retrieve == TAIR_RETURN_SUCCESS) {
                std::vector<uint64_t>::iterator it = std::find(inval_server_set.begin(), inval_server_set.end(),
                                                               inval_server);
                if (it == inval_server_set.end()) {
                    ret = TAIR_RETURN_FAILED;
                    fprintf(stderr, "can't find the inval server: %s \n", params[1]);
                } else {
                    std::string buffer;
                    ret = client_helper.get_invalidserver_info(inval_server, buffer);
                    if (ret == TAIR_RETURN_SUCCESS) {
                        fprintf(stdout, "inval server: %s info: %s \n\n",
                                tbsys::CNetUtil::addrToString(inval_server).c_str(),
                                buffer.c_str());
                    } else {
                        fprintf(stderr, "failed to get inval server: %s info.",
                                tbsys::CNetUtil::addrToString(inval_server).c_str());
                    }
                }
            } else {
                ret = TAIR_RETURN_FAILED;
                fprintf(stderr, "can't got the inval server list.");
            }
        }
    } else {
        ret = TAIR_RETURN_FAILED;
        print_help("invalcmd");
    }
    return ret;
}

int tair_client::do_cmd_inval_retrieve(VSTRING &params) {
    int ret = TAIR_RETURN_SUCCESS;
    std::vector<uint64_t> inval_server_set;
    ret = client_helper.retrieve_invalidserver(inval_server_set);
    if (ret == TAIR_RETURN_SUCCESS) {
        for (size_t i = 0; i < inval_server_set.size(); ++i) {
            fprintf(stdout, "inval server# %d: %s \n", (int) i,
                    tbsys::CNetUtil::addrToString(inval_server_set[i]).c_str());
        }
    } else {
        ret = TAIR_RETURN_FAILED;
        fprintf(stdout, "failed to get inval servers.");
    }
    return ret;
}

void tair_client::do_cmd_to_inval(VSTRING &params) {
    if (params.size() < 1) {
        print_help("invalcmd");
        return;
    }

    int ret = TAIR_RETURN_SUCCESS;
    if (strncmp("retryall", params[0], 8) == 0) {
        ret = do_cmd_inval_retryall(params);
    } else if (strncmp("info", params[0], 4) == 0) {
        ret = do_cmd_inval_info(params);
    } else if (strncmp("retrieve", params[0], 8) == 0) {
        ret = do_cmd_inval_retrieve(params);
    } else {
        ret = TAIR_RETURN_FAILED;
        fprintf(stderr, "unknown cmd: %s \n\n", params[0]);
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        fprintf(stdout, "success. \n");
    } else {
        fprintf(stderr, "failed, ret: %d\n\n", ret);
    }
}

void tair_client::do_cmd_alloc_area(VSTRING &alloc_area_params) {
    if (alloc_area_params.size() != 1) {
        print_help("alloc_area");
        return;
    }

    std::vector<std::string> params;
    char buf[4096];
    sprintf(buf, "%d", TAIR_AUTO_ALLOC_AREA);
    params.push_back(buf);
    params.push_back(alloc_area_params[0]);
    params.push_back(client_helper.get_group_name());
    vector<string> rets;

    int rc = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_ALLOC_AREA, &params, &rets);
    if (rc == TAIR_RETURN_SUCCESS && rets.size() > 0) {
        long area = strtol(rets[0].c_str(), NULL, 10);
        if (area <= 0 || area >= TAIR_MAX_AREA_COUNT) {
            fprintf(stderr, "failed, area out of range: %s, %ld\n\n", rets[0].c_str(), area);
        } else {
            // alloc success
            fprintf(stdout, "success, area is %ld\n", area);
        }
    } else {
        fprintf(stderr, "failed, rc : %d, rets size: %lu\n\n", rc, rets.size());
    }
}

void tair_client::do_cmd_modify_area(VSTRING &modify_area_params) {
    if (modify_area_params.size() != 2) {
        print_help("modify_area");
        return;
    }
    std::vector<std::string> params;
    params.push_back(modify_area_params[0]);
    params.push_back(modify_area_params[1]);
    params.push_back(client_helper.get_group_name());
    int rc = client_helper.op_cmd_to_cs(TAIR_SERVER_CMD_SET_QUOTA, &params, NULL);
    if (TAIR_RETURN_SUCCESS == rc) {
        fprintf(stdout, "success\n");
    } else {
        fprintf(stderr, "failed, rc : %d\n\n", rc);
    }
}

void tair_client::do_cmd_modify_high_ops_upper(VSTRING &params) {
    if (params.size() != 1 && params.size() != 2) {
        print_help("do_cmd_modify_high_ops_upper");
        return;
    }
    std::vector<std::string> cmd_params;
    cmd_params.push_back(params[0]);
    std::vector<std::string> response;
    int ret = -1;
    if (params.size() == 2) {
        ret = client_helper.op_cmd_to_ds(TAIR_ADMIN_SERVER_CMD_MODIFY_STAT_HIGN_OPS, &cmd_params, &response, params[1]);
    } else {
        ret = client_helper.op_cmd_to_ds(TAIR_ADMIN_SERVER_CMD_MODIFY_STAT_HIGN_OPS, &cmd_params, &response, NULL);
    }
    if (ret == TAIR_RETURN_SUCCESS) {
        for (size_t i = 0; i < response.size(); ++i) {
            fprintf(stderr, "%s\n", response[i].c_str());
        }
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_modify_chkexpir_hour_range(VSTRING &params) {
    if (params.size() != 1) {
        print_help("modify_chkexpir_hour_range");
        return;
    }
    std::vector<std::string> cmd_params;
    cmd_params.push_back(params[0]);
    std::vector<std::string> response;
    int ret = client_helper.op_cmd_to_ds(TAIR_SERVER_CMD_MODIFY_CHKEXPIR_HOUR_RANGE, &cmd_params, &response, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        for (size_t i = 0; i < response.size(); ++i) {
            fprintf(stderr, "%s\n", response[i].c_str());
        }
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_modify_mdb_check_granularity(VSTRING &params) {
    if (params.size() != 1) {
        print_help("modify_mdb_check_granularity");
        return;
    }
    std::vector<std::string> cmd_params;
    cmd_params.push_back(params[0]);
    std::vector<std::string> response;
    int ret = client_helper.op_cmd_to_ds(TAIR_SERVER_CMD_MODIFY_MDB_CHECK_GRANULARITY, &cmd_params, &response, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        for (size_t i = 0; i < response.size(); ++i) {
            fprintf(stderr, "%s\n", response[i].c_str());
        }
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_modify_slab_merge_hour_range(VSTRING &params) {
    if (params.size() != 1) {
        print_help("modify_slab_merge_hour_range");
        return;
    }
    std::vector<std::string> cmd_params;
    cmd_params.push_back(params[0]);
    std::vector<std::string> response;
    int ret = client_helper.op_cmd_to_ds(TAIR_SERVER_CMD_MODIFY_SLAB_MERGE_HOUR_RANGE, &cmd_params, &response, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        for (size_t i = 0; i < response.size(); ++i) {
            fprintf(stderr, "%s\n", response[i].c_str());
        }
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_cmd_modify_put_remove_expired(VSTRING &params) {
    if (params.size() != 1) {
        print_help("modify_put_remove_expired");
        return;
    }
    std::vector<std::string> cmd_params;
    cmd_params.push_back(params[0]);
    std::vector<std::string> response;
    int ret = client_helper.op_cmd_to_ds(TAIR_SERVER_CMD_MODIFY_PUT_REMOVE_EXPIRED, &cmd_params, &response, NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        for (size_t i = 0; i < response.size(); ++i) {
            fprintf(stderr, "%s\n", response[i].c_str());
        }
    } else {
        fprintf(stderr, "failed with %d\n", ret);
    }
}

void tair_client::do_rt_enable(VSTRING &params) {
    if (params.size() != 1) {
        fprintf(stderr, "rt_enable <IP:Port>\n");
        return;
    }
    uint64_t sid = tbsys::CNetUtil::strToAddr(params[0], 0);
    //~ i use magic number 1(RT_ENABLE) to avoid unnecessary dependencies
    int rc = client_helper.get_rt(sid, 1, -1, NULL);
    if (TAIR_RETURN_SUCCESS == rc) {
        fprintf(stdout, "success\n");
    } else {
        fprintf(stderr, "failed, rc : %d\n\n", rc);
    }
}

void tair_client::do_rt_disable(VSTRING &params) {
    if (params.size() != 1) {
        fprintf(stderr, "rt_disable <IP:Port>\n");
        return;
    }
    uint64_t sid = tbsys::CNetUtil::strToAddr(params[0], 0);
    //~ i use magic number 2(RT_DISABLE) to avoid unnecessary dependencies
    int rc = client_helper.get_rt(sid, 2, -1, NULL);
    if (TAIR_RETURN_SUCCESS == rc) {
        fprintf(stdout, "success\n");
    } else {
        fprintf(stderr, "failed, rc : %d\n\n", rc);
    }
}

void tair_client::do_rt_reload(VSTRING &params) {
    if (params.size() != 1) {
        fprintf(stderr, "rt_reload <IP:Port>\n");
        return;
    }
    uint64_t sid = tbsys::CNetUtil::strToAddr(params[0], 0);
    //~ i use magic number 3(RT_RELOAD) to avoid unnecessary dependencies
    client_helper.get_rt(sid, 3, -1, NULL);
}

void tair_client::do_rt_reload_light(VSTRING &params) {
    if (params.size() != 1) {
        fprintf(stderr, "rt_reload_light <IP:Port>\n");
        return;
    }
    uint64_t sid = tbsys::CNetUtil::strToAddr(params[0], 0);
    //~ i use magic number 4(RT_RELOAD_LIGHT) to avoid unnecessary dependencies
    client_helper.get_rt(sid, 4, -1, NULL);
}

void tair_client::do_rt(VSTRING &params) {
    if (params.empty() || params.size() > 2) {
        fprintf(stderr, "rt <IP:Port> <opcode>\n");
        return;
    }
    int opcode = -1;
    if (params.size() == 2) {
        opcode = atoi(params[1]);
    }
    uint64_t sid = tbsys::CNetUtil::strToAddr(params[0], 0);
    std::string str;
    //~ i use magic number 0(RT_GET) to avoid unnecessary dependencies
    int rc = client_helper.get_rt(sid, 0, opcode, &str);
    if (TAIR_RETURN_SUCCESS == rc) {
        fprintf(stdout, "success\n");
        if (0 != str.size()) {
            fprintf(stdout, "%s", str.c_str());
        } else {
            fprintf(stdout, "return null\n");
        }
    } else {
        fprintf(stderr, "failed, rc : %d\n\n", rc);
    }
}

} // namespace tair


/*-----------------------------------------------------------------------------
 *  main
 *-----------------------------------------------------------------------------*/

tair::tair_client _globalClient;

void sign_handler(int sig) {
    switch (sig) {
        case SIGTERM:
        case SIGINT:
            _globalClient.cancel();
    }
}

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);

    //TBSYS_LOGGER.setLogLevel("DEBUG");
    TBSYS_LOGGER.setLogLevel("ERROR");
    if (_globalClient.parse_cmd_line(argc, argv) == false) {
        return EXIT_FAILURE;
    }
    if (_globalClient.start()) {
        return EXIT_SUCCESS;
    } else {
        return EXIT_FAILURE;
    }
}
