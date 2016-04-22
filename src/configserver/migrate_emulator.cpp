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

#include <tbsys.h>
#include "define.hpp"
#include "easy_helper.hpp"
#include "table_builder.hpp"
#include "table_builder1.hpp"
#include "table_builder2.hpp"
#include "conf_server_table_manager.hpp"

namespace tair {

namespace migrate_emulator {
// default
int bucket_place_flag = 0;
int lost_tolerance_flag = tair::config_server::NO_DATA_LOST_FLAG;
int build_strategy = 1;
int data_need_move = TAIR_DATA_NEED_MIGRATE;
// for rack strategy
float diff_ratio = 0.0f;
int pos_mask = 0;

inline bool is_in_chs(const char *chs, const char ch) {
    while (*chs != '\0') {
        if (*chs == ch) return true;
        chs++;
    }
    return false;
}

inline char *strip(const char *str, const int length, const char *chs = "\r\n\t\f ") {
    int start = 0;
    int end = length - 1;
    while (start <= end && is_in_chs(chs, str[start])) start++;
    while (start <= end && is_in_chs(chs, str[end])) end--;
    if (start > end) return NULL;
    char *new_str = new char[(sizeof(char) * (end - start + 1 + 1))];
    memcpy(new_str, str + start, end - start + 1);
    new_str[end - start + 1] = '\0';
    return new_str;
}


void print_hash_table(tair::config_server::table_builder::hash_table_type &s_hash_table,
                      tair::config_server::table_builder::hash_table_type &d_hash_table,
                      int copy_count, int bucket_count) {
    if (d_hash_table.empty())
        return;
    for (int j = 0; j < bucket_count; ++j) {
        char kk[64];
        sprintf(kk, "%d-->", j);
        std::string ss(kk);
        for (int i = 0; i < copy_count; ++i) {
            char str[1024];
            sprintf(str, "%s(%-3d)  ",
                    tbsys::CNetUtil::addrToString(d_hash_table[i][j].first).
                            c_str(), d_hash_table[i][j].second);
            ss += str;
        }
        fprintf(stdout, "%s\n", ss.c_str());
    }

    fprintf(stdout, "----------------------------------\n");

    for (int j = 0; j < bucket_count; ++j) {
        for (int i = 0; i < copy_count; ++i) {
            if (s_hash_table[i][j].first != d_hash_table[i][j].first) {
                fprintf(stdout, "%d:%d %s -> %s\n", j, i,
                        tbsys::CNetUtil::addrToString(s_hash_table[i][j].first).c_str(),
                        tbsys::CNetUtil::addrToString(d_hash_table[i][j].first).c_str());
            }
        }
    }
}

// server_table_manager
// now group_table mhash table
// uint64_t *m_hash_table;

int simulate(tair::config_server::node_info_set &upnode_list, uint64_t *m_hash_table,
             int copy_count, int bucket_count) {
    tair::config_server::table_builder *p_table_builder = NULL;
    if (build_strategy == 1) {
        p_table_builder = new tair::config_server::table_builder1(bucket_count, copy_count);
    } else if (build_strategy == 2) {
        p_table_builder = new tair::config_server::table_builder2(diff_ratio, bucket_count, copy_count);
    } else if (build_strategy == 3) {
        log_error("now not support");
        return 1;
//        int tmp_strategy = select_build_strategy(upnode_list);
//        if (tmp_strategy == 1) {
//          p_table_builder = new tair::config_server::table_builder1(bucket_count, copy_count);
//        } else if (tmp_strategy == 2) {
//          p_table_builder = new tair::config_server::table_builder2(diff_ratio, bucket_count, copy_count);
//        } else {
//          log_error("can not find the table_builder object, build strategy: %d, tmp stategy: %d\n", build_strategy, tmp_strategy);
//          return;
//        }
    } else {
        log_error("can not find the table_builder object, build strategy: %d\n", build_strategy);
        return 1;
    }

    p_table_builder->set_pos_mask(pos_mask);
    p_table_builder->set_bucket_place_flag(bucket_place_flag);
    p_table_builder->set_available_server(upnode_list);
    p_table_builder->set_data_lost_flag((tair::config_server::DataLostToleranceFlag) lost_tolerance_flag);
#ifdef TAIR_DEBUG
    log_debug("print available server");
    p_table_builder->print_available_server();
#endif
    tair::config_server::table_builder::hash_table_type quick_table_tmp;
    tair::config_server::table_builder::hash_table_type hash_table_for_builder_tmp;
    tair::config_server::table_builder::hash_table_type dest_hash_table_for_builder_tmp;
    // use current mhash table
    p_table_builder->load_hash_table(hash_table_for_builder_tmp, m_hash_table);
    quick_table_tmp = hash_table_for_builder_tmp;
    bool first_run = false;
    if (data_need_move == TAIR_DATA_NEED_MIGRATE
        && copy_count > 1 && first_run == false) {
        log_debug("will build quick table");
        if (p_table_builder->build_quick_table(quick_table_tmp) == false) {
            delete p_table_builder;
            return 1;
        }
        log_debug("quick table build ok");
    }

    int ret = p_table_builder->rebuild_table(hash_table_for_builder_tmp,
                                             dest_hash_table_for_builder_tmp, true);
    if (ret == 0) {
        log_error("rebuild fail");
        return false;
    } else {
        print_hash_table(hash_table_for_builder_tmp,
                         dest_hash_table_for_builder_tmp, copy_count, bucket_count);
        fprintf(stdout, "-------------------------------\n");
    }

    delete p_table_builder;

    return true;
}

bool try_parse_key_value(const char *buffer, const int length,
                         const char *prefix, const int prefix_length, std::string &value) {
    if (length <= prefix_length /* strlen("groupname=")*/) return false;
    if (strncmp(buffer, prefix, prefix_length) != 0) return false;
    char *new_str = strip(buffer + prefix_length, length - prefix_length);
    value.assign(new_str, strlen(new_str));
    delete[]new_str;
    return true;
}

bool try_parse_key_number(const char *buffer, const int length,
                          const char *prefix, const int prefix_length, int &number) {
    std::string number_str;
    bool ok = try_parse_key_value(buffer, length, prefix, prefix_length, number_str);
    if (!ok) return false;
    char *endptr = NULL;
    const char *number_c_str = number_str.c_str();
    int n = strtoll(number_c_str, &endptr, 10);
    if (endptr != number_c_str + number_str.size()) {
        return false;
    }
    number = n;
    return true;
}

bool try_parse_group_name(const char *buffer, const int length, std::string &groupname) {
    return try_parse_key_value(buffer, length, "groupname=", 10, groupname);
}

bool try_parse_server(const char *buffer, const int length, std::string &server) {
    return try_parse_key_value(buffer, length, "_server_list=", 13, server);
}

bool try_parse_copy_count(const char *buffer, const int length, int &copy_count) {
    return try_parse_key_number(buffer, length, "copy_count=", 11, copy_count);
}

bool try_parse_bucket_count(const char *buffer, const int length, int &bucket_count) {
    return try_parse_key_number(buffer, length, "bucket_count=", 13, bucket_count);
}

uint64_t convert_server_name_to_id(std::string &server) {
    return tbsys::CNetUtil::strToAddr(server.c_str(), 5191);
}

bool load_config(const char *config_file_name,
                 std::vector<uint64_t> &server_ids, std::string &groupname,
                 int *copy_count, int *bucket_count) {
    /*
     *
     *  copy_count=2
     *  bucket_count=1023
     *  groupname=group_1
     *  _server_list=xxx.xxx.xxx.xxx:5191
     *  _server_list=xxx.xxx.xxx.xxx:5191
     *  _server_list=xxx.xxx.xxx.xxx:5191
     *  _server_list=xxx.xxx.xxx.xxx:5191
     *
     */
    *copy_count = -1;
    *bucket_count = -1;
    groupname = "";
    std::string server;
    char buffer[4096];
    FILE *fd = fopen(config_file_name, "r");
    if (fd == NULL) return false;
    while (fgets(buffer, 4096, fd) != NULL) {
        std::string temp_string;
        if (try_parse_group_name(buffer, strlen(buffer), temp_string) == true) {
            groupname = temp_string;
        } else if (try_parse_server(buffer, strlen(buffer), server) == true) {
            server_ids.push_back(convert_server_name_to_id(server));
        } else if (try_parse_copy_count(buffer, strlen(buffer), *copy_count) == true) {
        } else if (try_parse_bucket_count(buffer, strlen(buffer), *bucket_count) == true) {
        }
    }
    fclose(fd);

    if (groupname == "" || server_ids.size() == 0 ||
        *copy_count == -1 || *bucket_count == -1) {
        return false;
    }
    return true;
}

tair::config_server::node_info_set *get_node_info_list(std::vector<uint64_t> &server_ids,
                                                       std::string &groupname) {
    tair::config_server::node_info_set *ninfos = new tair::config_server::node_info_set();
    for (std::vector<uint64_t>::iterator it = server_ids.begin();
         it != server_ids.end(); it++) {
        tair::config_server::server_info *sinfo = new tair::config_server::server_info();
        memset(sinfo, 0, sizeof(tair::config_server::server_info));
        sinfo->server_id = *it;
        if (easy_helper::is_alive(*it)) {
            log_debug("new server [%s] is alive",
                      tbsys::CNetUtil::addrToString(sinfo->server_id).c_str());
            sinfo->status = 0;
            struct timespec tm;
            clock_gettime(CLOCK_MONOTONIC, &tm);
            sinfo->last_time = tm.tv_sec;
        } else {
            log_debug("new server [%s] is down",
                      tbsys::CNetUtil::addrToString(sinfo->server_id).c_str());
            sinfo->status = 1;
            struct timespec tm;
            clock_gettime(CLOCK_MONOTONIC, &tm);
            sinfo->last_time = tm.tv_sec - TAIR_SERVER_DOWNTIME * 2;
        }

        tair::config_server::node_info *node = new tair::config_server::node_info(sinfo);
        ninfos->insert(node);
    }

    return ninfos;
}

void destory_node_info_set(tair::config_server::node_info_set *list) {
    if (list == NULL) return;
    for (tair::config_server::node_info_set::iterator iter = list->begin();
         iter != list->end(); iter++) {
        if (*iter != NULL) {
            if ((*iter)->server != NULL) {
                delete (*iter)->server;
            }
            delete (*iter);
        }
    }
    delete list;
}

}
}

void print_usage() {
    fprintf(stderr, "./emulator config_file group_table_binary_file\n"
            "\tconfig file format:\n"
            "\t#groupname\n"
            "\tgroupname=group_1\n"
            "\t#server_list\n"
            "\t_server_list=xxx.xxx.xxx.xxx:5191\n"
            "\t_server_list=xxx.xxx.xxx.xxx:5191\n"
            "\t_server_list=xxx.xxx.xxx.xxx:5191\n"
            "\t_server_list=xxx.xxx.xxx.xxx:5191\n");
}

int main(int argc, char **argv) {
    TBSYS_LOGGER.setLogLevel("ERROR");

    int copy_count;
    int bucket_count;
    std::string groupname;
    std::vector<uint64_t> server_ids;
    if (argc < 2 || tair::migrate_emulator::load_config(argv[1],
                                                        server_ids, groupname, &copy_count, &bucket_count) == false) {
        print_usage();
        return 1;
    }
    tair::config_server::node_info_set *upnode_list = tair::migrate_emulator::get_node_info_list(server_ids, groupname);
    if (upnode_list == NULL) {
        fprintf(stderr, "get node info list fail");
        return 1;
    }

    tair::config_server::conf_server_table_manager *cstm = new tair::config_server::conf_server_table_manager();
    if (cstm->open(argv[2]) == false) {
        print_usage();
        return 1;
    }
    uint64_t *m_hash_table = cstm->get_m_hash_table();

    tair::migrate_emulator::simulate(*upnode_list, m_hash_table, copy_count, bucket_count);
    tair::migrate_emulator::destory_node_info_set(upnode_list);

    delete cstm;
    return 0;
}

