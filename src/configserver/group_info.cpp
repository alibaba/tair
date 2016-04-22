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

#include "group_info.hpp"
#include "packet_factory.hpp"
#include "easy_helper.hpp"
#include "server_info_allocator.hpp"
#include "table_builder1.hpp"
#include "table_builder2.hpp"
#include "get_server_table_packet.hpp"

namespace tair {
namespace config_server {
using namespace std;
using namespace __gnu_cxx;

easy_io_handler_pt group_info::handler;

group_info::group_info(const char *p_group_name,
                       server_info_map *p_server_info_map,
                       easy_io_t *eio) {
    this->eio = eio;
    easy_helper::init_handler(&handler, this);
    handler.process = packet_handler_cb;

    need_rebuild_hash_table = 0;
    need_send_server_table = 0;
    assert(p_group_name != NULL);
    assert(p_server_info_map != NULL);
    group_name = strdup(p_group_name);
    server_info_maps = p_server_info_map;
    load_config_count = 0;
    min_config_version = TAIR_CONFIG_MIN_VERSION;
    should_syn_mig_info = false;
    group_can_work = true;
    group_is_OK = true;
    build_strategy = 1;
    lost_tolerance_flag = NO_DATA_LOST_FLAG;
    accept_strategy = GROUP_DEFAULT_ACCEPT_STRATEGY;
    bucket_place_flag = 0;
    diff_ratio = 0.6;
    pos_mask = TAIR_POS_MASK;
    server_down_time = TAIR_SERVER_DOWNTIME;
    min_data_server_count = 0;
    pre_load_flag = 0;
    always_update_capacity_info = false;
    // allow_failover_server represent if the group is allowed to do failover
    allow_failover_server = false;
    // failovering represent if the group is doing failovering
    failovering = false;
    tmp_down_server.clear();
    memset(area_bitmap, 0, sizeof(area_bitmap));

    // server table name
    char server_table_file_name[256];
    const char *sz_data_dir =
            TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_DATA_DIR,
                                   TAIR_DEFAULT_DATA_DIR);
    snprintf(server_table_file_name, 256, "%s/%s_server_table",
             sz_data_dir, group_name);
    if (server_table_manager.open(server_table_file_name)) {
        if ((*server_table_manager.migrate_block_count) > 0) {
            log_warn("restore migrate machine list, size: %d",
                     *server_table_manager.migrate_block_count);
            fill_migrate_machine();
        }
        force_check_failover();

        deflate_hash_table();
    }
}

int group_info::fill_migrate_machine() {
    int migrate_count = 0;
    migrate_machine.clear();
    for (uint32_t i = 0; i < server_table_manager.get_server_bucket_count();
         i++) {
        bool migrate_this_bucket = false;
        if (server_table_manager.m_hash_table[i] != 0) {
            if (server_table_manager.m_hash_table[i] !=
                server_table_manager.d_hash_table[i]) {
                migrate_this_bucket = true;
            } else {
                for (uint32_t j = 1; j < server_table_manager.get_copy_count();
                     j++) {
                    bool found = false;
                    for (uint32_t k = 1; k < server_table_manager.get_copy_count();
                         k++) {
                        if (server_table_manager.
                                d_hash_table[j *
                                             server_table_manager.
                                                     get_server_bucket_count() + i]
                            == server_table_manager.m_hash_table[k *
                                                                 server_table_manager.
                                                                         get_server_bucket_count
                                                                         () + i]) {
                            found = true;
                            break;
                        }
                    }
                    if (found == false) {
                        migrate_this_bucket = true;
                        break;
                    }
                }
            }
            if (migrate_this_bucket) {
                migrate_machine[server_table_manager.m_hash_table[i]]++;
                migrate_count++;
                log_info("added migrating machine: %s bucket %d ",
                         tbsys::CNetUtil::addrToString(server_table_manager.
                                 m_hash_table[i]).c_str(),
                         i);
            }
        }
    }
    return migrate_count;
}

group_info::~group_info() {
    if (group_name) {
        free(group_name);
    }
    node_info_set::iterator it;
    for (it = node_list.begin(); it != node_list.end(); ++it) {
        delete (*it);
    }
}

void group_info::deflate_hash_table() {
    server_table_manager.deflate_hash_table();
}

uint32_t group_info::get_client_version() const {
    return *server_table_manager.client_version;
}

uint32_t group_info::get_server_version() const {
    return *server_table_manager.server_version;
}

uint32_t group_info::get_transition_version() const {
    return *server_table_manager.transition_version;
}

uint32_t group_info::get_recovery_version() const {
    return *server_table_manager.recovery_version;
}

uint32_t group_info::get_plugins_version() const {
    return *server_table_manager.plugins_version;
}

uint32_t group_info::get_area_capacity_version() const {
    return *server_table_manager.area_capacity_version;
}

const map<uint32_t, uint64_t> &group_info::get_area_capacity_info() const {
    return area_capacity_info;
}

const uint64_t *group_info::get_hash_table(int mode) const {
    if (mode == 0)
        return server_table_manager.hash_table;
    else if (mode == 1) {
        return server_table_manager.m_hash_table;
    }
    return server_table_manager.d_hash_table;
}

const char *group_info::get_group_name() const {
    return group_name;
}

tbsys::STR_STR_MAP *group_info::get_common_map() {
    return &common_map;
}

const char *group_info::get_hash_table_deflate_data(int mode) const {
    if (mode == 0)
        return server_table_manager.hash_table_deflate_data_for_client;
    return server_table_manager.hash_table_deflate_data_for_data_server;
}

int group_info::get_hash_table_deflate_size(int mode) const {
    if (mode == 0)
        return server_table_manager.hash_table_deflate_data_for_client_size;
    return server_table_manager.
            hash_table_deflate_data_for_data_server_size;
}

// server_list=ip:port,ip:port;...
void group_info::parse_server_list(node_info_set &list,
                                   const char *server_list,
                                   set<uint64_t> &server_id_list) {
    log_debug("parse server list: %s", server_list);
    char *str = strdup(server_list);

    pair<node_info_set::iterator, bool> ret;
    vector<char *> node_list;
    tbsys::CStringUtil::split(str, ";", node_list);
    int default_port =
            TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PORT,
                                TAIR_SERVER_DEFAULT_PORT);
    for (uint32_t i = 0; i < node_list.size(); i++) {
        vector<char *> ip_list;
        tbsys::CStringUtil::split(node_list[i], ",", ip_list);
        server_info *sinfo = NULL;
        for (uint32_t j = 0; j < ip_list.size(); j++) {
            uint64_t id = tbsys::CNetUtil::strToAddr(ip_list[j], default_port);
            if (id == 0) {
                log_error("parse host %s error, check configuration", ip_list[j]);
                continue;
            }
            server_id_list.insert(id);
            server_info_map::iterator it = server_info_maps->find(id);
            sinfo = NULL;
            if (it == server_info_maps->end()) {
                sinfo =
                        server_info_allocator::server_info_allocator_instance.
                                new_server_info(this, id, eio);
                server_info_maps->insert(server_info_map::value_type(id, sinfo));
            } else {
                sinfo = it->second;
                log_debug("add server: %s, %lu", ip_list[j], id);
                break;
            }
        }

        if (sinfo != NULL) {
            node_info *node = new node_info(sinfo);
            if (sinfo->group_info_data != this) {
                sinfo->last_time = 0;
                sinfo->group_info_data->node_list.erase(node);
                sinfo->group_info_data = this;
            }
            ret = list.insert(node);
            if (!ret.second)
                delete node;
            else
                sinfo->node_info_data = node;
        }
    }
    free(str);
}

void group_info::parse_plugins_list(vector<const char *> &plugins) {
    set<string> plugins_set;
    for (vector<const char *>::iterator it = plugins.begin();
         it != plugins.end(); it++) {
        log_debug("parse PlugIns list: %s", *it);
        vector<char *> plugin_list;
        char tmp_str[strlen(*it) + 1];
        strcpy(tmp_str, *it);
        tbsys::CStringUtil::split(tmp_str, ";", plugin_list);
        for (uint32_t i = 0; i < plugin_list.size(); i++) {
            string plugin_name = plugin_list[i];
            if (plugin_name != "") {
                plugins_set.insert(plugin_name);
            }
        }
    }
    if (plugins_set != plugins_name_info) {
        plugins_name_info = plugins_set;
        inc_version(server_table_manager.plugins_version);
    }

}

void group_info::parse_area_key_list(vector<const char *> &a_c) {
    map<std::string, uint32_t> area_key_info_tmp;
    for (vector<const char *>::iterator it = a_c.begin();
         it != a_c.end(); it++) {
        log_debug("parse area key list: %s", *it);
        vector<char *> info;
        char tmp_str[strlen(*it) + 1];
        strcpy(tmp_str, *it);
        tbsys::CStringUtil::split(tmp_str, ";", info);
        for (uint32_t i = 0; i < info.size(); i++) {
            char *p = strchr(info[i], ',');
            if (p) {
                int len = p - info[i];
                char area_key[TAIR_AREA_KEY_MAX_LEN];
                memcpy(area_key, info[i], len);
                area_key[len] = '\0';
                uint32_t area_num = strtoll(p + 1, NULL, 10);
                std::string key(area_key);
                area_key_info_tmp[key] = area_num;
                log_debug("area_key: %s, area_num: %u", area_key, area_num);
            }
        }
    }
    bool need_update = false;
    if (area_key_info.size() != area_key_info_tmp.size()) {
        need_update = true;
    } else {
        for (map<std::string, uint32_t>::iterator new_it =
                area_key_info_tmp.begin();
             new_it != area_key_info_tmp.end(); new_it++) {
            map<std::string, uint32_t>::iterator old_it =
                    area_key_info.find(new_it->first);
            if (old_it == area_key_info.end()
                || new_it->second != old_it->second) {
                need_update = true;
                break;
            }
        }
    }
    if (need_update) {
        area_key_info = area_key_info_tmp;
        inc_version(server_table_manager.area_capacity_version);
    }
}

//input will be like  area,capacity;area,capacity
void group_info::parse_area_capacity_list(vector<const char *> &a_c) {
    map<uint32_t, uint64_t> area_capacity_info_tmp;
    memset(area_bitmap, 0, sizeof(area_bitmap));
    for (vector<const char *>::iterator it = a_c.begin();
         it != a_c.end(); it++) {
        log_debug("parse area capacity list: %s", *it);
        vector<char *> info;
        char tmp_str[strlen(*it) + 1];
        strcpy(tmp_str, *it);
        tbsys::CStringUtil::split(tmp_str, ";", info);
        for (uint32_t i = 0; i < info.size(); i++) {
            char *p = strchr(info[i], ',');
            if (p) {
                uint32_t area = strtoll(info[i], NULL, 10);
                uint64_t capacity = strtoll(p + 1, NULL, 10);
                area_capacity_info_tmp[area] = capacity;
                log_debug("area %u capacity %"
                PRI64_PREFIX
                "u", area, capacity);
                set_bitmap_area(area);
            }
        }
    }
    bool need_update = false;
    if (always_update_capacity_info || (area_capacity_info.size() != area_capacity_info_tmp.size())) {
        need_update = true;
    } else {
        for (map<uint32_t, uint64_t>::iterator new_it =
                area_capacity_info_tmp.begin();
             new_it != area_capacity_info_tmp.end(); new_it++) {
            map<uint32_t, uint64_t>::iterator old_it =
                    area_capacity_info.find(new_it->first);
            if (old_it == area_capacity_info.end()
                || new_it->second != old_it->second) {
                need_update = true;
                break;
            }
        }
    }
    if (need_update) {
        area_capacity_info = area_capacity_info_tmp;
        map<uint32_t, uint64_t>::iterator it = area_capacity_info.begin();
        for (; it != area_capacity_info.end(); it++) {
            log_info("set area %u capacity is %"
            PRI64_PREFIX
            "u", it->first,
                    it->second);
        }
        inc_version(server_table_manager.area_capacity_version);
    }
}

int group_info::get_area_num(const char *area_key, uint32_t &area_num) {
    std::string key_string(area_key);
    std::map<std::string, uint32_t>::iterator it = area_key_info.find(key_string);
    if (it != area_key_info.end()) {
        area_num = it->second;
        return TAIR_RETURN_SUCCESS;
    } else { // if key does not exist, convert area_key to area_num if possible
        uint32_t num = (uint32_t) strtol(area_key, NULL, 10);
        if ((num >= 0 && num < TAIR_MAX_AREA_COUNT) || num == TAIR_AUTO_ALLOC_AREA) {
            area_num = num;
            return TAIR_RETURN_SUCCESS;
        }
    }
    return TAIR_RETURN_FAILED;
}

uint32_t group_info::find_available_area(const char *group_file_name, const char *group_name) {
    //find the most right '0' in bitmap
    static int bit_size = 8 * sizeof(*area_bitmap);
    for (int i = 0; i < TAIR_MAX_AREA_COUNT / bit_size; i++) {
        if (0xFFFFFFFFFFFFFFFF != area_bitmap[i]) {
            int k = ffsll(~area_bitmap[i]);
            //we do not alloc and remove area 0. from 1 on
            area_bitmap[i] |= (1ULL << (k - 1));
            log_warn("new area num %d", i * bit_size + k);
            return i * bit_size + k;
        }
    }
    return TAIR_MAX_AREA_COUNT;
}

void group_info::remove_bitmap_area(int area) {
    //we do not alloc and remove area 0. from 1 on
    static int bit_size = 8 * sizeof(*area_bitmap);
    if (area <= 0)
        return;
    else
        area--;
    int maps = area / bit_size;
    int offset = area % bit_size;
    area_bitmap[maps] &= ~(1ULL << offset);
}

void group_info::set_bitmap_area(int area) {
    //we do not alloc and remove area 0. from 1 on
    static int bit_size = 8 * sizeof(*area_bitmap);
    if (area <= 0)
        return;
    else
        area--;
    int maps = area / bit_size;
    int offset = area % bit_size;
    area_bitmap[maps] |= 1ULL << offset;
}


int group_info::add_area_key_list(const char *group_file_name, const char *group_name, const char *area_key,
                                  uint32_t area_num) {
    int ret = TAIR_RETURN_FAILED;
    char key[TAIR_AREA_KEY_MAX_LEN + 64], value[64];
    snprintf(key, sizeof(key), TAIR_STR_AREA_KEY_LIST);
    snprintf(value, sizeof(value), "%s,%u;", area_key, area_num);
    ret = util::file_util::change_conf(group_file_name, group_name, key, value, util::file_util::CHANGE_FILE_ADD, '=');
    return ret;
}

int group_info::add_area_capacity_list(const char *group_file_name, const char *group_name, const char *area_key,
                                       uint32_t &area_num, const char *quota) {
    int ret = TAIR_RETURN_FAILED;
    uint32_t new_area_num;
    std::string key_string(area_key);
    std::map<std::string, uint32_t>::iterator it = area_key_info.find(key_string);
    char key[64], value[64];
    if (strlen(area_key) == 5 && strncmp(area_key, "65536", 5) == 0) {  // if area_key == 65536, find available_area
        new_area_num = find_available_area(group_file_name, group_name);
        log_warn("allocate a new area %d. ", new_area_num);
        if (TAIR_MAX_AREA_COUNT == new_area_num)
            return TAIR_RETURN_NO_MORE_AREA;
        ret = TAIR_RETURN_SUCCESS;
    } else if (it != area_key_info.end()) { // if key exist, modify quota or delete qutao
        return TAIR_RETURN_AREA_EXISTED;
    } else { // if key does not exist, convert area_key to area_num if possible
        uint32_t num = (uint32_t) strtol(area_key, NULL, 10);
        if (num >= 0 && num < TAIR_MAX_AREA_COUNT) {
            new_area_num = num;
            //ret =  TAIR_RETURN_AREA_NOT_EXISTED;
        } else {  // if area does not exist, allcate a new area and add quota.
            new_area_num = find_available_area(group_file_name, group_name);
            if (TAIR_MAX_AREA_COUNT >= new_area_num)
                return TAIR_RETURN_NO_MORE_AREA;
        }
    }
    snprintf(key, sizeof(key), TAIR_STR_AREA_CAPACITY_LIST);
    snprintf(value, sizeof(value), "%u,%s;", new_area_num, quota);
    ret = util::file_util::change_conf(group_file_name, group_name, key, value, util::file_util::CHANGE_FILE_ADD, '=');
    if (TAIR_RETURN_SUCCESS == ret) {
        long quota_num = strtol(quota, NULL, 10);
        if (quota_num > 0) {
            area_capacity_info.insert(std::pair<uint32_t, uint64_t>(new_area_num, quota_num));
        }
        area_num = new_area_num;
    }
    return ret;
}

int group_info::update_area_capacity_list(const char *group_file_name, const char *group_name, uint32_t area_num,
                                          const char *quota) {
    int ret = TAIR_RETURN_FAILED;
    std::map<uint32_t, uint64_t>::iterator it = area_capacity_info.find(area_num);
    char key[64], value[64];
    snprintf(value, sizeof(value), "%s;", quota);
    if (it != area_capacity_info.end()) { // if key exist, modify quota or delete qutao
        snprintf(key, sizeof(key), TAIR_STR_AREA_CAPACITY_LIST
        "=%d", area_num);
        if (strlen(quota) == 1 && quota[0] == '0') {  // if quota == "0", remove this quota
            util::file_util::change_conf(group_file_name, group_name, key, value, util::file_util::CHANGE_FILE_DELETE,
                                         ',');
            snprintf(key, sizeof(key), TAIR_STR_AREA_KEY_LIST
            "=%d", area_num);
            snprintf(value, sizeof(value), "%d;", area_num);
            ret = util::file_util::change_conf(group_file_name, group_name, key, value,
                                               util::file_util::CHANGE_FILE_DELETE, ',');
            if (ret == TAIR_RETURN_SUCCESS) {
                remove_bitmap_area(area_num);
            }
        } else { // if quota != "0", modify quota
            ret = util::file_util::change_conf(group_file_name, group_name, key, value,
                                               util::file_util::CHANGE_FILE_MODIFY, ',');
        }
    } else {  // if area does not exist, allcate a new area and add quota.
        return TAIR_RETURN_AREA_NOT_EXISTED;
    }
    return ret;
}

void group_info::correct_server_info(bool is_sync) {
    set<uint64_t> live_machines;
    const uint64_t *p = get_hash_table(2);
    //get dest hash table. all server avialbe should in this table. we believe bucket count > machine count;
    for (uint32_t i = 0; i < get_hash_table_size(); ++i) {
        if (p[i] != 0) {
            live_machines.insert(p[i]);
        }
    }
    if (!live_machines.empty()) {
        node_info_set::iterator sit;
        for (sit = node_list.begin(); sit != node_list.end(); ++sit) {
            set<uint64_t>::iterator it =
                    live_machines.find((*sit)->server->server_id);
            if (it == live_machines.end()) {
                if (is_sync) {
                    (*sit)->server->status = server_info::DOWN;
                    //illegal server
                    log_warn("set server %s status is down",
                             tbsys::CNetUtil::addrToString((*sit)->server->
                                     server_id).c_str());

                    if (get_pre_load_flag() == 1) {
                        add_down_server((*sit)->server->server_id);
                    }
                }
            } else {
                (*sit)->server->status = server_info::ALIVE;
            }
        }
    }
}

bool group_info::load_config(tbsys::CConfig &config, uint32_t version,
                             set<uint64_t> &server_id_list) {
    load_config_count++;
    node_info_set server_list;
    bool node_list_updated = false;
    tbsys::STR_STR_MAP common_map_tmp;

    data_need_move =
            config.getInt(group_name, TAIR_STR_GROUP_DATA_NEED_MOVE,
                          TAIR_DATA_NEED_MIGRATE);
    int bucket_count =
            config.getInt(group_name, TAIR_STR_BUCKET_NUMBER,
                          TAIR_DEFAULT_BUCKET_NUMBER);
    int copy_count =
            config.getInt(group_name, TAIR_STR_COPY_COUNT,
                          TAIR_DEFAULT_COPY_COUNT);
    if (!server_table_manager.is_file_opened()) {
        char file_name[256];
        const char *sz_data_dir =
                TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_DATA_DIR,
                                       TAIR_DEFAULT_DATA_DIR);
        snprintf(file_name, 256, "%s/%s_server_table", sz_data_dir,
                 group_name);

        bool tmp_ret = server_table_manager.
                create(file_name, bucket_count, copy_count);
        if (!tmp_ret) {
            log_error("create file: %s failed, bucket_count: %d, copy_count: %d", file_name, bucket_count, copy_count);
            exit(1);
        }

        log_info("set bucket count = %u ok",
                 server_table_manager.get_server_bucket_count());
        log_info("set copy count = %u ok",
                 server_table_manager.get_copy_count());
    }
    if (server_table_manager.get_copy_count() > 1
        && data_need_move != TAIR_DATA_NEED_MIGRATE) {
        data_need_move = TAIR_DATA_NEED_MIGRATE;
        log_error("set _dataNeedMove to %d", TAIR_DATA_NEED_MIGRATE);
    }

    log_debug("loadconfig, version: %d, lastVersion: %d", version,
              *server_table_manager.last_load_config_time);
    min_config_version =
            config.getInt(group_name, TAIR_STR_MIN_CONFIG_VERSION,
                          TAIR_CONFIG_MIN_VERSION);
    if (min_config_version < TAIR_CONFIG_MIN_VERSION) {
        log_error("%s must large than %d. set it to %d",
                  TAIR_STR_MIN_CONFIG_VERSION, TAIR_CONFIG_MIN_VERSION,
                  TAIR_CONFIG_MIN_VERSION);
        min_config_version = TAIR_CONFIG_MIN_VERSION;
    }

    server_down_time =
            config.getInt(group_name, TAIR_STR_SERVER_DOWNTIME,
                          TAIR_SERVER_DOWNTIME);

    vector<const char *> str_list =
            config.getStringList(group_name, TAIR_STR_SERVER_LIST);
    for (uint32_t i = 0; i < str_list.size(); i++) {
        parse_server_list(server_list, str_list[i], server_id_list);
    }

    float f_min_data_server_count =
            strtof(config.
                           getString(group_name, TAIR_STR_MIN_DATA_SRVER_COUNT, "0"),
                   NULL);
    if (f_min_data_server_count < 1 && f_min_data_server_count > 0) {
        min_data_server_count =
                (uint32_t) (f_min_data_server_count * server_list.size());
    } else {
        if (f_min_data_server_count < 0)
            f_min_data_server_count = 0;
        min_data_server_count = (uint32_t) f_min_data_server_count;
    }
    if (min_data_server_count < server_table_manager.get_copy_count()) {
        log_error("%s must large than %d. set it to %d",
                  TAIR_STR_MIN_DATA_SRVER_COUNT,
                  server_table_manager.get_copy_count(),
                  server_table_manager.get_copy_count());
        min_data_server_count = server_table_manager.get_copy_count();
    }
    log_info(" %s = %d", TAIR_STR_MIN_DATA_SRVER_COUNT,
             min_data_server_count);

    int old_build_strategy = build_strategy;
    build_strategy =
            config.getInt(group_name, TAIR_STR_BUILD_STRATEGY,
                          TAIR_BUILD_STRATEGY);

    //modify accept_strategy & lost_tolerance_flag : no need to rebulid table
    accept_strategy =
            static_cast<GroupAcceptStrategy>(config.getInt(group_name, TAIR_STR_ACCEPT_STRATEGY,
                                                           static_cast<int>(GROUP_DEFAULT_ACCEPT_STRATEGY)));
    lost_tolerance_flag =
            static_cast<DataLostToleranceFlag>(config.getInt(group_name, TAIR_STR_DATALOST_FLAG,
                                                             static_cast<int>(NO_DATA_LOST_FLAG)));

    bucket_place_flag =
            config.getInt(group_name, TAIR_STR_BUCKET_DISTRI_FLAG,
                          0);

    log_info("build_strategy: %d, accept_strategy: %d, lost_tolerance_flag: %d, bucket_place_flag: %d",
             build_strategy, static_cast<int>(accept_strategy), static_cast<int>(lost_tolerance_flag),
             bucket_place_flag);

    float old_ratio = diff_ratio;
    diff_ratio =
            strtof(config.
                    getString(group_name, TAIR_STR_BUILD_DIFF_RATIO,
                              TAIR_BUILD_DIFF_RATIO), NULL);
    uint64_t old_pos_mask = pos_mask;
    pos_mask =
            strtoll(config.getString(group_name, TAIR_STR_POS_MASK, "0"), NULL,
                    10);
    if (pos_mask == 0)
        pos_mask = TAIR_POS_MASK;
    log_info(" %s = %"PRI64_PREFIX"X", TAIR_STR_POS_MASK, pos_mask);

    always_update_capacity_info = config.getInt(group_name, TAIR_ALWAYS_UPDATE_CAPACITY_INFO, 0) > 0;
    allow_failover_server = config.getInt(group_name, TAIR_ALLOW_FAILOVER_SERVER, 0) > 0;

    pre_load_flag = config.getInt(group_name, TAIR_PRE_LOAD_FLAG, 0);
    const char *down_servers = config.getString(group_name, TAIR_TMP_DOWN_SERVER, "");
    log_debug("pre load flag: %d", pre_load_flag);
    log_debug("down servers: %s", down_servers);
    if (pre_load_flag == 0) {
        if (strlen(down_servers) != 0) {
            log_error("no %s config or config 0, tmp down server %s config is ignored", TAIR_PRE_LOAD_FLAG,
                      down_servers);
        }
        tmp_down_server.clear();
    } else {
        // parse down server list and init tmp_down_server
        vector<char *> down_server_vec;
        char *tmp_servers = new char[strlen(down_servers) + 1];
        strcpy(tmp_servers, down_servers);
        tbsys::CStringUtil::split(tmp_servers, ";", down_server_vec);
        vector<char *>::iterator vit;
        for (vit = down_server_vec.begin(); vit != down_server_vec.end(); ++vit) {
            tmp_down_server.insert(tbsys::CNetUtil::strToAddr(*vit, 0));
        }
        delete[]tmp_servers;
    }


    vector<string> key_list;
    config.getSectionKey(group_name, key_list);
    for (uint32_t i = 0; i < key_list.size(); i++) {
        const char *key = key_list[i].c_str();
        if (key == NULL || key[0] == '_')
            continue;
        common_map_tmp[key] = config.getString(group_name, key);
    }
    vector<const char *> str_plugins_list =
            config.getStringList(group_name, TAIR_STR_PLUGINS_LIST);
    parse_plugins_list(str_plugins_list);
    vector<const char *> str_area_capacity_list =
            config.getStringList(group_name, TAIR_STR_AREA_CAPACITY_LIST);
    parse_area_capacity_list(str_area_capacity_list);
    vector<const char *> str_area_key_list =
            config.getStringList(group_name, TAIR_STR_AREA_KEY_LIST);
    parse_area_key_list(str_area_key_list);

    if (*server_table_manager.last_load_config_time >= version) {
        log_info("[%s] config file unchanged, load config count: %d",
                 group_name, load_config_count);
        if (load_config_count == 1) {
            common_map = common_map_tmp;
            // update node_list
            node_info_set::iterator it;
            for (it = node_list.begin(); it != node_list.end(); ++it) {
                delete (*it);
            }
            node_list = server_list;
            node_list_updated = true;
        }
        if (!node_list_updated) {
            node_info_set::iterator it;
            for (it = server_list.begin(); it != server_list.end(); ++it) {
                delete (*it);
            }
        }
        return true;
    }

    struct timespec tm;
    clock_gettime(CLOCK_MONOTONIC, &tm);
    time_t now = tm.tv_sec;
    need_rebuild_hash_table = now;
    interval_seconds =
            config.getInt(group_name, TAIR_STR_REPORT_INTERVAL,
                          TAIR_DEFAULT_REPORT_INTERVAL);
    *server_table_manager.last_load_config_time = version;

    bool changed1 = false;
    bool changed2 = false;

    if (common_map_tmp.size() == common_map.size()) {
        for (tbsys::STR_STR_MAP::iterator it = common_map_tmp.begin();
             it != common_map_tmp.end(); ++it) {
            tbsys::STR_STR_MAP::iterator it1 = common_map.find(it->first);
            if (it1 == common_map.end() || it1->second != it->second) {
                changed1 = true;
                break;
            }
        }
    } else {
        changed1 = true;
    }

    if (server_list.size() == node_list.size()) {
        node_info_set::iterator it1 = node_list.begin();
        for (; it1 != node_list.end(); it1++) {
            node_info_set::iterator it = server_list.find(*it1);
            if (it == server_list.end()) {
                changed2 = true;
                break;
            }
        }
    } else {
        changed2 = true;
    }

    if (old_build_strategy != build_strategy) {
        need_rebuild_hash_table = now;
    }
    if (old_pos_mask != pos_mask) {
        need_rebuild_hash_table = now;
    }

    float min =
            old_ratio >
            diff_ratio ? old_ratio - diff_ratio : diff_ratio - old_ratio;
    if (build_strategy != 1 && min > 0.0001) {
        need_rebuild_hash_table = now;
    }

    if (changed1 || changed2) {
        if (changed1) {
            common_map.clear();
            common_map = common_map_tmp;
            inc_version(server_table_manager.client_version);
            // if failovering, we don't want ds check table.
            if (!failovering) {
                inc_version(server_table_manager.server_version);
            }
            log_warn("[%s] version changed: clientVersion: %u", group_name,
                     *server_table_manager.client_version);
        }

        if (changed2) {
            node_info_set::iterator it;
            for (it = node_list.begin(); it != node_list.end(); ++it) {
                delete (*it);
            }
            node_list.clear();
            node_list = server_list;
            node_list_updated = true;

            // update node stat info
            stat_info_rw_locker.wrlock();
            server_info tmp_server_info;
            node_info tmp_node_info(&tmp_server_info);
            for (std::map<uint64_t, node_stat_info>::iterator stat_it = stat_info.begin();
                 stat_it != stat_info.end();) {
                tmp_node_info.server->server_id = stat_it->first;
                // This stat info is not needed any more.
                // NOTE: We only clear stat info here, 'cause this ds has been removed from _server_list,
                //       we do nothing when ds is DOWN (maybe also clear).
                if (node_list.find(&tmp_node_info) == node_list.end()) {
                    log_info("erase stat info of server: %s", tbsys::CNetUtil::addrToString(stat_it->first).c_str());
                    stat_info.erase(stat_it++);
                } else {
                    ++stat_it;
                }
            }
            stat_info_rw_locker.unlock();

            log_warn("nodeList changed");
            need_rebuild_hash_table = now;
        }
    }

    if (!node_list_updated) {
        node_info_set::iterator it;
        for (it = server_list.begin(); it != server_list.end(); ++it) {
            delete (*it);
        }
    }

    return true;
}

// isNeedRebuild
bool group_info::is_need_rebuild() const {
    if (need_rebuild_hash_table == 0)
        return false;
    struct timespec tm;
    clock_gettime(CLOCK_MONOTONIC, &tm);
    time_t now = tm.tv_sec;
    if (need_rebuild_hash_table < now - get_server_down_time())
        return true;
    return false;
}

void group_info::get_up_node(node_info_set &upnode_list) {
    node_info_set::iterator it;
    available_server.clear();
    for (it = node_list.begin(); it != node_list.end(); ++it) {
        node_info *node = (*it);
        if (node->server->status == server_info::ALIVE) {
            log_info("get up node: <%s>", tbsys::CNetUtil::addrToString(node->server->server_id).c_str());
            upnode_list.insert(node);
            available_server.insert(node->server->server_id);
        }
    }
}

void group_info::pending_failover_server(uint64_t server_id) {
    if (allow_failover_server) {
        // safe concurrent update for pending_failover_servers
        tbsys::CThreadGuard guard(&hash_table_set_mutex);
        pending_failover_servers.insert(server_id);
    }
}

void group_info::pending_recover_server(uint64_t server_id) {
    if (failovering) {
        // safe concurrent update for pending_recover_servers
        tbsys::CThreadGuard guard(&hash_table_set_mutex);
        pending_recover_servers.insert(server_id);
    }
}

// hold all lock
void group_info::failover_server(node_info_set &upnode_list, bool &change) {
    change = false;
    if (pending_failover_servers.empty()) {
        return;
    }
    if (!allow_failover_server ||
        server_table_manager.get_copy_count() < 2 ||
        is_migrating()) {
        pending_failover_servers.clear();
        return;
    }

    // store former recovery servers
    set<uint64_t> recovery_servers;
    map<uint64_t, int>::const_iterator it;
    for (it = recovery_machine.begin(); it != recovery_machine.end(); ++it) {
        log_debug("add server %s to recovery_ds", tbsys::CNetUtil::addrToString(it->first).c_str());
        recovery_servers.insert(it->first);
    }

    for (std::set<uint64_t>::const_iterator it = pending_failover_servers.begin();
         it != pending_failover_servers.end();
         ++it) {
        uint64_t server_id = *it;
        if (failover_one_ds(server_id, upnode_list, change)) {
            log_warn("add server: %s into failovering_servers, failover begin",
                     tbsys::CNetUtil::addrToString(server_id).c_str());
            failovering_servers.insert(server_id);
        }
        set<uint64_t>::iterator iter = recovery_servers.find(server_id);
        if (iter != recovery_servers.end()) {
            log_warn("current recover server %s down", tbsys::CNetUtil::addrToString(server_id).c_str());
            if (failovering_servers.find(server_id) == failovering_servers.end()) {
                log_warn("add server: %s into failovering_servers, failover begin",
                         tbsys::CNetUtil::addrToString(server_id).c_str());
                failovering_servers.insert(server_id);
            }
            recovery_servers.erase(iter);
            // cuurent recover server down, force change transition version
            change = true;
        }
    }

    // fill recovery machine
    int recover_count = fill_recovery_machine(recovery_servers);
    if (recover_count > 0) {
        log_warn("need recovery, count: %d", recover_count);
    }
    (*server_table_manager.recovery_block_count) = recover_count > 0 ? recover_count : -1;

    // check_failovering to modify failovring status
    // if pending_failover_server hold some buckets still used, failovring likely to be true
    check_failovering();
    pending_failover_servers.clear();
    log_debug("@@ after failover: %d %d", failovering ? 1 : 0, *server_table_manager.transition_version);
}

bool group_info::failover_one_ds(uint64_t server_id, node_info_set &upnode_list, bool &change, bool check_alive) {
    uint64_t *hash_table = server_table_manager.get_hash_table();
    uint64_t *m_hash_table = server_table_manager.get_m_hash_table();
    int32_t bucket_count = server_table_manager.get_server_bucket_count();
    int32_t copy_count = server_table_manager.get_copy_count();

    bool is_server_failover = false;

    do {
        // check if failover_servers is empty
        if (!failovering_servers.empty()) {
            log_warn("server(%s) already in failover",
                     tbsys::CNetUtil::addrToString(*failovering_servers.begin()).c_str());
            continue;
        }

        if ((*server_table_manager.recovery_block_count) != -1 &&
            recovery_machine.find(server_id) == recovery_machine.end()) {
            log_warn("group already in recovery status, refuse to failover");
            continue;
        }

        server_info sinfo;
        sinfo.server_id = server_id;
        node_info ninfo(&sinfo);
        // ALIVE now
        if (check_alive && upnode_list.find(&ninfo) != upnode_list.end()) {
            log_warn("failover server %s but alive now", tbsys::CNetUtil::addrToString(server_id).c_str());
            continue;
        }

        log_info("failover server: %s", tbsys::CNetUtil::addrToString(server_id).c_str());

        std::vector<int32_t> failover_buckets;
        uint64_t id;
        int32_t valid_copy = 0;
        bool got = false;
        // check is there some bucket whose last copy is on this server
        for (int32_t b = 0; b < bucket_count; ++b) {
            valid_copy = 0;
            got = false;
            for (int32_t c = 0; c < copy_count; ++c) {
                id = hash_table[b + c * bucket_count];
                if (id != 0) {
                    ++valid_copy;
                }
                if (id == server_id) {
                    got = true;
                }
            }

            if (got && valid_copy == 1) {
                log_error("can not failover %s, bucket %d has the last copy here.",
                          tbsys::CNetUtil::addrToString(server_id).c_str(), b);
                failover_buckets.clear();
                break;
            }
            if (got) {
                failover_buckets.push_back(b);
            }
        }

        if (failover_buckets.empty()) {
            continue;
        }

        // do failover
        int32_t bucket = 0;
        for (size_t i = 0; i < failover_buckets.size(); ++i) {
            bucket = failover_buckets[i];
            for (int32_t c = 0; c < copy_count; ++c) {
                if (hash_table[bucket + c * bucket_count] != server_id) {
                    continue;
                }

                if (c == 0) {
                    // bucket's master copy should be switched
                    for (++c; c < copy_count; ++c) {
                        if (hash_table[bucket + c * bucket_count] != 0) {
                            hash_table[bucket] = m_hash_table[bucket] = hash_table[bucket + c * bucket_count];
                            hash_table[bucket + c * bucket_count] = m_hash_table[bucket + c * bucket_count] = 0;
                            is_server_failover = true;
                            change = true;
                            break;
                        }
                    }
                } else {
                    // clear slave copy
                    // change hash_table / m_hash_table
                    hash_table[bucket + c * bucket_count] = m_hash_table[bucket + c * bucket_count] = 0;
                    is_server_failover = true;
                    change = true;
                }
                // this bucket has been handled
                break;
            }
        }

        if (!is_server_failover) {
            log_error("## server(%s) is down, but hold no bucket, then no failover...",
                      tbsys::CNetUtil::addrToString(server_id).c_str());
        }
    } while (false);

    return is_server_failover;
}

// hold all lock
void group_info::recover_server(node_info_set &upnode_list, bool &change) {
    change = false;
    if (pending_recover_servers.empty()) {
        return;
    }
    if (!failovering ||
        server_table_manager.get_copy_count() < 2 ||
        is_migrating()) {
        pending_recover_servers.clear();
        return;
    }

    set<uint64_t> recovery_servers;

    for (std::set<uint64_t>::const_iterator it = pending_recover_servers.begin();
         it != pending_recover_servers.end();
         ++it) {
        recover_one_ds(*it, upnode_list, change, recovery_servers);
    }

    if (!recovery_servers.empty()) {
        int recover_count = fill_recovery_machine(recovery_servers);
        if (recover_count > 0) {
            log_warn("need recovery, count: %d", recover_count);
        }
        (*server_table_manager.recovery_block_count) = recover_count > 0 ? recover_count : -1;
    }

    pending_recover_servers.clear();

    // only detect the recovery machine without change table
    // so need not check_failovering
}

void group_info::recover_one_ds(uint64_t server_id, node_info_set &upnode_list, bool &change,
                                set<uint64_t> &recovery_servers) {
    server_info sinfo;
    sinfo.server_id = server_id;
    node_info ninfo(&sinfo);

    // not ALIVE
    if (upnode_list.find(&ninfo) == upnode_list.end()) {
        log_warn("recover server %s but not alive now", tbsys::CNetUtil::addrToString(server_id).c_str());
        return;
    }

    // check if this server failover before, otherwise should begin migrate
    if (failovering_servers.find(server_id) != failovering_servers.end()) {
        failovering_servers.erase(server_id);
        recovery_servers.insert(server_id);
        log_warn("recover server(%s) detect, remove from failovering_servers and add into recovery_servers",
                 tbsys::CNetUtil::addrToString(server_id).c_str());
        change = true;
    } else {
        log_warn("recover server(%s) detect, but not exist in failovering servers",
                 tbsys::CNetUtil::addrToString(server_id).c_str());
    }
}

int group_info::fill_recovery_machine(set<uint64_t> &recovery_servers) {
    int32_t recovery_count = 0;
    recovery_machine.clear();

    uint64_t *m_hash_table = server_table_manager.get_m_hash_table();
    uint64_t *d_hash_table = server_table_manager.get_d_hash_table();
    int32_t bucket_count = server_table_manager.get_server_bucket_count();
    int32_t copy_count = server_table_manager.get_copy_count();

    if (copy_count != 2) {
        log_error("@error, copy_count isn't 2, but enter the failover process, check configuration");
        return 0;
    }

    uint64_t failover_server = 0, backup_server = 0;
    for (int32_t b = 0; b < bucket_count; ++b) {
        if (m_hash_table[bucket_count + b] != 0)
            continue;

        backup_server = m_hash_table[b];
        failover_server = (backup_server == d_hash_table[b] ? d_hash_table[bucket_count + b] : d_hash_table[b]);

        if (recovery_servers.find(failover_server) != recovery_servers.end()) {
            // test if the already exist in the map
            recovery_machine[failover_server]++;
            recovery_count++;
        }
    }

    return recovery_count;
}

// force check failover status whenever touch group.conf or cs restart
// as here, group table synced from the rightful master cs
void group_info::force_check_failover() {
    if ((*server_table_manager.migrate_block_count) <= 0) {
        check_failovering();
        if (failovering) {
            /*
             *  1. scan tables to get failovering_servers
             *  2. add all failovering_servers to pending_recover_servers
             */
            rebuild_failover_status();
            set_force_rebuild();
            log_warn("table is in failovering status, force rebuild");
        }
    }
}

void group_info::rebuild_failover_status() {
    // scan table to get failovering_servers
    // not pending_failover_servers, as we don't need to change the table
    uint64_t *m_hash_table = server_table_manager.get_m_hash_table();
    uint64_t *d_hash_table = server_table_manager.get_d_hash_table();
    int32_t bucket_count = server_table_manager.get_server_bucket_count();
    int32_t copy_count = server_table_manager.get_copy_count();

    if (copy_count != 2) {
        log_error("@error, copy_count isn't 2, but enter the failover process, check configuration");
        return;
    }

    uint64_t failover_server = 0, backup_server = 0;
    for (int32_t b = 0; b < bucket_count; ++b) {
        if (m_hash_table[bucket_count + b] != 0)
            continue;

        backup_server = m_hash_table[b];
        failover_server = (backup_server == d_hash_table[b] ? d_hash_table[bucket_count + b] : d_hash_table[b]);
        failovering_servers.insert(failover_server);
    }

    // add all failover_servers to pending_recover_servers
    if (failovering_servers.size() != 0) {
        set<uint64_t>::iterator iter(failovering_servers.begin());
        for (; iter != failovering_servers.end(); iter++) {
            pending_recover_servers.insert(*iter);
        }
    }
}

// set the value of failovering
void group_info::check_failovering() {
    if (server_table_manager.get_copy_count() < 2 ||
        is_migrating()) {
        failovering = false;
    } else {
        uint64_t *hash_table = server_table_manager.get_hash_table();
        uint64_t *m_hash_table = server_table_manager.get_m_hash_table();
        uint64_t *d_hash_table = server_table_manager.get_d_hash_table();
        int32_t table_size = server_table_manager.get_hash_table_byte_size();
        failovering =
                memcmp(hash_table, d_hash_table, table_size) != 0 ||
                memcmp(m_hash_table, d_hash_table, table_size) != 0 ||
                memcmp(hash_table, m_hash_table, table_size) != 0;
    }
}

void group_info::reset_failovering() {
    pending_failover_servers.clear();
    pending_recover_servers.clear();
    failovering = false;
}

int group_info::force_migrate_bucket(uint32_t bucket_no, uint32_t copy_index, uint64_t src_server_id,
                                     uint64_t dest_server_id,
                                     tbsys::CRWSimpleLock *p_grp_locker, tbsys::CRWSimpleLock *p_server_locker) {
    // hold all lock
    node_info_set upnode_list;
    p_grp_locker->wrlock();
    p_server_locker->rdlock();
    get_up_node(upnode_list);
    p_server_locker->unlock();

    ops_check_option option(__func__, upnode_list.size());
    option.check_migrate_down = true;
    option.check_failover_down = true;
    option.check_min_ds_num = true;

    int ret = force_table_ops_check(option);

    // check whether src and dest equal
    if (ret == TAIR_RETURN_SUCCESS && src_server_id == dest_server_id) {
        log_error("serverid equal");
        ret = TAIR_RETURN_FAILED;
    }

    // check bucket no
    if (ret == TAIR_RETURN_SUCCESS && bucket_no >= server_table_manager.get_server_bucket_count()) {
        log_error("bucket_no is illegal. bucket_no: %d, server_bucket_count: %d",
                  bucket_no, server_table_manager.get_server_bucket_count());
        ret = TAIR_RETURN_FAILED;
    }

    // check src_server_id and dest_server_id
    if (ret == TAIR_RETURN_SUCCESS && (available_server.find(src_server_id) == available_server.end()
                                       || available_server.find(dest_server_id) == available_server.end())) {
        log_error("server is not exist in available server. src_server: %s, dest_server: %s",
                  tbsys::CNetUtil::addrToString(src_server_id).c_str(),
                  tbsys::CNetUtil::addrToString(dest_server_id).c_str());
        ret = TAIR_RETURN_FAILED;
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        int bucket_index = bucket_no + copy_index * server_table_manager.get_server_bucket_count();
        // check src_server_id again
        if (server_table_manager.m_hash_table[bucket_index] != src_server_id) {
            log_error("src server is wrong. server in m_hash_table: %s, src_server: %s",
                      tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[bucket_index]).c_str(),
                      tbsys::CNetUtil::addrToString(src_server_id).c_str());
            ret = TAIR_RETURN_FAILED;
        } else {
            //check if dest_server_id already hold this bucket?
            for (uint32_t i = 0; i < server_table_manager.get_copy_count(); ++i) {
                if (i != copy_index) {
                    int tmp_bucket_index = bucket_no + i * server_table_manager.get_server_bucket_count();
                    if (server_table_manager.m_hash_table[tmp_bucket_index] == dest_server_id) {
                        log_error(
                                "dest server is already hold this bucket. line: %d, server in m_hash_table: %s, dest_server: %s",
                                i,
                                tbsys::CNetUtil::addrToString(
                                        server_table_manager.m_hash_table[tmp_bucket_index]).c_str(),
                                tbsys::CNetUtil::addrToString(dest_server_id).c_str());
                        ret = TAIR_RETURN_FAILED;
                        break;
                    }
                }
            }
        }

        if (ret == TAIR_RETURN_SUCCESS) {
            log_warn("d_hash_table, bucket_index: %d, src_server: %s, dest_server: %s",
                     bucket_index, tbsys::CNetUtil::addrToString(src_server_id).c_str(),
                     tbsys::CNetUtil::addrToString(dest_server_id).c_str());
            server_table_manager.d_hash_table[bucket_index] = dest_server_id;

            int diff_count = fill_migrate_machine();
            (*server_table_manager.migrate_block_count) =
                    diff_count > 0 ? diff_count : -1;

            inc_version(server_table_manager.client_version);
            inc_version(server_table_manager.server_version);
            *(server_table_manager.transition_version) = 0;
            deflate_hash_table();
            server_table_manager.sync();
            set_force_send_table();
        }
    }

    p_grp_locker->unlock();
    return ret;
}

int group_info::force_switch_bucket(set<uint32_t> bucket_nos, tbsys::CRWSimpleLock *p_grp_locker,
                                    tbsys::CRWSimpleLock *p_server_locker) {
    // hold all lock
    node_info_set upnode_list;
    p_grp_locker->wrlock();
    p_server_locker->rdlock();
    get_up_node(upnode_list);
    p_server_locker->unlock();

    ops_check_option option(__func__, upnode_list.size());
    option.check_migrate_down = true;
    option.check_failover_down = true;
    option.check_min_ds_num = true;

    int ret = force_table_ops_check(option);

    // check bucket num
    if (ret == TAIR_RETURN_SUCCESS) {
        for (set<uint32_t>::const_iterator iter = bucket_nos.begin(); iter != bucket_nos.end(); iter++) {
            uint32_t bucket_no = *iter;
            if (bucket_no >= server_table_manager.get_server_bucket_count()) {
                log_error("%s failed as bucket_no is illegal. bucket_no: %d, server_bucket_count: %d", option.ops_name,
                          bucket_no, server_table_manager.get_server_bucket_count());
                ret = TAIR_RETURN_FAILED;
                break;
            }
        }
    }

    // check the master and slave of bucket
    if (ret == TAIR_RETURN_SUCCESS) {
        for (set<uint32_t>::const_iterator iter = bucket_nos.begin(); iter != bucket_nos.end(); iter++) {
            uint32_t master_index = *iter, slave_index = master_index + server_table_manager.get_server_bucket_count();
            // check if master and slave is in service
            if (server_table_manager.m_hash_table[master_index] == 0
                || server_table_manager.m_hash_table[slave_index] == 0) {
                log_error("%s failed as bucket %d holds less than two copies, master is %s, slave is %s",
                          option.ops_name, master_index,
                          tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[master_index]).c_str(),
                          tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[slave_index]).c_str());
                ret = TAIR_RETURN_FAILED;
                break;
            }

            // check if m_hashtable same as d_hashtable
            if (server_table_manager.m_hash_table[master_index] != server_table_manager.d_hash_table[master_index]
                || server_table_manager.m_hash_table[slave_index] != server_table_manager.d_hash_table[slave_index]) {
                log_error("%s failed as bucket %d, m_hashtable diff from d_hashtable", option.ops_name, master_index);
                ret = TAIR_RETURN_FAILED;
                break;
            }
        }

        // change content of table
        if (ret == TAIR_RETURN_SUCCESS) {
            for (set<uint32_t>::const_iterator iter = bucket_nos.begin(); iter != bucket_nos.end(); iter++) {
                uint32_t master_index = *iter, slave_index =
                        master_index + server_table_manager.get_server_bucket_count();
                uint64_t master_server_id = server_table_manager.d_hash_table[master_index], slave_server_id = server_table_manager.d_hash_table[slave_index];
                log_warn("switch bucket %d : (%s, %s) => (%s, %s)", master_index,
                         tbsys::CNetUtil::addrToString(master_server_id).c_str(),
                         tbsys::CNetUtil::addrToString(slave_server_id).c_str(),
                         tbsys::CNetUtil::addrToString(slave_server_id).c_str(),
                         tbsys::CNetUtil::addrToString(master_server_id).c_str());
                server_table_manager.d_hash_table[master_index] = slave_server_id;
                server_table_manager.d_hash_table[slave_index] = master_server_id;
            }

            int diff_count = fill_migrate_machine();
            (*server_table_manager.migrate_block_count) =
                    diff_count > 0 ? diff_count : -1;

            inc_version(server_table_manager.client_version);
            inc_version(server_table_manager.server_version);
            *(server_table_manager.transition_version) = 0;
            deflate_hash_table();
            server_table_manager.sync();
            set_force_send_table();
        }
    }

    p_grp_locker->unlock();
    return ret;
}

int group_info::active_failover_ds(uint64_t server_id,
                                   tbsys::CRWSimpleLock *p_grp_locker, tbsys::CRWSimpleLock *p_server_locker) {
    // hold all lock
    node_info_set upnode_list;
    p_grp_locker->wrlock();
    p_server_locker->rdlock();
    get_up_node(upnode_list);
    p_server_locker->unlock();

    ops_check_option option(__func__, upnode_list.size());
    option.check_migrate_down = true;
    option.check_failover_allowed = true;
    option.check_failover_down = true;
    option.check_min_ds_num = true;

    int ret = force_table_ops_check(option);

    if (ret == TAIR_RETURN_SUCCESS) {
        bool changed = false, is_server_failover = failover_one_ds(server_id, upnode_list, changed, false);
        check_failovering();

        if (is_server_failover) {
            log_warn("add server: %s into failovering_servers, failover begin",
                     tbsys::CNetUtil::addrToString(server_id).c_str());
            failovering_servers.insert(server_id);
        }

        if (changed) {
            inc_version(server_table_manager.client_version);
            inc_transition_version();
            deflate_hash_table();
            server_table_manager.sync();
            set_force_send_table();
        }
    }

    p_grp_locker->unlock();
    return ret;
}

int group_info::active_recover_ds(uint64_t server_id,
                                  tbsys::CRWSimpleLock *p_grp_locker, tbsys::CRWSimpleLock *p_server_locker) {
    // hold all lock
    node_info_set upnode_list;
    p_grp_locker->wrlock();
    p_server_locker->rdlock();
    get_up_node(upnode_list);
    p_server_locker->unlock();

    ops_check_option option(__func__, upnode_list.size());
    option.check_migrate_down = true;
    option.check_failover_allowed = true;
    option.check_failover_up = true;
    option.check_min_ds_num = true;

    int ret = force_table_ops_check(option);

    // check if active recover server in failovering_servers
    if (ret == TAIR_RETURN_SUCCESS && failovering_servers.find(server_id) == failovering_servers.end()) {
        log_error("%s failed as active recover server %s, but it's not in failover", option.ops_name,
                  tbsys::CNetUtil::addrToString(server_id).c_str());
        ret = TAIR_RETURN_FAILED;
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        set<uint64_t> recovery_servers;
        bool change = false;
        recover_one_ds(server_id, upnode_list, change, recovery_servers);

        if (!recovery_servers.empty()) {
            int recover_count = fill_recovery_machine(recovery_servers);
            if (recover_count > 0) {
                log_error("active recover %s ok, recovery begin", tbsys::CNetUtil::addrToString(server_id).c_str());
                log_warn("need recovery, count: %d", recover_count);
            }
            (*server_table_manager.recovery_block_count) = recover_count > 0 ? recover_count : -1;
        }

        // no need to check failover status, as table not changed
        if (change) {
            inc_transition_version();
            deflate_hash_table();
            server_table_manager.sync();
            set_force_send_table();
        }
    }

    p_grp_locker->unlock();
    return ret;
}

int group_info::force_replace_ds(map<uint64_t, uint64_t> replace_ds_pairs,
                                 tbsys::CRWSimpleLock *p_grp_locker, tbsys::CRWSimpleLock *p_server_locker) {
    // hold all lock
    node_info_set upnode_list;
    p_grp_locker->wrlock();
    p_server_locker->rdlock();
    get_up_node(upnode_list);
    p_server_locker->unlock();

    set<uint64_t> dst_ds_set;
    set<int32_t> target_items;
    int32_t bucket_count = server_table_manager.get_server_bucket_count();
    int32_t copy_count = server_table_manager.get_copy_count();

    ops_check_option option(__func__, upnode_list.size());
    option.check_migrate_down = true;
    option.check_failover_down = true;
    option.check_min_ds_num = true;
    option.allow_single_copy = true;

    int ret = force_table_ops_check(option);

    if (ret == TAIR_RETURN_SUCCESS) {
        map<uint64_t, uint64_t>::const_iterator iter = replace_ds_pairs.begin();
        for (; iter != replace_ds_pairs.end(); iter++) {
            if (available_server.find(iter->first) == available_server.end() ||
                available_server.find(iter->second) == available_server.end()) {
                log_error("server is not exist in available server. src_server: %s, dest_server: %s",
                          tbsys::CNetUtil::addrToString(iter->first).c_str(),
                          tbsys::CNetUtil::addrToString(iter->second).c_str());
                ret = TAIR_RETURN_FAILED;
                break;
            }
            dst_ds_set.insert(iter->second);
        }
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        // make sure dst nodes not exist in table
        for (int i = 0; i < bucket_count && ret == TAIR_RETURN_SUCCESS; i++) {
            for (int j = 0; j < copy_count; j++) {
                int32_t bucket_index = i + j * bucket_count;
                uint64_t server_id = server_table_manager.m_hash_table[bucket_index];
                if (dst_ds_set.find(server_id) != dst_ds_set.end()) {
                    log_error("dst server(%s) hold bucket already", tbsys::CNetUtil::addrToString(server_id).c_str());
                    ret = TAIR_RETURN_FAILED;
                    break;
                }
                if (replace_ds_pairs.find(server_id) != replace_ds_pairs.end()) {
                    target_items.insert(bucket_index);
                }
            }
        }
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        if (target_items.empty()) {
            log_error("src ds hold no bucket");
            ret = TAIR_RETURN_FAILED;
        } else {
            log_warn("begin to replace ds:");
            map<uint64_t, uint64_t>::const_iterator iter = replace_ds_pairs.begin();
            for (; iter != replace_ds_pairs.end(); iter++) {
                log_warn("%s ==> %s", tbsys::CNetUtil::addrToString(iter->first).c_str(),
                         tbsys::CNetUtil::addrToString(iter->second).c_str());
            }

            set<int32_t>::const_iterator set_iter = target_items.begin();
            for (; set_iter != target_items.end(); set_iter++) {
                uint64_t src_server_id = server_table_manager.d_hash_table[*set_iter];
                map<uint64_t, uint64_t>::const_iterator map_iter = replace_ds_pairs.find(src_server_id);
                if (map_iter != replace_ds_pairs.end()) {
                    server_table_manager.d_hash_table[*set_iter] = map_iter->second;
                }
            }

            int diff_count = fill_migrate_machine();
            (*server_table_manager.migrate_block_count) =
                    diff_count > 0 ? diff_count : -1;

            inc_version(server_table_manager.client_version);
            inc_version(server_table_manager.server_version);
            *(server_table_manager.transition_version) = 0;
            deflate_hash_table();
            server_table_manager.sync();
            set_force_send_table();
        }
    }

    p_grp_locker->unlock();
    return ret;
}

int group_info::urgent_offline(uint64_t offline_id, std::set<uint64_t> replace_ids,
                               tbsys::CRWSimpleLock *p_grp_locker, tbsys::CRWSimpleLock *p_server_locker) {
    // hold all lock
    node_info_set upnode_list;
    p_grp_locker->wrlock();
    p_server_locker->rdlock();
    get_up_node(upnode_list);
    p_server_locker->unlock();

    ops_check_option option(__func__, upnode_list.size());
    option.check_min_ds_num = true;
    option.allow_single_copy = true;

    int ret = force_table_ops_check(option);

    // check if replace_ids all alive
    if (ret == TAIR_RETURN_SUCCESS) {
        for (set<uint64_t>::const_iterator iter = replace_ids.begin(); iter != replace_ids.end(); iter++) {
            if (available_server.find(*iter) == available_server.end()) {
                log_error("can not urgent offline, replace_node %s is not alive",
                          tbsys::CNetUtil::addrToString(*iter).c_str());
                ret = TAIR_RETURN_FAILED;
                break;
            }
        }
    }

    // when single_copy, urgent_offline only works in normal status
    bool single_copy = (1 == server_table_manager.get_copy_count());
    if (ret == TAIR_RETURN_SUCCESS && single_copy &&
        ((*server_table_manager.migrate_block_count) != -1 || failovering ||
         available_server.find(offline_id) == available_server.end())) {
        log_error("%s failed as group in abnormal status when copy count is 1", option.ops_name);
        ret = TAIR_RETURN_FAILED;
    }

    // make sure urgent_offline is under what condition
    // 1. normal status, just want to remove one ds
    // 2. migrate status, generate new migration as expect
    // 3. failover status, remove the failover ds
    bool need_quick_kick = false;
    if (ret == TAIR_RETURN_SUCCESS) {
        if ((*server_table_manager.migrate_block_count) != -1) {
            // migrate status, need to check:
            // 1. offline_id is not available
            // 2. change hash_table, m_hash_table
            log_info("urgent offline when migrating");
            if (available_server.find(offline_id) != available_server.end()) {
                log_error("%s failed when group is migrating as urgent offline server %s is still alive",
                          option.ops_name,
                          tbsys::CNetUtil::addrToString(offline_id).c_str());
                ret = TAIR_RETURN_FAILED;
            } else {
                need_quick_kick = true;
            }
        } else if (failovering) {
            // failover status, need to check:
            // 1. if offline_id is the only node in failover_servers
            log_info("urgent offline when failovering");
            if (failovering_servers.find(offline_id) == failovering_servers.end()) {
                log_error("%s failed when group is failovering as urgent offline server %s, but it's not in failover",
                          option.ops_name,
                          tbsys::CNetUtil::addrToString(offline_id).c_str());
                ret = TAIR_RETURN_FAILED;
            }
        } else {
            // normal status
            // offline_id is alive or down
            // if down, need to change hash_table and m_hashtable, if not alive, just change hash_table
            if (available_server.find(offline_id) == available_server.end()) {
                need_quick_kick = true;
            }
            log_info("urgent offline when normal status, offline_id is %s", need_quick_kick ? "dead" : "alive");
        }
    }

    // make sure offline_id not holds the only copy of any bucket
    uint64_t *m_hash_table = server_table_manager.get_m_hash_table();
    uint64_t *d_hash_table = server_table_manager.get_d_hash_table();
    int32_t bucket_count = server_table_manager.get_server_bucket_count();
    int32_t copy_count = server_table_manager.get_copy_count();
    uint64_t *tmp_mhashtable = NULL;
    uint64_t *tmp_dhashtable = NULL;

    // check is there some bucket whose last copy is on this server
    // in the meantime, quick_kick offline_id
    // no need to check when copy count is 1
    if (ret == TAIR_RETURN_SUCCESS && !single_copy) {
        if (need_quick_kick) {
            tmp_mhashtable = (uint64_t *) malloc(bucket_count * copy_count * sizeof(uint64_t));
            memcpy(tmp_mhashtable, m_hash_table, bucket_count * copy_count * sizeof(uint64_t));
        }

        uint64_t id = 0;
        int32_t valid_copy = 0;
        bool got = false;
        for (int32_t b = 0; b < bucket_count; ++b) {
            valid_copy = 0;
            got = false;
            for (int32_t c = 0; c < copy_count; ++c) {
                id = m_hash_table[b + c * bucket_count];
                if (id != 0) {
                    ++valid_copy;
                }
                if (id == offline_id) {
                    got = true;
                }
            }

            if (got && valid_copy == 1) {
                log_error("can not urgent offline %s, bucket %d has the last copy here.",
                          tbsys::CNetUtil::addrToString(offline_id).c_str(), b);
                ret = TAIR_RETURN_FAILED;
                break;
            }

            if (got && need_quick_kick) {
                if (m_hash_table[b] == offline_id) {
                    tmp_mhashtable[b] = tmp_mhashtable[b + bucket_count];
                }
                tmp_mhashtable[b + bucket_count] = 0;
            }
        }
    }

    // change d_hash_table
    // in case of change table fail, create a tmp d_hash_table
    if (ret == TAIR_RETURN_SUCCESS) {
        tmp_dhashtable = (uint64_t *) malloc(bucket_count * copy_count * sizeof(uint64_t));
        memcpy(tmp_dhashtable, d_hash_table, bucket_count * copy_count * sizeof(uint64_t));

        vector<uint64_t> vec_replace_ids(replace_ids.begin(), replace_ids.end());
        bool valid = true, got = false;
        int index = -1, len = vec_replace_ids.size();
        uint64_t partner_id = 0;

        for (int i = 0; i < bucket_count; i++) {
            got = false;
            if (tmp_dhashtable[i] == offline_id) {
                got = true;
                index = i;
                partner_id = single_copy ? 0 : tmp_dhashtable[i + bucket_count];
            } else if (!single_copy && tmp_dhashtable[i + bucket_count] == offline_id) {
                got = true;
                index = i + bucket_count;
                partner_id = tmp_dhashtable[i];
            }

            if (got) {
                int j = random() % len;
                if (vec_replace_ids[j] == partner_id) {
                    // traverse the vec to find a valid one
                    int k = j;
                    while (true) {
                        k++;
                        k = k % len;
                        if (k == j) {
                            log_error("%s failed as bucket %d is hold by (%s, %s), can't be replaced", option.ops_name,
                                      i,
                                      tbsys::CNetUtil::addrToString(tmp_dhashtable[i]).c_str(),
                                      tbsys::CNetUtil::addrToString(tmp_dhashtable[i + bucket_count]).c_str());
                            valid = false;
                            break;
                        }
                        if (vec_replace_ids[k] != partner_id) {
                            j = k;
                            break;
                        }
                    }
                    if (!valid) {
                        break;
                    }
                }
                tmp_dhashtable[index] = vec_replace_ids[j];
            }
        }

        if (valid) {
            // ignore failover once table is rebuilded
            reset_failovering();
            // clear reovery_machine and set recovery_block_count to -1
            failovering_servers.clear();
            recovery_machine.clear();
            *server_table_manager.recovery_block_count = -1;

            if (need_quick_kick) {
                uint64_t *hash_table = server_table_manager.get_hash_table();
                memcpy(hash_table, tmp_mhashtable, bucket_count * copy_count * sizeof(uint64_t));
                memcpy(m_hash_table, tmp_mhashtable, bucket_count * copy_count * sizeof(uint64_t));
            }
            memcpy(d_hash_table, tmp_dhashtable, bucket_count * copy_count * sizeof(uint64_t));

            int diff_count = fill_migrate_machine();
            (*server_table_manager.migrate_block_count) =
                    diff_count > 0 ? diff_count : -1;

            inc_version(server_table_manager.client_version);
            inc_version(server_table_manager.server_version);
            *(server_table_manager.transition_version) = 0;
            deflate_hash_table();
            server_table_manager.sync();
            set_force_send_table();
            log_warn("urgent_offline server: %s success, need migrate: %d",
                     tbsys::CNetUtil::addrToString(offline_id).c_str(), diff_count);
            log_warn("[%s] version changed: clientVersion: %u, serverVersion: %u",
                     group_name, *server_table_manager.client_version,
                     *server_table_manager.server_version);
        } else {
            ret = TAIR_RETURN_FAILED;
        }
    }

    if (NULL != tmp_mhashtable) {
        free(tmp_mhashtable);
        tmp_mhashtable = NULL;
    }

    if (NULL != tmp_dhashtable) {
        free(tmp_dhashtable);
        tmp_dhashtable = NULL;
    }

    p_grp_locker->unlock();
    return ret;
}

int group_info::force_table_ops_check(const ops_check_option &option) {
    int ret = TAIR_RETURN_SUCCESS;

    do {
        // check migrating
        if (option.check_migrate_down && (*server_table_manager.migrate_block_count) != -1) {
            log_error("%s failed as server is migrating. migrate block count: %d", option.ops_name,
                      *(server_table_manager.migrate_block_count));
            ret = TAIR_RETURN_FAILED;
            break;
        }

        // check if allow failover
        if (option.check_failover_allowed && !allow_failover_server) {
            log_error("%s failed as failover is not allowed", option.ops_name);
            ret = TAIR_RETURN_FAILED;
            break;
        }

        // check failover down
        if (option.check_failover_down && failovering) {
            log_error("%s failed as group is in failovering status", option.ops_name);
            ret = TAIR_RETURN_FAILED;
            break;
        }

        // check failover up
        if (option.check_failover_up && !failovering) {
            log_error("%s failed as group is not in failovering status", option.ops_name);
            ret = TAIR_RETURN_FAILED;
            break;
        }

        // check copy_index
        if (2 != server_table_manager.get_copy_count() &&
            (!option.allow_single_copy || 1 != server_table_manager.get_copy_count())) {
            log_error("%s failed as copy_count is %d, allow_single_copy is %d",
                      option.ops_name, server_table_manager.get_copy_count(), option.allow_single_copy);
            ret = TAIR_RETURN_FAILED;
            break;
        }

        // check min_data_server_count
        if (option.check_min_ds_num) {
            //make sure migrate is closed
            if (option.upnode_list_size >= static_cast<size_t>(min_data_server_count)) {
                log_error("%s failed as min_server_count(%d) is not larger than alive_server_num(%"PRI64_PREFIX"u)",
                          option.ops_name, min_data_server_count, option.upnode_list_size);
                ret = TAIR_RETURN_FAILED;
                break;
            }
        }

    } while (false);

    return ret;
}

void group_info::rebuild(uint64_t slave_server_id,
                         tbsys::CRWSimpleLock *p_grp_locker,
                         tbsys::CRWSimpleLock *p_server_locker) {
    log_warn("start rebuild table of %s, build_strategy: %d", group_name, build_strategy);
    node_info_set upnode_list;
    table_builder::hash_table_type quick_table_tmp;
    table_builder::hash_table_type hash_table_for_builder_tmp;        // will put rack info to this
    table_builder::hash_table_type dest_hash_table_for_builder_tmp;
    bool first_run = true;
    tbsys::CThreadGuard guard(&hash_table_set_mutex);
    p_grp_locker->wrlock();
    p_server_locker->rdlock();
    get_up_node(upnode_list);
    p_server_locker->unlock();

    int size = upnode_list.size();
    log_info("[%s] upnodeList.size = %d", group_name, size);
    for (uint32_t i = 0; i < server_table_manager.get_hash_table_size(); i++) {
        if (server_table_manager.m_hash_table[i]) {
            first_run = false;
            break;
        }
    }

    if (size <= (int) min_data_server_count) {
        bool need_rebuild = false;
        // may not rebuild table, try failover and recover server first
        bool failover_change = false, recover_change = false;
        failover_server(upnode_list, failover_change);
        recover_server(upnode_list, recover_change);
        log_debug("failover_change is %d, recover_change is %d, failovering is %d",
                  failover_change, recover_change, failovering);

        if (size < (int) min_data_server_count) {
            if (first_run == false) {
                log_error("can not get enough data servers. need %d lef %d ",
                          min_data_server_count, size);
            }

            inc_version(server_table_manager.client_version);
            // not rebuild actually, so don't touch
            if (!failovering) {
                inc_version(server_table_manager.server_version);
            }
            send_server_table_packet(slave_server_id);
        } else {
            // rebuild the table when neither failover nor recover
            // eg. start cluster first time, always set min_data_server equal server count
            need_rebuild = !failover_change && !recover_change;
        }

        log_debug("@@ need rebuild %d, failchange %d, recover change %d", need_rebuild, failover_change,
                  recover_change);
        if (!need_rebuild) {
            if (failover_change || recover_change) {
                inc_transition_version();
                *server_table_manager.recovery_version = 0;
                deflate_hash_table();
                server_table_manager.sync();
                set_force_send_table();
            }
            p_grp_locker->unlock();
            return;
        }
    }

    // ignore failover once table is rebuilded
    reset_failovering();
    // clear reovery_machine and set recovery_block_count to -1
    failovering_servers.clear();
    recovery_machine.clear();
    *server_table_manager.recovery_block_count = -1;

    table_builder *p_table_builder;
    if (build_strategy == 1) {
        p_table_builder =
                new table_builder1(server_table_manager.get_server_bucket_count(),
                                   server_table_manager.get_copy_count());
    } else if (build_strategy == 2) {
        p_table_builder =
                new table_builder2(diff_ratio,
                                   server_table_manager.get_server_bucket_count(),
                                   server_table_manager.get_copy_count());
    } else if (build_strategy == 3) {
        int tmp_strategy = select_build_strategy(upnode_list);
        if (tmp_strategy == 1) {
            p_table_builder =
                    new table_builder1(server_table_manager.get_server_bucket_count(),
                                       server_table_manager.get_copy_count());
        } else if (tmp_strategy == 2) {
            p_table_builder =
                    new table_builder2(diff_ratio,
                                       server_table_manager.get_server_bucket_count(),
                                       server_table_manager.get_copy_count());
        } else {
            log_error("can not find the table_builder object, build strategy: %d, tmp stategy: %d\n", build_strategy,
                      tmp_strategy);
            p_grp_locker->unlock();
            return;
        }
    } else {
        log_error("can not find the table_builder object, build strategy: %d\n", build_strategy);
        p_grp_locker->unlock();
        return;
    }

    p_table_builder->set_pos_mask(pos_mask);
    p_table_builder->set_bucket_place_flag(bucket_place_flag);
    p_table_builder->set_available_server(upnode_list);
    p_table_builder->set_data_lost_flag(lost_tolerance_flag);
#ifdef TAIR_DEBUG
    log_debug("print available server");
    p_table_builder->print_available_server();
#endif
    // use current mhash table
    p_table_builder->load_hash_table(hash_table_for_builder_tmp,
                                     server_table_manager.m_hash_table);
    quick_table_tmp = hash_table_for_builder_tmp;
    if ((data_need_move == TAIR_DATA_NEED_MIGRATE
         && server_table_manager.get_copy_count() > 1)
        && first_run == false) {
        log_debug("will build quick table");
        if (p_table_builder->build_quick_table(quick_table_tmp) == false) {
            set_group_is_OK(false);
            delete p_table_builder;
            inc_version(server_table_manager.client_version);
            inc_version(server_table_manager.server_version);
            send_server_table_packet(slave_server_id);
            p_grp_locker->unlock();
            return;
        }
        log_debug("quick table build ok");
        // init transition_version in cs;
        *(server_table_manager.transition_version) = 0;
        log_debug("reset transition_version of group, transition_version is 0");
    }
    // p_grp_locker->unlock();
    struct timeval t_start, t_end;
    gettimeofday(&t_start, NULL);
    int ret =
            p_table_builder->rebuild_table(hash_table_for_builder_tmp,
                                           dest_hash_table_for_builder_tmp, true);
    gettimeofday(&t_end, NULL);
    log_error("rebuild_table consume time : %lu us",
              1000000 * (t_end.tv_sec - t_start.tv_sec) + t_end.tv_usec - t_start.tv_usec);
    // p_grp_locker->wrlock();
    if (ret == 0) {
        log_error("build table fail. fatal error.");
        set_group_status(false);
        set_group_is_OK(false);
        delete p_table_builder;
        *server_table_manager.transition_version = 0;
        *server_table_manager.recovery_version = 0;
        inc_version(server_table_manager.client_version);
        inc_version(server_table_manager.server_version);
        send_server_table_packet(slave_server_id);
        p_grp_locker->unlock();
        return;
    }

    p_table_builder->write_hash_table(dest_hash_table_for_builder_tmp,
                                      server_table_manager.d_hash_table);
    if ((data_need_move == TAIR_DATA_NEED_MIGRATE) && first_run == false) {
        log_info("need migrate write quick table to _p_hashTable");
        p_table_builder->write_hash_table(quick_table_tmp,
                                          server_table_manager.m_hash_table);
        p_table_builder->write_hash_table(quick_table_tmp,
                                          server_table_manager.hash_table);
        if (server_table_manager.get_copy_count() <= 1) {
            for (uint32_t i = 0;
                 i <
                 server_table_manager.get_server_bucket_count() *
                 server_table_manager.get_copy_count(); i++) {
                if (available_server.find(server_table_manager.m_hash_table[i])
                    == available_server.end()) {
                    server_table_manager.m_hash_table[i] =
                    server_table_manager.hash_table[i] =
                            server_table_manager.d_hash_table[i];
                }
            }
        } else if (lost_tolerance_flag == ALLOW_DATA_LOST_FALG) {
            for (uint32_t i = 0; i < server_table_manager.get_server_bucket_count(); ++i) {
                // if master bucket is not available, use d_hash_table to fill m_hash_table
                if (available_server.find(server_table_manager.m_hash_table[i]) == available_server.end()) {
                    log_info("index %u:%s in m_hash_table is invalid, use d_hash_table: %s",
                             i, tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[i]).c_str(),
                             tbsys::CNetUtil::addrToString(server_table_manager.d_hash_table[i]).c_str());
                    for (uint32_t j = 0; j < server_table_manager.get_copy_count(); ++j) {
                        uint32_t bucket_index = i + j * server_table_manager.get_server_bucket_count();
                        server_table_manager.m_hash_table[bucket_index] =
                        server_table_manager.hash_table[bucket_index] =
                                server_table_manager.d_hash_table[bucket_index];
                    }
                }
            }
        }
    } else {
        log_info("need`t migrate write original table to _p_hashTable");
        p_table_builder->write_hash_table(dest_hash_table_for_builder_tmp,
                                          server_table_manager.m_hash_table);
        p_table_builder->write_hash_table(dest_hash_table_for_builder_tmp,
                                          server_table_manager.hash_table);
    }
#ifdef TAIR_DEBUG
    log_info("_confServerTableManager._p_hashTable");
    for(uint32_t i = 0;
        i <
        server_table_manager.get_server_bucket_count() *
        server_table_manager.get_copy_count(); i++)
      log_info("+++ line: %d bucket: %d => %s",
               i / server_table_manager.get_server_bucket_count(), i % server_table_manager.get_server_bucket_count(),
               tbsys::CNetUtil::addrToString(server_table_manager.
                                             hash_table[i]).c_str());
    log_info("_mhashTable");
    for(uint32_t i = 0;
        i <
        server_table_manager.get_server_bucket_count() *
        server_table_manager.get_copy_count(); i++)
      log_info("+++ line: %d bucket: %d => %s",
               i / server_table_manager.get_server_bucket_count(), i % server_table_manager.get_server_bucket_count(),
               tbsys::CNetUtil::addrToString(server_table_manager.
                                             m_hash_table[i]).c_str());
    log_info("_dhashTable");
    for(uint32_t i = 0;
        i <
        server_table_manager.get_server_bucket_count() *
        server_table_manager.get_copy_count(); i++)
      log_info("+++ line: %d bucket: %d => %s",
               i / server_table_manager.get_server_bucket_count(), i % server_table_manager.get_server_bucket_count(),
               tbsys::CNetUtil::addrToString(server_table_manager.
                                             d_hash_table[i]).c_str());
#endif
    //_p_hashTable is the table for client and data server to use now,
    //_p_m_hashTable is the state of migration process
    //_p_d_hashTable is the table when migration finished correctly
    //migration is a process which move data based on _p_m_hashTable to data based on _p_d_hashTable
    //when the migration is finished _p_hashTable will be covered by _p_d_hashTable, so client can use the new table
    //but if something interrupt the migrationg process, for example lost a data server,
    //we will use _p_m_hashTable to build a new _p_d_hashTable.
    //data server will receive _p_hashTable and  _p_d_hashTable so they know how to start a migration.
    //client will receive _p_hashTable they will use this table to decide which data server they should ask for a key.
    int diff_count = 0;
    if (first_run == false) {
        diff_count = fill_migrate_machine();
    }
    (*server_table_manager.migrate_block_count) =
            diff_count > 0 ? diff_count : -1;
    inc_version(server_table_manager.client_version);
    inc_version(server_table_manager.server_version);
    inc_version(
            server_table_manager.area_capacity_version);        //data server count changed, so capacity for every data server will be change at the same time
    log_warn("[%s] version changed: clientVersion: %u, serverVersion: %u",
             group_name, *server_table_manager.client_version,
             *server_table_manager.server_version);
    deflate_hash_table();
    server_table_manager.sync();
    {
        //will caculate different ratio
        diff_count = 0;
        for (uint32_t i = 0;
             i < server_table_manager.get_server_bucket_count(); i++) {
            for (uint32_t j = 0; j < server_table_manager.get_copy_count(); j++) {
                bool migrate_it = true;
                for (uint32_t k = 0; k < server_table_manager.get_copy_count();
                     ++k) {
                    if (server_table_manager.
                            d_hash_table[i +
                                         server_table_manager.get_server_bucket_count() *
                                         j] ==
                        server_table_manager.m_hash_table[i +
                                                          server_table_manager.
                                                                  get_server_bucket_count() *
                                                          k]) {
                        migrate_it = false;
                        break;
                    }
                }
                if (migrate_it) {
                    diff_count++;
                }
            }
        }
    }
    log_info("[%s] different ratio is : migrateCount:%d/%d=%.2f%%",
             group_name, diff_count,
             server_table_manager.get_server_bucket_count() *
             server_table_manager.get_copy_count(),
             100.0 * diff_count /
             (server_table_manager.get_server_bucket_count() *
              server_table_manager.get_copy_count()));

    if (diff_count > 0) {
        log_warn("need migrate, count: %d", diff_count);
    }

    print_server_count();
    send_server_table_packet(slave_server_id);
    p_grp_locker->unlock();
    set_group_status(true);
    set_group_is_OK(true);
    delete p_table_builder;
    return;
}

bool group_info::do_finish_migrate(uint64_t server_id,
                                   uint32_t server_version, int bucket_id,
                                   uint64_t slave_server_id) {
    if (data_need_move != TAIR_DATA_NEED_MIGRATE)
        return false;
    log_info
            ("migrate finish: from[%s] base version: [%d], now version: [%d] ",
             tbsys::CNetUtil::addrToString(server_id).c_str(), server_version,
             *server_table_manager.server_version);
    if (server_version != *server_table_manager.server_version) {
        log_error("migrate version conflict, ds version: %d, cs version: %d", server_version,
                  *server_table_manager.server_version);
        return false;
    }
    bool ret_code = true;        //migrate ok
    if (ret_code) {
        set_migrating_hashtable(bucket_id, server_id);
    }
    //else {
    //  set_force_rebuild();
    //}
    return true;
}

bool group_info::do_finish_recovery(uint64_t server_id, uint32_t server_version,
                                    uint32_t transition_version, int bucket_id) {
    log_info
            ("@@ recovery finish: from[%s] base version: [%d:%d], now version: [%d:%d], bucket_id: [%d] ",
             tbsys::CNetUtil::addrToString(server_id).c_str(), server_version, transition_version,
             *server_table_manager.server_version, *server_table_manager.transition_version, bucket_id);

    if (server_version != *server_table_manager.server_version) {
        log_error("recovery server version conflict, ds version: %d, cs version: %d", server_version,
                  *server_table_manager.server_version);
        return false;
    }
    // should also record the transistion version here
    if (transition_version != *server_table_manager.transition_version) {
        log_error("recovery transition version conflict, ds version: %d, cs version: %d", transition_version,
                  *server_table_manager.transition_version);
        return false;
    }

    // change the content of bucket in tables
    set_recovery_hashtable(bucket_id, server_id);

    return true;
}

bool group_info::check_migrate_complete(uint64_t slave_server_id, tbsys::CRWSimpleLock *p_grp_locker) {
    p_grp_locker->rdlock();
    if (data_need_move != TAIR_DATA_NEED_MIGRATE
        || (*server_table_manager.migrate_block_count) == -1) {
        p_grp_locker->unlock();
        return true;
    }
    if (migrate_machine.size() > 0U) {
        map<uint64_t, int>::iterator it;
        for (it = migrate_machine.begin(); it != migrate_machine.end(); ++it) {
            server_info_map::iterator ait = server_info_maps->find(it->first);
            if (ait == server_info_maps->end()
                || ait->second->status == server_info::DOWN) {
                log_info("migrate machine %s down",
                         tbsys::CNetUtil::addrToString(it->first).c_str());
                p_grp_locker->unlock();
                return true;
            }
        }
        log_info("[%s] still have %lu data server are in migrating.",
                 group_name, migrate_machine.size());
        if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG) {
            for (it = migrate_machine.begin(); it != migrate_machine.end(); it++) {
                log_debug
                        ("server %s still have %d buckets waiting to be migrated",
                         tbsys::CNetUtil::addrToString(it->first).c_str(), it->second);
            }
        }
        if (should_syn_mig_info) {
            should_syn_mig_info = false;
            set_force_send_table();
        }
        p_grp_locker->unlock();
        return false;
    }
    p_grp_locker->unlock();

    p_grp_locker->wrlock();
    assert(memcmp(server_table_manager.m_hash_table,
                  server_table_manager.d_hash_table,
                  server_table_manager.get_hash_table_byte_size()) == 0);
    inc_version(server_table_manager.client_version);
    inc_version(server_table_manager.server_version);
    // reset transition version
    *server_table_manager.transition_version = 0;
    log_warn("[%s] version changed: clientVersion: %u, serverVersion: %u",
             group_name, *server_table_manager.client_version,
             *server_table_manager.server_version);
    memcpy((void *) server_table_manager.hash_table,
           (const void *) server_table_manager.d_hash_table,
           server_table_manager.get_hash_table_byte_size());
    deflate_hash_table();
    (*server_table_manager.migrate_block_count) = -1;
    server_table_manager.sync();
    log_warn("migrate all done");
    log_info("[%s] migrating is completed", group_name);
    reported_serverid.clear();
    print_server_count();
    should_syn_mig_info = false;
    set_force_send_table();
    p_grp_locker->unlock();
    return true;
}

bool group_info::check_recovery_complete(uint64_t slave_server_id, tbsys::CRWSimpleLock *p_grp_locker) {
    p_grp_locker->rdlock();
    if ((*server_table_manager.recovery_block_count) == -1) {
        p_grp_locker->unlock();
        return true;
    }
    if (recovery_machine.size() > 0U) {
        log_info("[%s] still have %lu data server are in recovering.",
                 group_name, recovery_machine.size());
        map<uint64_t, int>::iterator it;
        if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG) {
            for (it = recovery_machine.begin(); it != recovery_machine.end(); it++) {
                log_debug
                        ("server %s still have %d buckets waiting to be recovered",
                         tbsys::CNetUtil::addrToString(it->first).c_str(), it->second);
            }
        }
        if (should_syn_mig_info) {
            should_syn_mig_info = false;
            set_force_send_table();
        }
        p_grp_locker->unlock();
        return false;
    }
    p_grp_locker->unlock();

    // check recovery done only means that current recovery process end
    // there may be ds that is still in failover but didn't recovery
    // we needn't do anything, not even change the table
    // just check if failovering is over, in that case, we reset transistion version

    log_warn("recovery all done for this round");
    p_grp_locker->wrlock();
    *server_table_manager.recovery_block_count = -1;
    check_failovering();
    if (!failovering) {
        // reset transition version, incr server version
        // incr server version so dataserver can delete fail log
        *server_table_manager.transition_version = 0;
        *server_table_manager.recovery_version = 0;
        log_info("[%s] transition version reset", group_name);
        inc_version(server_table_manager.client_version);
        inc_version(server_table_manager.server_version);
        server_table_manager.sync();
        should_syn_mig_info = false;
        set_force_send_table();
        log_warn("failover all done");
        log_info("[%s] failovering is completed", group_name);
    }
    p_grp_locker->unlock();

    return true;
}

void group_info::set_force_rebuild() {
    need_rebuild_hash_table = 1;
}

void group_info::print_server_count() {
    hash_map<uint64_t, int, hash<int> > m;
    for (uint32_t i = 0; i < server_table_manager.get_server_bucket_count();
         i++) {
        m[server_table_manager.hash_table[i]]++;
    }
    for (hash_map<uint64_t, int, hash<int> >::iterator it = m.begin();
         it != m.end(); ++it) {
        log_info("[%s] %s => %d", group_name,
                 tbsys::CNetUtil::addrToString(it->first).c_str(),
                 it->second);
    }
}

void group_info::send_server_table_packet(uint64_t slave_server_id) {
    if (slave_server_id == 0)
        return;

    response_get_server_table *packet = new response_get_server_table();
    packet->set_data(server_table_manager.get_data(),
                     server_table_manager.get_size());
    packet->set_group_name(group_name);

    log_info("[%s] send servertable to %s: hashcode: %u", group_name,
             tbsys::CNetUtil::addrToString(slave_server_id).c_str(),
             util::string_util::mur_mur_hash(packet->data, packet->size));

    if (easy_helper::easy_async_send(eio, slave_server_id, packet, this, &handler) == EASY_ERROR) {
        log_error("[%s] Send ResponseGetServerTablePacket %s failure.",
                  group_name,
                  tbsys::CNetUtil::addrToString(slave_server_id).c_str());
        delete packet;
    }
}

bool group_info::write_server_table_packet(char *data, int size) {
    char *mdata = server_table_manager.get_data();
    // header len
    int hlen = ((char *) server_table_manager.hash_table) - mdata;
    if (server_table_manager.get_size() == size && mdata != NULL
        && data != NULL && hlen < size) {
        memcpy(mdata + hlen, data + hlen, size - hlen);
        deflate_hash_table();
        memcpy(mdata, data, hlen);
        server_table_manager.sync();
        (*server_table_manager.last_load_config_time) = 0;        // force to reload config file, since the file is changed
        log_info
                ("[%s] accept servertable successed, hashcode: %u, migrateBlockCount: %d, "
                         "version changed: clientVersion: %u, serverVersion: %u",
                 group_name, util::string_util::mur_mur_hash(mdata, size),
                 *server_table_manager.migrate_block_count,
                 *server_table_manager.client_version,
                 *server_table_manager.server_version);
        log_info("clear local migrate machines");
        if ((*server_table_manager.migrate_block_count) > 0) {
            fill_migrate_machine();
        } else {
            *server_table_manager.transition_version = 0;
        }
        need_rebuild_hash_table = 0;
        correct_server_info(true);
        find_available_server();
        return true;
    }
    log_error("size does not match, check group.conf");
    return false;
}

int group_info::packet_handler(easy_request_t *r) {
    int rc = r->ipacket != NULL ? EASY_OK : EASY_ERROR;
    if (rc == EASY_ERROR) {
        log_error("[%s] syn servertable to %s, timeout\n", group_name,
                  easy_helper::easy_connection_to_str(r->ms->c).c_str());
    } else {
        log_info("[%s] syn servertable ok.\n", group_name);
    }
    easy_session_destroy(r->ms);
    return rc;
}

const char *group_info::get_server_table_data() const {
    return (const char *) server_table_manager.mmap_file.get_data();
}

int group_info::get_server_table_size() const {
    return server_table_manager.mmap_file.get_size();
}

bool group_info::is_migrating() const {
    return (*server_table_manager.migrate_block_count) != -1;
}

// check if this group is in recoverying
bool group_info::is_recoverying() const {
    return (*server_table_manager.recovery_block_count) != -1;
}

void group_info::set_migrating_hashtable(size_t bucket_id,
                                         uint64_t server_id) {
    bool ret = false;
    if (bucket_id > server_table_manager.get_server_bucket_count()
        || is_migrating() == false)
        return;
    if (server_table_manager.m_hash_table[bucket_id] != server_id) {
        log_error("where you got this ? old hashtable? bucket_id: %lu, m_server: %s, server: %s",
                  bucket_id, tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[bucket_id]).c_str(),
                  tbsys::CNetUtil::addrToString(server_id).c_str());
        return;
    }
    tbsys::CThreadGuard guard(&hash_table_set_mutex);

    for (uint32_t i = 0; i < server_table_manager.get_copy_count(); i++) {
        uint32_t idx =
                i * server_table_manager.get_server_bucket_count() + bucket_id;
        if (server_table_manager.m_hash_table[idx] !=
            server_table_manager.d_hash_table[idx]) {
            server_table_manager.m_hash_table[idx] =
                    server_table_manager.d_hash_table[idx];
            ret = true;
        }
        if (server_table_manager.hash_table[idx] !=
            server_table_manager.d_hash_table[idx]) {
            server_table_manager.hash_table[idx] =
                    server_table_manager.d_hash_table[idx];
        }
    }
    if (ret == true) {
        inc_version(server_table_manager.client_version);
        // increase transition_version in cs, to push table to ds
        inc_transition_version();
        deflate_hash_table();
        log_info("[%s] version changed: clientVersion: %u, serverVersion: %u",
                 group_name, *server_table_manager.client_version,
                 *server_table_manager.server_version);
        (*server_table_manager.migrate_block_count)--;
        map<uint64_t, int>::iterator it = migrate_machine.find(server_id);
        assert(it != migrate_machine.end());
        it->second--;
        if (it->second == 0) {
            migrate_machine.erase(it);
        }
        log_info("setMigratingHashtable bucketNo %lu serverId %s, finish migrate this bucket.",
                 bucket_id, tbsys::CNetUtil::addrToString(server_id).c_str());
        should_syn_mig_info = true;
    }
    return;
}

void group_info::set_recovery_hashtable(size_t bucket_id, uint64_t server_id) {
    if (bucket_id > server_table_manager.get_server_bucket_count()
        || is_recoverying() == false) {
        log_error("bucket_id invalid or is_recoverying false. bucket_id(%lu), bucket_num(%d), recovery_block_count(%d)",
                  bucket_id, server_table_manager.get_server_bucket_count(),
                  *server_table_manager.recovery_block_count);
        return;
    }

    if (server_table_manager.m_hash_table[bucket_id] != server_id) {
        log_error("where you got this ? old hashtable? bucket_id: %lu, m_server: %s, server: %s",
                  bucket_id, tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[bucket_id]).c_str(),
                  tbsys::CNetUtil::addrToString(server_id).c_str());
        return;
    }

    tbsys::CThreadGuard guard(&hash_table_set_mutex);

    // no need to do all copies, only support two copies
    // check if this copy is bucket is empty and the dest table

    uint32_t bucket_count = server_table_manager.get_server_bucket_count();
    if (server_table_manager.m_hash_table[bucket_count + bucket_id] != 0
        || server_table_manager.d_hash_table[bucket_id] == 0
        || server_table_manager.d_hash_table[bucket_count + bucket_id] == 0
        || (server_id != server_table_manager.d_hash_table[bucket_id]
            && server_id != server_table_manager.d_hash_table[bucket_count + bucket_id])) {
        log_error(
                "error happen, bucket_id: %lu, server: %s, m_server[0] is %s, m_server[1] is %s, d_server[0] is %s, d_server[1] is %s",
                bucket_id,
                tbsys::CNetUtil::addrToString(server_id).c_str(),
                tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[bucket_count + bucket_id]).c_str(),
                tbsys::CNetUtil::addrToString(server_table_manager.m_hash_table[bucket_id]).c_str(),
                tbsys::CNetUtil::addrToString(server_table_manager.d_hash_table[bucket_count + bucket_id]).c_str(),
                tbsys::CNetUtil::addrToString(server_table_manager.d_hash_table[bucket_id]).c_str());
        return;
    }

    uint64_t failover_ds = (server_id == server_table_manager.d_hash_table[bucket_id] ?
                            server_table_manager.d_hash_table[bucket_count + bucket_id] :
                            server_table_manager.d_hash_table[bucket_id]);

    // set the content of this bucket in d_hashtable to m_hashtable and hashtable
    for (uint32_t i = 0; i < server_table_manager.get_copy_count(); i++) {
        uint32_t idx = i * server_table_manager.get_server_bucket_count() + bucket_id;
        server_table_manager.m_hash_table[idx] = server_table_manager.d_hash_table[idx];
        server_table_manager.hash_table[idx] = server_table_manager.d_hash_table[idx];
    }

    // once recoveryed one bucket, notice client and force set the m_hashtable of ds
    inc_version(server_table_manager.client_version);
    // increase recovering_version in cs, to push table to ds
    inc_recoverying_version();
    deflate_hash_table();

    log_info("set the content of bucket %zu in dest back, (%s, %s)", bucket_id,
             tbsys::CNetUtil::addrToString(server_table_manager.d_hash_table[bucket_id]).c_str(),
             tbsys::CNetUtil::addrToString(server_table_manager.d_hash_table[bucket_count + bucket_id]).c_str());
    log_info("[%s] version changed: clientVersion: %u, serverVersion: %u, recovery_version: %u",
             group_name, *server_table_manager.client_version,
             *server_table_manager.server_version, *server_table_manager.recovery_version);

    // change recovery_machine here
    (*server_table_manager.recovery_block_count)--;
    map<uint64_t, int>::iterator it = recovery_machine.find(failover_ds);
    if (it != recovery_machine.end()) {
        if (it->second > 0) {
            it->second--;
            if (it->second == 0) {
                recovery_machine.erase(it);
            }
        } else {
            log_error("error happen, the num of remain recovery bucket in %s is negative(%d)",
                      tbsys::CNetUtil::addrToString(failover_ds).c_str(), it->second);
        }
    } else {
        log_error("error happen, ds %s not in recovery_machine", tbsys::CNetUtil::addrToString(failover_ds).c_str());
    }


    log_info("setMigratingHashtable bucketNo %lu serverId %s, finish recovery this bucket.",
             bucket_id, tbsys::CNetUtil::addrToString(server_id).c_str());
    should_syn_mig_info = true;

    return;
}

void group_info::hard_check_migrate_complete(uint64_t /* nouse */) {
    if (memcmp(server_table_manager.m_hash_table,
               server_table_manager.d_hash_table,
               server_table_manager.get_hash_table_byte_size()) == 0) {
        migrate_machine.clear();
    }
}

bool group_info::is_transition_status() {
    return is_migrating() || is_failovering();
}

void group_info::get_migrating_machines(vector<pair<uint64_t, uint32_t> > &vec_server_id_count) const {
    log_debug("machine size = %lu", migrate_machine.size());
    map<uint64_t, int>::const_iterator it = migrate_machine.begin();
    for (; it != migrate_machine.end(); ++it) {
        vec_server_id_count.push_back(make_pair(it->first, it->second));
    }
}

void group_info::inc_version(uint32_t *value, const uint32_t inc_step) {
    (*value) += inc_step;
    if (min_config_version > (*value)) {
        (*value) = min_config_version;
    }
}

void group_info::inc_transition_version() {
    if (get_transition_version() == cluster_transition_info::MAX_TRANSITION_VERSION) {
        *server_table_manager.transition_version = 0;
    }
    inc_version(server_table_manager.transition_version);
}

void group_info::inc_recoverying_version() {
    if (get_recovery_version() == cluster_transition_info::MAX_TRANSITION_VERSION) {
        *server_table_manager.recovery_version = 0;
    }
    inc_version(server_table_manager.recovery_version);
}

void group_info::find_available_server() {
    available_server.clear();
    node_info_set::iterator it;
    for (it = node_list.begin(); it != node_list.end(); ++it) {
        node_info *node = (*it);
        if (node->server->status == server_info::ALIVE)
            available_server.insert(node->server->server_id);
    }
}

void group_info::inc_version(const uint32_t inc_step) {
    inc_version(server_table_manager.client_version, inc_step);
    inc_version(server_table_manager.server_version, inc_step);
}

void group_info::set_stat_info(uint64_t server_id,
                               const node_stat_info &node_info) {
    if (server_id == 0)
        return;
    stat_info_rw_locker.wrlock();
    //map<uint64_t, node_stat_info>::iterator it =
    //  stat_info.find(server_id);
    //if(it == stat_info.end()) {
    //  stat_info_rw_locker.unlock();
    //  stat_info_rw_locker.wrlock();
    //}
    stat_info[server_id] = node_info;
    stat_info_rw_locker.unlock();
    return;
}

void group_info::get_stat_info(uint64_t server_id,
                               node_stat_info &node_info) const {
    map<uint64_t, node_stat_info>::const_iterator it;
    stat_info_rw_locker.wrlock();        //here we use write lock so no data server can update its stat info
    if (server_id == 0) {
        for (it = stat_info.begin(); it != stat_info.end(); it++) {
            node_info.update_stat_info(it->second);
        }

    } else {
        it = stat_info.find(server_id);
        if (it != stat_info.end()) {
            node_info.update_stat_info(it->second);
        }
    }
    stat_info_rw_locker.unlock();
    return;
}

int group_info::add_down_server(uint64_t server_id) {
    int ret = EXIT_SUCCESS;
    std::set<uint64_t>::const_iterator sit = tmp_down_server.find(server_id);
    if (sit == tmp_down_server.end()) {
        log_debug("add server: %"PRI64_PREFIX"u not exist", server_id);
        // add
        tmp_down_server.insert(server_id);
        // serialize to group.conf
        const char *group_file_name =
                TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        if (group_file_name == NULL) {
            log_error("not found %s:%s ", CONFSERVER_SECTION, TAIR_GROUP_FILE);
            return EXIT_FAILURE;
        }
        assert(group_name != NULL);
        string down_server_list;
        down_server_list.reserve(tmp_down_server.size() * 32);
        for (sit = tmp_down_server.begin(); sit != tmp_down_server.end(); ++sit) {
            string str = tbsys::CNetUtil::addrToString(*sit);
            down_server_list += str;
            down_server_list += ';';
        }
        log_debug("current down servers: %s", down_server_list.c_str());
        ret = tair::util::file_util::change_conf(group_file_name, group_name, TAIR_TMP_DOWN_SERVER,
                                                 down_server_list.c_str());
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("change conf failed, ret: %d, group_file: %s, group_name: %s", ret, group_file_name, group_name);
        }
    } else //do nothing
    {
        log_debug("add server: %"PRI64_PREFIX"u exist", server_id);
    }
    return ret;
}

// when reset;
int group_info::clear_down_server(const vector<uint64_t> &server_ids) {
    int ret = TAIR_RETURN_SUCCESS;
    log_debug("clear down server. server size: %lu, clear size: %lu", tmp_down_server.size(), server_ids.size());
    //do it when has down server
    if (!tmp_down_server.empty()) {
        const char *group_file_name =
                TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        if (group_file_name == NULL) {
            log_error("not found %s:%s ", CONFSERVER_SECTION, TAIR_GROUP_FILE);
            return TAIR_RETURN_FAILED;
        }

        std::string down_servers_str("");

        if (server_ids.empty()) { // mean clear all down servers
            tmp_down_server.clear();
        } else {
            for (vector<uint64_t>::const_iterator it = server_ids.begin(); it != server_ids.end(); ++it) {
                tmp_down_server.erase(*it);
            }
            if (!tmp_down_server.empty()) {
                down_servers_str.reserve(tmp_down_server.size() * 32);
                for (std::set<uint64_t>::iterator sit = tmp_down_server.begin(); sit != tmp_down_server.end(); ++sit) {
                    down_servers_str.append(tbsys::CNetUtil::addrToString(*sit));
                    down_servers_str.append(";");
                }
                log_info("current down servers: %s", down_servers_str.c_str());
            }
        }

        ret = tair::util::file_util::change_conf(group_file_name, group_name, TAIR_TMP_DOWN_SERVER,
                                                 down_servers_str.c_str());
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("change conf failed, ret: %d, group_file: %s, group_name: %s", ret, group_file_name, group_name);
        }
    }
    return ret;
}


int group_info::get_server_down_time() const {
    return server_down_time;
}

uint32_t group_info::get_server_bucket_count() const {
    return server_table_manager.get_server_bucket_count();
}

uint32_t group_info::get_copy_count() const {
    return server_table_manager.get_copy_count();
}

uint32_t group_info::get_hash_table_size() const {
    return server_table_manager.get_hash_table_size();
}

uint32_t group_info::get_hash_table_byte_size() const {
    return server_table_manager.get_hash_table_byte_size();
}

uint32_t group_info::get_SVR_table_size() const {
    return server_table_manager.get_SVR_table_size();
}

void group_info::get_recovery_ds(std::vector<uint64_t> &recovery_ds) const {
    if (recovery_machine.size() != 0) {
        map<uint64_t, int>::const_iterator it;
        for (it = recovery_machine.begin(); it != recovery_machine.end(); ++it) {
            log_debug("add server %s to recovery_ds", tbsys::CNetUtil::addrToString(it->first).c_str());
            recovery_ds.push_back(it->first);
        }
    }
}

void group_info::do_proxy_report(const request_heartbeat &req) {
    if (is_migrating()) {
        set<uint64_t>::iterator it = reported_serverid.find(req.get_server_id());
        if (it == reported_serverid.end()) {
            reported_serverid.insert(req.get_server_id());        //this will clear when migrate complete.
            log_info("insert reported server: %s, bucket_count: %lu",
                     tbsys::CNetUtil::addrToString(req.get_server_id()).c_str(), req.vec_bucket_no.size());
            for (size_t i = 0; i < req.vec_bucket_no.size(); i++) {
                set_migrating_hashtable(req.vec_bucket_no[i], req.get_server_id());
            }
        }
    }

}

int group_info::select_build_strategy(const node_info_set &ava_server) {
    int strategy = 2;
    //choose which strategy to select
    //serverid in ava_server will not be repeated
    map<uint32_t, int> pos_count;
    pos_count.clear();
    for (set<node_info *>::const_iterator it = ava_server.begin();
         it != ava_server.end(); it++) {
        log_info("mask %"PRI64_PREFIX"u:%s & %"PRI64_PREFIX"u -->%"PRI64_PREFIX"u",
                 (*it)->server->server_id, tbsys::CNetUtil::addrToString((*it)->server->server_id).c_str(),
                 pos_mask, (*it)->server->server_id & pos_mask);
        (pos_count[(*it)->server->server_id & pos_mask])++;
    }

    if (pos_count.size() <= 1) {
        log_warn("pos_count size: %lu, table builder3 use build_strategy 1", pos_count.size());
        strategy = 1;
    } else {
        int pos_max = 0;
        for (map<uint32_t, int>::const_iterator it = pos_count.begin();
             it != pos_count.end(); ++it) {
            log_info("pos rack: %d count: %d", it->first, it->second);
            if (it->second > pos_max) {
                pos_max = it->second;
            }
        }

        int diff_server = ava_server.size() - pos_max - pos_max;
        if (diff_server <= 0)
            diff_server = 0 - diff_server;
        float ratio = diff_server / (float) pos_max;
        if (ratio > diff_ratio || ava_server.size() < server_table_manager.get_copy_count()
            || ratio > 0.9999) {
            log_warn("pos_count size: %lu, ratio: %f, table builder3 use build_strategy 1", pos_count.size(), ratio);
            strategy = 1;
        } else {
            log_warn("pos_count size: %lu, ratio: %f, table builder3 use build_strategy 2", pos_count.size(), ratio);
            strategy = 2;
        }
        log_warn("diff_server = %d ratio = %f diff_ratio=%f",
                 diff_server, ratio, diff_ratio);

    }

    return strategy;
}
}
}
