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

#ifndef TAIR_GROUP_INFO_H
#define TAIR_GROUP_INFO_H

#include <string>
#include <map>
#include <set>
#include "server_info.hpp"
#include "table_builder.hpp"
#include "conf_server_table_manager.hpp"
#include "stat_info.hpp"
#include "heartbeat_packet.hpp"

namespace tair {
namespace config_server {

enum GroupAcceptStrategy {
    GROUP_DEFAULT_ACCEPT_STRATEGY = 0,
    GROUP_AUTO_ACCEPT_STRATEGY
};

#define TAIR_MAX_FUNC_NAME 100

class group_info {
public:
    group_info(const char *group_name, server_info_map *serverInfoMap, easy_io_t *eio);

    ~group_info();

    uint32_t get_client_version() const;

    uint32_t get_server_version() const;

    uint32_t get_transition_version() const;

    uint32_t get_plugins_version() const;

    uint32_t get_area_capacity_version() const;

    const std::map<uint32_t, uint64_t> &get_area_capacity_info() const;

    const uint64_t *get_hash_table(int mode = 0) const;

    const char *get_group_name() const;

    tbsys::STR_STR_MAP *get_common_map();

    const char *get_hash_table_deflate_data(int mode = 0) const;

    int get_hash_table_deflate_size(int mode = 0) const;

    bool is_need_rebuild() const;

    const char *get_server_table_data() const;

    int get_server_table_size() const;

    bool is_failovering() const { return failovering; }

    bool is_migrating() const;

    uint32_t get_server_bucket_count() const;

    uint32_t get_copy_count() const;

    uint32_t get_hash_table_size() const;

    uint32_t get_hash_table_byte_size() const;

    uint32_t get_SVR_table_size() const;

    int get_server_down_time() const;

    bool load_config(tbsys::CConfig &config, uint32_t version,
                     std::set<uint64_t> &server_id_list);

    void correct_server_info(bool is_sync);

    void set_force_rebuild();

    void rebuild(uint64_t slave_server_id,
                 tbsys::CRWSimpleLock *p_grp_locker,
                 tbsys::CRWSimpleLock *p_server_locker);

    bool do_finish_migrate(uint64_t server_id, uint32_t server_version,
                           int bucket_id, uint64_t slave_server_id);

    bool check_migrate_complete(uint64_t slave_server_id, tbsys::CRWSimpleLock *p_grp_locker);

    bool write_server_table_packet(char *data, int size);

    void set_migrating_hashtable(size_t bucket_no, uint64_t server_id);

    void hard_check_migrate_complete(uint64_t slave_server_id);

    int add_area_key_list(const char *group_file_name, const char *group_name, const char *area_key, uint32_t area_num);

    int update_area_capacity_list(const char *group_file_name, const char *group_name, uint32_t area_num,
                                  const char *quota);

    int add_area_capacity_list(const char *group_file_name, const char *group_name, const char *area_key,
                               uint32_t &area_num, const char *quota);

    int get_area_num(const char *area_key, uint32_t &area_num);

    // add for recovery
    void inc_recoverying_version();

    bool is_recoverying() const;

    uint32_t get_recovery_version() const;

    void get_recovery_ds(std::vector<uint64_t> &recovery_ds) const;

    int fill_recovery_machine(std::set<uint64_t> &recovery_servers);

    /*
     * force_chech_failover is used to support cs restart and touch_group.conf
     * 1. check if table is in migrate
     * 2. check if table is in failover
     * 3. scan table to get failover servers and add all into pending_recovery_servers
     */
    void force_check_failover();

    void rebuild_failover_status();

    /*
     * check_recovery_complete is similar with check_migrate_complete
     * invoked by server_conf_thread to check if recovery_completed
     * when recovery completed, check if failover completed
     * if so, incr server version
     */
    bool check_recovery_complete(uint64_t slave_server_id, tbsys::CRWSimpleLock *p_grp_locker);

    /*
     * set the second hashtable when received finish_recovery_packet
     */
    void set_recovery_hashtable(size_t bucket_no, uint64_t server_id);

    /*
     * deal with finish_recovery_packet
     */
    bool do_finish_recovery(uint64_t server_id, uint32_t server_version,
                            uint32_t transition_version, int bucket_id);

    bool is_transition_status();

    /*
     * only support migrate one bucket at a time.
     * bucket_no: bucket which should be migrate
     * copy_index: copy count index
     * src_server_id: server who hold bucket now, just for check
     * dest_server_id: server who hold bucket after migrate
     */
    int force_migrate_bucket(uint32_t bucket_no, uint32_t copy_index,
                             uint64_t src_server_id, uint64_t dest_server_id,
                             tbsys::CRWSimpleLock *p_grp_locker,
                             tbsys::CRWSimpleLock *p_server_locker);

    /*
     * switch the master and slave ds of input buckets
     * bucket_nums: buckets which should be switch
     */
    int force_switch_bucket(set<uint32_t> bucket_nums,
                            tbsys::CRWSimpleLock *p_grp_locker,
                            tbsys::CRWSimpleLock *p_server_locker);

    /*
     * active failover ds as it is still alive, remove it from serve list
     * server_id: the server to be failover
     */
    int active_failover_ds(uint64_t server_id,
                           tbsys::CRWSimpleLock *p_grp_locker,
                           tbsys::CRWSimpleLock *p_server_locker);

    /*
     * active recovery ds as it may be active failoverd before
     * server_id: the server to recover
     */
    int active_recover_ds(uint64_t server_id,
                          tbsys::CRWSimpleLock *p_grp_locker,
                          tbsys::CRWSimpleLock *p_server_locker);

    /*
     * repalce_offline, replace some old node by new nodes
     * replace_ds_pairs, replace the ds pairs
     */
    int force_replace_ds(std::map<uint64_t, uint64_t> replace_ds_pairs,
                         tbsys::CRWSimpleLock *p_grp_locker,
                         tbsys::CRWSimpleLock *p_server_locker);

    /*
     * urgent_offline, replace offline_id by replace_ids
     * in the case of we already start a special migration
     * but one node fail, we don't expect our former plan
     * to be interrupted, in the same time, we need cluster
     * back in server status
     */
    int urgent_offline(uint64_t offline_id, std::set<uint64_t> replace_ids,
                       tbsys::CRWSimpleLock *p_grp_locker,
                       tbsys::CRWSimpleLock *p_server_locker);

    typedef struct ops_check_option {
    public:
        bool check_migrate_down;
        bool check_min_ds_num;
        bool allow_single_copy;
        bool check_failover_allowed;
        bool check_failover_down;
        bool check_failover_up;
        char ops_name[TAIR_MAX_FUNC_NAME + 1];
        size_t upnode_list_size;

    public:
        ops_check_option(const char *name, size_t size) {
            check_migrate_down = check_min_ds_num = allow_single_copy = false;
            check_failover_allowed = check_failover_down = check_failover_up = false;
            snprintf(ops_name, TAIR_MAX_FUNC_NAME, "%s", name);
            ops_name[TAIR_MAX_FUNC_NAME] = 0;
            upnode_list_size = size;
        }
    } ops_check_option;

    int force_table_ops_check(const ops_check_option &option);

    uint32_t get_plugins_count() const {
        return plugins_name_info.size();
    }

    std::set<std::string>::const_iterator find_plugin(const std::string &dll_name) const {
        return plugins_name_info.find(dll_name);
    }

    std::set<std::string>::const_iterator plugin_end() const {
        return plugins_name_info.end();
    }

    std::set<std::string> get_plugins_info() const {
        return plugins_name_info;
    }

    int get_data_need_move() const {
        return data_need_move;
    }

    GroupAcceptStrategy get_accept_strategy() const {
        return accept_strategy;
    }

    bool get_group_status() const {
        return group_can_work;
    }

    void set_group_status(bool status) {
        group_can_work = status;
    }

    bool get_group_is_OK() const {
        return group_is_OK;
    }

    void set_group_is_OK(bool status) {
        group_is_OK = status;

    }

    std::set<uint64_t> get_available_server_id() const {
        return available_server;
    }

    node_info_set get_node_info() const {
        return node_list;
    }

    uint32_t get_min_config_version() const {
        return min_config_version;
    }

    void get_migrating_machines(vector<pair<uint64_t, uint32_t> > &vec_server_id_count) const;

    void set_table_builded() {
        need_rebuild_hash_table = 0;
    }

    void set_force_send_table() {
        need_send_server_table = 1;
    }

    void reset_force_send_table() {
        need_send_server_table = 0;
    }

    int get_send_server_table() const {
        return need_send_server_table;
    }

    int get_pre_load_flag() const {
        return pre_load_flag;
    }

    void send_server_table_packet(uint64_t slave_server_id);

    void find_available_server();

    void inc_version(const uint32_t inc_step = 1);

    void do_proxy_report(const request_heartbeat &req);

    void set_stat_info(uint64_t server_id, const node_stat_info &);

    void get_stat_info(uint64_t server_id, node_stat_info &) const;

    int add_down_server(uint64_t server_id);

    int clear_down_server(const vector<uint64_t> &server_ids);

    // ds is down, pending to failover
    void pending_failover_server(uint64_t server_id);

    // ds is up, pending to recover
    void pending_recover_server(uint64_t server_id);

    void remove_bitmap_area(int area);

    void set_bitmap_area(int area);

private:
    easy_io_t *eio;

    int packet_handler(easy_request_t *r);

    static int packet_handler_cb(easy_request_t *r) {
        group_info *_this = (group_info *) r->args;
        return _this->packet_handler(r);
    }

    static easy_io_handler_pt handler;

    // failover server: switch owning buckets' copy
    void failover_server(node_info_set &upnode_list, bool &change);

    // failover one ds, check failovering_servers, is alive, then change table
    bool failover_one_ds(uint64_t server_id, node_info_set &upnode_list, bool &change, bool check_alive = true);

    // recover server: try to switch back buckets' non-master copy
    void recover_server(node_info_set &upnode_list, bool &change);

    // failover one ds, check failovering_servers, is alive
    void recover_one_ds(uint64_t server_id, node_info_set &upnode_list, bool &change, set<uint64_t> &recovery_servers);

    // check whether we are failovering.
    void check_failovering();

    // reset failovering
    void reset_failovering();

private:
    group_info(const group_info &);

    group_info &operator=(const group_info &);

    int fill_migrate_machine();

    void deflate_hash_table();

    // parse server list
    void parse_server_list(node_info_set &list, const char *server_list,
                           std::set<uint64_t> &server_id_list);

    void parse_plugins_list(vector<const char *> &plugins);

    void parse_area_capacity_list(vector<const char *> &);

    void parse_area_key_list(vector<const char *> &a_c);

    void get_up_node(node_info_set &up_node_list);

    void print_server_count();

    void inc_version(uint32_t *value, const uint32_t inc_step = 1);

    void inc_transition_version();

    int select_build_strategy(const node_info_set &ava_server);

    uint32_t find_available_area(const char *group_file_name, const char *group_name);

private:
    tbsys::STR_STR_MAP common_map;
    node_info_set node_list;

    char *group_name;
    conf_server_table_manager server_table_manager;

    server_info_map *server_info_maps;        //  => server
    int need_rebuild_hash_table;
    int need_send_server_table;
    uint32_t load_config_count;
    int data_need_move;
    uint32_t min_config_version;
    uint32_t min_data_server_count;        // if we can not get at lesat min_data_server_count data servers,
    //config server will stop serve this group;
    std::map<uint64_t, int> migrate_machine;
    bool should_syn_mig_info;
    std::set<std::string> plugins_name_info;
    bool group_can_work;
    bool group_is_OK;
    int build_strategy;
    DataLostToleranceFlag lost_tolerance_flag;
    GroupAcceptStrategy accept_strategy;
    int bucket_place_flag;
    int server_down_time;
    float diff_ratio;
    uint64_t pos_mask;
    int pre_load_flag;  // 1: need preload; 0: no need; default 0
    std::set<uint64_t> available_server;

    std::set<uint64_t> reported_serverid;
    std::map<uint32_t, uint64_t> area_capacity_info;        //<area, capacity>
    std::map<std::string, uint32_t> area_key_info;   //<areakey, areaid>
    uint64_t area_bitmap[TAIR_MAX_AREA_COUNT / 64 + 1];
    bool always_update_capacity_info;                       // whether update capacity info everytime

    // failover stuff.
    // once server down, we try best to failover it to
    // make cluster still serviceable.
    // Process:
    //  1. failover: one ds is down, if rebuilding-table is not going to
    //     happen (min_data_server_count), and `allow_failover_server is true,
    //     and all buckets owned by this server still have at least
    //     one alive copy, then we update hash_table/h_hash_table to make those
    //     buckets' master copy alive to make cluster still serviceable
    //  2. recover: ds is up again, cs incr transition and return recovery ds info
    //     back to other ds when detect the recovery, ds start to recovery after
    //     receive information from cs, send a packet to cs whenever finish recovery
    //     one bucket, finally, when all bucket are recovered, cs incr server_version
    //     cluster back to normal status
    bool allow_failover_server; // whether failover ds
    bool failovering;           // failover status
    std::set<uint64_t> pending_failover_servers; // servers to be failovered
    std::set<uint64_t> pending_recover_servers;  // servers to be recovered

    std::set<uint64_t> failovering_servers; // servers in failovering
    // servers in recovering, key: the recovering server, value: the bucket count wait for recovery
    // like the migrate_machine, to record status of recovery
    std::map<uint64_t, int> recovery_machine;

    std::map<uint64_t, node_stat_info> stat_info;        //<server_id, statInfo>
    mutable tbsys::CRWSimpleLock stat_info_rw_locker;        //node_stat_info has its own lock, this only for the map stat_info
    uint32_t interval_seconds;

    std::set<uint64_t> tmp_down_server;  //save tmp down server in DATA_PRE_LOAD_MODE
    tbsys::CThreadMutex hash_table_set_mutex;

};

typedef __gnu_cxx::hash_map<const char *, group_info *,
        __gnu_cxx::hash<const char *>, tbsys::char_equal> group_info_map;
}
}

#endif
