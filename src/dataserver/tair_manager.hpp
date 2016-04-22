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

#ifndef TAIR_MANAGER_H
#define TAIR_MANAGER_H

#include <tbsys.h>
#include <easy_io.h>
#include "define.hpp"
#include "dump_data_info.hpp"
#include "util.hpp"
#include "databuffer.hpp"
#include "data_entry.hpp"
#include "storage_manager.hpp"
#include "dump_manager.hpp"
#include "dump_filter.hpp"
#include "table_manager.hpp"
#include "duplicate_base.hpp"
#include "update_log.hpp"
#include "stat_helper.hpp"
#include "plugin_manager.hpp"
#include "put_packet.hpp"
#include "get_packet.hpp"
#include "remove_packet.hpp"
#include "lock_packet.hpp"
#include "inc_dec_packet.hpp"
#include "prefix_puts_packet.hpp"
#include "prefix_removes_packet.hpp"
#include "response_mreturn_packet.hpp"
#include "mc_ops_packet.hpp"

namespace tair {
using namespace tair::common;

class migrate_manager;

class IRemoteSyncManager;

class request_prefix_hides;

class request_prefix_incdec;

class response_prefix_incdec;

class RTCollector;
namespace stat { class FlowController; }
class area_map;
namespace common { class TairStat; }

// for failover
class DupDepot;

class recovery_manager;

class tair_manager {
    enum {
        STATUS_NOT_INITED,
        STATUS_INITED,
        STATUS_CAN_WORK,
    };

    const static int mutex_array_size = 1000;

public:
    tair_manager(easy_io_t *eio);

    ~tair_manager();

    bool initialize(tair::stat::FlowController *flow_ctrl, RTCollector *rtc);

    int prefix_puts(request_prefix_puts *request, int version, uint32_t &resp_size);

    int prefix_removes(request_prefix_removes *request, int version, uint32_t &resp_size);

    int prefix_hides(request_prefix_hides *request, int version, uint32_t &resp_size);

    int prefix_incdec(request_prefix_incdec *request, int version, int low_bound, int upper_bound, bool bound_care,
                      response_prefix_incdec *resp);

    int put(int area, data_entry &key, data_entry &value, int expire_time, base_packet *request = NULL, int version = 0,
            bool with_stat = true);

    //int put(int area, data_entry &key, data_entry &value, int expire_time,request_put *request,int version);
    int add_count(int area, data_entry &key, int count, int init_value, int low_bound, int upper_bound,
                  int *result_value, int expire_time, base_packet *request, int version);

    int get(int area, data_entry &key, data_entry &value, bool with_stat = true);

    int hide(int area, data_entry &key, base_packet *request = NULL, int heart_version = 0);

    int get_hidden(int area, data_entry &key, data_entry &value);

    int remove(int area, data_entry &key, request_remove *request = NULL, int version = 0);

    int uniq_remove(int area, int bucket, data_entry &key);

    int batch_remove(int area, const tair_dataentry_set *key_list, request_remove *request, int version);

    int batch_put(int area, mput_record_vec *record_vec, request_mput *request, int version);

    int clear(int area);

    int sync(int32_t bucket, std::vector<common::Record *> *records, base_packet *request, int heart_version);

    int op_cmd(ServerCmdType cmd, std::vector<std::string> &params, std::vector<std::string> &infos);

    int expire(int area, data_entry &key, int expire_time, base_packet *request = NULL, int version = 0);

    int do_mc_ops(request_mc_ops *req, response_mc_ops *&resp, int heart_version);

    int direct_put(data_entry &key, data_entry &value, bool update_stat = true);

    int direct_remove(data_entry &key, bool update_stat = true);

    int get_range(int32_t area, data_entry &key_start, data_entry &key_end, int offset, int limit, int type,
                  std::vector<data_entry *> &result, bool &has_next);

    int del_range(int32_t area, data_entry &key_start, data_entry &key_end, int offset, int limit, int type,
                  std::vector<data_entry *> &result, bool &has_next);

    int lock(int area, LockType lock_type, data_entry &key, base_packet *request = NULL, int heart_version = 0);

    bool is_migrating();

    bool should_proxy(data_entry &key, uint64_t &targetServerId);

    bool is_local(data_entry &key);

    void set_migrate_done(int bucketNumber);

    void set_area_quota(int area, int64_t quota);

    void set_area_total_quota(int area, int64_t quota);

    int64_t get_area_quota(int area);

    void set_all_area_total_quota(const std::map<int, int64_t> &quotas);

    bool get_slaves(int server_flag, int bucket_number, vector <uint64_t> &slaves);

    void set_solitary(); // make this data server waitting for next direction
    bool is_working();

    void update_server_table(uint64_t *server_table, int server_table_size, uint32_t server_table_version,
                             int32_t data_need_remove, vector <uint64_t> &current_state_table, uint32_t copy_count,
                             uint32_t bucket_count, bool calc_migrate = true);

    void
    direct_update_table(uint64_t *server_table, uint32_t server_list_count, uint32_t copy_count, uint32_t bucket_count,
                        bool is_failover, bool version_changed = false, uint32_t server_table_version = 0);

    // for recovery
    bool is_recovering();

    void set_recovery_done(int bucketNumber);

    void update_recovery_bucket(vector <uint64_t> recovery_ds, uint32_t transition_version);

    void reset_recovery_task();

    void clear_dup_depot();

    void get_proxying_buckets(vector <uint32_t> &buckets);

    void do_dump(set <dump_meta_info> dump_meta_infos);

    int get_meta(int area, data_entry &key, item_meta_info &meta, int bucket = 0);

    plugin::plugins_manager plugins_manager;

    tair::storage::storage_manager *get_storage_manager() { return storage_mgr; }

    IRemoteSyncManager *get_remote_sync_manager() { return remote_sync_mgr; }

    bool is_localmode();

    uint32_t get_bucket_number(const data_entry &key);

    table_manager *get_table_manager() { return table_mgr; }

    void reload_config() {
        storage_mgr->reload_config();
    }

    tair::common::TairStat *get_stat_manager() { return stat_; }

    bool if_support_admin() { return supported_admin; }

private:
    bool verify_key(data_entry &key);

    tair::storage::storage_manager *get_storage_manager(data_entry &key);

    bool should_write_local(int bucket_number, int server_flag, int op_flag, int &rc);

    bool need_do_migrate_log(int bucket_number);

    int get_op_flag(int bucket_number, int server_flag);

    void init_migrate_log();

    int get_mutex_index(data_entry &key);

    int do_duplicate(int area, data_entry &key, data_entry &value, int bucket_number, base_packet *request,
                     int heart_vesion);

    int
    do_remote_sync(TairRemoteSyncType type, common::data_entry *key, common::data_entry *value, int rc, int op_flag);

    void set_area_total_quota_internal(int area, int64_t quota);

    void update_quota_on_bucket_changed();

private:
    int status;
    bool localmode;
    int localmode_bucket;
    tbsys::CThreadMutex counter_mutex[mutex_array_size];
    tbsys::CThreadMutex item_mutex[mutex_array_size];

    tbsys::CThreadMutex update_server_table_mutex;
    tair::storage::storage_manager *storage_mgr;
    table_manager *table_mgr;
    base_duplicator *duplicator;
    migrate_manager *migrate_mgr;
    tair::util::dynamic_bitset migrate_done_set;
    update_log *migrate_log;
    tair::storage::dump_manager *dump_mgr;
    IRemoteSyncManager *remote_sync_mgr;
    easy_io_t *eio;

    // for failover
    DupDepot *dup_depot;
    recovery_manager *recovery_mgr;
    tair::util::dynamic_bitset recovery_done_set;
    TairStat *stat_;

    int64_t current_bucket_count;
    int64_t total_bucket_count;
    bool supported_admin;
    int64_t area_total_quotas[TAIR_MAX_AREA_COUNT];
};
}
#endif
