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

#ifndef TAIR_TABLE_MANAGER_H
#define TAIR_TABLE_MANAGER_H

#include "define.hpp"
#include "util.hpp"
#include <tbsys.h>
#include <vector>
#include <map>
#include <set>

namespace tair {

class migrate_unit {
public:
    std::vector<uint64_t> dest_servers;
    uint32_t score;
    int32_t bucket;

    bool operator<(const migrate_unit &another) const { return score > another.score; }
};

typedef __gnu_cxx::hash_map<int, migrate_unit, __gnu_cxx::hash<int> > bucket_server_map;
typedef __gnu_cxx::hash_map<int, migrate_unit, __gnu_cxx::hash < int> >
::iterator bucket_server_map_it;
using namespace tair::util;

class table_manager {
public:
    table_manager();

    ~table_manager();

    bool is_master(int bucket_number, int server_flag);

    const uint64_t *get_server_table();

    std::vector<uint64_t> get_slaves(int bucket_number, bool is_migrating, bool &is_failover);

    uint64_t get_migrate_target(int bucket_number);

    void init_migrate_done_set(tair::util::dynamic_bitset &migrate_done_set,
                               const std::vector<uint64_t> &current_state_table);

    /////////////////////////////
    void do_update_table(uint64_t *new_server_table, size_t size, uint32_t table_version, uint32_t copy_count,
                         uint32_t bucket_count, bool calc_migrate = true);

    void
    direct_update_table(uint64_t *server_table, uint32_t server_list_count, uint32_t copy_count, uint32_t bucket_count,
                        bool is_failover, bool version_changed, uint32_t server_table_version);

    // for recovery, get recovery task list from recovery ds
    void update_recoverys(std::vector<uint64_t> recovery_ds, bucket_server_map &recoverys);

    const std::vector<int> &get_holding_buckets() const;

    const std::vector<int> &get_padding_buckets() const;

    const std::vector<int> &get_release_buckets() const;

    bool bucket_is_held(int bucket);

    bucket_server_map get_migrates() const;

    std::set<uint64_t> get_available_server_ids() const;

    uint32_t get_version() const;

    uint32_t get_copy_count() const;

    uint32_t get_bucket_count() const;

    uint32_t get_hash_table_size() const;

    void clear_available_server();

    void set_table_for_localmode();
    /////////////////////////////////

private:
    void calculate_release_bucket(std::vector<int> &newHoldingBucket);

    void calculate_migrates(uint64_t *table, int index);

private:
    uint64_t *server_table;
    uint32_t table_version;
    uint32_t copy_count;
    uint32_t bucket_count;
    bool is_failover;
    //tbsys::CThreadMutex update_mutex;
    pthread_rwlock_t m_mutex;

    std::vector<int> holding_buckets; // the buckets this server holding
    std::vector<int> padding_buckets;
    std::vector<int> release_buckets;
    std::set<uint64_t> available_server_ids;

    bucket_server_map migrates; // buckets that need to be migrate <bucket number, target server ids>
};
}

#endif
