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

#ifndef TAIR_MIGRATE_HPP
#define TAIR_MIGRATE_HPP

#include <stdio.h>
#include <vector>
#include <list>
#include <unistd.h>
#include <ext/hash_map>
#include <tbsys.h>
#include <databuffer.hpp>
#include "wait_object.hpp"
#include "data_file_reader.hpp"
#include "heartbeat_thread.hpp"
#include "duplicate_base.hpp"
#include "update_log.hpp"
#include "tair_manager.hpp"
#include "storage_manager.hpp"
#include "base_migrate.hpp"
#include "mupdate_packet.hpp"
#include "response_return_packet.hpp"
#include "migrate_finish_packet.hpp"

namespace tair {
using namespace std;
using namespace tair::storage;
const static uint32_t MIGRATE_LOCK_LOG_LEN = 1 * 1024 * 1024;
const static uint32_t MUPDATE_PACKET_HEADER_LEN = 8 + 4;
const static int32_t MISECONDS_WAITED_FOR_WRITE = 500000;

class heartbeat_thread;

class migrate_manager : public base_migrate, public tbsys::CDefaultRunnable {
public:
    migrate_manager(base_duplicator *this_duplicator, tair_manager *tair_mgr,
                    tair::storage::storage_manager *storage_mgr);

    ~migrate_manager();

    void set_log(update_log *log);

    void set_migrate_server_list(bucket_server_map migrate_server_list, uint32_t version);

    void run(tbsys::CThread *thread, void *arg);

    void do_run();

    void do_migrate();

    void do_migrate_one_bucket(int bucket_number, vector<uint64_t> dest_servers);

    void do_server_list_changed();

    void signal();

    void stop();

    bool is_bucket_available(int bucket_number);

    bool is_bucket_migrating(int bucket_number);

    bool is_migrating();

    vector<uint64_t> get_migrated_buckets();

private:
    bool migrate_data_file(int bucket_number, vector<uint64_t> dest_servers);

    bool migrate_log(int bucket_number, vector<uint64_t> dest_servers, lsn_type start_lsn, lsn_type end_lsn);

    template<typename P>
    bool send_packet(vector<uint64_t> dest_servers, P *packet, int db_id);

    bool finish_migrate_data(std::vector<uint64_t> &dest_servers, int db_id);

    void finish_migrate_bucket(int bucket_number);

    //~ handle response from target server of migration
    int packet_handler(easy_request_t *r);

    //~ callback by libeasy's I/O thread
    static int packet_handler_cb(easy_request_t *r) {
        if (r->ms->c == NULL) {
            easy_session_destroy(r->ms);
            return EASY_ERROR;
        }

        migrate_manager *_this = (migrate_manager *) r->ms->c->handler->user_data;
        return _this->packet_handler(r);
    }

    bucket_server_map migrate_servers;
    bucket_server_map temp_servers;
    vector<uint64_t> migrated_buckets;
    tbsys::CThreadMutex get_data_mutex;
    tbsys::CThreadMutex get_migrating_buckets_mutex;
    volatile int current_locked_bucket;
    uint32_t version;
    volatile int current_migrating_bucket;
    base_duplicator *duplicator;
    tair_manager *tair_mgr;
    tair::storage::storage_manager *storage_mgr;
    uint32_t timeout;
    vector<uint64_t> config_server_list;

    easy_io_t eio;
    //~ handler callbacks, used when sending migrating packet
    easy_io_handler_pt handler;

    wait_object_manager wait_object_mgr;
    update_log *log;

    tbsys::CThreadCond condition;
    bool is_stopped;
    int is_running;
    bool is_signaled;
    bool is_alive;
    tbsys::CThreadMutex mutex;
};
}
#endif
