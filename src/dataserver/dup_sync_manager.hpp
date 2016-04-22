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

#ifndef DUP_SYNC_MANAGER_H
#define DUP_SYNC_MANAGER_H

#include <tbsys.h>
#include <queue>
#include <map>
#include <ext/hash_map>
#include <easy_helper.hpp>
#include "databuffer.hpp"
#include "duplicate_base.hpp"

namespace tair { class RTCollector; }

namespace tair {
namespace stat {
class FlowController;
}
class table_manager;

class dup_sync_sender_manager : public base_duplicator, public tbsys::CDefaultRunnable {
public:
    dup_sync_sender_manager(tair::stat::FlowController *flow_ctrl, RTCollector *rtc, uint32_t ioth_count = 1);

    ~dup_sync_sender_manager();

    void do_hash_table_changed();

    bool is_bucket_available(uint32_t bucket_id);

    //! NOUSE for dup_sync mode
    void set_max_queue_size(uint32_t max_queue_size) {
        return;
    }

    void set_dup_timeout(int ms) {
        dup_timeout = ms;
    }

    int duplicate_data(int area,
                       const data_entry *key,
                       const data_entry *value,
                       int bucket_number,
                       const vector <uint64_t> &des_server_ids,
                       easy_request_t *r);

    int duplicate_data(int32_t bucket,
                       const std::vector<common::Record *> *records,
                       const std::vector<uint64_t> &des_server_ids,
                       easy_request_t *r);

    int direct_send(easy_request_t *r,
                    int area,
                    const data_entry *key,
                    const data_entry *value,
                    int bucket_number,
                    const vector <uint64_t> &des_server_ids,
                    uint32_t max_packet_id);

    int duplicate_data(int bucket,
                       easy_request_t *r,
                       vector <uint64_t> &des_servers);

    int duplicate_batch_data(int bucket_number,
                             const mput_record_vec *record_vec,
                             const std::vector<uint64_t> &des_server_ids,
                             base_packet *request);

    bool has_bucket_duplicate_done(int bucket_number);

    void run(tbsys::CThread *thread, void *arg);

private:
    int direct_send(easy_request_t *r,
                    int32_t bucket,
                    const std::vector<common::Record *> *records,
                    const std::vector<uint64_t> &des_server_ids,
                    uint32_t max_packet_id);

private:
    base_packet *create_return_packet(base_packet *bp, int hearbeat_version);

    int do_response(easy_request_t *r, easy_request_t *remote, int hearbeat_version);

    int packet_handler(easy_request_t *r);

    static int packet_handler_cb(easy_request_t *r) {
        if (r->ms->c == NULL) {
            easy_session_destroy(r->ms);
            return EASY_ERROR;
        }

        dup_sync_sender_manager *_this = (dup_sync_sender_manager *) r->ms->c->handler->user_data;
        return _this->packet_handler(r);
    }

    uint32_t get_chid() {
        uint32_t chid = 0;
        while (chid == 0 || chid == (uint32_t) -1) {
            chid = atomic_add_return(1, &packet_id_creater);
        }
        return chid;
    }

    uint32_t get_ioth_index(const data_entry *key);

    uint32_t get_ioth_index(const base_packet *p);

private:
    easy_io_t eio;
    easy_io_handler_pt handler;
    uint32_t io_thread_count;

    int dup_timeout;
    atomic_t packet_id_creater;
    tair::stat::FlowController *flow_ctrl;
    RTCollector *rtc;
};
}
#endif
