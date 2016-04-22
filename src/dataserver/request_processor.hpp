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

#ifndef TAIR_REQUEST_PROCESSOR_HPP
#define TAIR_REQUEST_PROCESSOR_HPP

#include "data_entry.hpp"

#include "put_packet.hpp"
#include "get_packet.hpp"
#include "statistics_packet.hpp"
#include "remove_packet.hpp"
#include "uniq_remove_packet.hpp"
#include "inc_dec_packet.hpp"
#include "inc_dec_bounded_packet.hpp"
#include "duplicate_packet.hpp"
#include "mupdate_packet.hpp"
#include "lock_packet.hpp"
#include "hide_packet.hpp"
#include "get_hidden_packet.hpp"
#include "prefix_puts_packet.hpp"
#include "simple_prefix_get_packet.hpp"
#include "prefix_removes_packet.hpp"
#include "response_mreturn_packet.hpp"
#include "prefix_incdec_packet.hpp"
#include "prefix_incdec_bounded_packet.hpp"
#include "prefix_gets_packet.hpp"
#include "prefix_hides_packet.hpp"
#include "prefix_get_hiddens_packet.hpp"
#include "prefix_locker.hpp"
#include "sync_packet.hpp"
#include "op_cmd_packet.hpp"
#include "get_range_packet.hpp"
#include "expire_packet.hpp"
#include "mc_ops_packet.hpp"

namespace tair {
class tair_manager;

class heartbeat_thread;

class tair_server;

class request_processor {
public:
    request_processor(tair_manager *tair_mgr, heartbeat_thread *heart_beat, tair_server *server);

    ~request_processor();

    int process(request_put *request, bool &send_return, uint32_t &resp_size);

    int process(request_get *request, bool &send_return, uint32_t &resp_size, uint32_t &weight);

    int process(request_uniq_remove *request, bool &send_return);

    int process(request_statistics *request, bool &send_return);

    int process(request_get_range *request, bool &send_return, uint32_t &resp_size, uint32_t &weight);

    int process(request_hide *request, bool &send_return, uint32_t &resp_size);

    int process(request_get_hidden *request, bool &send_return, uint32_t &resp_size);

    int process(request_remove *request, bool &send_return, uint32_t &resp_size);

    int process(request_mput *request, bool &send_return, uint32_t &resp_size);

    int process(request_inc_dec *request, bool &send_return, uint32_t &resp_size);

    int process(request_batch_duplicate *request, bool &send_return);

    int process(request_duplicate *request, bool &send_return, uint32_t &resp_size);

    int process(request_mupdate *request, bool &send_return);

    int process(request_lock *request, bool &send_return, uint32_t &resp_size);

    int process(request_prefix_puts *request, bool &send_return, uint32_t &resp_size, uint32_t &weight);

    int process(request_prefix_removes *request, bool &send_return, uint32_t &resp_size, uint32_t &weight);

    int process(request_prefix_incdec *request, bool &send_return, uint32_t &resp_size, uint32_t &weight);

    int process(request_prefix_gets *request, bool &send_return, uint32_t &resp_size, uint32_t &weight);

    int process(request_prefix_hides *request, bool &send_return, uint32_t &resp_size, uint32_t &weight);

    int process(request_prefix_get_hiddens *request, bool &send_return, uint32_t &resp_size, uint32_t &weight);

    int process(request_op_cmd *request, bool &send_return);

    int process(request_expire *request, bool &send_return, uint32_t &resp_size);

    int process(request_mc_ops *request, bool &send_return, uint32_t &resp_size);

    int process(request_simple_get *request, uint32_t &resp_size, uint32_t &weight);

    int process(request_sync *request, bool &send_return);

private:
    bool do_proxy(uint64_t target_server_id, base_packet *proxy_packet, easy_request_t *r);

private:
    tair_server *server;
    tair_manager *tair_mgr;
    heartbeat_thread *heart_beat;
    prefix_locker plocker;
};
}
#endif
