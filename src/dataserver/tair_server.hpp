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

#ifndef TAIR_SERVER_H
#define TAIR_SERVER_H

#include <string>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>

#include <tbsys.h>
#include <easy_io.h>

#include "databuffer.hpp"
#include "packet_factory.hpp"
#include "easy_helper.hpp"
#include "remove_area_packet.hpp"
#include "response_return_packet.hpp"
#include "dump_packet.hpp"
#include "statistics_packet.hpp"
#include "heartbeat_thread.hpp"
#include "tair_manager.hpp"
#include "wait_object.hpp"
#include "request_processor.hpp"
#include "stat_processor.h"
#include "stat_helper.hpp"

#include "stat_manager.h"
#include "flow_controller.h"
#include "rt_collector.hpp"
#include "hotkey.hpp"
#include "configuration_manager.h"

using namespace __gnu_cxx;
namespace tair { class RTCollector; }
namespace tair { class OPKiller; }

namespace tair {

class tair_server {
public:
    tair_server();

    ~tair_server();

    void start();

    void stop();

    tair_manager *get_tair_manager(void) { return tair_mgr; }

    easy_io_t *get_eio() {
        return &eio;
    }

    easy_io_handler_pt *get_proxy_handler() {
        return &eio_proxy_handler;
    }

    int get_heartbeat_version() {
        return heartbeat.get_client_version();
    }

    void set_heartbeat_update(bool flag) {
        heartbeat.set_update(flag);
    }

    void reload_config() {
        tair_mgr->reload_config();
    }

    void rt_enable() {
        rtc->enable();
    }

    void rt_disable() {
        rtc->disable();
    }

    void rt_reap() {
        (void) rtc->reap(NULL, 0);
    }

    void hot_key() {
        hotk->load_config(config_file.c_str());
        hotk->enable();
    }

    void set_admin_file(const char *file) {
        configuration_mgr->set_admin_file(file);
    }

    void set_ns_load() {
        configuration_mgr->set_ns_load();
    }

    void set_flow_load() {
        configuration_mgr->set_flow_load();
    }

    int64_t get_cur_queued_mem() {
        return cur_queued_mem;
    }

    void load_max_queued_mem();

    void set_config_file(const char *file) {
        config_file = file;
    }

    bool is_flow_control_log_time(int ns);

private:
    int is_stoped;
    int task_thread_count;
    int io_thread_count;

    easy_io_t eio;
    easy_io_handler_pt eio_handler;
    easy_io_handler_pt eio_proxy_handler;
    //~ regular task queue
    easy_thread_pool_t *task_queue;
    //~ separate task queue for potential slow requests
    easy_thread_pool_t *slow_task_queue;;
    easy_thread_pool_t *async_task_queue;
    easy_thread_pool_t *control_task_queue;

    bool rsync_enabled;
    easy_io_t rsync_eio;
    easy_io_handler_pt rsync_eio_handler;
    easy_thread_pool_t *rsync_task_queue;
    int rsync_port;
    int rsync_io_thread_num;
    int rsync_task_thread_num;

    tair_manager *tair_mgr;
    heartbeat_thread heartbeat;
    request_processor *req_processor;

    tair::util::StaticBitMap *readonly_ns;

    configuration_manager *configuration_mgr;
    tair::stat::StatManager *stat_mgr;
    tair::stat::FlowController *flow_ctrl;
    time_t last_flow_control_log_time_[TAIR_MAX_AREA_COUNT];
    stat_processor *stat_prc;
    easy_atomic_t cur_queued_mem;
    int64_t max_queued_mem;

    friend class request_processor;

    tair::common::TairStat *server_stat;
    RTCollector *rtc;
    HotKey *hotk;
    OPKiller *opkiller;
    std::string config_file;
private:
    inline bool initialize();

    inline bool destroy();

    void easy_io_initialize();

    //~ called by `easy_io_packet_handler_cb'
    int easy_io_packet_handler(easy_request_t *r);

    //~ called by `easy_rsync_io_packet_handler_cb`
    int easy_rsync_io_packet_handler(easy_request_t *r);

    //~ called by `packet_handler_cb'
    int packet_handler(easy_request_t *r);

    //~ called by `proxy_packet_handler_cb'
    int proxy_packet_handler(easy_request_t *r);

    bool rsync_initialize();

    bool rsync_start();

    void rsync_stop();

    void rsync_wait();

    //~ callback by libeasy's I/O thread
    inline static int easy_io_handler_cb(easy_request_t *r) {
        tair_server *_this = (tair_server *) r->ms->c->handler->user_data;
        return _this->easy_io_packet_handler(r);
    }

    //~ callback by libeasy's worker thread pool
    inline static int packet_handler_cb(easy_request_t *r, void *args) {
        tair_server *_this = (tair_server *) args;
        return _this->packet_handler(r);
    }

    //~ callback by libeasy's I/O thread
    inline static int easy_rsync_io_handler_cb(easy_request_t *r) {
        tair_server *_this = (tair_server *) r->ms->c->handler->user_data;
        return _this->easy_rsync_io_packet_handler(r);
    }

    //~ callback by libeasy's rsync thread pool
    inline static int rsync_packet_handler_cb(easy_request_t *r, void *args) {
        tair_server *_this = (tair_server *) args;
        return _this->packet_handler(r);
    }

    //~ callback by libeasy's I/O thread, only used in `do_proxy' when necessary
    inline static int proxy_packet_handler_cb(easy_request_t *r) {
        tair_server *_this = (tair_server *) r->ms->c->handler->user_data;
        return _this->proxy_packet_handler(r);
    }

    int send_proxy(easy_addr_t &addr, base_packet *proxy_packet, void *args) {
        return easy_helper::easy_async_send(&eio, addr, proxy_packet, args, &eio_proxy_handler);
    }

    int do_op_kill(request_op_cmd *p, bool &send_return);

    int do_set_flow_policy(request_op_cmd *p, bool &send_return);

    int operate_read_only_switch(request_op_cmd *request, bool on, bool &send_return);

    int get_read_only_status(request_op_cmd *request, bool &send_return);

    int get_valid_command_operating_ns(std::vector<std::string> &params, int &ns);
};
}

#endif
///////////////////////END
