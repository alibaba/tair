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

#ifndef TAIR_CONFIG_SERVER_H
#define TAIR_CONFIG_SERVER_H

#include <string>
#include <ext/hash_map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <tbsys.h>
#include <easy_io.h>
#include "server_conf_thread.hpp"
#include "packet_factory.hpp"

using namespace std;
using namespace __gnu_cxx;

namespace tair {
namespace config_server {
class tair_config_server {
public:
    tair_config_server();

    ~tair_config_server();

    void start();

    void stop();

    static tair_config_server *get_server() {
        return _this;
    }

private:
    int packet_handler(easy_request_t *r);

    static int packet_handler_cb(easy_request_t *r, void *) {
        _this->packet_handler(r);
        return EASY_OK;
    }

    int easy_io_packet_handler(easy_request_t *r);

    static int easy_io_packet_handler_cb(easy_request_t *r) {
        return _this->easy_io_packet_handler(r);
    }

    easy_io_t eio;
    easy_io_handler_pt handler;
    static tair_config_server *_this;

    easy_thread_pool_t *task_queue;
    int stop_flag;
    server_conf_thread my_server_conf_thread;

private:
    inline int initialize();

    inline int destroy();
};
}
}
#endif
///////////////////////END
