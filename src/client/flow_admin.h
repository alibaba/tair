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

#ifndef TAIR_CLIENT_FLOW_ADMIN_H
#define TAIR_CLIENT_FLOW_ADMIN_H

#include <tr1/unordered_map>
#include <tbsys.h>

#include "flowrate.h"

namespace tair {

class flow_limit_handler {
public:
    flow_limit_handler();

    bool is_over_flow();

    bool limit_level_up();

    bool limit_level_down();

    time_t last_time() { return last_time_; }

    void set_last_time(time_t last) { this->last_time_ = last; }

    const static int32_t MAX_THRESHOLD = 1000;
    const static time_t UP_INTERVAL = 10; // 10s
    const static time_t DOWN_INTERVAL = 5; // 10s
    const static double UP_FACTOR = 0.3;
    const static double DOWN_FACTOR = 0.5;

private:
    time_t last_time_;
    int32_t threshold_;
};

class flow_admin {
public:

    ~flow_admin() { clear(); }

    bool is_over_flow(int64_t server_id, int32_t ns);

    void limit_level_keep(int64_t server_id, int32_t ns);

    bool limit_level_up(int64_t server_id, int32_t ns);

    bool limit_level_down(int64_t server_id, int32_t ns);

    void clear();

    void control(int64_t server_id, int32_t ns, tair::stat::FlowStatus status);

    bool should_check_flow_down(int64_t server_id, int32_t ns);

private:
    typedef std::tr1::unordered_map<int32_t, flow_limit_handler *> flow_limit_handler_map;
    typedef std::tr1::unordered_map<int64_t, flow_limit_handler_map> flow_limit_set;

    flow_limit_set flow_limit_set_;

    tbsys::CRWLock rwlock_;

    flow_limit_handler *get_handler(int64_t server_id, int32_t ns);

    flow_limit_handler *make_handler(int64_t server_id, int32_t ns);
};

}


#endif
