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

#include <cstdlib>

#include "log.hpp"
#include "flow_admin.h"


namespace tair {

void flow_admin::control(int64_t server_id, int32_t ns, tair::stat::FlowStatus status) {
    using namespace tair::stat;
    switch (status) {
        case KEEP:
            limit_level_keep(server_id, ns);
            break;
        case DOWN:
            limit_level_down(server_id, ns);
            break;
        case UP:
            limit_level_up(server_id, ns);
            break;
    }
}

void flow_admin::limit_level_keep(int64_t server_id, int32_t ns) {
    tbsys::CWLockGuard lock(rwlock_);
    flow_limit_handler *handler = get_handler(server_id, ns);
    if (handler == NULL) {
        handler = make_handler(server_id, ns);
    }
    handler->set_last_time(time(NULL));
}

bool flow_admin::limit_level_down(int64_t server_id, int32_t ns) {
    tbsys::CWLockGuard lock(rwlock_);
    flow_limit_handler *handler = get_handler(server_id, ns);
    if (handler == NULL) {
        handler = make_handler(server_id, ns);
    }
    return handler->limit_level_down();
}

bool flow_admin::limit_level_up(int64_t server_id, int32_t ns) {
    tbsys::CWLockGuard lock(rwlock_);
    flow_limit_handler *handler = get_handler(server_id, ns);
    if (handler == NULL) {
        handler = make_handler(server_id, ns);
    }
    return handler->limit_level_up();
}

bool flow_admin::should_check_flow_down(int64_t server_id, int32_t ns) {
    time_t now = time(NULL);
    tbsys::CWLockGuard lock(rwlock_);
    flow_limit_handler *handler = get_handler(server_id, ns);
    if (handler == NULL)
        return false;
    time_t last = handler->last_time();
    if (now - last < flow_limit_handler::DOWN_INTERVAL)
        return false;
    return true;
}

bool flow_admin::is_over_flow(int64_t server_id, int32_t ns) {
    tbsys::CRLockGuard rlock(rwlock_);
    flow_limit_handler *handler = get_handler(server_id, ns);
    if (handler == NULL)
        return false;
    return handler->is_over_flow();
}

void flow_admin::clear() {
    tbsys::CWLockGuard wlock(rwlock_);
    for (flow_limit_set::iterator fl_iter = flow_limit_set_.begin();
         fl_iter != flow_limit_set_.end(); ++fl_iter) {
        for (flow_limit_handler_map::iterator h_iter = fl_iter->second.begin();
             h_iter != fl_iter->second.end(); ++h_iter) {
            delete h_iter->second;
        }
        fl_iter->second.clear();
    }
    flow_limit_set_.clear();
}

flow_limit_handler *flow_admin::get_handler(int64_t server_id, int32_t ns) {
    flow_limit_set::iterator handler_ds_iter = flow_limit_set_.find(server_id);
    if (flow_limit_set_.end() == handler_ds_iter)
        return NULL;
    flow_limit_handler_map::iterator handler_ds_and_area_iter = handler_ds_iter->second.find(ns);
    if (handler_ds_iter->second.end() == handler_ds_and_area_iter)
        return NULL;
    return handler_ds_and_area_iter->second;
}

flow_limit_handler *flow_admin::make_handler(int64_t server_id, int32_t ns) {
    flow_limit_handler_map &handler_map = flow_limit_set_[server_id];
    flow_limit_handler_map::iterator iter = handler_map.find(ns);
    flow_limit_handler *handler = NULL;
    if (iter != handler_map.end())
        handler = iter->second;
    else {
        handler = new flow_limit_handler();
        handler_map[ns] = handler;
    }
    return handler;
}

// implement flow_limit_handler

flow_limit_handler::flow_limit_handler()
        : last_time_(0), threshold_(0) {
}

bool flow_limit_handler::limit_level_up() {
    time_t now = time(NULL);
    if (now - last_time_ < UP_INTERVAL) {
        return false;
    }
    if (threshold_ < MAX_THRESHOLD - 10) {
        threshold_ = static_cast<int32_t>(UP_FACTOR * (MAX_THRESHOLD - threshold_) + threshold_);
        last_time_ = now;
        log_debug("level up, current flow threshold %d", threshold_);
    }
    return true;
}

bool flow_limit_handler::limit_level_down() {
    time_t now = time(NULL);
    if (now - last_time_ < DOWN_INTERVAL) {
        return false;
    }
    threshold_ = static_cast<int32_t>(threshold_ - DOWN_FACTOR * threshold_);
    if (threshold_ < 50)
        threshold_ = 0;
    last_time_ = now;
    log_debug("level down, current flow threshold %d", threshold_);
    //TODO: log warn
    return true;
}

bool flow_limit_handler::is_over_flow() {
    if (threshold_ == 0)
        return false;
    else if (threshold_ >= MAX_THRESHOLD) {
        // TODO: log warn
        return true;
    } else
        return random() % MAX_THRESHOLD < threshold_;
}

}
