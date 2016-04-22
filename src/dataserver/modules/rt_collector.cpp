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

#include <sys/prctl.h>
#include <string>
#include <tbsys.h>
#include <easy_io.h>
#include "rt_collector.hpp"
#include "rt_object.hpp"
#include "base_packet.hpp"
#include "rt_packet.hpp"
#include "log.hpp"

namespace tair {

bool RTCollector::load_config(const char *file) {
    if (config_file_.empty() || config_file_ != (file)) {
        config_file_ = file;
    }
    tbsys::CConfig config;
    if (config.load(config_file_.c_str()) == EXIT_FAILURE) {
        return false;
    }
    const char *oplist_s = config.getString("extras", "rt_oplist", NULL);
    size_t len = 0;
    bool use_default_ops = false;
    if (oplist_s == NULL || (len = strlen(oplist_s) == 0)) {
        log_error("no config found for `rt_oplist' in section[extras]");
        use_default_ops = true;
    }
    std::vector<std::string> vs;
    if (!use_default_ops) {
        vs.reserve(64);
        string_util::split_str(oplist_s, ", ", vs);
        if (vs.empty()) {
            log_error("config `rt_oplist' is illegal: %s", oplist_s);
            use_default_ops = true;
        }
    }

    rt_dump_threshold_ = config.getInt("extras", "rt_threshold", 8000);
    auto_enable_ = config.getInt("extras", "rt_auto_enable", 0);
    rt_percent_ = config.getInt("extras", "rt_percent", 100);
    rt_reset_interval_ = config.getInt("extras", "rt_reset_interval", 10);
    bool save_active = active_;

    disable(); //~ assure that no new users will come.

    rwlock_.write_lock(); //~ assure that all old users leave
    if (!cont_.empty()) {
        RTOMap::iterator itr = cont_.begin();
        while (itr != cont_.end()) {
            delete itr->second;
            ++itr;
        }
        cont_.clear();
    }
    if (use_default_ops) {
        add_default_ops();
    } else {
        for (size_t i = 0; i < vs.size(); ++i) {
            int op = atoi(vs[i].c_str());
            add_op(op);
        }
    }
    rwlock_.write_unlock();

    if (auto_enable_ || save_active) {
        enable();
    }

    return true;
}

bool RTCollector::load_config_light(const char *file) {
    tbsys::CConfig config;
    if (config.load(config_file_.c_str()) == EXIT_FAILURE) {
        return false;
    }

    rt_dump_threshold_ = config.getInt("extras", "rt_threshold", 8000);
    rt_percent_ = config.getInt("extras", "rt_percent", 100);
    rt_reset_interval_ = config.getInt("extras", "rt_reset_interval", 10);

    return true;
}

void RTCollector::enable() {
    //~ no lock needed
    active_ = true;
    if (stop_) {
        stop_ = false;
        launch_reset_thread();
    }
}

void RTCollector::disable() {
    //~ no lock needed
    active_ = false;
}

void RTCollector::reset() {
    string access_info;
    access_info.reserve(1 << 10);
    char buf[1 << 10];
    const size_t size = sizeof(buf);
    rwlock_.read_lock();
    RTOMap::iterator itr = cont_.begin();
    while (itr != cont_.end()) {
        itr->second->serialize(access_info, buf, size, base_packet::name);
        itr->second->reset();
        ++itr;
    }
    rwlock_.read_unlock();
    if (0 != access_info.size()) {
        log_warn("access info : \n %s", access_info.c_str());
    }
}

void RTCollector::do_rt(easy_request_t *r) {
    base_packet *req, *resp;
    if (NULL != r &&
        NULL != (req = reinterpret_cast<base_packet *>(r->ipacket))) {
        if (!req->rt_granted()) return;
        if ((rand() % 100) >= rt_percent_) {
            req->rt_not_grant();
            return;
        }

        RTOMap::iterator itr;
        rwlock_.read_lock();
        if ((itr = cont_.find(req->getPCode())) == cont_.end()) {
            //~ not our target op
            rwlock_.read_unlock();
            return;
        }
        if (req->request_time == 0UL) {
            //~ it's a request
            rwlock_.read_unlock();
            req->request_time = us_now();
            log_debug("rt_beg() for op[%d]", req->getPCode());
            return;
        }
        if ((resp = reinterpret_cast<base_packet *>(r->opacket)) == NULL) {
            //~ this should not happen
            (void) &resp;
            rwlock_.read_unlock();
            return;
        }
        log_debug("rt_end() for op[%d]", req->getPCode());
        int us = us_now() - req->request_time;
        if (us > rt_dump_threshold_) {
            fprintf(stderr, "op[%d] consumed %dus\n", req->getPCode(), us);
            req->dump();
        }
        itr->second->rt(us, (RTObject::RTType) resp->failed());
        rwlock_.read_unlock();
    }
}

size_t RTCollector::reap(char *buf, size_t size) {
    rwlock_.read_lock();
    RTOMap::iterator itr = cont_.begin();
    while (itr != cont_.end()) {
        (void) itr->second->serialize(NULL, 0);
        ++itr;
    }
    rwlock_.read_unlock();
    return 0;
}

void RTCollector::do_req_rt(const request_rt *req, response_rt *resp) {
    if (resp == NULL) return;
    if (req->cmd_ == request_rt::RT_ENABLE) {
        log_warn("enable rt");
        enable();
        return;
    }
    if (req->cmd_ == request_rt::RT_DISABLE) {
        log_warn("disable rt");
        disable();
        return;
    }
    if (req->cmd_ == request_rt::RT_RELOAD) {
        (void) load_config(config_file_.c_str());
        return;
    }
    if (req->cmd_ == request_rt::RT_RELOAD_LIGHT) {
        (void) load_config_light(config_file_.c_str());
        return;
    }

    assert(req->cmd_ == request_rt::RT_GET);
    int op = req->opcode_;
    RTOMap::iterator itr;
    rwlock_.read_lock();
    if (op != -1) {
        itr = cont_.find(op);
        if (itr != cont_.end()) {
            resp->add_rto(new RTObject(*itr->second));
        }
    } else { //~ all we have
        itr = cont_.begin();
        while (itr != cont_.end()) {
            resp->add_rto(new RTObject(*itr->second));
            ++itr;
        }
    }
    rwlock_.read_unlock();
}

void RTCollector::add_op(int op) {
    log_error("new RTO for op %d", op);
    RTObject *&rto = cont_[op];
    if (rto == NULL) {
        rto = new RTObject(op);
    }
}

void RTCollector::add_default_ops() {
    static int default_ops[] = {
            TAIR_REQ_PUT_PACKET,
            TAIR_REQ_GET_PACKET,
            TAIR_REQ_REMOVE_PACKET,
            TAIR_REQ_INCDEC_PACKET,
            TAIR_REQ_LOCK_PACKET,
            TAIR_REQ_HIDE_PACKET,
            TAIR_REQ_GET_HIDDEN_PACKET,
            TAIR_REQ_GET_RANGE_PACKET,
            TAIR_REQ_PREFIX_PUTS_PACKET,
            TAIR_REQ_PREFIX_REMOVES_PACKET,
            TAIR_REQ_PREFIX_INCDEC_PACKET,
            TAIR_REQ_PREFIX_GETS_PACKET,
            TAIR_REQ_PREFIX_HIDES_PACKET,
            TAIR_REQ_PREFIX_GET_HIDDENS_PACKET,
    };
    for (size_t i = 0; i < sizeof(default_ops) / sizeof(int); ++i) {
        add_op(default_ops[i]);
    }
}

void *RTCollector::reset_thread(void *arg) {
    prctl(PR_SET_NAME, "rt_thread", 0, 0, 0);
    reinterpret_cast<RTCollector *>(arg)->do_reset();
    return NULL;
}

void RTCollector::do_reset() {
    while (!stop_) {
        TAIR_SLEEP(stop_, rt_reset_interval_);
        reset();
    }
}

void RTCollector::launch_reset_thread() {
    pthread_create(&reset_thread_, NULL, reset_thread, this);
}

void RTCollector::join_reset_thread() {
    if (reset_thread_ != 0) {
        pthread_join(reset_thread_, NULL);
    }
}

void RTCollector::release_rtos() {
    RTOMap::iterator itr = cont_.begin();
    while (itr != cont_.end()) {
        delete itr->second;
        ++itr;
    }
}

}
