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

#ifndef TAIR_RT_COLLECTOR_HPP
#define TAIR_RT_COLLECTOR_HPP

#include <ext/hash_map>
#include "rwlock.hpp"

struct easy_request_t;
namespace tair {

class RTObject;

class base_packet;

class request_rt;

class response_rt;

class RTCollector {
public:
    RTCollector() {
        active_ = false;
        auto_enable_ = false;
        stop_ = true;
        rt_dump_threshold_ = 0;
        rt_percent_ = 100;
        rt_reset_interval_ = 10;
        reset_thread_ = 0;
    }

    ~RTCollector() {
        stop_ = true;
        join_reset_thread();
        release_rtos();
    }

public:
    /*
     * Load or reload configures from specific file.
     * REQUIRES: Must have been disabled(by default).
     */
    bool load_config(const char *config_file);

    /*
     * Enable the collector
     */
    void enable();

    /*
     * Disable the collector
     */
    void disable();

    /*
     * enabled?
     */
    bool active() {
        return active_;
    }

    /*
     * Reset all rto, i.e. rt statistics
     */
    void reset();

    /*
     * Reap the statistics into the outter buffer
     * @param buf & size, buffer and her size
     * @return , bytes that the `buf' has been written.
     */
    size_t reap(char *buf, size_t size);

    /*
     * Wrapper for `rt'
     */
    void rt_beg(easy_request_t *r) {
        rt(r);
    }

    void rt_end(easy_request_t *r) {
        rt(r);
    }

    /*
     * Do necessary RT jobs.
     * Inline the `active' judgement.
     */
    void rt(easy_request_t *r) {
        if (active()) {
            do_rt(r);
        }
    }

    void do_req_rt(const request_rt *req, response_rt *resp);

private:
    void add_op(int op);

    void add_default_ops();

    void do_rt(easy_request_t *r);

    bool load_config_light(const char *);

    uint64_t us_now() {
        struct timeval t;
        gettimeofday(&t, NULL);
        return t.tv_sec * 1000000 + t.tv_usec;
    }

    static void *reset_thread(void *arg);

    void do_reset();

    void launch_reset_thread();

    void join_reset_thread();

    void release_rtos();

private:
    /*
     * Map `opcode' to its RTObject
     */
    typedef __gnu_cxx::hash_map<int, RTObject *> RTOMap;
    RTOMap cont_;
    /*
     * We need a lock to protect RTCollector's members:
     *   the hash map itself while inserting new `opcode',
     *   and others while reloading.
     * rwlock is preferred, spinning one would be better.
     */
    mutable rwlock_t rwlock_;
    bool active_;
    bool auto_enable_;
    bool stop_;
    int rt_dump_threshold_; //~ threshold to dump packet, by microseconds.
    int rt_percent_;
    int rt_reset_interval_;
    pthread_t reset_thread_;
    std::string config_file_;
};

}

#endif
