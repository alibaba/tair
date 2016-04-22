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

#ifndef TAIR_OP_KILLER_HPP
#define TAIR_OP_KILLER_HPP

#include <bitset>
#include <set>
#include "base_packet.hpp"
#include "rwlock.hpp"

namespace tair {

class OPKiller {
public:
    OPKiller() {
        active_ = false;
        reset();
    }

    /*
     * Return false if this op should be refused
     */
    bool test(const base_packet *p);

    void add_target(int area, int opcode);

    void enable();

    void disable();

private:
    void reset();

    bool active() { return active_; }

    bool do_test(const base_packet *p);

private:
    enum {
        BLOOM_FILTER_COUNT = 1024,
        BLOOM_FILTER_MASK = BLOOM_FILTER_COUNT - 1,
    };
    std::bitset<BLOOM_FILTER_COUNT> opcodes_filter_;
    std::set<int> target_opcodes_;
    std::bitset<BLOOM_FILTER_COUNT> areas_filter_;
    std::set<int> target_areas_;

    rwlock_t rwlock_;
    bool active_;
};

void OPKiller::add_target(int area, int opcode) {
    rwlock_.write_lock();
    areas_filter_.set(area & BLOOM_FILTER_MASK);
    opcodes_filter_.set(opcode & BLOOM_FILTER_MASK);
    target_areas_.insert(area);
    target_opcodes_.insert(opcode);
    enable();
    rwlock_.write_unlock();
}

bool OPKiller::test(const base_packet *p) {
    bool ret = true;
    if (active()) {
        rwlock_.read_lock();
        ret = do_test(p);
        rwlock_.read_unlock();
    }
    return ret;
}

bool OPKiller::do_test(const base_packet *p) {
    //~ Test namespace first.
    int area = p->ns();
    if (!areas_filter_.test(area & BLOOM_FILTER_MASK)) {
        return true;
    }
    if (target_areas_.find(area) == target_opcodes_.end()) {
        return true;
    }

    int opcode = p->getPCode();
    if (!opcodes_filter_.test(opcode & BLOOM_FILTER_MASK)) {
        return true;
    }
    if (target_opcodes_.find(opcode) == target_opcodes_.end()) {
        return true;
    }

    log_info("op[%d] of area[%d] refused.", opcode, area);
    return false;
}

void OPKiller::enable() {
    active_ = true;
}

void OPKiller::disable() {
    active_ = false;
    rwlock_.write_lock();
    reset();
    rwlock_.write_unlock();
}

void OPKiller::reset() {
    opcodes_filter_.reset();
    target_opcodes_.clear();
    areas_filter_.reset();
    target_areas_.clear();
}

}

#endif
