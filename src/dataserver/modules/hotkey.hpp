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

#ifndef TAIR_HOTKEY_HPP
#define TAIR_HOTKEY_HPP

#include <set>
#include <vector>
#include <bitset>
#include <pthread.h>
#include "rwlock.hpp"

namespace tair {

class base_packet;
namespace common { class data_entry; }
class HotKey {
public:
    HotKey() {
        reset();
        pthread_spin_init(&owner_lock_, PTHREAD_PROCESS_PRIVATE);
    }

    ~HotKey() {
        release_hot_cache();
        release_hot_keys();
        pthread_spin_destroy(&owner_lock_);
    }

    void hot(const base_packet *p) {
        if (active()) {
            if (pthread_spin_trylock(&owner_lock_) != 0) {
                return;
            }
            do_hot(p);
            pthread_spin_unlock(&owner_lock_);
        }
    }

    void tag_if_hot(const base_packet *p) {
        if (active() && need_feedback_) {
            read_guard_t guard(&hot_keys_lock_); //~ protect from `load_config'
            do_tag_if_hot(p);
        }
    }

    /*
     * Load or reload configures from specific file.
     * REQUIRES: Must have been disabled(by default).
     */
    bool load_config(const char *file);

    void enable() {
        active_ = true;
    }

    void disable() {
        active_ = false;
    }

    bool active() const {
        return active_;
    }

private:
    void do_hot(const base_packet *p);

    /*
     * Pick a key from packet, then hash it.
     */
    int64_t hash(const base_packet *p);

    /*
     * Do sampling
     */
    void do_sampling(const base_packet *p);

    /*
     * Do reaping
     */
    void do_reaping(const base_packet *p);

    /*
     * Whether target opcode
     */
    bool hit(int op) const;

    /*
     * switch phase to PHASE_REAPING
     */
    void phase_try_to_reap();

    /*
     * To next round
     */
    void enter_next_round();

    /*
     * Dump packet
     */
    void dump_packet(const base_packet *p) const;

    /*
     * Reset all fields
     */
    void reset();

    /*
     * Preserve opcodes if not specified in configure file
     */
    void add_default_ops();

    /*
     * Add opcode
     */
    void add_op(int op);

    /*
     * Calculate the Standard Deviation,
     * This tells us whether hot keys exist.
     */
    typedef std::pair<size_t, size_t> SigAvgPair;

    SigAvgPair sigma(const size_t *beg, const size_t *end);

    bool justify_hot(const SigAvgPair &sig_avg);

    /*
     * Allocate hot cache
     */
    void allocate_hot_cache();

    /*
     * Release hot cache
     */
    void release_hot_cache();

    /*
     * Add a hot key to cache, or increase the corresponding counter
     */
    void add_to_hot_cache(const base_packet *p);

    /*
     * Dump out the cache
     */
    void dump_cache();

    /*
     * Fetch the hottest key
     */
    common::data_entry *fetch_hottest_key();

    /*
     * Call p->tag_hot() if this packet had hot key
     */
    void do_tag_if_hot(const base_packet *p);

    /*
     * Add hot key to hot_keys_
     */
    bool add_to_hot_keys(common::data_entry *key);

    /*
     * Release hot keys we hold.
     */
    void release_hot_keys();

    /*
     * Print out config infos
     */
    void log_config() const;

    bool key_equal(const common::data_entry *, const common::data_entry *);

private:
    enum HotKind {
        HOT_QPS,
        HOT_SIZE,

        HOT_KIND_NUM
    } hot_kind_;
    /*
     * Distinguishing hot keys need three phases.
     */
    enum PhaseType {
        PHASE_SAMPLING = 0,
        PHASE_REAPING = 1,
        PHASE_DONE = 2,
    } phase_;

    /*
     * Buckets accounting key counts of specific hash values.
     */
    enum {
        BUCKET_COUNT = 1024,
        BUCKET_MASK = BUCKET_COUNT - 1,
    };

    size_t key_buckets_[HOT_KIND_NUM][BUCKET_COUNT];

    /*
     * Certainly, we don't want to `hot' all packets of the world.
     */
    std::set<int> target_opcodes_;
    /*
     * A simple bloom-filter
     */
    enum {
        BLOOM_FILTER_COUNT = 1024,
        BLOOM_FILTER_MASK = BLOOM_FILTER_COUNT - 1,
    };
    std::bitset<BLOOM_FILTER_COUNT> opcodes_filter_;

    /*
     * HotCache is a facility to find the specific hot key.
     */
    struct HotCache {
        union {
            size_t count;
            size_t size;
        };
        common::data_entry *de;
    };
    HotCache *cache_;
    size_t cache_capacity_;
    size_t cache_size_;

    /*
     * Once we find a hot key, add to this list
     */
    std::vector<common::data_entry *> hot_keys_;
    rwlock_t hot_keys_lock_;

    pthread_spinlock_t owner_lock_;

    size_t sample_count_;
    size_t sample_max_;
    size_t reap_count_;
    size_t reap_max_;
    size_t hot_bucket_;
    double hot_factor_;
    bool need_feedback_;
    bool need_dump_;
    bool one_shot_;
    bool active_;
};

}

#endif
