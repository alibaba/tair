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

#include <algorithm>
#include <cmath>
#include "hotkey.hpp"
#include "base_packet.hpp"
#include "data_entry.hpp"

namespace tair {

bool HotKey::load_config(const char *file) {
    pthread_spin_lock(&owner_lock_);
    bool saved_active = active();
    disable();
    reset();

    tbsys::CConfig config;
    bool success = true;
    do {
        if (config.load(file) == EXIT_FAILURE) {
            success = false;
            break;
        }

        sample_max_ = config.getInt("extras", "hotk_sample_max", 50000);
        reap_max_ = config.getInt("extras", "hotk_reap_max", 32);
        cache_capacity_ = reap_max_;
        need_feedback_ = config.getInt("extras", "hotk_need_feedback", 0) == 1;
        need_dump_ = config.getInt("extras", "hotk_need_dump", 0) == 1;
        one_shot_ = config.getInt("extras", "hotk_one_shot", 0) == 1;
        const char *str = config.getString("extras", "hotk_hot_factor", "0.25");
        sscanf(str, "%lf", &hot_factor_);
        log_config();

        const char *oplist_s = config.getString("extras", "hotk_oplist", NULL);
        size_t len = 0;
        if (oplist_s == NULL || (len = strlen(oplist_s) == 0)) {
            add_default_ops();
            break;
        }
        std::vector<std::string> vs;
        vs.reserve(64);
        string_util::split_str(oplist_s, ", ", vs);
        if (vs.empty()) {
            add_default_ops();
            break;
        }

        for (size_t i = 0; i < vs.size(); ++i) {
            add_op(atoi(vs[i].c_str()));
        }
    } while (false);

    if (saved_active) {
        enable();
    }
    pthread_spin_unlock(&owner_lock_);
    return success;
}

void HotKey::reset() {
    active_ = false;
    hot_kind_ = HOT_QPS;
    hot_factor_ = 0.25;
    need_feedback_ = false;
    need_dump_ = false;
    one_shot_ = false;
    phase_ = PHASE_SAMPLING;
    sample_count_ = 0UL;
    sample_max_ = 0UL;
    reap_count_ = 0UL;
    reap_max_ = 0UL;
    cache_capacity_ = 0UL;
    hot_bucket_ = 0UL;
    memset(key_buckets_, 0, sizeof(size_t) * BUCKET_COUNT * HOT_KIND_NUM);
    target_opcodes_.clear();
    opcodes_filter_.reset();
    release_hot_cache();
    release_hot_keys();
    hot_keys_.reserve(64);
}

void HotKey::add_op(int op) {
    log_error("add op[%d] for HotKey", op);
    target_opcodes_.insert(op);
    opcodes_filter_.set(op & BLOOM_FILTER_MASK);
}

void HotKey::add_default_ops() {
    static int default_opcodes[] = {
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
    for (size_t i = 0; i < sizeof(default_opcodes) / sizeof(int); ++i) {
        add_op(default_opcodes[i]);
    }
}

void HotKey::do_hot(const base_packet *p) {
    if (!hit(p->getPCode())) {
        return;
    }

    PhaseType phase = phase_; // read only once
    if (phase == PHASE_SAMPLING) {
        do_sampling(p);
    } else if (phase == PHASE_REAPING) {
        do_reaping(p);
    } else if (phase == PHASE_DONE) {
        disable();
    } else {
        assert(false);
    }
    if (!one_shot_ && phase_ == PHASE_DONE) {
        enter_next_round();
    }
}

void HotKey::do_sampling(const base_packet *p) {
    int64_t hv = hash(p);
    if (hv == -1) {
        return;
    }
    if (++sample_count_ > sample_max_) {
        phase_try_to_reap();
        return;
    }

    ++key_buckets_[HOT_QPS][hv & BUCKET_MASK];
    key_buckets_[HOT_SIZE][hv & BUCKET_MASK] += p->size();
}

void HotKey::do_reaping(const base_packet *p) {
    int64_t hv = hash(p);
    if (hv == -1 || (hv & BUCKET_MASK) != hot_bucket_) {
        return;
    }
    if (++reap_count_ > reap_max_) {
        phase_ = PHASE_DONE;
        dump_cache();
        data_entry *hottest = fetch_hottest_key();
        if (hottest != NULL && !add_to_hot_keys(hottest)) {
            delete hottest;
        }
        log_info("HotKey done.");
        return;
    }
    add_to_hot_cache(p);
    dump_packet(p);
}

int64_t HotKey::hash(const base_packet *p) {
    const data_entry *de = p->pick_key();
    if (de == NULL) {
        return -1;
    }
    return de->get_hashcode();
}

bool HotKey::hit(int op) const {
    if (!opcodes_filter_.test(op & BLOOM_FILTER_MASK)) {
        return false;
    }
    return target_opcodes_.find(op) != target_opcodes_.end();
}

void HotKey::phase_try_to_reap() {
    SigAvgPair sig_avg = sigma(key_buckets_[HOT_QPS], key_buckets_[HOT_QPS] + BUCKET_COUNT);
    if (!justify_hot(sig_avg)) {
        sig_avg = sigma(key_buckets_[HOT_SIZE], key_buckets_[HOT_SIZE] + BUCKET_COUNT);
        if (!justify_hot(sig_avg)) {
            log_error("sigma: %lu, mean: %lu, no hot key found",
                      sig_avg.first, sig_avg.second);
            phase_ = PHASE_DONE;
            return;
        }
        hot_kind_ = HOT_SIZE;
    }
    hot_bucket_ = std::max_element(key_buckets_[hot_kind_],
                                   key_buckets_[hot_kind_] + BUCKET_COUNT) - key_buckets_[hot_kind_];

    allocate_hot_cache();
    phase_ = PHASE_REAPING;
    log_error("sigma: %lu, mean: %lu, hot bucket[%lu] found",
              sig_avg.first, sig_avg.second, hot_bucket_);
}

void HotKey::enter_next_round() {
    log_error("HotKey enters to next round");
    hot_kind_ = HOT_QPS;
    hot_bucket_ = 0;
    sample_count_ = 0;
    reap_count_ = 0;
    release_hot_cache();
    memset(key_buckets_, 0, sizeof(size_t) * BUCKET_COUNT * HOT_KIND_NUM);
    phase_ = PHASE_SAMPLING;
    enable();
}

void HotKey::dump_packet(const base_packet *p) const {
    //~ dump yourself
    if (need_dump_) {
        p->dump();
    }
}

HotKey::SigAvgPair HotKey::sigma(const size_t *beg, const size_t *end) {
    size_t sum = 0;
    size_t peak = 0;
    size_t sum_sqr = 0;
    size_t n = end - beg;
    while (beg != end) {
        sum += *beg;
        sum_sqr += *beg * *beg;
        if (*beg > peak) peak = *beg;
        ++beg;
    }

    size_t sigma = static_cast<size_t>(sqrt((1. * sum_sqr / n) - (1. * sum / n) * (1. * sum / n)));
    log_error("sigma: %lu, mean: %lu, peak: %lu", sigma, sum / n, peak);

    return SigAvgPair(sigma, sum / n);
}

bool HotKey::justify_hot(const SigAvgPair &sig_avg) {
    if (sig_avg.second == 0 ||
        sig_avg.first < (size_t) sig_avg.second * hot_factor_) {
        return false;
    }
    return true;
}

void HotKey::allocate_hot_cache() {
    release_hot_cache();
    cache_ = new HotCache[cache_capacity_];
    cache_size_ = 0UL;
}

void HotKey::release_hot_cache() {
    if (cache_ != NULL) {
        for (size_t i = 0; i < cache_size_; ++i) {
            if (cache_[i].de != NULL) {
                delete cache_[i].de;
                cache_[i].de = NULL;
                cache_[i].count = 0UL;
            }
        }
        delete[] cache_;
        cache_ = NULL;
    }
    cache_size_ = 0UL;
}

void HotKey::add_to_hot_cache(const base_packet *p) {
    const data_entry *picked = p->pick_key();
    if (picked == NULL) {
        return;
    }

    size_t index = 0;
    for (; index < cache_size_; ++index) {
        if (key_equal(cache_[index].de, picked)) {
            if (hot_kind_ == HOT_QPS) {
                ++cache_[index].count;
            } else {
                cache_[index].size += p->size();
            }
            return;
        }
    }

    data_entry *de = new data_entry(*picked);
    de->change_area(p->ns()); //~ Ensure that we have the correct area.
    ++cache_size_;
    cache_[index].de = de;
    if (hot_kind_ == HOT_QPS) {
        cache_[index].count = 1;
    } else {
        cache_[index].size += p->size();
    }
}

void HotKey::dump_cache() {
    if (!need_dump_) {
        return;
    }

    size_t cache_size = cache_size_;
    fprintf(stderr, "dumping hot cache, cache size: %lu\n", cache_size);
    for (size_t i = 0; i < cache_size; ++i) {
        char buf[128];
        size_t len = snprintf(buf, sizeof(buf), "%s: %lu, ",
                              hot_kind_ == HOT_QPS ? "key count" : "size", cache_[i].count);
        cache_[i].de->to_ascii(buf + len, sizeof(buf) - len);
        fprintf(stderr, "%s\n", buf);
    }
    fprintf(stderr, "dumping done.\n");
}

data_entry *HotKey::fetch_hottest_key() {
    if (cache_size_ == 0) {
        return NULL;
    }
    data_entry *hottest = NULL;
    size_t max = 0;
    size_t max_i = 0;
    for (size_t i = 0; i < cache_size_; ++i) {
        if (cache_[i].count > max) {
            hottest = cache_[i].de;
            max = cache_[i].count;
            max_i = i;
        }
    }
    cache_[max_i].de = NULL;
    cache_[max_i].count = 0;

    return hottest;
}

void HotKey::do_tag_if_hot(const base_packet *p) {
    if (!need_feedback_ || hot_keys_.empty()) {
        return;
    }
    const data_entry *picked = p->pick_key();
    if (picked == NULL) {
        return;
    }

    {
        read_guard_t guard(&hot_keys_lock_);
        for (size_t i = 0; i < hot_keys_.size(); ++i) {
            if (key_equal(hot_keys_[i], picked) && hot_keys_[i]->area == p->ns()) {
                log_info("packet[%d] of ns[%d] is tagged hot",
                         p->getPCode(), hot_keys_[i]->area);
                p->tag_hot();
                return;
            }
        }
    }
}

bool HotKey::add_to_hot_keys(data_entry *key) {
    log_error("add to hot keys");
    // try see if duplicated first
    if (!hot_keys_.empty()) {
        read_guard_t guard(&hot_keys_lock_);
        for (size_t i = 0; i < hot_keys_.size(); ++i) {
            if (key_equal(hot_keys_[i], key)) {
                return false;
            }
        }
    }
    //~ mostly may be a new hot key
    {
        write_guard_t guard(&hot_keys_lock_);
        for (size_t i = 0; i < hot_keys_.size(); ++i) {
            if (key_equal(hot_keys_[i], key)) {
                return false;
            }
        }
        hot_keys_.push_back(key);
    }

    return true;
}

void HotKey::release_hot_keys() {
    write_guard_t guard(&hot_keys_lock_);
    for (size_t i = 0; i < hot_keys_.size(); ++i) {
        delete hot_keys_[i];
        hot_keys_[i] = NULL;
    }
    hot_keys_.clear();
}

void HotKey::log_config() const {
    log_warn("sample_count_: %lu, reap_count_: %lu, cache_size_: %lu",
             sample_count_, reap_count_, cache_size_);
    log_warn("sample_max_: %lu, reap_max_: %lu, cache_capacity_: %lu",
             sample_max_, reap_max_, cache_capacity_);
    log_warn("need_feedback: %d, need_dump: %d, hot_factor_: %lf",
             need_feedback_, need_dump_, hot_factor_);
}

bool HotKey::key_equal(const data_entry *lhs, const data_entry *rhs) {
    if (lhs->get_size() != rhs->get_size()) return false;
    if (lhs->get_prefix_size() != rhs->get_prefix_size()) return false;
    if (lhs->get_hashcode() != rhs->get_hashcode()) return false;
    if (lhs->area != 0 && rhs->area != 0 && lhs->area != rhs->area) return false;

    int size = lhs->get_prefix_size() == 0 ? lhs->get_size() : lhs->get_prefix_size();
    return memcmp(lhs->get_data(), rhs->get_data(), size) == 0;
}

}
