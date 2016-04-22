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

#include <time.h>
#include <assert.h>
#include <map>

#include <tbsys.h>

#include "timed_collections.hpp"


namespace tair {
namespace common {
namespace timedcollections {

#define NO_NODE      1
#define EXPIRE_ONE   2
#define EXPIRE_NONE  3

namespace {

typedef struct ServerAccessCount {
    uint64_t server_id_;
    uint64_t count_;
    uint64_t last_access_time_;

    ServerAccessCount(uint64_t server_id);

    inline uint64_t incr_count();

    inline uint64_t decr_count();
} ServerAccessCount;

typedef struct LRUNode {
    LRUNode *prev_;
    LRUNode *next_;
    ServerAccessCount *data_;

    LRUNode(LRUNode *prev, LRUNode *next, ServerAccessCount *data);

    ~LRUNode();

    uint64_t incr_count();

    uint64_t decr_count();

    uint64_t get_count();

    uint64_t get_server_id();

} LRUNode;

typedef struct LRU {
    LRUNode *head_;
    LRUNode *tail_;

    LRU(uint64_t timeout);

    ~LRU();

    LRUNode *insert(uint64_t server_id);

    int try_expire(LRUNode *&expired_node);

    uint64_t timeout_;
} LRU;

ServerAccessCount::ServerAccessCount(uint64_t server_id) {
    server_id_ = server_id;
    count_ = 1;
    last_access_time_ = time(NULL);
}

inline uint64_t ServerAccessCount::incr_count() {
    count_++;
    last_access_time_ = time(NULL);
    return count_;
}

inline uint64_t ServerAccessCount::decr_count() {
    count_ = (count_ == 0 ? 0 : count_ - 1);
    return count_;
}

LRUNode::LRUNode(LRUNode *prev, LRUNode *next, ServerAccessCount *data) {
    prev_ = prev;
    next_ = next;
    data_ = data;
}

LRUNode::~LRUNode() {
    if (data_ != NULL) {
        delete data_;
    }
}

uint64_t LRUNode::incr_count() {
    return data_->incr_count();
}

uint64_t LRUNode::decr_count() {
    return data_->decr_count();
}

uint64_t LRUNode::get_count() {
    return data_->count_;
}

uint64_t LRUNode::get_server_id() {
    return data_->server_id_;
}


LRU::LRU(uint64_t timeout) {
    head_ = NULL;
    tail_ = NULL;
    timeout_ = timeout;
}

LRU::~LRU() {
    while (head_ != NULL) {
        LRUNode *tmp = head_;
        head_ = head_->next_;
        delete tmp;
    }
}

LRUNode *LRU::insert(uint64_t server_id) {
    LRUNode *node = NULL;
    if (head_ == NULL) {
        node = new LRUNode(NULL, NULL, new ServerAccessCount(server_id));
        head_ = node;
        tail_ = node;
    } else {
        node = new LRUNode(tail_, NULL, new ServerAccessCount(server_id));
        tail_->next_ = node;
        tail_ = node;
    }
    return node;
}

int LRU::try_expire(LRUNode *&expired_node) {
    expired_node = NULL;
    LRUNode *node = head_;
    if (node == NULL) {
        return NO_NODE;
    }
    uint64_t now = time(NULL);
    if (now - node->data_->last_access_time_ > timeout_) {
        head_ = head_->next_;
        if (head_ != NULL) {
            head_->prev_ = NULL;
        } else {
            tail_ = NULL;
        }
        expired_node = node;
        return EXPIRE_ONE;
    } else {
        return EXPIRE_NONE;
    }
}

class TimedCollections : public ITimedCollections {
public:
    TimedCollections(uint64_t timeout, uint64_t max_wait_count);

    virtual ~TimedCollections();

    virtual uint64_t incr_count(uint64_t server_id);

    virtual uint64_t decr_count(uint64_t server_id);

    virtual uint64_t get_count(uint64_t server_id);

    virtual void try_expire();

    virtual const std::string view();

private:
    std::map<uint64_t, LRUNode *> map_;
    LRU *lru_;
    uint64_t count_;
    uint64_t max_wait_count_;

    tbsys::CRWSimpleLock rwlock_;
};

TimedCollections::TimedCollections(uint64_t timeout, uint64_t max_wait_count) {
    map_.clear();
    lru_ = new LRU(timeout);
    count_ = 0;
    max_wait_count_ = max_wait_count;
}

TimedCollections::~TimedCollections() {
    map_.clear();
    if (lru_ != NULL) {
        delete lru_;
    }
}

uint64_t TimedCollections::incr_count(uint64_t server_id) {
    rwlock_.wrlock();

    uint64_t count = 0;
    std::map<uint64_t, LRUNode *>::iterator iter = map_.find(server_id);
    if (iter != map_.end()) {
        LRUNode *node = iter->second;
        count = node->incr_count();
        if (node != lru_->tail_) {
            if (node != lru_->head_) {
                node->prev_->next_ = node->next_;
            } else {
                lru_->head_ = lru_->head_->next_;
            }
            node->next_->prev_ = node->prev_;
            node->next_ = NULL;
            node->prev_ = lru_->tail_;
            lru_->tail_->next_ = node;
            lru_->tail_ = node;
        }
    } else {
        LRUNode *node = lru_->insert(server_id);
        map_.insert(std::make_pair<uint64_t, LRUNode *>(server_id, node));
        count = 1;
    }

    count_++;
    if (count_ > max_wait_count_) {
        count_ = 0;
        LRUNode *expired_node = NULL;
        int ret = lru_->try_expire(expired_node);
        assert(ret != NO_NODE);
        if (ret == EXPIRE_ONE) {
            assert(expired_node != NULL);
            map_.erase(expired_node->get_server_id());
            delete expired_node;
        }
    }

    rwlock_.unlock();

    return count;
}

uint64_t TimedCollections::decr_count(uint64_t server_id) {
    rwlock_.wrlock();

    uint64_t count = 0;
    std::map<uint64_t, LRUNode *>::iterator iter = map_.find(server_id);
    if (iter != map_.end()) {
        count = iter->second->decr_count();
    }

    rwlock_.unlock();

    return count;
}

uint64_t TimedCollections::get_count(uint64_t server_id) {
    rwlock_.rdlock();

    uint64_t count = 0;
    std::map<uint64_t, LRUNode *>::iterator iter = map_.find(server_id);
    if (iter != map_.end()) {
        count = iter->second->get_count();
    }

    rwlock_.unlock();

    return count;
}

const std::string TimedCollections::view() {
    rwlock_.rdlock();

    std::map<uint64_t, int> m;
    std::map<uint64_t, LRUNode *>::iterator iter;
    for (iter = map_.begin(); iter != map_.end(); iter++) {
        m.insert(std::make_pair<uint64_t, int>(iter->first, iter->second->get_count()));
    }

    rwlock_.unlock();

    std::stringstream ss;
    ss << "WaitCallbackQueue => { ";
    std::map<uint64_t, int>::iterator it;
    it = m.begin();
    if (it != m.end()) {
        ss << "\"" << it->first << "\": \"" << it->second << "\"";
        it++;
        for (; it != m.end(); it++) {
            ss << " ,\"" << it->first << "\": \"" << it->second << "\"";
        }
    }
    ss << " }";
    return ss.str();
}

void TimedCollections::try_expire() {
    rwlock_.wrlock();

    int ret;
    LRUNode *expired_node = NULL;
    while ((ret = lru_->try_expire(expired_node)) == EXPIRE_ONE) {
        assert(expired_node != NULL);
        map_.erase(expired_node->get_server_id());
        delete expired_node;
    }

    rwlock_.unlock();
}
}

ITimedCollections *NewTimedCollections(uint64_t timeout, uint64_t max_wait_count) {
    return new TimedCollections(timeout, max_wait_count);
}

#undef NO_NODE
#undef EXPIRE_ONE
#undef EXPIRE_NONE

}
}
}
