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

#include "duplicate_manager.hpp"
#include <easy_io.h>

namespace tair {
bucket_waiting_queue::bucket_waiting_queue(duplicate_sender_manager *psm, uint32_t bucket_number) {
    assert(psm != NULL);
    this->psm = psm;
    atomic_set(&packet_id_creater, 0);
    this->bucket_number = bucket_number;
}

void bucket_waiting_queue::push(request_duplicate_packet &p_packet, uint64_t server_id) {
    tbsys::CThreadGuard guard(&mutex);
    if (p_packet == 0) return;
    p_packet->packet_id = atomic_add_return(1, &packet_id_creater);
    packets_queue[server_id].push(p_packet);
    return;
}

void bucket_waiting_queue::pop(uint64_t server_id) {
    tbsys::CThreadGuard guard(&mutex);
    map<uint64_t, packets_queue_type>::iterator it = packets_queue.find(server_id);
    if (it == packets_queue.end()) {
        return;
    }
    if (it->second.empty()) return;
    last_send_time[server_id] = 0;
    it->second.pop();
}

bool bucket_waiting_queue::pop(uint32_t packet_id, uint64_t server_id) {
    tbsys::CThreadGuard guard(&mutex);
    map<uint64_t, packets_queue_type>::iterator it = packets_queue.find(server_id);
    if (it == packets_queue.end()) {
        return false;
    }
    if (it->second.empty()) return false;
    request_duplicate_packet &p_packet = it->second.front();
    if (p_packet->packet_id != packet_id) return false;
    it->second.pop();
    last_send_time[server_id] = 0;
    return true;
}

size_t bucket_waiting_queue::size() {
    tbsys::CThreadGuard guard(&mutex);
    size_t size = 0;
    map<uint64_t, packets_queue_type>::const_iterator it = packets_queue.begin();
    for (it = packets_queue.begin(); it != packets_queue.end(); ++it) {
        size += it->second.size();
    }

    return size;
}

bool bucket_waiting_queue::send(int64_t now) {
    if (!psm->eio.started) {
        easy_io_start(&psm->eio);
    }
    bool empty = true;
    tbsys::CThreadGuard guard(&mutex);
    map<uint64_t, packets_queue_type>::iterator it = packets_queue.begin();
    for (it = packets_queue.begin(); it != packets_queue.end(); ++it) {
        packets_queue_type queue = it->second;
        if (queue.empty()) continue;
        empty = false;
        int64_t &last_time = last_send_time[it->first];
        if (now - last_time < MISECONDS_BEFOR_SEND_RETRY) {
            continue;
        }
        request_duplicate *packet = new request_duplicate(*queue.front());
        log_debug("will send packet pid = %d", packet->packet_id);
        PROFILER_START("do duplicate");
        PROFILER_BEGIN("send duplicate packet");
        if (easy_helper::easy_async_send(&psm->eio, it->first, packet, NULL, &psm->handler, psm->dup_timeout) ==
            EASY_ERROR) {
            log_debug("send duplicate packet failed: %s", tbsys::CNetUtil::addrToString(it->first).c_str());
            delete packet;
        } else {
            log_debug("duplicate packet sent: %s", tbsys::CNetUtil::addrToString(it->first).c_str());
            last_time = now;
        }
        PROFILER_END();
        PROFILER_DUMP();
        PROFILER_STOP();
    }
    return !empty;
}

void bucket_waiting_queue::do_server_table_change(const set<uint64_t> &available_server_tmp) {
    tbsys::CThreadGuard guard(&mutex);
    map<uint64_t, packets_queue_type>::iterator it_q = packets_queue.begin();
    for (; it_q != packets_queue.end();) {
        if (available_server_tmp.find(it_q->first) == available_server_tmp.end()) {
            log_warn("duplicate queue manager will delete the queue to %s",
                     tbsys::CNetUtil::addrToString(it_q->first).c_str());
            last_send_time.erase(it_q->first);
            packets_queue.erase(it_q++);
        } else if (it_q->second.empty()) {
            last_send_time.erase(it_q->first);
            packets_queue.erase(it_q++);
        } else {
            it_q++;
        }
    }
    return;
}

bucket_waiting_queue::~bucket_waiting_queue() {
}

duplicate_sender_manager::duplicate_sender_manager(table_manager *table_mgr) {
    easy_helper::init_handler(&handler, this);
    handler.process = packet_handler_cb;
    easy_io_create(&eio, 1);
    this->table_mgr = table_mgr;
    have_data_to_send = 0;
    max_queue_size = 0;
    this->start();
}

duplicate_sender_manager::~duplicate_sender_manager() {
    this->stop();
    this->wait();
    map<uint32_t, bucket_waiting_queue>::iterator it = packets_mgr.begin();
    for (; it != packets_mgr.end(); ++it) {
        int size = it->second.size();
        if (size) log_error("data not be duplicated:bucket id =%d  num = %d", it->first, size);
    }
    if (eio.started) {
        easy_io_stop(&eio);
        easy_io_wait(&eio);
        easy_io_destroy(&eio);
    }
}

void duplicate_sender_manager::do_hash_table_changed() {
    set<uint64_t> available_server_tmp(table_mgr->get_available_server_ids());
    map<uint32_t, bucket_waiting_queue>::iterator it;
    while (packages_mgr_mutex.wrlock()) {
        usleep(100);
    }
    for (it = packets_mgr.begin(); it != packets_mgr.end();) {
        it->second.do_server_table_change(available_server_tmp);
        if (it->second.size() == 0) {
            packets_mgr.erase(it++);
        } else {
            it++;
        }
    }
    packages_mgr_mutex.unlock();
    have_data_to_send = 1;
}

bool duplicate_sender_manager::is_bucket_available(uint32_t bucket_number) {
    PROFILER_BEGIN("acquire lock");
    while (packages_mgr_mutex.rdlock()) {
        usleep(10);
    }
    PROFILER_END();

    map<uint32_t, bucket_waiting_queue>::iterator it = packets_mgr.find(bucket_number);
    bool res = (it == packets_mgr.end() || it->second.size() <= max_queue_size);
    packages_mgr_mutex.unlock();
    return res;

}

int duplicate_sender_manager::duplicate_data(int area,
                                             const data_entry *key,
                                             const data_entry *value,
                                             int bucket_number,
                                             const vector<uint64_t> &des_server_ids,
                                             easy_request_t *) {
    if (!is_bucket_available(bucket_number)) {
        log_error("bucket:%d is not avaliable, duplicate busy, ignore it", bucket_number);
        return 0;
    }

    if (des_server_ids.empty()) return 0;
    bucket_waiting_queue::request_duplicate_packet tmp_packet(new bucket_waiting_queue::request_duplicate_ptr());
    tmp_packet->area = area;
    tmp_packet->key = *key;
    tmp_packet->key.server_flag = TAIR_SERVERFLAG_DUPLICATE;
    if (tmp_packet->key.is_alloc() == false) {
        tmp_packet->key.set_data(key->get_data(), key->get_size(), true);
    }
    if (value) {
        tmp_packet->data = *value;
        if (tmp_packet->data.is_alloc() == false) {
            tmp_packet->data.set_data(value->get_data(), value->get_size(), true);
        }
    } else { //remove
        tmp_packet->data.data_meta.flag = TAIR_ITEM_FLAG_DELETED;
    }
    tmp_packet->bucket_number = bucket_number;

    vector<uint64_t>::const_iterator it;

    while (packages_mgr_mutex.rdlock()) {
        usleep(10);
    }

    map<uint32_t, bucket_waiting_queue>::iterator it_map = packets_mgr.find(bucket_number);
    if (it_map == packets_mgr.end()) {
        packages_mgr_mutex.unlock();
        while (packages_mgr_mutex.wrlock()) {
            usleep(10);
        }
    }
    it_map = packets_mgr.find(bucket_number);
    if (it_map == packets_mgr.end()) {
        log_debug("add deplicate queue, bucket: %d", bucket_number);
        bucket_waiting_queue tmpq(this, bucket_number);
        pair<map<uint32_t, bucket_waiting_queue>::iterator, bool> pit;
        pit = packets_mgr.insert(make_pair(bucket_number, tmpq));
        assert(pit.second == true);
        it_map = pit.first;
    }
    for (it = des_server_ids.begin(); it != des_server_ids.end(); it++) {
        it_map->second.push(tmp_packet, *it);
    }
    have_data_to_send = 1;
    packages_mgr_mutex.unlock();
    return 0;
}

int duplicate_sender_manager::direct_send(easy_request_t *,
                                          int area,
                                          const data_entry *key,
                                          const data_entry *value,
                                          int bucket_number,
                                          const vector<uint64_t> &des_server_ids,
                                          uint32_t max_packet_id) {
    return duplicate_data(area, key, value, bucket_number, des_server_ids, NULL);
}

bool duplicate_sender_manager::has_bucket_duplicate_done(int bucketNumber) {
    while (packages_mgr_mutex.rdlock()) {
        usleep(10);
    }
    map<uint32_t, bucket_waiting_queue>::iterator it = packets_mgr.find(bucketNumber);
    bool res = (it == packets_mgr.end() || it->second.size() == 0);
    packages_mgr_mutex.unlock();
    return res;
}

void duplicate_sender_manager::do_duplicate_response(uint32_t bucket_id, uint64_t d_serverId, uint32_t packet_id) {
    while (packages_mgr_mutex.rdlock()) {
        usleep(10);
    }
    map<uint32_t, bucket_waiting_queue>::iterator it = packets_mgr.find(bucket_id);
    if (it == packets_mgr.end()) {
        packages_mgr_mutex.unlock();
        return;
    }
    it->second.pop(packet_id, d_serverId);
    packages_mgr_mutex.unlock();
    return;
}

void duplicate_sender_manager::run(tbsys::CThread *thread, void *arg) {
    UNUSED(thread);
    UNUSED(arg);
    while (!_stop) {
        if (have_data_to_send == 1) {
            have_data_to_send = 0;
            uint64_t now = tbsys::CTimeUtil::getTime();
            while (packages_mgr_mutex.rdlock()) {
                usleep(10);
            }
            map<uint32_t, bucket_waiting_queue>::iterator it = packets_mgr.begin();
            for (; it != packets_mgr.end(); it++) {
                if (it->second.send(now)) have_data_to_send = 1;
            }
            packages_mgr_mutex.unlock();
        }
        usleep(SLEEP_MISECONDS);
    }
}

int duplicate_sender_manager::packet_handler(easy_request_t *r) {
    int rc = r->ipacket != NULL ? EASY_OK : EASY_ERROR;
    base_packet *packet = (base_packet *) r->ipacket;
    if (rc == EASY_ERROR) {
        log_error("duplicate to %s timeout", easy_helper::easy_connection_to_str(r->ms->c).c_str());
        have_data_to_send = 1;
    }
    int pcode = packet->getPCode();
    if (TAIR_RESP_DUPLICATE_PACKET == pcode) {
        response_duplicate *resp = (response_duplicate *) packet;
        do_duplicate_response(resp->bucket_id, resp->server_id, resp->packet_id);
        have_data_to_send = 1;
    } else {
        log_warn("unknow packet! pcode: %d", pcode);
    }
    easy_session_destroy(r->ms);
    return rc;
}
}
