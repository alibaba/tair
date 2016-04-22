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

#include <tbsys.h>
#include "packets/op_cmd_packet.hpp"
#include "util.hpp"
#include "define.hpp"
#include "data_entry.hpp"
#include "migrate_manager.hpp"
#include "easy_helper.hpp"
#include <vector>

namespace tair {
migrate_manager::migrate_manager(base_duplicator *this_duplicator,
                                 tair_manager *tair_mgr, tair::storage::storage_manager *storage_mgr) {
    easy_helper::init_handler(&handler, this);
    handler.process = packet_handler_cb;

    duplicator = this_duplicator;
    this->tair_mgr = tair_mgr;
    this->storage_mgr = storage_mgr;
    is_signaled = false;
    is_running = 0;
    is_stopped = false;
    is_alive = false;
    current_locked_bucket = -1;
    current_migrating_bucket = -1;
    version = 0;
    timeout = 1000;
    vector<const char *> cs_str_list = TBSYS_CONFIG.getStringList(TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
    if (cs_str_list.size() == 0U) {
        log_warn("miss config item %s:%s", TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
        return;
    }
    int port = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
    uint64_t id;
    for (uint32_t i = 0; i < cs_str_list.size(); i++) {
        id = tbsys::CNetUtil::strToAddr(cs_str_list[i], port);
        if (id == 0) continue;
        config_server_list.push_back(id);
        if (config_server_list.size() == 2U) break;
    }
    if (config_server_list.size() == 0U) {
        log_error("config invalid %s:%s", TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
    }
    bzero(&eio, sizeof(easy_io_t));
    easy_io_create(&eio, 1);
    eio.do_signal = 0;
    eio.no_redispatch = 1;
    eio.tcp_nodelay = 1;
    eio.tcp_cork = 0;
    easy_io_start(&eio);

    this->start();
}

migrate_manager::~migrate_manager() {
    log_warn("migrate_manager destruct");
    this->stop();
    int i = 0;
    while (is_running && i++ < 2) {
        sleep(1);
    }
    easy_io_stop(&eio);
    easy_io_wait(&eio);
    easy_io_destroy(&eio);
}

void migrate_manager::set_log(update_log *log) {
    this->log = log;
}

void migrate_manager::run(tbsys::CThread *thread, void *arg) {
    is_alive = true;
    while (true) {
        mutex.lock();
        if (is_signaled) {
            is_signaled = false;
            mutex.unlock();
        } else {
            mutex.unlock();
            condition.wait(0);
        }
        if (_stop) {
            break;
        }
        is_running = 1;
        is_stopped = false;
        uint64_t before, after;
        before = tbsys::CTimeUtil::getTime();
        do_run();
        after = tbsys::CTimeUtil::getTime();
        uint64_t interval = after - before;
        log_debug("this migrate consume time:%"PRI64_PREFIX"u", interval);
        {
            tbsys::CThreadGuard guard(&mutex);
            is_running = 0;
            is_stopped = false;
        }
    }
    is_alive = false;
}

void migrate_manager::signal() {
    {
        tbsys::CThreadGuard guard(&mutex);
        if (!is_alive || _stop)
            return;
        if (is_running == 1)
            is_signaled = true;
        else if (!is_signaled) {
            is_signaled = true;
            condition.signal();
        }
    }
}

void migrate_manager::stop() {
    {
        tbsys::CThreadGuard guard(&mutex);
        if (!is_alive || _stop) {
            _stop = true;
            return;
        }
        is_stopped = true;
    }
    _stop = true;
    condition.signal();
    wait();
}

void migrate_manager::do_run() {
    get_data_mutex.lock();
    bool have_task = migrate_servers.empty();
    if (!have_task) {
        migrate_servers.swap(temp_servers);
    } else {
        get_data_mutex.unlock();
        return;
    }
    get_data_mutex.unlock();
    do_migrate();

    temp_servers.clear();
    migrated_buckets.clear();
}

void migrate_manager::do_migrate() {
    // sorte by score reverse, firce migrate buckets with less copies
    vector<migrate_unit> sorted_units;
    for (bucket_server_map_it it = temp_servers.begin(); it != temp_servers.end(); it++) {
        sorted_units.push_back(it->second);
    }
    sort(sorted_units.begin(), sorted_units.end());

    vector<migrate_unit>::iterator it = sorted_units.begin();
    for (; it != sorted_units.end() && !is_stopped; it++) {
        int bucket_number = it->bucket;
        log_error("begin migrate bucket: %d", bucket_number);
        vector<uint64_t> servers = it->dest_servers;
        if (servers.empty()) {
            log_info("switch master and slave");
            current_locked_bucket = bucket_number;
            usleep(5000);
            while (!(duplicator->has_bucket_duplicate_done(bucket_number))) {
                usleep(5000);
            }
            finish_migrate_bucket(bucket_number);
            current_locked_bucket = -1;
            log_error("finish %d bucket migrate", bucket_number);
            continue;
        }
        for (uint i = 0; i < servers.size(); i++) {
            log_warn("migrate %d to server: %s", bucket_number, tbsys::CNetUtil::addrToString(servers[i]).c_str());
        }
        do_migrate_one_bucket(bucket_number, servers);
        log_error("finish %d bucket migrate", bucket_number);
    }
}

void migrate_manager::do_migrate_one_bucket(int bucket_number, vector<uint64_t> dest_servers) {
    lsn_type last_lsn = log->get_current_lsn();
    current_migrating_bucket = bucket_number;
    usleep(MISECONDS_WAITED_FOR_WRITE);
    bool is_sucess = migrate_data_file(bucket_number, dest_servers);
    if (!is_sucess) {
        log_debug("migrate be stopped or file error!");
        current_migrating_bucket = -1;
        return;
    }
    lsn_type current_lsn = log->get_current_lsn();

    while ((current_lsn - last_lsn >= MIGRATE_LOCK_LOG_LEN)) {
        is_sucess = migrate_log(bucket_number, dest_servers, last_lsn, current_lsn);
        if (!is_sucess) {
            current_migrating_bucket = -1;
            return;
        }
        last_lsn = current_lsn;
        current_lsn = log->get_current_lsn();
    }

    current_locked_bucket = bucket_number;
    usleep(MISECONDS_WAITED_FOR_WRITE);
    current_lsn = log->get_current_lsn();
    is_sucess = migrate_log(bucket_number, dest_servers, last_lsn, current_lsn);
    if (!is_sucess) {
        current_migrating_bucket = -1;
        current_locked_bucket = -1;
        return;
    }

    last_lsn = current_lsn;
    current_lsn = log->get_current_lsn();
    while (current_lsn != last_lsn) {
        is_sucess = migrate_log(bucket_number, dest_servers, last_lsn, current_lsn);
        if (!is_sucess) break;
        last_lsn = current_lsn;
        current_lsn = log->get_current_lsn();
    }

    if (!is_sucess) {
        log_debug("migrate be stopped or error while migrate log!");
    } else if (tair_mgr->get_storage_manager()->should_flush_data()
               && !finish_migrate_data(dest_servers, bucket_number)) {
        log_error("finish migrate data fail");
    } else {
        while (!(duplicator->has_bucket_duplicate_done(bucket_number))) {
            usleep(1000);
            if (is_stopped || _stop) {
                current_migrating_bucket = -1;
                current_locked_bucket = -1;
                return;
            }
        }
        finish_migrate_bucket(bucket_number);
    }
    current_migrating_bucket = -1;
    current_locked_bucket = -1;
}

template<typename P>
bool migrate_manager::send_packet(vector<uint64_t> dest_servers, P *packet, int db_id) {
    bool flag = true;
    bool ret;
    _Reput:
    for (vector<uint64_t>::iterator it = dest_servers.begin(); it < dest_servers.end() && !is_stopped;) {
        ret = true;
        uint64_t server_id = *it;
        P *temp_packet = new P(*packet);

        base_packet *bp = (base_packet *) easy_helper::easy_sync_send(&eio, server_id, temp_packet, NULL, &handler);
        if (bp == NULL || bp->getPCode() != TAIR_RESP_RETURN_PACKET) {
            log_error("send migrate packet failed, server: %s, bucket: %d",
                      tbsys::CNetUtil::addrToString(server_id).c_str(), db_id);
            ret = false;
        } else {
            response_return *rp = (response_return *) bp;
            if (rp->get_code() != TAIR_RETURN_SUCCESS) {
                log_error("migrate not return success, server: %s, ret: %d",
                          tbsys::CNetUtil::addrToString(server_id).c_str(), rp->get_code());
                TAIR_SLEEP(_stop, 2);
                ret = false;
            }
            delete rp;
        }
        if (ret) {
            it = dest_servers.erase(it);
        } else {
            ++it;
        }
    }
    if (is_stopped || _stop) {
        flag = false;
        return flag;
    } else {
        if (!dest_servers.empty()) {
            goto _Reput;
        }
    }
    return flag;
}

bool migrate_manager::migrate_data_file(int db_id, vector<uint64_t> dest_servers) {
    bool flag = true;
    bool tag = false;
    md_info info;
    info.is_migrate = true;
    info.db_id = db_id;
    storage_mgr->begin_scan(info);
    vector<item_data_info *> items;
    bool have_item = storage_mgr->get_next_items(info, items);
    request_mupdate *packet = new request_mupdate();
    packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
    uint64_t total_count = 0;

    while (true) {
        if (!items.empty()) {
            total_count += items.size();
            tag = true;
            for (vector<item_data_info *>::iterator itor = items.begin(); itor != items.end(); itor++) {
                item_data_info *item = *itor;
                data_entry key(item->m_data, item->header.keysize, false);
                key.data_meta = item->header;
                // skip embedded cache when migrating
                key.data_meta.flag = TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
                key.server_flag = TAIR_SERVERFLAG_MIGRATE;
                key.has_merged = true;
                key.set_prefix_size(item->header.prefixsize);
                data_entry value(item->m_data + item->header.keysize, item->header.valsize, false);
                value.data_meta = item->header;
                bool is_succuss = packet->add_put_key_data(key, value);
                log_debug("thiskey is %d", key.has_merged);
                if (!is_succuss) {
                    flag = send_packet(dest_servers, packet, db_id);
                    delete packet;
                    packet = new request_mupdate();
                    packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
                    is_succuss = packet->add_put_key_data(key, value);
                    assert(is_succuss);
                }
            }
            for (vector<item_data_info *>::iterator it = items.begin(); it < items.end(); it++) {
                free(*it);
            }
            items.clear();
        }

        if (flag == false) {
            log_error("send migrate packet fail. bucket: %d", db_id);
            break;
        }
        if (have_item) {
            have_item = storage_mgr->get_next_items(info, items);
        } else {
            break;
        }
    }

    if (flag && tag) {
        flag = send_packet(dest_servers, packet, db_id);
    }
    delete packet;
    packet = NULL;
    storage_mgr->end_scan(info);
    log_warn("migrate bucket db data end. total count: %"PRI64_PREFIX"d, all_done: %d, send suc: %d", total_count,
             !have_item, flag);
    return flag;
}

bool migrate_manager::migrate_log(int db_id, vector<uint64_t> dest_servers, lsn_type start_lsn, lsn_type end_lsn) {
    bool flag = true, ret_flag = true, tag = false;
    log_scan_hander *handle = log->begin_scan(db_id, start_lsn, end_lsn);
    log_info("begin scan, db_id is %d, start_lsn is %"PRI64_PREFIX"u, end_lsn is %"PRI64_PREFIX"u", db_id, start_lsn,
             end_lsn);
    handle = log->get_next(handle);
    const log_record_entry *log_entry = handle->get_log_entry();
    uint count = 0;
    request_mupdate *packet = new request_mupdate();
    packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
    uint offset = 0;
    bool is_succuss = false;
    while (log_entry != NULL) {
        tag = true;
        data_entry key = log_entry->key;
        key.server_flag = TAIR_SERVERFLAG_MIGRATE;
        key.data_meta = log_entry->header;
        // get prefix size here
        key.set_prefix_size(key.data_meta.prefixsize);
        key.has_merged = true;
        log_debug("key data: %s", key.get_data());
        data_entry value;
        if (log_entry->operation_type == (uint8_t) SN_PUT) {
            // skip embedded cache when migrating
            key.data_meta.flag = TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
            value = log_entry->value;
            value.data_meta = log_entry->header;
            is_succuss = packet->add_put_key_data(key, value);
        }
        if (log_entry->operation_type == (uint8_t) SN_REMOVE) {
            key.set_version(log_entry->header.version);
            is_succuss = packet->add_del_key(key);
        }

        if (!is_succuss) {
            ret_flag = send_packet(dest_servers, packet, db_id);
            count += offset;
            delete packet;
            packet = new request_mupdate();
            packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
            if (log_entry->operation_type == (uint8_t) SN_PUT) {
                is_succuss = packet->add_put_key_data(key, value);
            }
            if (log_entry->operation_type == (uint8_t) SN_REMOVE) {
                is_succuss = packet->add_del_key(key);
            }
            assert(is_succuss == true);
            offset = 1;
        } else {
            offset++;
        }

        if (!ret_flag) {
            delete log_entry;
            flag = false;
            break;
        }

        //delete packet;
        delete log_entry;
        log_entry = NULL;
        handle = log->get_next(handle);
        log_entry = handle->get_log_entry();
        if (count >= 100) {
            log->set_hlsn(handle->current_lsn());
            count = 0;
        }
    }

    if (flag && tag) {
        flag = send_packet(dest_servers, packet, db_id);
    }
    delete packet;

    log->set_hlsn(handle->current_lsn());
    log->end_scan(handle);
    return flag;
}

bool migrate_manager::finish_migrate_data(std::vector<uint64_t> &dest_servers, int db_id) {
    // maybe data not flush
    request_op_cmd *packet = new request_op_cmd();
    packet->cmd = TAIR_SERVER_CMD_FLUSH_MMT;
    bool ret = send_packet(dest_servers, packet, db_id);
    delete packet;
    return ret;
}

void migrate_manager::finish_migrate_bucket(int bucket_number) {
    request_migrate_finish packet;
    packet.server_id = local_server_ip::ip;
    packet.bucket_no = bucket_number;
    packet.version = version;
    bool flag = false;

    _ReSend:
    for (size_t i = 0; i < config_server_list.size(); ++i) {
        uint64_t server_id = config_server_list[i];
        request_migrate_finish *req = new request_migrate_finish(packet);

        //~ once success, notify the rest asynchronously
        if (flag) {
            easy_helper::easy_async_send(&eio, server_id, req, NULL, &handler);
            continue;
        }
        base_packet *bp = (base_packet *) easy_helper::easy_sync_send(&eio, server_id, req, NULL, &handler);

        if (bp == NULL || bp->getPCode() != TAIR_RESP_RETURN_PACKET) {
            log_error("send MigrateFinishPacket failed, CS: %s, bucket: %d",
                      tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_number);
        } else {
            response_return *rp = (response_return *) bp;
            if (rp->get_code() == EXIT_FAILURE) {
                log_error("send migrateFinishPacket to config serve, but version is error");
            } else {
                flag = true;
            }
        }
        if (bp != NULL) {
            delete bp;
            bp = NULL;
        }
    }

    if (is_stopped || _stop) {
        return;
    }

    if (!flag) {
        usleep(1000);
        log_error("send MigrateFinishPacket all failed, retry");
        goto _ReSend;
    }

    {
        tbsys::CThreadGuard guard(&get_migrating_buckets_mutex);
        migrated_buckets.push_back(bucket_number);
        tair_mgr->set_migrate_done(bucket_number);
    }
}

void migrate_manager::set_migrate_server_list(bucket_server_map migrate_server_list, uint32_t version) {
    // in this case:
    // 1. server A and B owns bucket 1
    // 2. when server C joins in cluster, cs will rebuild table: server C and B owns bucket 1
    // 3. when server A has migrated bucket 1 to server C, we touch group.conf. In server_table we see server C and A will own bucket 1.
    // 4. if server C receive server_table before server A, C will migrate bucket 1 to server A. when A receive server_table, A will close bucket 1. no.. bucket 1 lost data;
    // sleep heartbeat_time before migrate.
    TAIR_SLEEP(_stop, 1);
    tbsys::CThreadGuard guard(&get_data_mutex);
    migrate_servers.swap(migrate_server_list);
    this->version = version;
    log_error("my migrate version is %d", version);
    is_running = 0;
    signal();
}

void migrate_manager::do_server_list_changed() {
    if (is_running == 1) {
        is_stopped = true;
    }
    while (is_running == 1) {
        usleep(5000);
    }
    is_running = 2;
}

bool migrate_manager::is_bucket_available(int bucket_number) {
    return current_locked_bucket != bucket_number;
}

bool migrate_manager::is_bucket_migrating(int bucket_number) {
    return current_migrating_bucket == bucket_number;
}

bool migrate_manager::is_migrating() {
    return is_running == 1;
}

vector<uint64_t> migrate_manager::get_migrated_buckets() {
    tbsys::CThreadGuard guard(&get_migrating_buckets_mutex);
    return migrated_buckets;
}

int migrate_manager::packet_handler(easy_request_t *r) {
    int rc = r->ipacket != NULL ? EASY_OK : EASY_ERROR;
    easy_session_destroy(r->ms);
    if (rc == EASY_ERROR) { //~ slave cs would not respond
        log_debug("send migrate finish packet to slave cs timeout");
    }
    return rc;
}
}
