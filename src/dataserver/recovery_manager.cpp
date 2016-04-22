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
#include "recovery_manager.hpp"
#include "migrate_manager.hpp"
#include "easy_helper.hpp"
#include <vector>

namespace tair {

recovery_manager::recovery_manager(base_duplicator *this_duplicator, tair_manager *tair_mgr,
                                   tair::storage::storage_manager *storage_mgr) {
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
    server_version = 0;
    transition_version = 0;
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

recovery_manager::~recovery_manager() {
    log_warn("recovery_manager destruct");
    this->stop();
    int i = 0;
    while (is_running && i++ < 2) {
        sleep(1);
    }
    easy_io_stop(&eio);
    easy_io_wait(&eio);
    easy_io_destroy(&eio);
}

void recovery_manager::set_log(update_log *log) {
    this->log = log;
}

void recovery_manager::run(tbsys::CThread *thread, void *arg) {
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
        log_debug("this recovery consume time:%"PRI64_PREFIX"u", interval);
        {
            tbsys::CThreadGuard guard(&mutex);
            is_running = 0;
            is_stopped = false;
        }
    }
    is_alive = false;
}

void recovery_manager::signal() {
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

void recovery_manager::stop() {
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

void recovery_manager::do_run() {
    get_data_mutex.lock();
    bool have_task = recovery_servers.empty();
    if (!have_task) {
        recovery_servers.swap(temp_servers);
    } else {
        get_data_mutex.unlock();
        return;
    }
    get_data_mutex.unlock();
    do_recovery();

    temp_servers.clear();
}

void recovery_manager::do_recovery() {
    bucket_server_map_it it = temp_servers.begin();
    for (; it != temp_servers.end() && !is_stopped; it++) {
        int bucket_number = it->first;
        log_error("begin recovery bucket: %d", bucket_number);
        vector<uint64_t> servers = it->second.dest_servers;
        if (servers.empty()) { // when did this happened ???
            current_locked_bucket = bucket_number;
            usleep(5000);
            while (!(duplicator->has_bucket_duplicate_done(bucket_number))) {
                usleep(5000);
            }
            finish_recovery_bucket(bucket_number);
            current_locked_bucket = -1;
            continue;
        }
        for (uint i = 0; i < servers.size(); i++) {
            log_warn("recovery %d to server: %s", bucket_number, tbsys::CNetUtil::addrToString(servers[i]).c_str());
        }
        do_recovery_one_bucket(bucket_number, servers);
        log_error("finish %d bucket recovery", bucket_number);
    }
}

void recovery_manager::do_recovery_one_bucket(int bucket_number, vector<uint64_t> dest_servers) {
    // as this is volatile, we need no lock here
    current_recovering_bucket = bucket_number;
    usleep(MISECONDS_WAITED_FOR_WRITE);

    // here different from migrate, we have no data file, just log
    // only transfer increment to recovery ds

    // recovery from the very start
    // TODO delete update log, as the update log always remain
    lsn_type last_lsn = 0;
    lsn_type current_lsn = log->get_current_lsn();
    bool is_sucess = true;

    while ((current_lsn - last_lsn >= MIGRATE_LOCK_LOG_LEN)) {
        is_sucess = recovery_log(bucket_number, dest_servers, last_lsn, current_lsn);
        if (!is_sucess) {
            current_recovering_bucket = -1;
            return;
        }
        last_lsn = current_lsn;
        current_lsn = log->get_current_lsn();
    }

    // as the increment is less than MIGRATE_LOCK_LOG_LEN, we block write of this bucket
    // reopen after finished the recovery of this bucket
    current_locked_bucket = bucket_number;
    usleep(MISECONDS_WAITED_FOR_WRITE);
    current_lsn = log->get_current_lsn();
    is_sucess = recovery_log(bucket_number, dest_servers, last_lsn, current_lsn);
    if (!is_sucess) {
        current_recovering_bucket = -1;
        current_locked_bucket = -1;
        return;
    }

    last_lsn = current_lsn;
    current_lsn = log->get_current_lsn();
    while (current_lsn != last_lsn) {
        is_sucess = recovery_log(bucket_number, dest_servers, last_lsn, current_lsn);
        if (!is_sucess) break;
        last_lsn = current_lsn;
        current_lsn = log->get_current_lsn();
    }

    if (!is_sucess) {
        log_debug("recovery be stopped or error while recovery log!");
    } else if (!finish_recovery_data(dest_servers, bucket_number)) {
        log_error("finish recovery data fail");
    } else {
        while (!(duplicator->has_bucket_duplicate_done(bucket_number))) {
            usleep(1000);
            if (is_stopped || _stop) {
                current_recovering_bucket = -1;
                current_locked_bucket = -1;
                return;
            }
        }
        finish_recovery_bucket(bucket_number);
    }
    current_recovering_bucket = -1;
    current_locked_bucket = -1;
}

template<typename P>
bool recovery_manager::send_packet(vector<uint64_t> dest_servers, P *packet, int db_id) {
    bool flag = true;
    bool ret;
    _Reput:
    for (vector<uint64_t>::iterator it = dest_servers.begin(); it < dest_servers.end() && !is_stopped;) {
        ret = true;
        uint64_t server_id = *it;
        P *temp_packet = new P(*packet);

        base_packet *bp = (base_packet *) easy_helper::easy_sync_send(&eio, server_id, temp_packet, NULL, &handler);
        if (bp == NULL || bp->getPCode() != TAIR_RESP_RETURN_PACKET) {
            log_error("send recovery packet failed, server: %s, bucket: %d",
                      tbsys::CNetUtil::addrToString(server_id).c_str(), db_id);
            ret = false;
        } else {
            response_return *rp = (response_return *) bp;
            if (rp->get_code() != TAIR_RETURN_SUCCESS) {
                log_error("recovery not return success, server: %s, ret: %d",
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

bool recovery_manager::recovery_log(int db_id, vector<uint64_t> dest_servers, lsn_type start_lsn, lsn_type end_lsn) {
    log_debug("## recovery bucket(%d) from log(%"PRI64_PREFIX"u, %"PRI64_PREFIX"u)", db_id, start_lsn, end_lsn);
    bool flag = true, ret_flag = true, tag = false;
    log_scan_hander *handle = log->begin_scan(db_id, start_lsn, end_lsn);
    handle = log->get_next(handle);
    const log_record_entry *log_entry = handle->get_log_entry();
    uint count = 0;
    request_mupdate *packet = new request_mupdate();
    packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
    uint offset = 0;
    bool is_succuss = false;
    while (log_entry != NULL) {
        data_entry key = log_entry->key;
        key.server_flag = TAIR_SERVERFLAG_MIGRATE;
        key.data_meta = log_entry->header;
        // get prefix size here
        key.set_prefix_size(key.data_meta.prefixsize);
        key.has_merged = true;
        log_debug("key data: %s", key.get_data());

        if ((int32_t) (tair_mgr->get_bucket_number(key)) == db_id) {
            tag = true;
            data_entry value;
            if (log_entry->operation_type == (uint8_t) SN_PUT) {
                // skip embedded cache when recoverying
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
            count = 0;
        }
    }

    if (flag && tag) {
        flag = send_packet(dest_servers, packet, db_id);
    }
    delete packet;

    log->end_scan(handle);
    return flag;
}

bool recovery_manager::finish_recovery_data(std::vector<uint64_t> &dest_servers, int db_id) {
    // maybe data not flush
    request_op_cmd *packet = new request_op_cmd();
    packet->cmd = TAIR_SERVER_CMD_SYNC_BUCKET;
    char buf[10];
    snprintf(buf, sizeof(buf), "%d", db_id);
    string str_bucket(buf, sizeof(buf));
    packet->params.push_back(str_bucket);

    bool ret = send_packet(dest_servers, packet, db_id);
    delete packet;
    return ret;
}

void recovery_manager::finish_recovery_bucket(int bucket_number) {
    request_recovery_finish packet;
    packet.server_id = local_server_ip::ip;
    packet.bucket_no = bucket_number;
    packet.version = server_version;
    packet.transition_version = transition_version;
    bool flag = false;

    _ReSend:
    for (size_t i = 0; i < config_server_list.size(); ++i) {
        uint64_t server_id = config_server_list[i];
        request_recovery_finish *req = new request_recovery_finish(packet);

        //~ once success, notify the rest asynchronously
        if (flag) {
            easy_helper::easy_async_send(&eio, server_id, req, NULL, &handler);
            continue;
        }
        base_packet *bp = (base_packet *) easy_helper::easy_sync_send(&eio, server_id, req, NULL, &handler);

        if (bp == NULL || bp->getPCode() != TAIR_RESP_RETURN_PACKET) {
            log_error("send recoveryFinishPacket failed, CS: %s, bucket: %d",
                      tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_number);
        } else {
            response_return *rp = (response_return *) bp;
            if (rp->get_code() == EXIT_FAILURE) {
                log_error("send recoveryFinishPacket to config serve, but version is error");
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
        usleep(10000);
        log_error("send RecoveryFinishPacket all failed, retry");
        goto _ReSend;
    }

    tair_mgr->set_recovery_done(bucket_number);
}

void recovery_manager::set_recovery_server_list(bucket_server_map recovery_server_list, uint32_t server_version,
                                                uint32_t transition_version) {
    TAIR_SLEEP(_stop, 1);
    tbsys::CThreadGuard guard(&get_data_mutex);
    recovery_servers.swap(recovery_server_list);
    this->server_version = server_version;
    this->transition_version = transition_version;
    log_error("my recovery server_version is %d, transition version is %d", server_version, transition_version);
    is_running = 0;
    signal();
}

void recovery_manager::do_server_list_changed() {
    // stop current recovery task if exist
    if (is_running == 1) {
        is_stopped = true;
    }
    while (is_running == 1) {
        usleep(5000);
    }
    is_running = 2;
}

bool recovery_manager::is_bucket_available(int bucket_number) {
    return current_locked_bucket != bucket_number;
}

bool recovery_manager::is_recovering() {
    return is_running == 1;
}

int recovery_manager::packet_handler(easy_request_t *r) {
    easy_session_destroy(r->ms);
    return EASY_OK;
}

}
