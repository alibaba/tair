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

#include <string>
#include <map>
#include <vector>
#include <ext/hash_map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>

#include <tbsys.h>
#include <easy_io.h>

#include "define.hpp"
#include "stat_define.hpp"
#include "tair_client_api_impl.hpp"
#include "easy_helper.hpp"
#include "tair_client_api.hpp"
#include "wait_object.hpp"
#include "response_return_packet.hpp"
#include "response_mreturn_packet.hpp"
#include "put_packet.hpp"
#include "remove_area_packet.hpp"
#include "remove_packet.hpp"
#include "uniq_remove_packet.hpp"
#include "inc_dec_packet.hpp"
#include "dump_packet.hpp"
#include "get_packet.hpp"
#include "get_range_packet.hpp"
#include "get_group_packet.hpp"
#include "query_info_packet.hpp"
#include "get_migrate_machine.hpp"
#include "data_server_ctrl_packet.hpp"
#include "scoped_wrlock.hpp"
#include "base_packet.hpp"
#include "flow_control_packet.hpp"
#include "flow_view.hpp"
#include "flowrate.h"
#include "sync_packet.hpp"
#include "op_cmd_packet.hpp"
#include "expire_packet.hpp"
#include "retry_all_packet.hpp"
#include "inval_stat_packet.hpp"
#include "hide_by_proxy_packet.hpp"
#include "prefix_gets_packet.hpp"
#include "prefix_puts_packet.hpp"
#include "prefix_removes_packet.hpp"
#include "prefix_hides_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "prefix_invalids_packet.hpp"
#include "mupdate_packet.hpp"
#include "inval_stat.hpp"
#include "key_value_pack.hpp"
#include "lock_packet.hpp"
#include "group_names_packet.hpp"
#include "invalid_packet.hpp"
#include "hide_packet.hpp"
#include "get_hidden_packet.hpp"
#include "mc_ops_packet.hpp"
#include "statistics_packet.hpp"
#include "rt_packet.hpp"
#include "timed_collections.hpp"

namespace tair {


using namespace std;
using namespace __gnu_cxx;
using namespace tair::common;


/*-----------------------------------------------------------------------------
   *  tair_client_impl
   *-----------------------------------------------------------------------------*/

int tair_cleanup_cb_client_wrapper(easy_request_t *r, void *packet);

void init_client_handler(easy_io_handler_pt *handler, void *user_data);

tair_client_impl::tair_client_impl() {
    inited = false;
    is_stop = false;
    direct = false;
    force_service = false;
    rand_read_flag = false;
    light_mode_ = false;

    rsync_mode_ = false;
    timed_collections_ = NULL;

    data_server = 0;
    timeout = 2000;
    config_version = 0;
    new_config_version = 0;
    send_fail_count = 0;
    bucket_count = 0;
    copy_count = 0;
    thread_count = 1;
    atomic_set(&read_seq, 0);

    // it is always successfull, no block
    deschema_ = new DeStatSchema();
    pthread_rwlock_init(&m_init_mutex, NULL);
    this_wait_object_manager = NULL;
}

tair_client_impl::~tair_client_impl() {
    close();
    if (deschema_ != NULL) {
        delete deschema_;
        deschema_ = NULL;
    }
    pthread_rwlock_destroy(&m_init_mutex);
    delete timed_collections_;
    timed_collections_ = NULL;
}

bool tair_client_impl::startup(const char *master_addr, const char *slave_addr, const char *group_name) {
    if (master_addr == NULL || group_name == NULL) {
        return false;
    }

    if (inited) {
        return true;
    }
    is_stop = false;

    if (!initialize()) {
        return false;
    }

    uint64_t master_cfgsvr = tbsys::CNetUtil::strToAddr(master_addr, TAIR_CONFIG_SERVER_DEFAULT_PORT);
    uint64_t slave_cfgsvr =
            slave_addr == NULL ? 0 : tbsys::CNetUtil::strToAddr(slave_addr, TAIR_CONFIG_SERVER_DEFAULT_PORT);
    return startup(master_cfgsvr, slave_cfgsvr, group_name);
}

bool tair_client_impl::directup(const char *server_addr) {
    if (server_addr == NULL) {
        return false;
    }

    if (inited) {
        return true;
    }
    is_stop = false;

    if (!initialize()) {
        return false;
    }

    int default_port = 5191;
    uint64_t data_server = tbsys::CNetUtil::strToAddr(server_addr, default_port);

    return directup(data_server);
}

bool tair_client_impl::startup(uint64_t master_cfgsvr, uint64_t slave_cfgsvr, const char *group_name) {
    if (master_cfgsvr == 0 || group_name == NULL) {
        return false;
    }

    log_debug("master_cfg:%s,group:%s \n", tbsys::CNetUtil::addrToString(master_cfgsvr).c_str(), group_name);

    if (master_cfgsvr) {
        config_server_list.push_back(master_cfgsvr);
    }
    if (slave_cfgsvr) {
        config_server_list.push_back(slave_cfgsvr);
    }

    this->group_name = group_name;

    if (inited) return true;
    CScopedRwLock __scoped_lock(&m_init_mutex, true);
    if (inited) return true;

    start_net();

    inited = true;
    if (!retrieve_server_addr()) {
        log_error("retrieve_server_addr falied.\n");
        close();
        return false;
    }
    log_info("retrieve_server_addr success.\n");
    return true;
}

bool tair_client_impl::directup(uint64_t data_server) {
    if (data_server == 0) {
        return false;
    }

    if (rsync_mode_) {
        uint64_t ds = data_server;
        data_server = get_rsync_server(ds);
    }

    my_server_list.push_back(data_server);

    if (inited) return true;  //~ pre-check
    CScopedRwLock __scoped_lock(&m_init_mutex, true);
    if (inited) return true;

    bucket_count = 1;
    copy_count = 1;

    start_net();

    inited = true;
    this->data_server = data_server;
    this->direct = true;

    return easy_helper::is_alive(data_server);
}

uint64_t tair_client_impl::get_rsync_server(uint64_t data_server) {
    int port = TAIR_SERVER_DEFAULT_PORT;
    std::string addr_str = tbsys::CNetUtil::addrToString(data_server);
    const char *ip = addr_str.c_str();

    uint32_t nip = 0;
    const char *p = strchr(ip, ':');
    if (p != NULL && p > ip) {
        int len = p - ip;
        if (len > 64) len = 64;
        char tmp[128];
        strncpy(tmp, ip, len);
        tmp[len] = '\0';
        nip = tbsys::CNetUtil::getAddr(tmp);
        port = atoi(p + 1);
    } else {
        nip = tbsys::CNetUtil::getAddr(ip);
    }
    if (nip == 0) {
        return 0;
    }

    port = port + 1;

    uint64_t ipport = port;
    ipport <<= 32;
    ipport |= nip;
    return ipport;
}

bool tair_client_impl::initialize() {

    memset(&eio, 0, sizeof(eio));
    if (easy_io_create(&eio, thread_count) == NULL) {
        log_error("create eio failed, thread_count: %u", thread_count);
        return false;
    }
    eio.do_signal = 0;
    eio.no_redispatch = 0;
    eio.tcp_nodelay = 1;
    eio.tcp_cork = 0;
    eio.affinity_enable = 1;
    easy_eio_set_uthread_start(&eio, easy_helper::easy_set_thread_name < tair_client_impl > ,
                               const_cast<char *>("client_io"));
    this_wait_object_manager = new wait_object_manager(invoke_callback, this);
    init_client_handler(&handler, this);
    handler.process = packet_handler_cb;
    return true;
}

// startup, server
bool tair_client_impl::startup(uint64_t dataserver) {
    my_server_list.push_back(dataserver);
    bucket_count = 1;
    copy_count = 1;
    if (!initialize()) {
        return false;
    }
    start_net();
    inited = true;
    return easy_helper::is_alive(dataserver);
}

void tair_client_impl::close() {
    if (is_stop || !inited)
        return;
    is_stop = true;
    stop_net();
    wait_net();
    if (!light_mode_) {
        thread.join();
        response_thread.join();
    }
    easy_io_destroy(&eio);
    if (this_wait_object_manager != NULL) {
        delete this_wait_object_manager;
        this_wait_object_manager = NULL;
    }
    reset();
}

void tair_client_impl::set_rsync_mode(bool rsync) {
    rsync_mode_ = rsync;
    if (rsync_mode_) {
        if (timed_collections_ != NULL) {
            delete timed_collections_;
        }
        timed_collections_ = tair::common::timedcollections::NewTimedCollections();
    }
}

const std::string tair_client_impl::stat() {
    if (timed_collections_ != NULL) {
        return timed_collections_->view();
    }
    return "NOT RSYNC MODE";
}

int tair_client_impl::slow_wait_if_rsync_mode(uint64_t dataserver, int pcode) {
    if (rsync_mode_) {
        if (pcode == TAIR_REQ_REMOVE_PACKET ||
            pcode == TAIR_REQ_PUT_PACKET ||
            pcode == TAIR_REQ_SYNC_PACKET) {
            uint64_t send_count = timed_collections_->get_count(dataserver);
            // large than 3/4 need write log
            if (send_count >= ((3 * queue_limit) >> 2)) {
                // need slow down the request send
                if (log_last_time_ != time(NULL)) {
                    log_last_time_ = time(NULL);
                    const char *host = tbsys::CNetUtil::addrToString(dataserver).c_str();
                    log_warn("%s send count %"
                    PRI64_PREFIX
                    "u >= %"
                    PRI64_PREFIX
                    "u",
                            host, send_count, queue_limit);
                }
                return TAIR_RETURN_REMOTE_SLOW;
            }
        }
    }

    return TAIR_RETURN_SUCCESS;
}

void tair_client_impl::update_send_count_if_rsync_mode(uint64_t dataserver, int pcode) {
    if (rsync_mode_) {
        if (pcode == TAIR_REQ_REMOVE_PACKET ||
            pcode == TAIR_REQ_SYNC_PACKET ||
            pcode == TAIR_REQ_PUT_PACKET) {
            timed_collections_->incr_count(dataserver);
        }
    }
}

int tair_client_impl::put(int area,
                          const data_entry &key,
                          const data_entry &data,
                          int expired, int version, bool fill_cache,
                          TAIRCALLBACKFUNC pfunc, void *parg) {
    if (!(key_entry_check(key)) || (!data_entry_check(data))) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (key.get_size() + data.get_size() > (TAIR_MAX_DATA_SIZE + TAIR_MAX_KEY_SIZE)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT || version < 0) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_debug("can not find serverId, return false");
        return -1;
    }

    log_debug("put to server:%s", tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    wait_object *cwo = this_wait_object_manager->create_wait_object(TAIR_REQ_PUT_PACKET,
                                                                    pfunc, parg, expired, server_list[0]);

    request_put *packet = new request_put();
    packet->area = area;
    packet->key = key;
    // set fill cache flag
    packet->key.data_meta.flag |= fill_cache ? TAIR_CLIENT_PUT_PUT_CACHE_FLAG : TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
    packet->data = data;
    packet->expired = expired;
    packet->version = version;
    packet->server_flag = key.server_flag;
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_return *resp = 0;

    if ((ret = send_request(server_list[0], packet, cwo->get_id())) < 0) {
        delete packet;
        goto FAIL;
    }

    //if it's a async call,just return ok and wait callback.
    if (pfunc != NULL) return TAIR_RETURN_SUCCESS;

    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        goto FAIL;
    }

    if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
        goto FAIL;
    }

    resp = (response_return *) tpacket;
    new_config_version = resp->config_version;
    ret = resp->get_code();
    if (ret != TAIR_RETURN_SUCCESS) {
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
            //update server table immediately
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }

        goto FAIL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);

    return 0;

    FAIL:

    this_wait_object_manager->destroy_wait_object(cwo);
    log_info("put failure: %s ", get_error_msg(ret));

    return ret;
}

int
tair_client_impl::mput(int area, const tair_client_kv_map &kvs, int &fail_request/*tair_dataentry_vector& fail_keys*/,
                       bool compress) {
    fail_request = 0;
    request_put_map request_puts;
    int ret = TAIR_RETURN_SUCCESS;
    tair_client_kv_map::const_iterator kv_iter = kvs.begin();
    const tair_client_kv_map::const_iterator kv_end_iter = kvs.end();
    // return once fail
    while (ret == TAIR_RETURN_SUCCESS && kv_iter != kv_end_iter) {
        if ((ret = init_put_map(area, kv_iter, kv_end_iter, request_puts)) < 0) {
            return ret;
        }

        //typedef map<uint64_t, map<uint32_t, request_mput*> > request_put_map;
        wait_object *cwo = this_wait_object_manager->create_wait_object();
        request_put_map::iterator rq_iter = request_puts.begin();
        int send_packet_size = 0;
        while (rq_iter != request_puts.end()) {
            map<uint32_t, request_mput *>::iterator mit = rq_iter->second.begin();
            while (mit != rq_iter->second.end()) {
                // compress here
                if (compress) {
                    mit->second->compress();
                }
                //server_id, request, wait_id
                if (send_request(rq_iter->first, mit->second, cwo->get_id()) < 0) {
                    log_error("send request fail: %s", tbsys::CNetUtil::addrToString(rq_iter->first).c_str());
                    delete mit->second;
                    rq_iter->second.erase(mit++);
                    ++fail_request;
                } else {
                    ++send_packet_size;
                    ++mit;
                }
            }
            //erase
            if (rq_iter->second.size() == 0) {
            }

            rq_iter++;
        }

        vector<response_return *> resps;
        ret = TAIR_RETURN_SEND_FAILED;

        vector<base_packet *> tpk;
        if ((ret = get_response(cwo, send_packet_size, tpk)) < 1) {
            this_wait_object_manager->destroy_wait_object(cwo);
            log_error("all requests are failed");
            return ret == 0 ? TAIR_RETURN_FAILED : ret;
        }

        vector<base_packet *>::iterator bp_iter = tpk.begin();
        for (; bp_iter != tpk.end(); ++bp_iter) {
            if ((*bp_iter)->getPCode() == TAIR_RESP_RETURN_PACKET) {
                response_return *tpacket = dynamic_cast<response_return *>(*bp_iter);
                if (tpacket != NULL) {
                    ret = tpacket->get_code();
                    if (0 != ret) {
                        if (TAIR_RETURN_SERVER_CAN_NOT_WORK == ret) {
                            new_config_version = tpacket->config_version;
                            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
                        }
                        log_error("get response fail: ret: %d", ret);
                        ++fail_request;
                    }
                }
            } else {
                log_error("not get response packet: %d", (*bp_iter)->getPCode());
                ++fail_request;
            }
        }

        ret = TAIR_RETURN_SUCCESS;
        if (fail_request > 0) {
            ret = TAIR_RETURN_PARTIAL_SUCCESS;
        }
        this_wait_object_manager->destroy_wait_object(cwo);
    }

    return ret;
}

int tair_client_impl::direct_update(std::vector<uint64_t> &servers, tair_operc_vector *opercs) {
    if (opercs == NULL || servers.empty()) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    for (size_t i = 0; i < servers.size(); ++i) {
        uint64_t server_id = servers[i];
        request_mupdate *packet = new request_mupdate();
        packet->server_flag = TAIR_SERVERFLAG_MIGRATE;
        packet->key_and_values = opercs;
        packet->alloc = false;
        packet->count = opercs->size();

        if ((ret = send_request(server_id, packet, cwo->get_id())) != TAIR_RETURN_SUCCESS) {
            delete packet;
            log_error("send direct update fail, %s", tbsys::CNetUtil::addrToString(server_id).c_str());
            break;
        }
    }

    if (ret != TAIR_RETURN_SUCCESS) {
        this_wait_object_manager->destroy_wait_object(cwo);
        return ret;
    }

    vector<base_packet *> tpk;
    if (get_response(cwo, servers.size(), tpk) < static_cast<int32_t>(servers.size())) {
        this_wait_object_manager->destroy_wait_object(cwo);
        log_error("all requests are failed");
        return TAIR_RETURN_TIMEOUT;
    }

    for (size_t i = 0; i < tpk.size(); ++i) {
        if (tpk[i]->getPCode() != TAIR_RESP_RETURN_PACKET ||
            (ret = ((response_return *) tpk[i])->get_code()) != TAIR_RETURN_SUCCESS) {
            break;
        }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

int tair_client_impl::set_count(int area, const data_entry &key, int count,
                                int expire, int version) {
    // This is an ugly trick, 'cause Tair's key/value has two bits flag to indicate
    // type in Java client implementation but count type's flag is added in server,
    // we don't want to add a new packet for this set_count() function, so we add two
    // bits type flag here and use put() to make following incr()/decr() happy.
    // We ignore compress type here as same as what CPP client always does.
#ifdef WITH_COMPRESS
                                                                                                                            char buf[INCR_DATA_SIZE - 2];
    buf[0] = (count & 0xFF);
    buf[1] = ((count >> 8) & 0xFF);
    buf[2] = ((count >> 16) & 0xFF);
    buf[3] = ((count >> 24) & 0xFF);
    data_entry value;
    value.set_data(buf, INCR_DATA_SIZE - 2, false);
#else
    char buf[INCR_DATA_SIZE];
    SET_INCR_DATA_COUNT(buf, count);
    data_entry value;
    value.set_data(buf, INCR_DATA_SIZE, false);
#endif
    // force set count type flag
    value.data_meta.flag = TAIR_ITEM_FLAG_ADDCOUNT;
    return put(area, key, value, expire, version, true);
}

int tair_client_impl::lock(int area, const data_entry &key, LockType type) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_debug("can not find serverId, return false");
        return -1;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object(TAIR_REQ_LOCK_PACKET);
    request_lock *packet = new request_lock();
    packet->area = area;
    packet->key.data_meta = key.data_meta;
    // not copy
    packet->key.set_data(key.get_data(), key.get_size(), false);
    packet->lock_type = type;
    base_packet *tpacket = 0;
    response_return *resp = 0;

    int ret = TAIR_RETURN_SEND_FAILED;
    if ((ret = send_request(server_list[0], packet, cwo->get_id())) < 0) {

        delete packet;
        goto FAIL;
    }

    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        goto FAIL;
    }
    if (tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
        goto FAIL;
    }

    resp = (response_return *) tpacket;
    new_config_version = resp->config_version;
    if ((ret = resp->get_code()) < 0) {
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        goto FAIL;
    }


    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
    FAIL:

    this_wait_object_manager->destroy_wait_object(cwo);

    log_info("lock(%d) failure: %s:%s",
              type, tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
              get_error_msg(ret));
    return ret;
}

int tair_client_impl::expire(int area, const data_entry &key, int expired) {
    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (key.get_size() > TAIR_MAX_KEY_SIZE) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT || expired < 0) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_debug("can not find serverId, return false");
        return -1;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_expire *packet = new request_expire();
    packet->area = area;
    packet->key = key;
    packet->expired = expired;
    base_packet *tpacket = 0;
    response_return *resp = 0;

    int ret = TAIR_RETURN_SEND_FAILED;
    if ((ret = send_request(server_list[0], packet, cwo->get_id())) < 0) {
        delete packet;
        goto FAIL;
    }

    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        log_error("go failure from here");
        goto FAIL;
    }

    if (tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
        goto FAIL;
    }

    resp = (response_return *) tpacket;
    new_config_version = resp->config_version;
    ret = resp->get_code();
    if (ret != TAIR_RETURN_SUCCESS) {
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
            //update server table immediately
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        goto FAIL;
    }
    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
    FAIL:
    this_wait_object_manager->destroy_wait_object(cwo);
    log_info("expire failure: %s:%s",
              tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
              get_error_msg(ret));
    return ret;
}

int tair_client_impl::uniq_remove(int area, int bucket, const data_entry &key) {
    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (key.get_size() > TAIR_MAX_KEY_SIZE) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(bucket, server_list)) {
        log_debug("can not find serverId, return false");
        return -1;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_uniq_remove *packet = new request_uniq_remove();
    packet->area = area;
    packet->bucket = bucket;
    packet->key = new data_entry(key);
    base_packet *tpacket = 0;
    response_return *resp = 0;

    int ret = TAIR_RETURN_SEND_FAILED;
    if ((ret = send_request(server_list[0], packet, cwo->get_id())) < 0) {
        delete packet;
        goto FAIL;
    }

    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        log_error("go failure from here");
        goto FAIL;
    }

    if (tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
        goto FAIL;
    }

    resp = (response_return *) tpacket;
    ret = resp->get_code();
    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
    FAIL:
    this_wait_object_manager->destroy_wait_object(cwo);
    log_info("uniq_remove failure: %s:%s",
              tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
              get_error_msg(ret));
    return ret;
}

int tair_client_impl::sync(int32_t bucket, std::vector<Record *> *records, bool fill_cache,
                           TAIRCALLBACKFUNC callback, void *data) {
    vector<uint64_t> server_list;
    // we assume destination cluster have same buckets
    // so please make sure two cluster have same buckets
    // if not sure, please let the vector of record just has one record
    if (!get_server_id(bucket, server_list)) {
        log_debug("can not find serverId, return false");
        return -1;
    }

    log_debug("sync to server:%s", tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    uint64_t ds = server_list[0];

    if (callback != NULL) {
        if (slow_wait_if_rsync_mode(ds, TAIR_REQ_SYNC_PACKET) == TAIR_RETURN_REMOTE_SLOW) {
            return TAIR_RETURN_REMOTE_SLOW;
        }
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object(TAIR_REQ_SYNC_PACKET,
                                                                    callback, data, 0, ds);

    // we use a trick here, don't copy and release "records" in packet,
    // if you use async way, you should put the records pointer into argument "data",
    // when callback is implemented, then you can release "records", or you make sure
    // it never release before callback is implemented, you can store records pointer anywhere,
    // like what new_remote_sync_manager do
    request_sync *packet = new request_sync();
    packet->bucket = bucket;
    // set fill cache flag
    std::vector<common::Record *> &rh = *records;
    for (size_t i = 0; i < rh.size(); i++) {
        rh[i]->key_->data_meta.flag |= fill_cache ? TAIR_CLIENT_PUT_PUT_CACHE_FLAG : TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
    }

    packet->extflag = 0; // now just support generic format
    packet->bucket = bucket;
    packet->records = records;

    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = NULL;
    response_return *resp = NULL;

    if (callback != NULL) {
        update_send_count_if_rsync_mode(ds, TAIR_REQ_SYNC_PACKET);
    }

    if ((ret = send_request(ds, packet, cwo->get_id())) < 0) {
        if (rsync_mode_) {
            timed_collections_->decr_count(ds);
        }
        delete packet;
        goto FAIL;
    }

    if (callback != NULL) {
        return TAIR_RETURN_SUCCESS;
    }

    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        goto FAIL;
    }

    if (tpacket == NULL || tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
        goto FAIL;
    }

    resp = static_cast<response_return *>(tpacket);
    new_config_version = resp->config_version;
    ret = resp->get_code();
    if (ret != TAIR_RETURN_SUCCESS) {
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
            //update server table immediately
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }

        goto FAIL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);

    return TAIR_RETURN_SUCCESS;

    FAIL:

    this_wait_object_manager->destroy_wait_object(cwo);
    log_info("sync failure: %s ", get_error_msg(ret));

    return ret;
}

int tair_client_impl::get(int area, const data_entry &key, data_entry *&data) {
    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_debug("can not find serverId, return false");
        return -1;
    }

    log_debug("get from server:%s", tbsys::CNetUtil::addrToString(server_list[0]).c_str());
    wait_object *cwo = 0;

    base_packet *tpacket = 0;
    response_get *resp = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    int send_success = 0;

    int s_size = server_list.size();
    int index = 0;
    if (rand_read_flag && s_size > 1) {
        index = atomic_add_return(1, &read_seq) % s_size;
    }
    int loop_count = 0;
    while (loop_count < s_size) {
        request_get *packet = new request_get();
        packet->area = area;

        packet->add_key(const_cast<data_entry *>(&key), true);
        cwo = this_wait_object_manager->create_wait_object();

        if (send_request(server_list[index], packet, cwo->get_id()) < 0) {
            this_wait_object_manager->destroy_wait_object(cwo);

            //need delete packet
            delete packet;
        } else {
            if ((ret = get_response(cwo, 1, tpacket)) < 0) {
                this_wait_object_manager->destroy_wait_object(cwo);
            } else {
                ++send_success;
                break;
            }
        }
        ++loop_count;
        ++index;
        if (rand_read_flag && s_size > 1) {
            index %= s_size;
        }
    }

    if (send_success < 1) {
        log_error("all requests are failed");
        return ret;
    }
    if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_PACKET) {
        goto FAIL;
    }
    resp = (response_get *) tpacket;
    ret = resp->get_code();
    new_config_version = resp->config_version;
    if (ret != TAIR_RETURN_SUCCESS) {
        goto FAIL;
    }
    if (resp->data) {
        data = resp->data;
        resp->data = 0;
        ret = TAIR_RETURN_SUCCESS;
    } else {
        ret = TAIR_RETURN_PROXYED_ERROR;
        log_error("proxyed get error: %d.", ret);
        goto FAIL;
    }
    log_debug("end get:ret:%d", ret);

    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
    FAIL:
    if (tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
        response_return *r = (response_return *) tpacket;
        new_config_version = resp->config_version;
        ret = r->get_code();
    }

    this_wait_object_manager->destroy_wait_object(cwo);


    log_info("get failure: %s:%s",
              tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
              get_error_msg(ret));

    return ret;
}

int tair_client_impl::get(int area, const data_entry &key, callback_get_pt pf, void *pargs) {
    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_debug("can not find serverId, return false");
        return -1;
    }
    for (size_t i = 0; i < server_list.size(); ++i) {
        request_get *packet = new request_get();
        packet->area = area;
        packet->add_key(const_cast<data_entry *> (&key), true);
        wait_object *cwo = this_wait_object_manager->create_wait_object(TAIR_REQ_GET_PACKET, pf, pargs);
        if (send_request(server_list[i], packet, cwo->get_id()) < 0) {
            this_wait_object_manager->destroy_wait_object(cwo);
            delete packet;
        } else {
            return TAIR_RETURN_SUCCESS;
        }
    }

    return TAIR_RETURN_SEND_FAILED;
}

int tair_client_impl::init_put_map(int area, tair_client_kv_map::const_iterator &kv_iter,
                                   const tair_client_kv_map::const_iterator &kv_end_iter,
                                   request_put_map &request_puts) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;
    request_puts.clear();
    request_put_map::iterator rq_iter;

    //typedef std::map<data_entry*, value_entry*, data_entry_hash> tair_client_kv_map;
    for (; kv_iter != kv_end_iter; ++kv_iter) {
        if (!key_entry_check(*(kv_iter->first)) || !data_entry_check(kv_iter->second->get_d_entry())) {
            ret = TAIR_RETURN_ITEMSIZE_ERROR;
            break;
        }

        //if (kv_iter->second->get_version() < 0 || kv_iter->second->get_expire() < 0)
        if (kv_iter->second->get_expire() < 0) {
            ret = TAIR_RETURN_INVALID_ARGUMENT;
            break;
        }

        if (kv_iter->first->get_size() + kv_iter->second->get_d_entry().get_size() >
            (TAIR_MAX_DATA_SIZE + TAIR_MAX_KEY_SIZE)) {
            ret = TAIR_RETURN_ITEMSIZE_ERROR;
            break;
        }

        vector<uint64_t> server_list;
        uint32_t bucket_number = 0;
        if (!get_send_para(*(kv_iter->first), server_list, bucket_number)) {
            log_error("can not find serverId, return false");
            ret = -1;
            break;
        }

        //typedef map<uint64_t, map<uint32_t, request_mput*> > request_put_map;
        request_mput *packet = NULL;
        rq_iter = request_puts.find(server_list[0]);
        if (rq_iter != request_puts.end()) {
            map<uint32_t, request_mput *>::iterator mit = rq_iter->second.find(bucket_number);
            if (mit != rq_iter->second.end()) {
                packet = mit->second;
            } else {
                packet = new request_mput();
                rq_iter->second[bucket_number] = packet;
            }
        } else {
            map<uint32_t, request_mput *> bp_map;
            packet = new request_mput();
            bp_map[bucket_number] = packet;
            request_puts[server_list[0]] = bp_map;
        }

        if (packet->area != 0) {
            if (packet->area != area) {
                log_error("mput packet is conflict, packet area: %d, input area: %d", packet->area, area);
                ret = -1;
            }
        } else {
            packet->area = area;
        }

        // enough for this round
        if (!packet->add_put_key_data(*(kv_iter->first), *(kv_iter->second))) {
            break;
        }
        log_debug("get from server:%s, bucket_number: %u",
                  tbsys::CNetUtil::addrToString(server_list[0]).c_str(), bucket_number);
    }

    if (ret < 0) {
        for (rq_iter = request_puts.begin(); rq_iter != request_puts.end(); ++rq_iter) {
            map<uint32_t, request_mput *>::iterator mit = rq_iter->second.begin();
            for (; mit != rq_iter->second.end(); ++mit) {
                request_mput *req = mit->second;
                if (NULL != req) {
                    delete req;
                    req = NULL;
                }
            }
        }
        request_puts.clear();
    }

    return ret;
}

bool tair_client_impl::get_send_para(const data_entry &key, vector<uint64_t> &server, uint32_t &bucket_number) {
    uint32_t hash = util::string_util::mur_mur_hash(key.get_data(), key.get_size());
    server.clear();

    if (my_server_list.size() > 0U) {
        hash %= bucket_count;
        bucket_number = hash;
        for (uint32_t i = 0; i < copy_count && i < my_server_list.size(); ++i) {
            uint64_t server_id = my_server_list[hash + i * bucket_count];
            if (server_id != 0) {
                server.push_back(server_id);
            }
        }
    }
    return server.size() > 0 ? true : false;
}

int tair_client_impl::init_request_map(int area, const vector<data_entry *> &keys, request_get_map &request_gets,
                                       int server_select) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT || server_select < 0) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;

    request_get_map::iterator rq_iter;

    vector<data_entry *>::const_iterator key_iter;
    for (key_iter = keys.begin(); key_iter != keys.end(); ++key_iter) {
        if (!key_entry_check(**key_iter)) {
            ret = TAIR_RETURN_ITEMSIZE_ERROR;
            break;
        }

        vector<uint64_t> server_list;
        if (!get_server_id(**key_iter, server_list)) {
            log_debug("can not find serverId, return false");
            ret = -1;
            break;
        }
        uint32_t ser_idx = server_select;
        if (server_list.size() <= ser_idx) {
            log_debug("select:%d not in server_list,size %lu", server_select, server_list.size());
            ser_idx = 0;
        }
        request_get *packet = NULL;
        rq_iter = request_gets.find(server_list[ser_idx]);
        if (rq_iter != request_gets.end()) {
            packet = rq_iter->second;
        } else {
            packet = new request_get();
            request_gets[server_list[ser_idx]] = packet;
        }

        packet->area = area;
        packet->add_key((*key_iter)->get_data(), (*key_iter)->get_size(), (*key_iter)->get_prefix_size());
        log_debug("get from server:%s", tbsys::CNetUtil::addrToString(server_list[ser_idx]).c_str());
    }

    if (ret < 0) {
        rq_iter = request_gets.begin();
        while (rq_iter != request_gets.end()) {
            request_get *req = rq_iter->second;
            ++rq_iter;
            delete req;
        }
        request_gets.clear();
    }
    return ret;
}

int
tair_client_impl::mget_impl(int area, const vector<data_entry *> &keys, tair_keyvalue_map &data, int server_select) {
    request_get_map request_gets;
    int ret = TAIR_RETURN_SUCCESS;
    if ((ret = init_request_map(area, keys, request_gets, server_select)) < 0) {
        return ret;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_get_map::iterator rq_iter = request_gets.begin();
    while (rq_iter != request_gets.end()) {
        if (send_request(rq_iter->first, rq_iter->second, cwo->get_id()) < 0) {
            delete rq_iter->second;
            request_gets.erase(rq_iter++);
        } else {
            ++rq_iter;
        }
    }

    vector<response_get *> resps;
    ret = TAIR_RETURN_SEND_FAILED;

    vector<base_packet *> tpk;
    if ((ret = get_response(cwo, request_gets.size(), tpk)) < 1) {
        this_wait_object_manager->destroy_wait_object(cwo);
        log_error("all requests are failed");
        return ret;
    }

    vector<base_packet *>::iterator bp_iter = tpk.begin();
    for (; bp_iter != tpk.end(); ++bp_iter) {
        response_get *tpacket = dynamic_cast<response_get *> (*bp_iter);
        if (tpacket == NULL || tpacket->getPCode() != TAIR_RESP_GET_PACKET) {
            if (tpacket != NULL && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
                response_return *r = (response_return *) tpacket;
                ret = r->get_code();
            }
            if (tpacket != NULL && ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = tpacket->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
            continue;
        }
        resps.push_back(tpacket);
    }

    vector<response_get *>::iterator rp_iter = resps.begin();
    bool kv_map_null = false;
    for (; rp_iter != resps.end(); ++rp_iter) {
        ret = (*rp_iter)->get_code();
        if ((ret != TAIR_RETURN_SUCCESS) && (ret != TAIR_RETURN_PARTIAL_SUCCESS)) {
            log_warn("get response code: %d error", ret);
            continue;
        }

        if ((*rp_iter)->key_count == 1) {
            data_entry *key = new data_entry(*((*rp_iter)->key));
            data_entry *value = new data_entry(*((*rp_iter)->data));
            data.insert(tair_keyvalue_map::value_type(key, value));
        } else if ((*rp_iter)->key_count > 1) {
            tair_keyvalue_map *kv_map = (*rp_iter)->key_data_map;
            if (kv_map == NULL) {
                log_error("mget response's key data map is null");
                //why? break;
                kv_map_null = true;
                break;
            }
            tair_keyvalue_map::iterator it = kv_map->begin();
            while (it != kv_map->end()) {
                data_entry *key = new data_entry(*(it->first));
                data_entry *value = new data_entry(*(it->second));
                data.insert(tair_keyvalue_map::value_type(key, value));
                ++it;
            }
        }
        new_config_version = (*rp_iter)->config_version;
    }

    if (kv_map_null) {
        //return fail
        ret = TAIR_RETURN_FAILED;
        //free
        defree(data);
        data.clear();
    } else {
        ret = TAIR_RETURN_SUCCESS;
        if (keys.size() > data.size()) {
            ret = TAIR_RETURN_PARTIAL_SUCCESS;
        }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

int tair_client_impl::mget(int area, const vector<data_entry *> &keys, tair_keyvalue_map &data) {
    return mget_impl(area, keys, data, 0);
}

base_packet *tair_client_impl::init_packet(int pcode, int area, const tair_dataentry_set &mkey_set) {

    base_packet *packet = NULL;
    if (pcode == TAIR_REQ_PREFIX_REMOVES_PACKET) {
        packet = new request_prefix_removes(mkey_set, area);
    } else {
        packet = new request_prefix_hides(mkey_set, area);
    }
    return packet;
}

base_packet *tair_client_impl::init_packet(int pcode, int area, const data_entry &key) {
    base_packet *packet = packet_factory::create_packet(pcode);
    if (pcode == TAIR_REQ_REMOVE_PACKET) {
        request_remove *req = (request_remove *) packet;
        req->area = area;
        req->add_key(const_cast<data_entry *>(&key), true);
        req->server_flag = key.server_flag;
    } else if (pcode == TAIR_REQ_HIDE_PACKET) {
        request_hide *req = (request_hide *) packet;
        req->area = area;
        req->add_key(key.get_data(), key.get_size(), key.get_prefix_size()); //~ prefix_hide may call this method
    } else {
        log_error("[FATAL ERROR] init_packet, unknown pcode: %d", pcode);
    }
    return packet;
}

int tair_client_impl::do_process_with_key(int pcode, int area,
                                          const data_entry &key, TAIRCALLBACKFUNC pfunc, void *parg) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_debug("can not find serverId, return false");
        return TAIR_RETURN_NONE_DATASERVER;
    }

    int ret = TAIR_RETURN_SUCCESS;
    wait_object *cwo = this_wait_object_manager->create_wait_object(pcode,
                                                                    pfunc, parg, 0, server_list[0]);
    base_packet *packet = init_packet(pcode, area, key);

    base_packet *tpacket = NULL;
    response_return *resp = NULL;

    ret = send_request(server_list[0], packet, cwo->get_id());
    if (ret < 0) {
        //release the packet, the following phases will not be executed.
        delete packet;
    } else {
        if (pfunc != NULL) return TAIR_RETURN_SUCCESS;

        ret = get_response(cwo, 1, tpacket);

        if (ret >= 0 && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            resp = (response_return *) tpacket;
            new_config_version = resp->config_version;
            ret = resp->get_code();
            if (ret < 0 && ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

int tair_client_impl::invalidate(int area, const data_entry &key, const char *groupname, bool is_sync) {
    if (groupname == NULL) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (invalid_server_list.size() == 0) {
        log_error("invalidate server list is empty.");
        return TAIR_RETURN_FAILED;
    }
    //static hash_map<uint64_t, int, __gnu_cxx::hash<int> > fail_count_map;


    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_invalid *req = new request_invalid();
    req->set_group_name(groupname);
    req->area = area;
    req->is_sync = is_sync ? SYNC_INVALID : ASYNC_INVALID;
    req->add_key(key.get_data(), key.get_size());

    int ret_code = TAIR_RETURN_SUCCESS;
    do {
        const int inval_size = invalid_server_list.size();
        uint64_t server_id = invalid_server_list[rand() % inval_size];
        base_packet *tpacket = NULL;
        //~ send request.
        log_debug("send request_invalid to %s.", tbsys::CNetUtil::addrToString(server_id).c_str());
        if (send_request(server_id, req, cwo->get_id()) != TAIR_RETURN_SUCCESS) {
            log_error("send request_invalid to %s failed.", tbsys::CNetUtil::addrToString(server_id).c_str());
            int fail_count = ++fail_count_map[server_id];
            if (fail_count >= 10) {
                //! remove invalid server, if 10 consecutive timeout encountered.
                log_error("invalid server %s regarded to be down.",
                          tbsys::CNetUtil::addrToString(server_id).c_str());
                shrink_invalidate_server(server_id);
            }
            send_fail_count++;
            ret_code = TAIR_RETURN_SEND_FAILED;
            delete req;
            break;
        }
        //~ get response
        if ((ret_code = get_response(cwo, 1, tpacket)) != 0) {
            log_warn("get invalid response timeout.");
            int fail_count = ++fail_count_map[server_id];
            if (fail_count >= 10) {
                //! remove invalid server, if 10 consecutive timeout encountered.
                log_error("invalid server %s regarged to be down.",
                          tbsys::CNetUtil::addrToString(server_id).c_str());
                shrink_invalidate_server(server_id);
            }
        }
        //~ got response
        if (tpacket != NULL) {
            response_return *resp = dynamic_cast<response_return *>(tpacket);
            if (resp != NULL) {
                if ((ret_code = resp->get_code()) != TAIR_RETURN_SUCCESS) {
                    log_error("invalidate failure, ret_code: %d", ret_code);
                } else {
                    fail_count_map[server_id] = 0;
                }
            } else {
                log_error("packet cannot be casted to response_return");
            }
        } else {
            log_error("tpacket is null");
            ret_code = TAIR_RETURN_FAILED;
        }

    } while (false);

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret_code;
}

int tair_client_impl::invalidate(int area, const data_entry &key, bool is_sync) {
    if (invalid_server_list.empty())
        return remove(area, key);
    else
        return invalidate(area, key, group_name.c_str(), is_sync);
}

int tair_client_impl::hide(int area, const data_entry &key) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_debug("can not find serverId, return false");
        return -1;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_hide *packet = new request_hide();
    packet->area = area;
    packet->add_key(key.get_data(), key.get_size(), key.get_prefix_size()); //~ prefix_hide may call this method

    base_packet *tpacket = 0;
    response_return *resp = 0;

    int ret = TAIR_RETURN_SEND_FAILED;
    if ((ret = send_request(server_list[0], packet, cwo->get_id())) < 0) {
        delete packet;
        goto FAIL;
    }
    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        goto FAIL;
    }
    if (tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
        goto FAIL;
    }

    resp = (response_return *) tpacket;
    new_config_version = resp->config_version;
    if ((ret = resp->get_code()) < 0) {
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        goto FAIL;
    }
    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
    FAIL:
    this_wait_object_manager->destroy_wait_object(cwo);
    log_info("remove failure: %s:%s",
              tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
              get_error_msg(ret));
    return ret;
}

int tair_client_impl::get_hidden(int area, const data_entry &key, data_entry *&value) {
    return get_hidden_impl(area, key, value, 0);
}

int tair_client_impl::get_hidden_impl(int area, const data_entry &key, data_entry *&value, int32_t server_select) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT || server_select < 0) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    request_get_hidden *req = new request_get_hidden();
    req->add_key(const_cast<data_entry *>(&key), true);
    req->area = area;
    response_get *resp = NULL;

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_warn("no dataserver available");
        return TAIR_RETURN_FAILED;
    }

    if ((uint64_t) server_select >= server_list.size()) {
        log_error("select:%d not in server_list,size %lu", server_select, server_list.size());
        return TAIR_RETURN_FAILED;
    }

    log_debug("get from server: %s", tbsys::CNetUtil::addrToString(server_list[server_select]).c_str());

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    int ret = do_request(req, resp, cwo, server_list[server_select]);
    if (ret == TAIR_RETURN_SEND_FAILED) delete req;
    if (resp != NULL) {
        new_config_version = resp->config_version;
        value = resp->data;
        // if (value == NULL) {
        //   ret = TAIR_RETURN_PROXYED;
        // }
        resp->data = NULL;
    }
    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

int tair_client_impl::prefix_get(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);

    return get(area, mkey, value);
}

int tair_client_impl::prefix_gets(int area, const data_entry &pkey, const tair_dataentry_set &skeys_set,
                                  tair_keyvalue_map &result_map, key_code_map_t &failed_map) {
    tair_dataentry_vector skeys(skeys_set.size());
    copy(skeys_set.begin(), skeys_set.end(), skeys.begin());
    return prefix_gets(area, pkey, skeys, result_map, failed_map);
}

/*
   * you should better ensure that keys in `skeys' are not duplicated,
   * or use the overrided one with `tair_dataentry_set'
   */
int tair_client_impl::prefix_gets(int area, const data_entry &pkey, const tair_dataentry_vector &skeys,
                                  tair_keyvalue_map &result_map, key_code_map_t &failed_map) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    int ret = TAIR_RETURN_SUCCESS;

    vector<uint64_t> ds;
    if (!get_server_id(pkey, ds)) {
        log_error("no dataserver available");
        ret = TAIR_RETURN_SEND_FAILED;
        return ret;
    }

    bool query_slave = true;
    for (size_t i = 0; i < ds.size() && query_slave; ++i) {
        request_prefix_gets *req = new request_prefix_gets;
        response_prefix_gets *resp = NULL;
        req->area = area;
        tair_dataentry_vector::const_iterator itr = skeys.begin();
        while (itr != skeys.end()) {
            data_entry *skey = *itr;
            data_entry *mkey = new data_entry();
            merge_key(pkey, *skey, *mkey);
            if (!key_entry_check(*mkey)) {
                delete mkey;
                ret = TAIR_RETURN_ITEMSIZE_ERROR;
                break;
            }
            req->add_key(mkey);
            ++itr;
        }
        if (ret != TAIR_RETURN_SUCCESS) {
            delete req;
            break;
        }
        wait_object *cwo = this_wait_object_manager->create_wait_object();
        ret = do_request(req, resp, cwo, ds[i]);
        if (ret == TAIR_RETURN_SEND_FAILED) delete req;
        if (ret != TAIR_RETURN_SEND_FAILED && ret != TAIR_RETURN_TIMEOUT && resp != NULL) {
            if (resp->key_value_map != NULL) {
                resp->key_value_map->swap(result_map);
            }
            if (resp->key_code_map != NULL) {
                resp->key_code_map->swap(failed_map);
            }
            query_slave = false;
        }
        this_wait_object_manager->destroy_wait_object(cwo);
    }

    return ret;
}

int tair_client_impl::prefix_put(int area, const data_entry &pkey, const data_entry &skey,
                                 const data_entry &value, int expire, int version) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey) || !data_entry_check(value)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);

    return put(area, mkey, value, expire, version);
}

int tair_client_impl::prefix_puts(int area, const data_entry &pkey,
                                  const vector<key_value_pack_t *> &skey_value_packs, key_code_map_t &failed_map) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    request_prefix_puts *req = new request_prefix_puts;
    response_mreturn *resp = NULL;
    req->set_pkey(const_cast<data_entry *>(&pkey), true);
    req->area = area;
    int ret = TAIR_RETURN_SUCCESS;
    do {
        vector<key_value_pack_t *>::const_iterator itr = skey_value_packs.begin();
        data_entry *mkey = NULL;
        while (itr != skey_value_packs.end()) {
            key_value_pack_t *pack = *itr;
            data_entry *skey = pack->key;
            data_entry *value = pack->value;
            mkey = new data_entry;
            merge_key(pkey, *skey, *mkey);
            if (!key_entry_check(*mkey) || !data_entry_check(*value)) {
                delete mkey;
                ret = TAIR_RETURN_ITEMSIZE_ERROR;
                break;
            }
            mkey->data_meta.version = pack->version;
            mkey->data_meta.edate = pack->expire;
            req->add_key_value(mkey, new data_entry(*value));
            ++itr;
        }
        if (ret != TAIR_RETURN_SUCCESS) {
            delete req;
            break;
        }

        vector<uint64_t> ds;
        if (!get_server_id(*mkey, ds)) {
            delete req;
            log_error("no dataserver available");
            ret = TAIR_RETURN_SEND_FAILED;
            break;
        }

        wait_object *cwo = this_wait_object_manager->create_wait_object();
        ret = do_request(req, resp, cwo, ds[0]);
        if (resp != NULL && resp->key_code_map != NULL) {
            resp->key_code_map->swap(failed_map);
        }
        if (ret == TAIR_RETURN_SEND_FAILED) delete req;
        this_wait_object_manager->destroy_wait_object(cwo);
    } while (false);
    return ret;
}

int tair_client_impl::prefix_hide(int area, const data_entry &pkey, const data_entry &skey) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);

    return hide(area, mkey);
}

/**
   * hide with multiple merged key
   * should only be called by invalid server
   */
int tair_client_impl::do_process_with_multi_keys(int pcode, int area,
                                                 const tair_dataentry_set &mkey_set,
                                                 key_code_map_t *key_code_map,
                                                 TAIRCALLBACKFUNC_EX pfunc, void *parg) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (mkey_set.empty()) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (pfunc == NULL && key_code_map == NULL) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    base_packet *packet = init_packet(pcode, area, mkey_set);

    vector<uint64_t> server_list;
    data_entry *mkey = *(mkey_set.begin());
    if (!get_server_id(*mkey, server_list)) {
        log_warn("no dataserver available");
        delete packet;
        return TAIR_RETURN_NONE_DATASERVER;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object(pcode, pfunc, parg);
    base_packet *tpacket = NULL;
    response_mreturn *resp = NULL;

    int ret = send_request(server_list[0], packet, cwo->get_id());
    if (ret < 0) {
        //release the packet, the following phases will not be executed.
        delete packet;
    }
    if (ret >= 0 && pfunc != NULL) {
        return TAIR_RETURN_SUCCESS;
    }

    if (ret >= 0) {
        ret = get_response(cwo, 1, tpacket);
    }

    if (ret >= 0 && (resp = dynamic_cast<response_mreturn *>(tpacket)) != NULL) {
        ret = resp->get_code();
    } else {
        ret = TAIR_RETURN_FAILED;
    }

    if (ret >= 0 && ret != TAIR_RETURN_SUCCESS && resp->key_code_map != NULL) {
        key_code_map->clear();
        resp->key_code_map->swap(*key_code_map);
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

/**
   * hide with multiple merged key
   * should only be called by invalid server
   */
int tair_client_impl::hides(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (mkey_set.empty()) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    request_prefix_hides *req = new request_prefix_hides();
    req->area = area;
    response_mreturn *resp = NULL;
    int ret = TAIR_RETURN_SUCCESS;

    do {
        tair_dataentry_set::const_iterator itr = mkey_set.begin();
        data_entry *mkey = NULL;
        while (itr != mkey_set.end()) {
            mkey = *itr;
            req->add_key(mkey, true);
            ++itr;
        }

        vector<uint64_t> server_list;
        if (!get_server_id(*mkey, server_list)) {
            log_warn("no dataserver available");
            delete req;
            ret = TAIR_RETURN_FAILED;
            break;
        }

        wait_object *cwo = this_wait_object_manager->create_wait_object();
        ret = do_request(req, resp, cwo, server_list[0]);
        if (resp != NULL && ret != TAIR_RETURN_SUCCESS && resp->key_code_map != NULL) {
            key_code_map.clear();
            resp->key_code_map->swap(key_code_map);
        }
        if (ret == TAIR_RETURN_SEND_FAILED) delete req;
        this_wait_object_manager->destroy_wait_object(cwo);
    } while (false);
    return ret;
}

int tair_client_impl::prefix_hides(int area, const data_entry &pkey,
                                   const tair_dataentry_set &skey_set, key_code_map_t &key_code_map) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    request_prefix_hides *req = new request_prefix_hides();
    req->area = area;
    response_mreturn *resp = NULL;
    int ret = TAIR_RETURN_SUCCESS;

    do {
        tair_dataentry_set::const_iterator itr = skey_set.begin();
        data_entry *mkey = NULL;
        while (itr != skey_set.end()) {
            data_entry *skey = *itr;
            mkey = new data_entry();
            merge_key(pkey, *skey, *mkey);
            if (!key_entry_check(*mkey)) {
                delete mkey;
                ret = TAIR_RETURN_ITEMSIZE_ERROR;
                break;
            }
            req->add_key(mkey);
            ++itr;
        }
        if (ret != TAIR_RETURN_SUCCESS) {
            delete req;
            break;
        }

        vector<uint64_t> server_list;
        if (!get_server_id(*mkey, server_list)) {
            log_debug("no dataserver available");
            delete req;
            ret = TAIR_RETURN_FAILED;
            break;
        }

        wait_object *cwo = this_wait_object_manager->create_wait_object();
        ret = do_request(req, resp, cwo, server_list[0]);
        if (resp != NULL && ret != TAIR_RETURN_SUCCESS && resp->key_code_map != NULL) {
            key_code_map.clear();
            resp->key_code_map->swap(key_code_map);
        }
        if (ret == TAIR_RETURN_SEND_FAILED) delete req;
        this_wait_object_manager->destroy_wait_object(cwo);
    } while (false);
    return ret;
}

int tair_client_impl::prefix_get_hidden(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);

    return get_hidden(area, mkey, value);
}

int tair_client_impl::prefix_remove(int area, const data_entry &pkey, const data_entry &skey) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    data_entry mkey;
    merge_key(pkey, skey, mkey);
    return remove(area, mkey);
}

int tair_client_impl::removes(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (mkey_set.empty()) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    request_prefix_removes *req = new request_prefix_removes();
    req->area = area;
    response_mreturn *resp = NULL;
    int ret = TAIR_RETURN_SUCCESS;

    do {
        tair_dataentry_set::const_iterator itr = mkey_set.begin();
        data_entry *mkey = NULL;
        while (itr != mkey_set.end()) {
            mkey = *itr;
            req->add_key(mkey, true);
            ++itr;
        }

        vector<uint64_t> server_list;
        if (!get_server_id(*mkey, server_list)) {
            log_warn("no dataserver available");
            delete req;
            ret = TAIR_RETURN_FAILED;
            break;
        }

        wait_object *cwo = this_wait_object_manager->create_wait_object();
        ret = do_request(req, resp, cwo, server_list[0]);
        if (ret == TAIR_RETURN_SEND_FAILED) delete req;
        if (resp == NULL) {
            this_wait_object_manager->destroy_wait_object(cwo);
            break;
        }
        if (ret != TAIR_RETURN_SUCCESS && resp->key_code_map != NULL) {
            key_code_map.clear();
            resp->key_code_map->swap(key_code_map);
        }
        this_wait_object_manager->destroy_wait_object(cwo);
    } while (false);
    return ret;
}

int
tair_client_impl::get_range(int area, const data_entry &pkey, const data_entry &start_key, const data_entry &end_key,
                            int offset, int limit, vector<data_entry *> &values, short type) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (RANGE_FROM_CUR != offset && RANGE_FROM_NEXT != offset) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (limit < 0) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    data_entry merge_skey, merge_ekey;
    merge_key(pkey, start_key, merge_skey);
    merge_key(pkey, end_key, merge_ekey);

    if (!key_entry_check(merge_skey) || !key_entry_check(merge_ekey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    vector<uint64_t> server_list;
    if (!get_server_id(merge_skey, server_list)) {
        log_warn("no dataserver available");
        return TAIR_RETURN_FAILED;
    }
    if (limit == 0)
        limit = RANGE_DEFAULT_LIMIT;

    request_get_range *packet = new request_get_range();
    packet->area = area;
    packet->cmd = type;
    packet->offset = offset;
    packet->limit = limit;
    packet->key_start.set_data(merge_skey.get_data(), merge_skey.get_size());
    packet->key_start.set_prefix_size(merge_skey.get_prefix_size());
    packet->key_end.set_data(merge_ekey.get_data(), merge_ekey.get_size());
    packet->key_end.set_prefix_size(merge_ekey.get_prefix_size());

    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_get_range *resp = 0;

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    if ((ret = send_request(server_list[0], packet, cwo->get_id())) < 0) {
        delete packet;
        this_wait_object_manager->destroy_wait_object(cwo);
        log_error("get_range failure: %s %d", get_error_msg(ret), ret);
        return ret;
    }

    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        this_wait_object_manager->destroy_wait_object(cwo);
        log_error("get_range get_response failure: %s %d ", get_error_msg(ret), ret);
        return ret;
    }

    if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_RANGE_PACKET) {
        log_error("get_range response packet error. pcode: %d", tpacket->getPCode());
        ret = TAIR_RETURN_FAILED;
    } else {
        resp = (response_get_range *) tpacket;
        new_config_version = resp->config_version;
        ret = resp->get_code();
        if (ret != TAIR_RETURN_SUCCESS) {
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
                //update server table immediately
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        } else {
            for (size_t i = 0; i < resp->key_data_vector->size(); i++) {
                data_entry *data = new data_entry(*((*resp->key_data_vector)[i]));
                values.push_back(data);
            }
            if (resp->get_hasnext()) {
                ret = TAIR_HAS_MORE_DATA;
            }
        }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

int tair_client_impl::prefix_removes(int area, const data_entry &pkey,
                                     const tair_dataentry_set &skey_set, key_code_map_t &key_code_map) {
    //~ duplicate logic from prefix_hides
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    request_prefix_removes *req = new request_prefix_removes();
    req->area = area;
    response_mreturn *resp = NULL;
    int ret = TAIR_RETURN_SUCCESS;

    do {
        tair_dataentry_set::const_iterator itr = skey_set.begin();
        data_entry *mkey = NULL;
        while (itr != skey_set.end()) {
            data_entry *skey = *itr;
            mkey = new data_entry();
            merge_key(pkey, *skey, *mkey);
            if (!key_entry_check(*mkey)) {
                delete mkey;
                ret = TAIR_RETURN_ITEMSIZE_ERROR;
                break;
            }
            req->add_key(mkey);
            ++itr;
        }
        if (ret != TAIR_RETURN_SUCCESS) {
            delete req;
            break;
        }

        vector<uint64_t> server_list;
        if (!get_server_id(*mkey, server_list)) {
            log_debug("no dataserver available");
            delete req;
            ret = TAIR_RETURN_FAILED;
            break;
        }

        wait_object *cwo = this_wait_object_manager->create_wait_object();
        ret = do_request(req, resp, cwo, server_list[0]);
        if (ret == TAIR_RETURN_SEND_FAILED) delete req;
        if (resp != NULL && resp->key_code_map != NULL) {
            key_code_map.clear();
            resp->key_code_map->swap(key_code_map);
        }
        this_wait_object_manager->destroy_wait_object(cwo);
    } while (false);
    return ret;
}

int
tair_client_impl::init_request_map(int area, const vector<data_entry *> &keys, request_remove_map &request_removes) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;
    request_remove_map::iterator rq_iter;

    vector<data_entry *>::const_iterator key_iter;
    for (key_iter = keys.begin(); key_iter != keys.end(); ++key_iter) {
        if (!key_entry_check(**key_iter)) {
            ret = TAIR_RETURN_ITEMSIZE_ERROR;
            break;
        }

        vector<uint64_t> server_list;
        if (!get_server_id(**key_iter, server_list)) {
            log_error("can not find serverId, return false");
            ret = -1;
            break;
        }

        request_remove *packet = NULL;
        rq_iter = request_removes.find(server_list[0]);
        if (rq_iter != request_removes.end()) {
            packet = rq_iter->second;
        } else {
            packet = new request_remove();
            request_removes[server_list[0]] = packet;
        }

        packet->area = area;
        packet->add_key((*key_iter)->get_data(), (*key_iter)->get_size());
    }

    if (ret < 0) {
        rq_iter = request_removes.begin();
        while (rq_iter != request_removes.end()) {
            request_remove *req = rq_iter->second;
            ++rq_iter;
            delete req;
        }
        request_removes.clear();
    }
    return ret;
}

int tair_client_impl::mdelete(int area, const vector<data_entry *> &keys) {
    request_remove_map request_removes;
    int ret = TAIR_RETURN_SUCCESS;
    if ((ret = init_request_map(area, keys, request_removes)) < 0) {
        return ret;
    }
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_remove_map::iterator rq_iter = request_removes.begin();

    while (rq_iter != request_removes.end()) {
        if (send_request(rq_iter->first, rq_iter->second, cwo->get_id()) < 0) {
            delete rq_iter->second;
            request_removes.erase(rq_iter++);
        } else {
            ++rq_iter;
        }
    }

    ret = TAIR_RETURN_SEND_FAILED;
    vector<base_packet *> tpk;
    if ((ret = get_response(cwo, request_removes.size(), tpk)) < 1) {
        this_wait_object_manager->destroy_wait_object(cwo);
        log_error("all requests are failed, ret: %d", ret);
        return ret;
    }

    uint32_t send_success = 0;
    vector<base_packet *>::iterator bp_iter = tpk.begin();
    for (; bp_iter != tpk.end(); ++bp_iter) {
        response_return *tpacket = dynamic_cast<response_return *> (*bp_iter);
        if (tpacket->getPCode() != TAIR_RESP_RETURN_PACKET) {
            log_error("mdelete return pcode: %d", tpacket->getPCode());
            continue;
        }
        new_config_version = tpacket->config_version;
        if ((ret = tpacket->get_code()) < 0) {
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
            continue;
        }
        ++send_success;
    }

    log_debug("mdelete keys size: %lu, send success: %d, return packet size: %lu",
              keys.size(), send_success, tpk.size());
    // here we are sure that tpk.size() >= 1
    // if send_success == 0, all failed
    if (send_success && (tpk.size() != send_success)) {
        ret = TAIR_RETURN_PARTIAL_SUCCESS;
    }
    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

// add count

int tair_client_impl::add_count(int area,

                                const data_entry &key,
                                int count, int *ret_count,
                                int init_value /*=0*/, int expire_time  /*=0*/) {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT || expire_time < 0) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_debug("can not find serverId, return false");
        return -1;
    }


    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_inc_dec *packet = new request_inc_dec();
    packet->area = area;
    packet->key = key;
    packet->add_count = count;
    packet->init_value = init_value;
    packet->expired = expire_time;

    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_inc_dec *resp = 0;


    if ((ret = send_request(server_list[0], packet, cwo->get_id())) < 0) {

        delete packet;
        goto FAIL;
    }

    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        log_debug("get response failed.");
        goto FAIL;
    }

    if (tpacket->getPCode() != TAIR_RESP_INCDEC_PACKET) {
        goto FAIL;
    }
    resp = (response_inc_dec *) tpacket;
    *ret_count = resp->value;
    ret = 0;
    new_config_version = resp->config_version;


    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;

    FAIL:
    if (tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
        response_return *r = (response_return *) tpacket;
        ret = r->get_code();
        new_config_version = r->config_version;
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {//just in case
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    log_info("add count failure: %s", get_error_msg(ret));

    return ret;
}

int tair_client_impl::clear_area(int area) {
    int ret = TAIR_RETURN_SUCCESS;
    std::set<uint64_t> ds_set;
    get_servers(ds_set);
    for (std::set<uint64_t>::iterator it = ds_set.begin(); it != ds_set.end(); it++) {
        ret = remove_area(area, *it);
        if (ret != TAIR_RETURN_SUCCESS)
            break;
    }
    return ret;
}

int tair_client_impl::remove_area(int area, uint64_t server_id) {
    //0. param avaliable
    if (UNLIKELY(area < -4 || area >= TAIR_MAX_AREA_COUNT || server_id == 0)) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    std::set<uint64_t> ds_set;
    get_servers(ds_set);
    if (ds_set.find(server_id) == ds_set.end()) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //1.create request packet.
    request_remove_area *packet = new request_remove_area();
    packet->area = area;

    //2. send request
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    int ret = TAIR_RETURN_SEND_FAILED;
    log_info("request_remove_area=>%s", tbsys::CNetUtil::addrToString(server_id).c_str());
    if ((ret = send_request(server_id, packet, cwo->get_id())) != TAIR_RETURN_SUCCESS) {
        log_debug("failed to send request to ds: %s , rc: %d", tbsys::CNetUtil::addrToString(server_id).c_str(),
                  ret);
        delete packet;
    }

    //3. get response
    if (ret == TAIR_RETURN_SUCCESS) {
        base_packet *tpacket = NULL;
        ret = get_response(cwo, 1, tpacket);
        if (TAIR_RETURN_SUCCESS == ret)
            ret = ((response_return *) tpacket)->get_code();
        else
            log_debug("failed to get response from ds: %s , rc: %d",
                      tbsys::CNetUtil::addrToString(server_id).c_str(), ret);
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

int tair_client_impl::dump_area(set <dump_meta_info> &info) {
    //1.send request to all server
    map<uint64_t, request_dump *> request_list;
    map<uint64_t, request_dump *>::iterator it;
    for (uint32_t i = 0; i < my_server_list.size(); ++i) {
        uint64_t serverId = my_server_list[i];
        if (serverId == 0) {
            continue;
        }
        it = request_list.find(serverId);
        if (it == request_list.end()) {
            request_dump *packet = new request_dump();
            packet->info_set = info;
            request_list[serverId] = packet;
        }
    }
    if (request_list.empty()) {
        return TAIR_RETURN_SUCCESS;
    }
    //2. send request

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    int sendCount = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    for (it = request_list.begin(); it != request_list.end(); ++it) {
        uint64_t serverId = it->first;
        request_dump *packet = it->second;
        log_info("dump_area=>%s", tbsys::CNetUtil::addrToString(serverId).c_str());

        if ((ret = send_request(serverId, packet, cwo->get_id())) < 0) {

            delete packet;
        } else {
            ++sendCount;
        }
    }
    base_packet *tpacket = 0;
    if (sendCount > 0) {
        if ((ret = get_response(cwo, sendCount, tpacket)) < 0) {
            //TODO log
        }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;

}

int tair_client_impl::op_cmd_to_cs(ServerCmdType cmd, std::vector<std::string> *params,
                                   std::vector<std::string> *ret_values) {
    if (config_server_list.empty() || config_server_list[0] == 0L) {
        log_error("no configserver available");
        return TAIR_RETURN_FAILED;
    }

    request_op_cmd *req = new request_op_cmd();
    req->cmd = cmd;
    if (params != NULL) {
        req->params = *params;
    }

    response_op_cmd *resp = NULL;
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    uint64_t master = config_server_list[0];

    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = NULL;
    do {
        if ((ret = send_request(master, req, cwo->get_id())) != TAIR_RETURN_SUCCESS) {
            delete req;
            break;
        }
        if ((ret = get_response(cwo, 1, tpacket)) < 0) {
            break;
        }
        resp = dynamic_cast<response_op_cmd *>(tpacket);
        if (resp == NULL) {
            ret = TAIR_RETURN_FAILED;
            break;
        }
        ret = resp->code;
        if (ret_values != NULL) {
            *ret_values = resp->infos;
        }
    } while (false);

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

int tair_client_impl::op_cmd_to_ds(ServerCmdType cmd, std::vector<std::string> *params,
                                   std::vector<std::string> *infos, const char *dest_server_addr) {
    std::map<uint64_t, request_op_cmd *> request_map;
    std::map<uint64_t, request_op_cmd *>::iterator it;

    if (dest_server_addr != NULL) {       // specify server_id
        uint64_t dest_server_id = tbsys::CNetUtil::strToAddr(dest_server_addr, 0);
        if (dest_server_id == 0) {
            log_error("invalid dest_server_addr: %s", dest_server_addr);
        } else {
            request_op_cmd *packet = new request_op_cmd();
            packet->cmd = cmd;
            if (params != NULL) {
                packet->params = *params;
            }
            request_map[dest_server_id] = packet;
        }
    } else {                    // send request to all server
        for (uint32_t i = 0; i < my_server_list.size(); i++) {
            uint64_t server_id = my_server_list[i];
            if (server_id == 0) {
                continue;
            }
            it = request_map.find(server_id);
            if (it == request_map.end()) {
                request_op_cmd *packet = new request_op_cmd();
                packet->cmd = cmd;
                if (params != NULL) {
                    packet->params = *params;
                }
                request_map[server_id] = packet;
            }
        }
    }

    if (request_map.empty()) {
        log_error("no request");
        return TAIR_RETURN_FAILED;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    int send_count = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    it = request_map.begin();
    while (it != request_map.end()) {
        uint64_t server_id = it->first;
        request_op_cmd *packet = it->second;
        log_info("request_op_cmd %d=>%s", cmd, tbsys::CNetUtil::addrToString(server_id).c_str());

        if ((ret = send_request(server_id, packet, cwo->get_id())) != TAIR_RETURN_SUCCESS) {
            log_error("send request_op_cmd request fail: %s", tbsys::CNetUtil::addrToString(server_id).c_str());
            request_map.erase(it++);
            delete packet;
        } else {
            ++send_count;
            it++;
        }
    }

    vector<base_packet *> tpk;
    if ((ret = get_response(cwo, send_count, tpk)) < 1) {
        this_wait_object_manager->destroy_wait_object(cwo);
        log_error("all requests are failed, ret: %d", ret);
        return ret == 0 ? TAIR_RETURN_FAILED : ret;
    }

    int fail_request = 0;
    vector<base_packet *>::iterator bp_iter = tpk.begin();
    for (; bp_iter != tpk.end(); ++bp_iter) {
        if ((*bp_iter)->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *tpacket = dynamic_cast<response_return *>(*bp_iter);
            if (tpacket != NULL) {
                ret = tpacket->get_code();
                if (TAIR_RETURN_SUCCESS != ret) {
                    if (TAIR_RETURN_SERVER_CAN_NOT_WORK == ret) {
                        new_config_version = tpacket->config_version;
                        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
                    }
                    log_error("get response fail: ret: %d", ret);
                    ++fail_request;
                }
            }
        } else if ((*bp_iter)->getPCode() == TAIR_RESP_OP_CMD_PACKET) {
            response_op_cmd *tpacket = dynamic_cast<response_op_cmd *>(*bp_iter);
            if (tpacket != NULL) {
                ret = tpacket->get_code();
                if (TAIR_RETURN_SUCCESS != ret) {
                    log_error("get response fail: ret: %d", ret);
                    ++fail_request;
                } else {
                    std::vector<std::string>::iterator iter;
                    for (iter = tpacket->infos.begin(); iter != tpacket->infos.end(); iter++) {
                        infos->push_back(*iter);
                    }
                }
            }
        } else {
            log_error("not get response packet: %d", (*bp_iter)->getPCode());
            ++fail_request;
        }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return fail_request > 0 ? TAIR_RETURN_PARTIAL_SUCCESS : TAIR_RETURN_SUCCESS;
}

void tair_client_impl::op_cmd_to_ds_new(ServerCmdType cmd, const std::vector<std::string> &params,
                                        const char *dest_server_addr, op_cmd_result_map &op_cmd_result) {
    set <uint64_t> ds;
    if (dest_server_addr != NULL) { // specify server_id
        uint64_t dest_server_id = tbsys::CNetUtil::strToAddr(dest_server_addr, 0);
        if (dest_server_id == 0) {
            tair::op_cmd_result &one_op_cmd_result = op_cmd_result[dest_server_id];
            char error_info[80];
            snprintf(error_info, sizeof(error_info), "invalid dest_server_addr: %s", dest_server_addr);
            one_op_cmd_result.retcode = TAIR_RETURN_FAILED;
            one_op_cmd_result.infos.push_back(error_info);
            return;
        }
        ds.insert(dest_server_id);
    } else
        get_servers(ds);  // send request to all server

    int32_t send_count = 0;
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    for (set<uint64_t>::const_iterator it = ds.begin(); it != ds.end(); ++it) {
        uint64_t server_id = *it;
        request_op_cmd *packet = new request_op_cmd();
        packet->cmd = cmd;
        packet->params.assign(params.begin(), params.end());

        int32_t ret = send_request(server_id, packet, cwo->get_id());
        if (TAIR_RETURN_SUCCESS == ret)
            ++send_count;
        else {
            delete packet;
            tair::op_cmd_result &one_op_cmd_result = op_cmd_result[server_id];
            one_op_cmd_result.retcode = ret;
            one_op_cmd_result.infos.push_back("send request_op_cmd request fail");
        }
    }

    vector<base_packet *> tpk;
    get_response_new(cwo, send_count, tpk);

    for (vector<base_packet *>::const_iterator bp_iter = tpk.begin(); bp_iter != tpk.end(); ++bp_iter) {
        tair::op_cmd_result &one_op_cmd_result = op_cmd_result[(*bp_iter)->getPeerId()];
        if ((*bp_iter)->getPCode() == TAIR_RESP_OP_CMD_PACKET) {
            response_op_cmd *tpacket = dynamic_cast<response_op_cmd *>(*bp_iter);
            one_op_cmd_result.retcode = tpacket->get_code();
            one_op_cmd_result.infos.assign(tpacket->infos.begin(), tpacket->infos.end());
        } else {
            one_op_cmd_result.retcode = TAIR_RETURN_FAILED;
            one_op_cmd_result.infos.push_back("response packet wrong type");
        }
    }

    // now dataservers which send failed and dataservers which get response is already in op_cmd_result
    // left is dataserver which get response time out.
    for (set<uint64_t>::const_iterator it = ds.begin(); it != ds.end(); ++it) {
        pair<op_cmd_result_map::iterator, bool> insert_result;
        if (op_cmd_result.find(*it) == op_cmd_result.end()) {
            tair::op_cmd_result &one_op_cmd_result = op_cmd_result[*it];
            one_op_cmd_result.retcode = TAIR_RETURN_TIMEOUT;
            one_op_cmd_result.infos.push_back("get reponse packet time out");
        }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
}

void tair_client_impl::set_timeout(int this_timeout) {
    if (this_timeout > 0) {
        timeout = this_timeout;
    }
}

//!FIXME libeasy does not support user-defined queue limit, for the present
void tair_client_impl::set_queue_limit(int limit) {
    if (limit > 0) {
        queue_limit = limit;
    }
}

uint64_t tair_client_impl::get_queue_limit() {
    return queue_limit;
}

void tair_client_impl::set_randread(bool rand_flag) {
    rand_read_flag = rand_flag;
}

/* memcached-related */
int tair_client_impl::mc_ops(int8_t mc_opcode,
                             int area,
                             const data_entry *key,
                             const data_entry *value,
                             int expire,
                             int version,
                             callback_mc_ops_pt callback, void *args) {
    if (key == NULL) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if (!key_entry_check(*key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (value != NULL && !data_entry_check(*value)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if (area < 0 || area >= TAIR_MAX_AREA_COUNT || version < 0) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }

    request_mc_ops *req = new request_mc_ops;
    req->mc_opcode = mc_opcode;
    req->area = area;
    req->key = *key;
    if (value != NULL) req->value = *value;
    req->expire = expire;
    req->version = version;
    req->server_flag = key->server_flag;

    vector<uint64_t> ds_list;
    if (!get_server_id(*key, ds_list)) {
        log_warn("no server available");
        return TAIR_RETURN_SEND_FAILED;
    }
    log_debug("The key:<%*.*s> is put to data server:%s", key->get_size(), key->get_size(), key->get_data(),
              tbsys::CNetUtil::addrToString(ds_list[0]).c_str());
    wait_object *cwo = this_wait_object_manager->create_wait_object(TAIR_REQ_MC_OPS_PACKET, callback, args);
    if (send_request(ds_list[0], req, cwo->get_id()) < 0) {
        delete req;
        return TAIR_RETURN_SEND_FAILED;
    }
    return TAIR_RETURN_SUCCESS;
}

int tair_client_impl::resolve_packet(base_packet *packet, uint64_t server_id, std::string &info) {
    //~ got response
    int ret = TAIR_RETURN_SUCCESS;
    if (packet != NULL) {
        response_op_cmd *resp = dynamic_cast<response_op_cmd *>(packet);
        if (resp != NULL) {
            if ((ret = resp->get_code()) != TAIR_RETURN_SUCCESS) {
                log_error("invalidate failure, ret_code: %d", ret);
            } else {
                fail_count_map[server_id] = 0;
                std::vector<std::string> &resp_infos = resp->infos;
                if (resp_infos.empty()) {
                    ret = TAIR_RETURN_FAILED;
                } else {
                    info = resp->infos[0];
                }
            }
        } else {  //resp == NULL
            log_error("packet cannot be casted to response_return, pcode: %d", packet->getPCode());
            ret = TAIR_RETURN_FAILED;
        }
    } else {  //packet == NULL
        log_error("tpacket is null");
        ret = TAIR_RETURN_FAILED;
    }
    return ret;
}

int tair_client_impl::resolve_packet(base_packet *packet, uint64_t server_id) {
    //~ got response
    int ret = TAIR_RETURN_SUCCESS;
    if (packet != NULL) {
        response_return *resp = dynamic_cast<response_return *>(packet);
        if (resp != NULL) {
            if ((ret = resp->get_code()) != TAIR_RETURN_SUCCESS) {
                log_error("invalidate failure, ret_code: %d", ret);
            } else {
                fail_count_map[server_id] = 0;
            }
        } else {  //resp == NULL
            log_error("packet cannot be casted to response_return, pcode: %d", packet->getPCode());
            ret = TAIR_RETURN_FAILED;
        }
    } else {  //packet == NULL
        log_error("tpacket is null");
        ret = TAIR_RETURN_FAILED;
    }
    return ret;
}

int tair_client_impl::retrieve_invalidserver(vector<uint64_t> &invalid_servers) {
    int ret = TAIR_RETURN_SUCCESS;
    if (invalid_server_list.size() == 0) {
        log_error("invalid server list is empty.");
        ret = TAIR_RETURN_FAILED;
    } else {
        invalid_servers.clear();
        uint64_t id = 0;
        for (size_t i = 0; i < invalid_server_list.size(); i++) {
            id = invalid_server_list[i];
            if (id != 0 && std::find(invalid_servers.begin(), invalid_servers.end(), id) == invalid_servers.end()) {
                invalid_servers.push_back(id);
            }
        }
        if (invalid_servers.empty()) {
            ret = TAIR_RETURN_FAILED;
        }
    }
    return ret;
}

int tair_client_impl::get_invalidserver_info(const uint64_t &invalid_server_id, std::string &buffer) {
    if (invalid_server_id == 0) {
        log_error("invalidate server id is not alive.");
        return TAIR_RETURN_FAILED;
    }
    if (std::find(invalid_server_list.begin(), invalid_server_list.end(),
                  invalid_server_id) == invalid_server_list.end()) {
        log_error("the invalid server: %s is not in the list.",
                  tbsys::CNetUtil::addrToString(invalid_server_id).c_str());
        return TAIR_RETURN_FAILED;
    }
    request_op_cmd *req = new request_op_cmd();
    int ret = TAIR_RETURN_SUCCESS;
    uint64_t server_id = invalid_server_id;
    req->add_param("info");
    // send request.
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    log_debug("send request_invalid to %s.", tbsys::CNetUtil::addrToString(server_id).c_str());
    ret = send_packet_to_is(server_id, req, cwo->get_id());
    if (ret != TAIR_RETURN_SUCCESS) {
        delete req;
    }
    // get respone packet.
    base_packet *packet = NULL;
    if (ret == TAIR_RETURN_SUCCESS) {
        ret = get_packet_from_is(cwo, server_id, packet);
    }
    // resolve packet
    if (ret == TAIR_RETURN_SUCCESS) {
        ret = resolve_packet(packet, server_id, buffer);
    }
    return ret;
}

int tair_client_impl::retry_all() {
    int ret = TAIR_RETURN_SUCCESS;
    size_t counter = 0;
    vector<uint64_t> invalid_servers;
    retrieve_invalidserver(invalid_servers);
    if (invalid_servers.size() == 0) {
        ret = TAIR_RETURN_FAILED;
    } else {
        for (size_t i = 0; i < invalid_servers.size(); i++) {
            ret = retry_all(invalid_servers[i]);
            if (ret == TAIR_RETURN_SUCCESS) {
                counter++;
                ret = TAIR_RETURN_PARTIAL_SUCCESS;
            }
        }
    }
    if (counter == 0) {
        ret = TAIR_RETURN_FAILED;
    } else {
        ret = (counter == invalid_servers.size()) ? TAIR_RETURN_SUCCESS : TAIR_RETURN_PARTIAL_SUCCESS;
    }
    return ret;
}

int tair_client_impl::retry_all(uint64_t invalid_server_id) {
    if (invalid_server_id == 0) {
        log_error("invalidate server is not alive.");
        return TAIR_RETURN_FAILED;
    }
    //create the request packet
    request_retry_all *req = new request_retry_all();
    //interact with invalid server(s)
    return do_interaction_with_is(req, invalid_server_id);
}

int tair_client_impl::send_packet_to_is(uint64_t server_id, base_packet *packet, int id) {
    //~ send request.
    int ret = TAIR_RETURN_SUCCESS;
    log_debug("send %d to %s.", packet->getPCode(), tbsys::CNetUtil::addrToString(server_id).c_str());
    if (send_request(server_id, packet, id) < 0) {
        log_error("send %d to %s failed.", packet->getPCode(), tbsys::CNetUtil::addrToString(server_id).c_str());
        int fail_count = ++fail_count_map[server_id];
        if (fail_count >= 10) {
            //! remove invalid server, if 10 consecutive timeout encountered.
            log_error("invalid server %s regarded to be down.", tbsys::CNetUtil::addrToString(server_id).c_str());
            shrink_invalidate_server(server_id);
        }
        send_fail_count++;
        ret = TAIR_RETURN_SEND_FAILED;
    }
    return ret;
}

int tair_client_impl::get_packet_from_is(wait_object *wo, uint64_t server_id, base_packet *&packet) {
    int ret = TAIR_RETURN_SUCCESS;
    if ((ret = get_response(wo, 1, packet)) != TAIR_RETURN_SUCCESS) {
        log_warn("get response  timeout.");
        int fail_count = ++fail_count_map[server_id];
        if (fail_count >= 10) {
            //! remove invalid server, if 10 consecutive timeout encountered.
            log_error("invalid server %s regarged to be down.", tbsys::CNetUtil::addrToString(server_id).c_str());
            shrink_invalidate_server(server_id);
        }
    }
    return ret;
}

int tair_client_impl::resolve_packet(base_packet *packet, uint64_t server_id, char *&buffer,
                                     unsigned long &buffer_size, int &group_count) {
    int ret_code = TAIR_RETURN_SUCCESS;
    if (packet != NULL) {
        response_inval_stat *resp = dynamic_cast<response_inval_stat *>(packet);
        if (resp != NULL) {
            if ((ret_code = resp->get_code()) != TAIR_RETURN_SUCCESS) {
                log_error("[FATAL ERROR] invalidate failure, ret_code: %d", ret_code);
                ret_code = TAIR_RETURN_FAILED;
            } else { //ret_code == TAIR_RETURN_SUCCESS
                fail_count_map[server_id] = 0;
                unsigned long uncompressed_data_size = resp->uncompressed_data_size;
                buffer = new char[uncompressed_data_size];
                if (buffer != NULL) {
                    int unret = uncompress((unsigned char *) buffer, &uncompressed_data_size,
                                           (unsigned char *) resp->stat_value->get_data(),
                                           resp->stat_value->get_size());
                    log_info("uncompressed_data (%d => %lu)", resp->stat_value->get_size(), uncompressed_data_size);
                    if (unret == Z_OK) {
                        group_count = (int) resp->group_count; //safe
                        buffer_size = uncompressed_data_size;
                    } else { //unret != Z_OK
                        log_error("error,failed to uncompress data, ret= %d", unret);
                        group_count = 0;
                        buffer_size = 0;
                        delete[] buffer;
                        buffer = NULL;
                        ret_code = TAIR_RETURN_FAILED;
                    }
                } else { //buffer == NULL
                    log_error("[FATAL ERROR] failed to alloc memory for buffer");
                    buffer_size = 0;
                    group_count = 0;
                    ret_code = TAIR_RETURN_FAILED;
                }
            }
        } else {  //resp == NULL
            log_error("packet cannot be casted to response_return");
            ret_code = TAIR_RETURN_FAILED;
        }
    } else {  //packet == NULL
        log_error("packet is null");
        ret_code = TAIR_RETURN_FAILED;
    }
    return ret_code;
}

//get statistics information of invalid server, indicated by `invalid_server_id.
int tair_client_impl::query_from_invalidserver(uint64_t invalid_server_id, inval_stat_data_t *&stat) {
    if (invalid_server_id == 0) {
        log_info("invalidate server id is not alive.");
        return TAIR_RETURN_FAILED;
    }
    if (std::find(invalid_server_list.begin(), invalid_server_list.end(),
                  invalid_server_id) == invalid_server_list.end()) {
        log_error("the invalid server: %"PRI64_PREFIX"u is not in the list.", invalid_server_id);
        return TAIR_RETURN_FAILED;
    }
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_inval_stat *req = new request_inval_stat();
    int ret = TAIR_RETURN_SUCCESS;
    uint64_t server_id = invalid_server_id;
    base_packet *packet = NULL;
    // send request.
    log_debug("send request_invalid to %s.", tbsys::CNetUtil::addrToString(server_id).c_str());
    ret = send_packet_to_is(server_id, req, cwo->get_id());
    if (ret != TAIR_RETURN_SUCCESS) {
        delete req;
    }
    // get respone packet.
    if (ret == TAIR_RETURN_SUCCESS) {
        ret = get_packet_from_is(cwo, server_id, packet);
    }
    // resolve packet
    char *data = NULL;
    unsigned long data_size = 0;
    int group_count = 0;
    if (ret == TAIR_RETURN_SUCCESS) {
        ret = resolve_packet(packet, server_id, data, data_size, group_count);
    }
    if (ret == TAIR_RETURN_SUCCESS) {
        log_info("buffer: %s, size: %lu", (data == NULL ? "empty" : "ok"), data_size);
        stat = new inval_stat_data_t();
        stat->group_count = group_count;
        stat->stat_data = data;
    }
    return ret;
}

//query statistics data form invalid server(s), caller needn't known the invalid server id.
int tair_client_impl::query_from_invalidserver(std::map<uint64_t, inval_stat_data_t *> &stats) {
    int ret = TAIR_RETURN_SUCCESS;
    size_t counter = 0;
    vector<uint64_t> inval_servers;
    retrieve_invalidserver(inval_servers);
    if (inval_servers.empty()) {
        ret = TAIR_RETURN_FAILED;
    } else {
        for (size_t i = 0; i < inval_servers.size(); i++) {
            inval_stat_data_t *stat = NULL;
            ret = query_from_invalidserver(inval_servers[i], stat);
            if (ret == TAIR_RETURN_SUCCESS) {
                if (stats.insert(map<uint64_t, inval_stat_data_t *>::value_type(inval_servers[i], stat)).second ==
                    false) {
                    log_error("[FATAL ERROR] failed to insert invalid server: %s.",
                              tbsys::CNetUtil::addrToString(inval_servers[i]).c_str());
                } else {
                    counter++;
                }
            } else {
                log_error("[FATAL ERROR] failed to query the stat. invalid server: %s",
                          tbsys::CNetUtil::addrToString(inval_servers[i]).c_str());
            }
        }
    }
    if (counter == 0) {
        ret = TAIR_RETURN_FAILED;
    } else {
        ret = (counter == inval_servers.size()) ? TAIR_RETURN_SUCCESS : TAIR_RETURN_PARTIAL_SUCCESS;
    }
    return ret;
}

//interact with invalid server, send packet and recieve packet.
int tair_client_impl::do_interaction_with_is(base_packet *packet, uint64_t invalid_server_id) {
    int ret = TAIR_RETURN_SUCCESS;
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    // send request.
    uint64_t server_id = 0;
    if (invalid_server_id == 0) {
        const int inval_size = invalid_server_list.size();
        server_id = invalid_server_list[rand() % inval_size];
    } else {
        server_id = invalid_server_id;
    }
    ret = send_packet_to_is(server_id, packet, cwo->get_id());
    if (ret != TAIR_RETURN_SUCCESS) {
        delete packet;
    }
    // get respone packet.
    if (ret == TAIR_RETURN_SUCCESS) {
        ret = get_packet_from_is(cwo, server_id, packet);
    }
    // resolve packet
    if (ret == TAIR_RETURN_SUCCESS) {
        ret = resolve_packet(packet, server_id);
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

//removed the data with prefix key by invalid server.
int tair_client_impl::prefix_invalidate(int area, const data_entry &pkey, const data_entry &skey, bool is_sync) {
    if (invalid_server_list.empty())
        return prefix_remove(area, pkey, skey);
    else
        return prefix_invalidate(area, pkey, skey, group_name.c_str(), is_sync);
}

//removed the data with prefix key by invalid server.
int tair_client_impl::prefix_invalidate(int area, const data_entry &pkey, const data_entry &skey, const char *groupname,
                                        bool is_sync) {
    if (groupname == NULL) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if (invalid_server_list.size() == 0) {
        log_error("invalidate server list is empty.");
        return TAIR_RETURN_FAILED;
    }
    data_entry mkey;
    merge_key(pkey, skey, mkey);
    //create the request packet
    request_prefix_invalids *req = new request_prefix_invalids();
    req->set_group_name(groupname);
    req->area = area;
    req->is_sync = is_sync ? SYNC_INVALID : ASYNC_INVALID;
    req->add_key(mkey.get_data(), mkey.get_size());
    //interact with invalid server(s)
    return do_interaction_with_is(req, 0);
}

int tair_client_impl::prefix_hide_by_proxy(int area, const data_entry &pkey, const data_entry &skey, bool is_sync) {
    return prefix_hide_by_proxy(area, pkey, skey, group_name.c_str(), is_sync);
}

int
tair_client_impl::prefix_hide_by_proxy(int area, const data_entry &pkey, const data_entry &skey, const char *groupname,
                                       bool is_sync) {
    if (groupname == NULL) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(pkey) || !key_entry_check(skey)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if (invalid_server_list.size() == 0) {
        log_error("invalidate server list is empty.");
        return TAIR_RETURN_FAILED;
    }
    data_entry mkey;
    merge_key(pkey, skey, mkey);
    //create the request packet.
    request_prefix_hides_by_proxy *req = new request_prefix_hides_by_proxy();
    req->set_group_name(groupname);
    req->area = area;
    req->is_sync = is_sync ? SYNC_INVALID : ASYNC_INVALID;
    req->add_key(mkey.get_data(), mkey.get_size());

    //interact with invalid server(s)
    return do_interaction_with_is(req, 0);
}

//hide the key by invalid server.
int tair_client_impl::hide_by_proxy(int area, const data_entry &key, bool is_sync) {
    return hide_by_proxy(area, key, group_name.c_str(), is_sync);
}

int tair_client_impl::hide_by_proxy(int area, const data_entry &key, const char *groupname, bool is_sync) {
    if (groupname == NULL) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        return TAIR_RETURN_INVALID_ARGUMENT;
    }
    if (!key_entry_check(key)) {
        return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if (invalid_server_list.size() == 0) {
        log_error("invalidate server list is empty.");
        return TAIR_RETURN_FAILED;
    }
    //create the request packet
    request_hide_by_proxy *req = new request_hide_by_proxy();
    req->set_group_name(groupname);
    req->area = area;
    req->is_sync = is_sync ? SYNC_INVALID : ASYNC_INVALID;
    req->add_key(key.get_data(), key.get_size());
    //interact with invlaid server(s)
    return do_interaction_with_is(req, 0);
}

/*-----------------------------------------------------------------------------
   *  tair_client_impl::ErrMsg
   *-----------------------------------------------------------------------------*/

void tair_client_impl::get_server_with_key(const data_entry &key, std::vector<std::string> &servers) {
    if (!key_entry_check(key)) {
        return;
    }

    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
        log_debug("can not find server_id, return false");
        return;
    }

    for (vector<uint64_t>::iterator it = server_list.begin(); it != server_list.end(); ++it) {
        servers.push_back(tbsys::CNetUtil::addrToString(*it));
    }
    return;
}

bool tair_client_impl::get_group_name_list(uint64_t id1, uint64_t id2, std::vector<std::string> &groupnames) {
    if (id1 == 0 && id2 == 0) {
        return false;
    }
    if (initialize() == false) {
        return false;
    }
    start_net();
    inited = true;

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_group_names *req = new request_group_names();
    bool ret = false;
    do {
        if (id1 != 0) {
            if (send_request(id1, req, cwo->get_id()) == false) {
                log_error("send request_group_names packet to %s failed.",
                          tbsys::CNetUtil::addrToString(id1).c_str());
            } else {
                ret = true;
            }
        }
        if (!ret && id2 != 0) {
            if (send_request(id2, req, cwo->get_id()) == false) {
                log_error("send request_group_names packet to %s failed.",
                          tbsys::CNetUtil::addrToString(id2).c_str());
            } else {
                ret = true;
            }
        }
        if (!ret) {
            delete req;
            break;
        }
        base_packet *tpacket = NULL;
        response_group_names *resp = NULL;
        if (get_response(cwo, 1, tpacket) == TAIR_RETURN_TIMEOUT) {
            log_error("get reponse_group_names packet from timeout.");
            ret = false;
            break;
        }
        if (tpacket != NULL && tpacket->getPCode() == TAIR_RESP_GROUP_NAMES_PACKET) {
            resp = dynamic_cast<response_group_names *>(tpacket);
            if (resp != NULL) {
                groupnames = resp->group_name_list;
                ret = true;
            }
        } else {
            ret = false;
            break;
        }
    } while (false);

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;

}

int tair_client_impl::get_servers(std::set<uint64_t> &servers) {
    set <uint64_t> tmp(my_server_list.begin(), my_server_list.end());
    servers.swap(tmp);
    return 0;
}

int tair_client_impl::get_flow(uint64_t addr, int ns, tair::stat::Flowrate &rate) {
    int ret = TAIR_RETURN_SEND_FAILED;

    flow_view_request *request = new flow_view_request();
    request->setNamespace(ns);
    flow_view_response *response = NULL;

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    do {
        ret = send_request(addr, request, cwo->get_id());
        if (ret != 0) {
            delete request;
            break;
        }
        base_packet *temp = NULL;
        ret = get_response(cwo, 1, temp);
        if (ret < 0 || temp == 0) {
            break;
        }
        if (temp->getPCode() != TAIR_RESP_FLOW_VIEW) {
            ret = TAIR_RETURN_FAILED;
            break;
        }

        response = dynamic_cast<flow_view_response *>(temp);
        rate = response->getFlowrate();
    } while (false);
    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
}

int tair_client_impl::set_flow_limit_bound(uint64_t addr, int &ns, int &lower, int &upper, tair::stat::FlowType &type) {
    int ret = TAIR_RETURN_SEND_FAILED;

    flow_control_set *request = new flow_control_set();
    request->setNamespace(ns);
    request->setLimit(lower, upper);
    request->setType(type);
    flow_control_set *response = NULL;

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    do {
        ret = send_request(addr, request, cwo->get_id());
        if (ret != 0) {
            delete request;
            break;
        }
        base_packet *temp = NULL;
        ret = get_response(cwo, 1, temp);
        if (ret < 0 || temp == 0) {
            break;
        }
        if (temp->getPCode() != TAIR_FLOW_CONTROL_SET) {
            ret = TAIR_RETURN_FAILED;
            break;
        }

        response = dynamic_cast<flow_control_set *>(temp);
        ns = response->getNamespace();
        lower = response->getLowerMB();
        upper = response->getUpperMB();
        type = response->getType();
        if (response->isSuccess() == false)
            ret = TAIR_RETURN_FAILED;
    } while (false);
    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
}

int tair_client_impl::set_flow_limit_bound(int ns, int lower, int upper, tair::stat::FlowType type) {
    set <uint64_t> servers;
    get_servers(servers);
    if (servers.empty()) {
        log_error("no available data servers left");
        return TAIR_RETURN_FAILED;
    }
    int rc = TAIR_RETURN_SUCCESS;
    set<uint64_t>::iterator itr = servers.begin();
    int local_ns = ns;
    int local_lower = (int) (lower + servers.size() - 1) / servers.size();
    int local_upper = (int) (upper + servers.size() - 1) / servers.size();
    tair::stat::FlowType local_type = type;
    while (itr != servers.end()) {
        if (*itr == 0) {
            ++itr;
            continue;
        }
        int ret = set_flow_limit_bound(*itr, local_ns, local_lower, local_upper, local_type);
        if (ret != 0) {
            log_error("set flow limit failed: server: %s, ns: %d, lower: %d, upper: %d, type: %d",
                      tbsys::CNetUtil::addrToString(*itr).c_str(), ns, lower, upper, type);
            rc = ret;
        }
        ++itr;
    }
    return rc;
}

int tair_client_impl::packet_handler(easy_request_t *r) {
    base_packet *packet = (base_packet *) r->ipacket;
    int id = (int) ((long) r->args);
    if (packet == NULL) {
        // ipacket is null, timeout
        log_warn("timeout, wait object id: %d", id);
        this_wait_object_manager->wakeup_wait_object(id, NULL);
        easy_session_destroy(r->ms);
        ++send_fail_count;
        return EASY_OK; // return EASY_ERROR will disconnect
    }

    if (id) {
        if (packet->isRegularPacket()) {
            this_wait_object_manager->wakeup_wait_object(id, (base_packet *) packet);
        } else {
            this_wait_object_manager->wakeup_wait_object(id, NULL);
        }
    } else if (packet->isRegularPacket()) {
        if (packet->getPCode() == TAIR_RESP_GET_GROUP_PACKET) {
            response_get_group *rggp = (response_get_group *) packet;
            new_config_version = rggp->config_version;
            if (config_version != new_config_version) {
                uint64_t *server_list = rggp->get_server_list(bucket_count, copy_count);
                for (uint32_t i = 0; server_list != NULL && i < (uint32_t) rggp->server_list_count
                                     && i < my_server_list.size(); i++) {
                    uint64_t data_server = server_list[i];
                    if (rsync_mode_) {
                        uint64_t ds = data_server;
                        data_server = get_rsync_server(ds);
                    }
                    log_debug("update server table: [%d] => [%s]", i,
                              tbsys::CNetUtil::addrToString(data_server).c_str());
                    my_server_list[i] = data_server;
                }
                parse_invalidate_server(rggp);
                log_info("config_version: %u => %u", config_version, rggp->config_version);
                config_version = new_config_version;
            }
        }
        delete packet;
    }
    r->ipacket = NULL;
    easy_session_destroy(r->ms);
    return EASY_OK;
}

//@override Runnable
void tair_client_impl::run(tbsys::CThread *thread, void *arg) {
    easy_helper::set_thread_name("client_asyn");
    int type = static_cast<int> ((reinterpret_cast<long>(arg)));
    if (1 == type) {
        //start the send thread.
        return do_queue_response();
    }

    if (config_server_list.size() == 0U) {
        return;
    }

    int config_server_index = 0;
    //uint32_t old_config_version = 0;
    // int loopCount = 0;

    while (!is_stop) {
        //++loopCount;
        TAIR_SLEEP(is_stop, 1);
        if (is_stop) break;

        if (config_version < TAIR_CONFIG_MIN_VERSION) {
            //ought to update
        } else if (config_version == new_config_version && send_fail_count < UPDATE_SERVER_TABLE_INTERVAL) {
            continue;
        }

        //    if (new_config_version == old_config_version && (loopCount % 30) != 0) {
        //        continue;
        //    }
        // old_config_version = new_config_version;

        config_server_index++;
        config_server_index %= config_server_list.size();
        uint64_t serverId = config_server_list[config_server_index];

        log_warn("send request to configserver(%s), config_version: %u, new_config_version: %d",
                  tbsys::CNetUtil::addrToString(serverId).c_str(), config_version, new_config_version);

        request_get_group *packet = new request_get_group();
        packet->set_group_name(group_name.c_str());
        if (send_request(serverId, packet, 0) != TAIR_RETURN_SUCCESS) {
            log_error("send request_get_group to %s failure.",
                      tbsys::CNetUtil::addrToString(serverId).c_str());
            delete packet;
        }
        send_fail_count = 0;
    }
}

void tair_client_impl::manipulate_ds_to_cs(const vector<uint64_t> &server_id_list, DataserverCtrlOpType cmd,
                                           vector<DataserverCtrlReturnType> &return_code) {
    if (config_server_list.size() == 0) {
        return_code.assign(server_id_list.size(), DATASERVER_CTRL_RETURN_NO_CONFIGSERVER);
        log_warn("config server list is empty");
        return;
    }

    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_data_server_ctrl *req = new request_data_server_ctrl();
    base_packet *tpacket = NULL;
    req->server_id_list = server_id_list;
    req->set_group_name(group_name.c_str());
    req->op_cmd = cmd;
    if (send_request(config_server_list[0], req, cwo->get_id()) == TAIR_RETURN_SUCCESS) {
        if (get_response(cwo, 1, tpacket) == TAIR_RETURN_SUCCESS) {
            response_data_server_ctrl *resp = (response_data_server_ctrl *) tpacket;
            return_code = resp->return_code;
        } else
            return_code.assign(server_id_list.size(), DATASERVER_CTRL_RETURN_TIMEOUT);
    } else {
        return_code.assign(server_id_list.size(), DATASERVER_CTRL_RETURN_SEND_FAILED);
        log_error("send request_data_server_ctrl to %s failed",
                  tbsys::CNetUtil::addrToString(config_server_list[0]).c_str());
        delete req;
    }
    this_wait_object_manager->destroy_wait_object(cwo);
}

void tair_client_impl::get_migrate_status(uint64_t server_id, vector<pair<uint64_t, uint32_t> > &result) {
    if (config_server_list.size() == 0) {
        log_warn("config server list is empty");
        return;
    }

    request_get_migrate_machine *packet = new request_get_migrate_machine();
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_get_migrate_machine *resp = 0;

    packet->server_id = server_id;


    wait_object *cwo = this_wait_object_manager->create_wait_object();


    if ((ret = send_request(config_server_list[0], packet, cwo->get_id())) < 0) {

        delete packet;
        goto FAIL;
    }

    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        goto FAIL;
    }

    if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_MIGRATE_MACHINE_PACKET) {
        goto FAIL;
    }

    resp = (response_get_migrate_machine *) tpacket;
    log_debug("resp->_vec_ms = %lu", resp->vec_ms.size());
    result = resp->vec_ms;

    this_wait_object_manager->destroy_wait_object(cwo);

    return;
    FAIL:

    log_debug("get_migrate_status failed:%s", get_error_msg(ret));
    this_wait_object_manager->destroy_wait_object(cwo);

}

void tair_client_impl::query_from_configserver(uint32_t query_type, const string group_name, map<string, string> &out,
                                               uint64_t serverId) {

    if (config_server_list.size() == 0) {
        log_warn("config server list is empty");
        return;
    }


    request_query_info *packet = new request_query_info();
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_query_info *resp = 0;

    packet->query_type = query_type;
    packet->group_name = group_name;
    packet->server_id = serverId;


    wait_object *cwo = this_wait_object_manager->create_wait_object();


    if ((ret = send_request(config_server_list[0], packet, cwo->get_id())) < 0) {

        delete packet;
        goto FAIL;
    }

    if ((ret = get_response(cwo, 1, tpacket)) < 0) {
        goto FAIL;
    }

    if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_QUERY_INFO_PACKET) {
        goto FAIL;
    }

    resp = (response_query_info *) tpacket;
    out = resp->map_k_v;

    this_wait_object_manager->destroy_wait_object(cwo);

    return;
    FAIL:

    log_debug("query from config server failed:%s", get_error_msg(ret));
    this_wait_object_manager->destroy_wait_object(cwo);

}

/*
   *@brief get statistics info from dataserver specified by serverId
   *
   *@param serverId  specify which dataserver statistics info to fetch, 0 means all dataserver.
   *@parma stat_info  statistics data is stored in stat_info
   *
   *@return  TAIR_RETURN_SUCCESS -- all dataserver statistics is fetched successfully.
   *         TAIR_RETURN_PARTIAL_SUCCESS -- partial dataserver statistics is fetched successfully.
   *         other -- all failed, and log is put into log file.
   */
int tair_client_impl::query_from_dataserver(uint64_t serverId, tair_statistics &stat_info) {
    std::set<uint64_t> ds_set;
    if (serverId != 0)
        ds_set.insert(serverId);
    else
        get_servers(ds_set);

    // this function should not be executed parallelly,
    // a lock here doesn't do a harm to all performance.
    tbsys::CThreadGuard guard(&deschema_lock_);
    map<uint64_t, vector<int64_t> > tmp_stat;
    size_t success = 0, send_success = 0;
    wait_object *cwo;
    cwo = this_wait_object_manager->create_wait_object();
    for (std::set<uint64_t>::const_iterator it = ds_set.begin(); it != ds_set.end(); ++it) {
        request_statistics *packet = new request_statistics();
        // schema info doesn't exist before
        if (deschema_->get_version() == 0)
            packet->set_with_schema(true);
        if (UNLIKELY(send_request(*it, packet, cwo->get_id()) < 0))
            delete packet;
        else
            ++send_success;
    }
    vector<response_statistics *> all_resp;
    vector<base_packet *> tpk;
    int32_t ret = TAIR_RETURN_SEND_FAILED;
    if ((ret = get_response(cwo, send_success, tpk)) < 1) {
        this_wait_object_manager->destroy_wait_object(cwo);
        log_error("all requests are failed");
        return ret;
    }

    for (vector<base_packet *>::const_iterator bp_iter = tpk.begin(); bp_iter != tpk.end(); ++bp_iter) {
        if ((*bp_iter) == NULL || (*bp_iter)->getPCode() != TAIR_RESP_STATISTICS_PACKET)
            continue;
        response_statistics *resp = dynamic_cast<response_statistics *>(*bp_iter);
        new_config_version = resp->config_version_;
        all_resp.push_back(resp);
    }

    bool schema_ok = true;
    //process schema header
    if (UNLIKELY(deschema_->get_version() == 0)) {
        int32_t max_schema_version = -1;
        const response_statistics *resp_with_max_schema_version = NULL;
        for (vector<response_statistics *>::const_iterator it = all_resp.begin(); it != all_resp.end(); ++it)
            if (max_schema_version < (*it)->get_version()) {
                max_schema_version = (*it)->get_version();
                resp_with_max_schema_version = *it;
            }

        if (resp_with_max_schema_version != NULL) {
            char *schema_str = NULL;
            int32_t schema_str_len = 0;
            if (UNLIKELY(resp_with_max_schema_version->get_schema(schema_str, schema_str_len) == false)) {
                log_error("get stat schema fail");
                schema_ok = false;
            } else {
                if (!deschema_->init(schema_str, schema_str_len, resp_with_max_schema_version->get_version()))
                    schema_ok = false;
                delete[] schema_str;
            }
        }
        if (deschema_->get_version() == 0)
            schema_ok = false;
    }// end of if (UNLIKELY(deschema_->get_version() == 0))

    bool need_update_schema = false;
    if (schema_ok) {
        for (vector<response_statistics *>::const_iterator it = all_resp.begin(); it != all_resp.end(); ++it) {
            const response_statistics *resp = *it;

            if (deschema_->get_version() == resp->get_version()) {
                int32_t data_len = 0;
                char *data = NULL;
                if (resp->get_data(data, data_len) == true) {
                    uint64_t dataserver_id = resp->get_peer_id();
                    vector<int64_t> &value = tmp_stat[dataserver_id];
                    value.clear();
                    if (deschema_->deserialize(data, data_len, value) == true)
                        ++success;
                    else {
                        log_error("client schema with version %d deserialize the statistics data of dataserver %s with schema version %d failed",
                                deschema_->get_version(), tbsys::CNetUtil::addrToString(dataserver_id).c_str(),
                                resp->get_version());
                        tmp_stat.erase(dataserver_id);
                    }
                    delete[] data;
                }
            } else {
                log_error("dataserver %s stat schema %d dismatch with client schema version %d",
                          tbsys::CNetUtil::addrToString(resp->get_peer_id()).c_str(),
                          resp->get_version(), deschema_->get_version());
                if (deschema_->get_version() < resp->get_version())
                    need_update_schema = true;
            }// end of if (deschema_->get_version() == resp->get_version())
        } // end of for
    } // end of schema ok
    this_wait_object_manager->destroy_wait_object(cwo);

    const vector<string> &deschema_desc = deschema_->get_desc();
    if (deschema_->get_desc().empty() || 0 == success)
        ret = TAIR_RETURN_FAILED;
    else { // load statistics
        stat_info.load_data(deschema_desc, tmp_stat);
        if (ds_set.size() == success)
            ret = TAIR_RETURN_SUCCESS;
        else
            ret = TAIR_RETURN_PARTIAL_SUCCESS;
    }
    if (need_update_schema)
        deschema_->clear();
    return ret;
}

int64_t tair_client_impl::ping(uint64_t server_id) {
    if (!inited) {
        if (!startup(server_id))
            return 0;
    }

    request_ping *req = new request_ping();
    req->value = 0;
    base_packet *bp = NULL;
    wait_object *cwo = this_wait_object_manager->create_wait_object();

    struct timeval tm_beg;
    struct timeval tm_end;
    gettimeofday(&tm_beg, NULL);
//    bool ret = true;
    do {
        if (send_request(server_id, req, cwo->get_id()) != 0) {
            log_error("send ping packet failed.");
//        ret = false;
            delete req;
            break;
        }

        if (get_response(cwo, 1, bp) != 0) {
            log_error("get ping packet timeout.");
//        ret = false;
            break;
        }
        if (bp == NULL || bp->getPCode() != TAIR_RESP_RETURN_PACKET) {
            log_error("got bad packet.");
//        ret = false;
            break;
        }
    } while (false);
    gettimeofday(&tm_end, NULL);

    this_wait_object_manager->destroy_wait_object(cwo);
    return static_cast<int64_t>((tm_end.tv_sec - tm_beg.tv_sec) * 1000000
                                + (tm_end.tv_usec - tm_beg.tv_usec));
}

int
tair_client_impl::retrieve_server_config(bool update_server_table, tbsys::STR_STR_MAP &config_map, uint32_t &version) {
    if (config_server_list.size() == 0U || group_name.empty()) {
        log_warn("config server list is empty, or groupname is NULL, return false");
        return TAIR_RETURN_FAILED;
    }
    wait_object *cwo = 0;
    int send_success = 0;

    base_packet *tpacket = NULL;

    for (uint32_t i = 0; i < config_server_list.size(); i++) {
        uint64_t server_id = config_server_list[i];
        request_get_group *packet = new request_get_group();
        packet->set_group_name(group_name.c_str());

        cwo = this_wait_object_manager->create_wait_object();
        if (send_request(server_id, packet, cwo->get_id()) != TAIR_RETURN_SUCCESS) {
            log_error("Send RequestGetGroupPacket to %s failure.",
                      tbsys::CNetUtil::addrToString(server_id).c_str());
            this_wait_object_manager->destroy_wait_object(cwo);

            delete packet;
            continue;
        }
        cwo->wait_done(1, timeout);
        tpacket = cwo->get_packet();
        if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_GROUP_PACKET) {
            log_error("get group packet failed,retry");
            tpacket = 0;

            this_wait_object_manager->destroy_wait_object(cwo);

            cwo = 0;
            continue;
        } else {
            ++send_success;
            break;
        }
    }

    if (send_success <= 0) {
        // return timeout when get_group req not get response
        log_error("cann't connect");
        return TAIR_RETURN_TIMEOUT;
    }

    int ret = TAIR_RETURN_SUCCESS;
    response_get_group *rggp = dynamic_cast<response_get_group *>(tpacket);

    if (rggp->config_version <= 0) {
        log_error("group doesn't exist: %s", group_name.c_str());
        ret = TAIR_RETURN_FAILED;
    } else {
        bucket_count = rggp->bucket_count;
        copy_count = rggp->copy_count;

        if (bucket_count <= 0 || copy_count <= 0) {
            log_error("bucket or copy count doesn't correct");
            ret = TAIR_RETURN_FAILED;
        } else {
            if (update_server_table) {           // update server table
                uint32_t server_list_count = 0;
                uint64_t *server_list = rggp->get_server_list(bucket_count, copy_count);
                if (rggp->server_list_count <= 0 || server_list == NULL) {
                    if (!force_service) {
                        log_warn("server table is empty");
                        ret = TAIR_RETURN_FAILED;
                    }
                } else {
                    server_list_count = (uint32_t) (rggp->server_list_count);
                    assert(server_list_count == bucket_count * copy_count);
                    for (uint32_t i = 0; server_list != 0 && i < server_list_count; ++i) {
                        uint64_t data_server = server_list[i];
                        if (rsync_mode_) {
                            uint64_t ds = data_server;
                            data_server = get_rsync_server(ds);
                        }
                        log_debug("server table: [%d] => [%s]", i, tbsys::CNetUtil::addrToString(data_server).c_str());
                        my_server_list.push_back(data_server);
                    }
                }
            }

            if (TAIR_RETURN_SUCCESS == ret) {
                new_config_version = config_version = rggp->config_version;
                // set return value
                config_map = rggp->config_map;
                version = rggp->config_version;
            }
        }
    }

    this_wait_object_manager->destroy_wait_object(cwo);

    return TAIR_RETURN_SUCCESS;
}

void tair_client_impl::get_buckets_by_server(uint64_t server_id, std::set<int32_t> &buckets) {
    if (server_id <= 0) {
        return;
    }

    // only parse the first copy of bucket in bucket server list.
    for (size_t i = 0; i < bucket_count; i++) {
        if (my_server_list[i] == server_id) {
            buckets.insert(i);
        }
    }
}

bool tair_client_impl::retrieve_server_addr() {
    if (config_server_list.size() == 0U || group_name.empty()) {
        log_warn("config server list is empty, or groupname is NULL, return false");
        return false;
    }
    wait_object *cwo = 0;
    int send_success = 0;

    response_get_group *rggp = 0;
    base_packet *tpacket = NULL;

    for (uint32_t i = 0; i < config_server_list.size(); i++) {
        uint64_t server_id = config_server_list[i];
        request_get_group *packet = new request_get_group();
        packet->set_group_name(group_name.c_str());


        cwo = this_wait_object_manager->create_wait_object();
        if (send_request(server_id, packet, cwo->get_id()) != TAIR_RETURN_SUCCESS) {
            log_error("Send RequestGetGroupPacket to %s failure.",
                      tbsys::CNetUtil::addrToString(server_id).c_str());
            this_wait_object_manager->destroy_wait_object(cwo);

            delete packet;
            continue;
        }
        cwo->wait_done(1, timeout);
        tpacket = cwo->get_packet();
        if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_GROUP_PACKET) {
            log_error("get group packet failed,retry");
            tpacket = 0;

            this_wait_object_manager->destroy_wait_object(cwo);

            cwo = 0;
            continue;
        } else {
            ++send_success;
            break;
        }
    }

    if (send_success <= 0) {
        //delete packet;
        log_error("cann't connect");
        return false;
    }
    uint64_t *server_list = NULL;
    uint32_t server_list_count = 0;
    long heart_type = 0;
    long response_type = 1;

    rggp = dynamic_cast<response_get_group *>(tpacket);
    if (rggp->config_version <= 0) {
        log_error("group doesn't exist: %s", group_name.c_str());
        goto OUT;
    }
    bucket_count = rggp->bucket_count;
    copy_count = rggp->copy_count;

    if (bucket_count <= 0 || copy_count <= 0) {
        log_error("bucket or copy count doesn't correct");
        goto OUT;
    }
    server_list = rggp->get_server_list(bucket_count, copy_count);
    if (!force_service && (rggp->server_list_count <= 0 || server_list == 0)) {
        log_warn("server table is empty");
        goto OUT;
    }

    server_list_count = (uint32_t) (rggp->server_list_count);
    if (!force_service && (server_list_count != bucket_count * copy_count)) {
        log_error("server table is wrong, server_list_count: %u, bucket_count: %u, copy_count: %u",
                  server_list_count, bucket_count, copy_count);
        goto OUT;
    }

    for (uint32_t i = 0; server_list != 0 && i < server_list_count; ++i) {
        uint64_t data_server = server_list[i];
        if (rsync_mode_) {
            uint64_t ds = data_server;
            data_server = get_rsync_server(ds);
        }
        log_debug("server table: [%d] => [%s]", i, tbsys::CNetUtil::addrToString(data_server).c_str());
        my_server_list.push_back(data_server);
    }

    new_config_version = config_version = rggp->config_version; //set the same value on the first time.

    //~ parse invalid servers
    parse_invalidate_server(rggp);

    this_wait_object_manager->destroy_wait_object(cwo);

    if (!light_mode_) {
        thread.start(this, (void *) heart_type);
        response_thread.start(this, (void *) response_type);
    }

    return true;
    OUT:
    this_wait_object_manager->destroy_wait_object(cwo);

    return false;
}

void tair_client_impl::parse_invalidate_server(response_get_group *resp) {
    tbsys::STR_STR_MAP::iterator it;
    it = resp->config_map.find("invalidate_server");
    if (it == resp->config_map.end()) {
        log_warn("no invalid server info found.");
        return;
    }

    char *str = strdup(it->second.c_str());
    vector<char *> iplist;
    tbsys::CStringUtil::split(str, ";,", iplist);

    std::vector<uint64_t> tmp;
    tmp.reserve(TAIR_MAX_INVALSVR_CNT);

    for (size_t i = 0; i < TAIR_MAX_INVALSVR_CNT && i < iplist.size(); ++i) {
        uint64_t id = tbsys::CNetUtil::strToAddr(iplist[i], TAIR_INVAL_SERVER_DEFAULT_PORT);
        if (id != 0 && easy_helper::is_alive(id)) {
            log_warn("got invalid server %s.", iplist[i]);
            tmp.push_back(id);
        }
    }
    free(str);
    //~ duplicate tmp, if neccessary.
    size_t size = tmp.size();
    if (size > 0) {
        size_t loop = TAIR_MAX_INVALSVR_CNT / size;
        for (size_t i = 1; i < loop; ++i) {
            for (size_t j = 0; j < size; ++j) {
                tmp.push_back(tmp[j]);
            }
        }
    }
    //~ replace invalid_server_list with tmp.
    invalid_server_list.swap(tmp);
}

void tair_client_impl::shrink_invalidate_server(uint64_t server_id) {
    //~ remove server_id from invalid_server_list.
    std::vector<uint64_t> tmp;
    tmp.reserve(TAIR_MAX_INVALSVR_CNT);
    for (size_t i = 0; i < invalid_server_list.size(); ++i) {
        if (invalid_server_list[i] != server_id) {
            tmp.push_back(invalid_server_list[i]);
        }
    }
    //~ duplicate
    size_t size = tmp.size();
    if (size > 0) {
        size_t loop = TAIR_MAX_INVALSVR_CNT / size;
        for (size_t i = 1; i < loop; ++i) {
            for (size_t j = 0; j < size; ++j) {
                tmp.push_back(tmp[j]);
            }
        }
    }
    invalid_server_list.swap(tmp);
}

uint32_t tair_client_impl::get_config_version() const {
    return config_version;
}

void tair_client_impl::start_net() {
    easy_io_start(&eio);
}


void tair_client_impl::stop_net() {
    easy_io_stop(&eio);
}

void tair_client_impl::wait_net() {
    easy_io_wait(&eio);
}

void tair_client_impl::reset() //reset enviroment
{
    timeout = 2000;
    config_version = 0;
    new_config_version = 0;
    send_fail_count = 0;
    bucket_count = 0;
    copy_count = 0;
    my_server_list.clear();
    config_server_list.clear();
    group_name = "";

    this_wait_object_manager = 0;
    inited = false;
}

bool tair_client_impl::get_server_id(int32_t bucket, vector<uint64_t> &server, bool only_slaves) {
    if (this->direct) {
        server.push_back(this->data_server);
        return true;
    }
    for (uint32_t i = (uint32_t) only_slaves; i < copy_count && i < my_server_list.size(); ++i) {
        uint64_t server_id = my_server_list[bucket + i * bucket_count];
        if (server_id != 0) {
            server.push_back(server_id);
        }
    }
    return server.size() > 0 ? true : false;
}

bool tair_client_impl::get_server_id(const data_entry &key, vector<uint64_t> &server) {
    if (this->direct) {
        server.push_back(this->data_server);
        return true;
    }

    uint32_t hash;
    int prefix_size = key.get_prefix_size();
    int32_t diff_size = key.has_merged ? TAIR_AREA_ENCODE_SIZE : 0;
    if (prefix_size == 0) {
        hash = util::string_util::mur_mur_hash(key.get_data() + diff_size, key.get_size() - diff_size);
    } else {
        hash = util::string_util::mur_mur_hash(key.get_data() + diff_size, prefix_size);
    }
    server.clear();

    if (my_server_list.size() > 0U) {
        hash %= bucket_count;
        for (uint32_t i = 0; i < copy_count && i < my_server_list.size(); ++i) {
            uint64_t server_id = my_server_list[hash + i * bucket_count];
            if (server_id != 0) {
                server.push_back(server_id);
            }
        }
    }
    return server.size() > 0 ? true : false;
}

void flow_control_callback_mock(int retcode, void *arg) {}

void tair_client_impl::check_flow_down(uint64_t server_id, int ns) {
    flow_check *packet = new flow_check();
    packet->setNamespace(ns);

    wait_object *cwo = this_wait_object_manager->create_wait_object(TAIR_FLOW_CHECK, flow_control_callback_mock, NULL);

    if (easy_helper::easy_async_send(&eio, server_id, packet, (void *) ((long) cwo->get_id()), &handler, 500) !=
        EASY_OK) {
        this_wait_object_manager->destroy_wait_object(cwo);
        delete packet;
        log_error("Send to %s failure.",
                  tbsys::CNetUtil::addrToString(server_id).c_str());
    }
}

int tair_client_impl::send_request(uint64_t server_id, base_packet *packet, int waitId) {
    //TODO: check flow
    int ret = TAIR_RETURN_SUCCESS;
    if (server_id == 0 || packet == NULL) {
        log_error("param invalid. server id: %"PRI64_PREFIX"u", server_id);
        ret = TAIR_RETURN_FAILED;
    } else if (packet->ns() != 0 && flow_admin_.is_over_flow(server_id, packet->ns())) {
        log_info("flow control [%s] packet:%d",
                  tbsys::CNetUtil::addrToString(server_id).c_str(),
                  packet->_packetHeader._pcode);
        if (flow_admin_.should_check_flow_down(server_id, packet->ns())) {
            check_flow_down(server_id, packet->ns());
        }
        ret = TAIR_RETURN_FLOW_CONTROL;
    } else {
        if (easy_helper::easy_async_send(&eio, server_id, packet, (void *) ((long) waitId), &handler, timeout) !=
            EASY_OK) {
            log_error("Send to %s failure.",
                      tbsys::CNetUtil::addrToString(server_id).c_str());
            send_fail_count++;
            ret = TAIR_RETURN_SEND_FAILED;
        }
    }
    return ret;
}

#if 0
                                                                                                                        int tair_client_impl::send_request(vector<uint64_t>& server,base_packet *packet,int waitId)
  {
    assert(!server.empty() && packet != 0 && waitId >= 0);

    int send_success = 0;
    for(uint32_t i=0;i<server.size();++i){
      uint64_t serverId = server[i];
      if (conn_manager->sendPacket(serverId, packet, NULL, (void*)((long)waitId)) == false) {
        log_error("Send RequestGetPacket to %s failure.",
            tbsys::CNetUtil::addrToString(serverId).c_str());
        ++m_sendFailCount;
        continue;
      }
      ++send_success;
      break;
    }
    return send_success > 0 ? TAIR_RETURN_SUCCESS : TAIR_RETURN_SEND_FAILED;
  }
#endif


int tair_client_impl::get_response(wait_object *cwo, int wait_count, base_packet *&tpacket) {
    int ret = TAIR_RETURN_SUCCESS;
    if (cwo == NULL) {
        log_error("param invalid, cwo is null, wait count: %d", wait_count);
        ret = TAIR_RETURN_FAILED;
    } else {
        cwo->wait_done(wait_count, timeout);
        base_packet *packet = cwo->get_packet();
        if (packet == 0) {
            ret = TAIR_RETURN_TIMEOUT;
        }
        tpacket = packet;
    }
    return ret;
}

int tair_client_impl::get_response(wait_object *cwo, int wait_count, vector<base_packet *> &tpacket) {
    if (cwo == NULL) {
        log_error("param invalid, cwo is null, wait count: %d", wait_count);
        return TAIR_RETURN_FAILED;
    }
    cwo->wait_done(wait_count, timeout);
    int r_num = cwo->get_packet_count();
    int push_num = (r_num < wait_count) ? r_num : wait_count;

    for (int idx = 0; idx < push_num; ++idx) {
        base_packet *packet = cwo->get_packet(idx);
        if (packet == NULL) {
            return TAIR_RETURN_TIMEOUT;
        }
        tpacket.push_back(packet);
    }
    return push_num;
}

int tair_client_impl::get_response_new(wait_object *cwo, int wait_count, vector<base_packet *> &tpacket) {
    if (cwo == NULL) {
        log_error("param invalid, cwo is null, wait count: %d", wait_count);
        return TAIR_RETURN_FAILED;
    }
    cwo->wait_done(wait_count, timeout);
    int r_num = cwo->get_packet_count();
    int push_num = (r_num < wait_count) ? r_num : wait_count;

    for (int idx = 0; idx < push_num; ++idx) {
        base_packet *packet = cwo->get_packet(idx);
        if (packet != NULL)
            tpacket.push_back(packet);
    }
    return push_num;
}

bool tair_client_impl::key_entry_check(const data_entry &key) {
    if (key.get_size() == 0 || key.get_data() == 0) {
        return false;
    }
    if ((key.get_size() == 1) && (*(key.get_data()) == '\0')) {
        return false;
    }
    if (key.get_size() >= TAIR_MAX_KEY_SIZE) {
        return false;
    }
    return true;
}


bool tair_client_impl::data_entry_check(const data_entry &data) {
    if (data.get_size() == 0 || data.get_data() == 0) {
        return false;
    }
    if (data.get_size() >= TAIR_MAX_DATA_SIZE) {
        return false;
    }
    return true;
}

int tair_client_impl::invoke_callback(void *phandler, wait_object *obj) {
    tair_client_impl *pclient = (tair_client_impl *) phandler;
    return pclient->push_waitobject(obj);
}

int tair_client_impl::push_waitobject(wait_object *obj) {
    //push it to queue,let another thread to send it.
    //todo check queue length;
    log_debug("push_waitobject to queue,id=%d\n", obj->get_id());
    m_return_object_queue.put(obj);
    return 0;
}

int tair_client_impl::handle_response_obj(wait_object *cwo) {
    log_debug("do_async_response id=%d\n", cwo->get_id());
    base_packet *tpacket = cwo->get_packet();

    uint64_t data_server_id = cwo->get_data_server_id();
    if (rsync_mode_) {
        timed_collections_->decr_count(data_server_id);
    }

    if (tpacket == 0) {
        //tell the caller is failed with timeout.
        return do_async_timeout(cwo);
    }
    //now check the response code.
    int _cmd = tpacket->getPCode();

    if (cwo->get_cmd() == TAIR_REQ_SYNC_PACKET && _cmd != TAIR_RESP_RETURN_PACKET) {
        log_error("sync request get unknown response pcode: %d\n", _cmd);
        return cwo->do_async_response(TAIR_RETURN_TIMEOUT);
    }

    int ret;
    switch (_cmd) {
        case TAIR_RESP_RETURN_PACKET: {
            response_return *resp_return = NULL;
            resp_return = (response_return *) tpacket;
            new_config_version = resp_return->config_version;
            ret = resp_return->get_code();
            if (ret != TAIR_RETURN_SUCCESS) {
                if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
                    //update server table immediately
                    send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
                }
            }
            return cwo->do_async_response(ret);
        }
        case TAIR_RESP_MRETURN_PACKET: {
            response_mreturn *resp_mreturn = NULL;
            resp_mreturn = (response_mreturn *) tpacket;
            new_config_version = resp_mreturn->config_version;
            ret = resp_mreturn->get_code();
            if (ret != TAIR_RETURN_SUCCESS) {
                if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
                    //update server table immediately
                    send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
                }
            }
            return cwo->do_async_response(ret, resp_mreturn->key_code_map);
            break;
        }
        case TAIR_RESP_GET_PACKET: {
            response_get *resp = (response_get *) tpacket;
            new_config_version = resp->config_version;
            ret = resp->get_code();
            return cwo->do_async_response(ret, resp->key, resp->data);
            break;
        }
        case TAIR_RESP_MC_OPS_PACKET: {
            response_mc_ops *resp = (response_mc_ops *) tpacket;
            new_config_version = resp->config_version;
            ret = resp->code;
            return cwo->do_async_response(ret, resp);
            break;
        }
        default:
            log_error("unknown pcode: %d\n", _cmd);
            break;
    }
    return 0;
}

int tair_client_impl::do_async_timeout(wait_object *cwo) {
    ++send_fail_count;
    switch (cwo->get_cmd()) {
        case TAIR_REQ_PUT_PACKET:
        case TAIR_REQ_REMOVE_PACKET:
        case TAIR_REQ_SYNC_PACKET:
            return cwo->do_async_response(TAIR_RETURN_TIMEOUT);
            break;
        case TAIR_REQ_GET_PACKET:
            return cwo->do_async_response(TAIR_RETURN_TIMEOUT, (data_entry *) NULL, (data_entry *) NULL);
            break;
        case TAIR_REQ_MC_OPS_PACKET:
            return cwo->do_async_response(TAIR_RETURN_TIMEOUT, (response_mc_ops *) NULL);
            break;
        case TAIR_FLOW_CHECK:
            return cwo->do_async_response(TAIR_RETURN_TIMEOUT);
            break;
        default:
            return cwo->do_async_response(TAIR_RETURN_TIMEOUT, (key_code_map_t *) NULL);
    }
}

void tair_client_impl::do_queue_response() {
    log_debug("start thread do_queue_response");
    while (!is_stop) {
        //TAIR_SLEEP(is_stop, 1);
        //if (is_stop) break;
        try {
            wait_object *cwo = m_return_object_queue.get(1000); //1s
            if (!cwo) continue;
            handle_response_obj(cwo);
            delete cwo;
        }
        catch (...) {
            log_warn("unknow error! get timeoutqueue error!");
        }
    }
}

const char *tair_client_impl::get_group_name() {
    return this->group_name.c_str();
}

int tair_client_impl::get_rt(uint64_t sid, uint32_t cmd,
                             int op, std::string *str, bool json) {
    int ret = TAIR_RETURN_SUCCESS;
    request_rt *req = new request_rt;
    base_packet *bp = NULL;
    req->opcode_ = op;
    req->cmd_ = static_cast<request_rt::RT_TYPE>(cmd);
    wait_object *cwo = this_wait_object_manager->create_wait_object();
    do {
        if (send_request(sid, req, cwo->get_id()) != 0) {
            log_error("send packet failed %s", tbsys::CNetUtil::addrToString(sid).c_str());
            ret = TAIR_RETURN_SEND_FAILED;
            delete req;
            break;
        }
        if (get_response(cwo, 1, bp) != 0 ||
            bp == NULL ||
            bp->getPCode() != TAIR_RESP_RT_PACKET) {
            log_error("%s timeout", tbsys::CNetUtil::addrToString(sid).c_str());
            ret = TAIR_RETURN_TIMEOUT;
            break;
        }
        response_rt *resp = (response_rt *) bp;
        if (str != NULL) {
            resp->serialize(*str, json);
        }
    } while (false);

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
}

void init_client_handler(easy_io_handler_pt *handler, void *user_data) {
    easy_helper::init_handler(handler, user_data);
    handler->cleanup = tair_cleanup_cb_client_wrapper;
}

int tair_cleanup_cb_client_wrapper(easy_request_t *r, void *packet) {
    if (r == NULL && packet != NULL && ((base_packet *) packet)->_packetHeader._pcode == TAIR_FLOW_CONTROL) {
        flow_control *fc_packet = (flow_control * )(packet);
        void *user_data = fc_packet->get_user_data();
        if (user_data != NULL) {
            uint64_t serverid = easy_helper::convert_addr(fc_packet->get_addr());
            tair_client_impl *client = (tair_client_impl *) user_data;
            client->get_flow_admin().control(
                    serverid,
                    fc_packet->get_ns(),
                    static_cast<tair::stat::FlowStatus>(fc_packet->get_status())
            );
        }
    }
    return easy_helper::tair_cleanup_cb(r, packet);
}

}
