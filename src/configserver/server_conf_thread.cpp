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

#include "server_conf_thread.hpp"
#include "server_info_allocator.hpp"
#include "util.hpp"
#include "response_return_packet.hpp"
#include "easy_helper.hpp"
#include "packet_factory.hpp"

namespace {
const int HARD_CHECK_MIG_COMPLETE = 300;
}

namespace tair {
namespace config_server {
server_conf_thread::server_conf_thread(easy_io_t *eio) : builder_thread(this) {
    master_config_server_id = 0;
    struct timespec tm;
    clock_gettime(CLOCK_MONOTONIC, &tm);
    heartbeat_curr_time = tm.tv_sec;
    stat_interval_time = 5;
    is_ready = false;
    down_slave_config_server = 0U;

    this->eio = eio;
    easy_helper::init_handler(&handler, this);
    handler.process = packet_handler_cb;
}

server_conf_thread::~server_conf_thread() {
    group_info_map::iterator it;
    vector<group_info *> group_info_list;
    for (it = group_info_map_data.begin(); it != group_info_map_data.end();
         ++it) {
        group_info_list.push_back(it->second);
    }
    for (size_t i = 0; i < group_info_list.size(); i++) {
        delete group_info_list[i];
    }
}

int server_conf_thread::packet_handler(easy_request_t *r) {
    int rc = r->ipacket != NULL ? EASY_OK : EASY_ERROR;
    easy_session_destroy(r->ms);
    return rc;
}

uint32_t server_conf_thread::get_file_time(const char *file_name) {
    if (file_name == NULL) {
        return 0;
    }
    struct stat stats;
    if (lstat(file_name, &stats) == 0) {
        return stats.st_mtime;
    }
    return 0;
}

void server_conf_thread::load_group_file(const char *file_name,
                                         uint32_t version,
                                         uint64_t sync_config_server_id) {
    if (file_name == NULL)
        return;
    tbsys::CThreadGuard guard(&group_file_mutex);
    tbsys::CConfig config;
    if (TAIR_RETURN_SUCCESS != load_conf(file_name, config)) {
        log_error("load config file %s error", file_name);
        return;
    }

    log_info("begin load group file, filename: %s, version: %d", file_name,
             version);
    vector<string> section_list;
    config.getSectionName(section_list);
    // start up, get hash table from an other config server if exist one.
    if (sync_config_server_id && sync_config_server_id != util::local_server_ip::ip) {
        get_server_table(sync_config_server_id, NULL, GROUP_CONF);
        // here we need reload group configfile, since the file has been synced
        if (config.load((char *) file_name) == EXIT_FAILURE) {
            log_error("reload config file error: %s", file_name);
            return;
        }
        section_list.clear();
        config.getSectionName(section_list);
        for (size_t i = 0; i < section_list.size(); i++) {
          get_server_table(sync_config_server_id, (char *)section_list[i].c_str(), GROUP_DATA);
        }
    }

    set<uint64_t> server_id_list;
    group_info_rw_locker.wrlock();
    server_info_rw_locker.wrlock();
    for (size_t i = 0; i < section_list.size(); i++) {
        group_info_map::iterator it =
                group_info_map_data.find(section_list[i].c_str());
        group_info *group_info_tmp = NULL;
        if (it == group_info_map_data.end()) {
            group_info_tmp =
                    new group_info((char *) section_list[i].c_str(),
                                   &data_server_info_map, eio);
            group_info_map_data[group_info_tmp->get_group_name()] =
                    group_info_tmp;
        } else {
            group_info_tmp = it->second;
            // rebuild failover status here, table not change
            group_info_tmp->force_check_failover();
        }
        group_info_tmp->load_config(config, version, server_id_list);
    }

    set<uint64_t> map_id_list;
    set<uint64_t> should_del_id_list;
    server_info_map::iterator sit;
    for (sit = data_server_info_map.begin();
         sit != data_server_info_map.end(); ++sit) {
        map_id_list.insert(sit->second->server_id);
    }
    std::set_difference(map_id_list.begin(), map_id_list.end(),
                        server_id_list.begin(), server_id_list.end(),
                        inserter(should_del_id_list,
                                 should_del_id_list.begin()));
    for (set<uint64_t>::iterator it = should_del_id_list.begin();
         it != should_del_id_list.end(); ++it) {
        sit = data_server_info_map.find(*it);
        if (sit != data_server_info_map.end()) {
            sit->second->server_id = 0;
            data_server_info_map.erase(sit);
        }
    }
    for (group_info_map::iterator it = group_info_map_data.begin();
         it != group_info_map_data.end(); ++it) {
        it->second->correct_server_info(sync_config_server_id
                                        && sync_config_server_id !=
                                           util::local_server_ip::ip);
        it->second->find_available_server();
    }
    log_info("machine count: %lu, group count: %lu",
             data_server_info_map.size(), group_info_map_data.size());
    server_info_rw_locker.unlock();
    group_info_rw_locker.unlock();
}

void server_conf_thread::check_server_status(uint32_t loop_count) {
    set<group_info *> change_group_info_list;

    group_info_map::iterator it;
    for (it = group_info_map_data.begin(); it != group_info_map_data.end(); ++it) {
        if (it->second->is_need_rebuild())
            change_group_info_list.insert(it->second);
    }
    for (server_info_map::iterator sit = data_server_info_map.begin(); sit != data_server_info_map.end(); ++sit) {
        if (sit->second->status == server_info::DOWN)
            continue;
        //a bad one is alive again, we do not mark it ok unless some one tould us to.
        //administrator can touch group.conf to let these data serve alive again.
        // this decide how many seconds since last heart beat, we will mark this data server as a bad one.
        int32_t down_interval;
        if (LIKELY(sit->second->group_info_data))
            down_interval = sit->second->group_info_data->get_server_down_time();
        else
            down_interval = TAIR_SERVER_DOWNTIME;

        uint32_t one_interval_before = heartbeat_curr_time - down_interval;

        // recieve heartbeat in one interval,last time belong to [one_interval_before, +Infinity), ok
        if (LIKELY(sit->second->last_time >= one_interval_before))
            continue;

        // recieve heartbeat, last time belong to [two_interval_before, one_interval_before) && is_alive() == true, ok
        uint32_t two_interval_before = one_interval_before - down_interval;
        if ((sit->second->last_time >= two_interval_before) && easy_helper::is_alive(sit->second->server_id, eio))
            continue;

        //  (+Infinity, two_interval_before), or ([two_interval_before, one_interval_before) && is_alive() == false), down
        if (sit->second->status == server_info::ALIVE) {
            // rebuild, downhost
            change_group_info_list.insert(sit->second->group_info_data);
            sit->second->status = server_info::DOWN;
            log_warn("HOST DOWN: %s lastTime is %u now is %u ",
                     tbsys::CNetUtil::addrToString(sit->second->server_id).
                             c_str(), sit->second->last_time, heartbeat_curr_time);
            // try to failover this down ds.
            if (master_config_server_id == util::local_server_ip::ip)
                sit->second->group_info_data->pending_failover_server(sit->first);

            // if (need add down server config) then set downserver in group.conf
            if (sit->second->group_info_data->get_pre_load_flag() == 1) {
                log_error("add down host: %s lastTime is %u now is %u ",
                          tbsys::CNetUtil::addrToString(sit->second->server_id).
                                  c_str(), sit->second->last_time, heartbeat_curr_time);
                sit->second->group_info_data->add_down_server(sit->second->server_id);
                sit->second->group_info_data->set_force_send_table();
            }
        } else if (sit->second->status == server_info::STANDBY) {
            // standby server down, no need to rebuild, only set status
            sit->second->status = server_info::DOWN;
            log_warn("STANDBY NODE DOWN: %s lastTime is %u now is %u ",
                     tbsys::CNetUtil::addrToString(sit->second->server_id).
                             c_str(), sit->second->last_time, heartbeat_curr_time);
        }
    } // end of for()
    // only master config server can update hashtable.
    if (master_config_server_id != util::local_server_ip::ip)
        return;

    uint64_t slave_server_id = get_slave_server_id();

    group_info_rw_locker.rdlock();
    if (loop_count % HARD_CHECK_MIG_COMPLETE == 0) {
        for (group_info_map::const_iterator it = group_info_map_data.begin();
             it != group_info_map_data.end(); it++) {
            it->second->hard_check_migrate_complete(slave_server_id);
        }
    }
    group_info_rw_locker.unlock();

    for (it = group_info_map_data.begin(); it != group_info_map_data.end();
         ++it) {
        it->second->check_migrate_complete(slave_server_id, &group_info_rw_locker);
        it->second->check_recovery_complete(slave_server_id, &group_info_rw_locker);
    }

    group_info_rw_locker.rdlock();
    for (it = group_info_map_data.begin(); it != group_info_map_data.end(); ++it) {
        // check if send server_table
        if (1 == it->second->get_send_server_table()) {
            log_info("group: %s need send server table", it->second->get_group_name());
            it->second->send_server_table_packet(slave_server_id);
            it->second->reset_force_send_table();
        }
    }

    if (change_group_info_list.size() == 0U) {
        group_info_rw_locker.unlock();
        return;
    }

    set<group_info *>::iterator cit;
    tbsys::CThreadGuard guard(&mutex_grp_need_build);
    for (cit = change_group_info_list.begin(); cit != change_group_info_list.end(); ++cit) {
        builder_thread.group_need_build.push_back(*cit);
        (*cit)->set_table_builded();
    }
    group_info_rw_locker.unlock();
}

void server_conf_thread::check_config_server_status(uint32_t loop_count) {
    if (config_server_info_list.size() != 2U) {
        return;
    }
    bool master_status_changed = false;

    uint32_t now = heartbeat_curr_time - TAIR_SERVER_DOWNTIME * 2;
    bool config_server_up = false;
    for (size_t i = 0; i < config_server_info_list.size(); i++) {
        server_info *server_info_tmp = config_server_info_list[i];
        if (server_info_tmp->server_id == util::local_server_ip::ip) {        // this is myself
            continue;
        }
        if (server_info_tmp->last_time < now && server_info_tmp->status == server_info::ALIVE) {        // downhost
            if (easy_helper::is_alive(server_info_tmp->server_id, eio) ==
                true)
                continue;
            server_info_tmp->status = server_info::DOWN;
            if (i == 0)
                master_status_changed = true;
            log_warn("CONFIG HOST DOWN: %s",
                     tbsys::CNetUtil::addrToString(server_info_tmp->server_id).
                             c_str());
        } else if (server_info_tmp->last_time > now && server_info_tmp->status == server_info::DOWN) {        // uphost
            server_info_tmp->status = server_info::ALIVE;
            if (i == 0)
                master_status_changed = true;
            config_server_up = true;
            log_warn("CONFIG HOST UP: %s",
                     tbsys::CNetUtil::addrToString(server_info_tmp->server_id).
                             c_str());
        }
    }

    if (master_status_changed == false) {
        if (config_server_up
            && master_config_server_id == util::local_server_ip::ip) {
            uint64_t slave_id = get_slave_server_id();
            group_info_map::iterator it;
            group_info_rw_locker.rdlock();
            for (it = group_info_map_data.begin();
                 it != group_info_map_data.end(); ++it) {
                // for we can't get peer's version from protocol, add 10 to client version and server
                it->second->inc_version(server_up_inc_step);
                it->second->send_server_table_packet(slave_id);
                log_warn("config server up and master not changed, version changed. "
                                 "group name: %s, client version: %u, server version: %u",
                         it->second->get_group_name(), it->second->get_client_version(),
                         it->second->get_server_version());
            }
            group_info_rw_locker.unlock();
        }
        return;
    }

    server_info *server_info_master = config_server_info_list[0];
    if (server_info_master->status == server_info::ALIVE) {
        master_config_server_id = server_info_master->server_id;
    } else {
        master_config_server_id = util::local_server_ip::ip;
    }
    if (master_config_server_id == util::local_server_ip::ip) {
        const char *group_file_name =
                TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        if (group_file_name == NULL) {
            log_error("not found %s:%s ", CONFSERVER_SECTION, TAIR_GROUP_FILE);
            return;
        }

        uint32_t curr_version = get_file_time(group_file_name);
        load_group_file(group_file_name, curr_version, 0);
    }
    log_warn("MASTER_CONFIG changed %s",
             tbsys::CNetUtil::addrToString(master_config_server_id).
                     c_str());
}

void server_conf_thread::load_config_server() {
    vector<const char *> str_list =
            TBSYS_CONFIG.getStringList(TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
    int port =
            TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT,
                                TAIR_CONFIG_SERVER_DEFAULT_PORT);
    uint64_t id;
    for (size_t i = 0; i < str_list.size(); i++) {
        id = tbsys::CNetUtil::strToAddr(str_list[i], port);
        if (id == 0)
            continue;
        if (config_server_info_map.find(id) == config_server_info_map.end()) {
            server_info *server_info =
                    server_info_allocator::server_info_allocator_instance.
                            new_server_info(NULL, id, eio, true);
            config_server_info_map[id] = server_info;
            config_server_info_list.push_back(server_info);
        }
        // only the first 2 configserver is useful
        if (config_server_info_list.size() == 2)
            break;
    }
    if (config_server_info_list.size() == 0U) {
        master_config_server_id = util::local_server_ip::ip;
        return;
    }
    if (config_server_info_list.size() == 1U
        && config_server_info_list[0]->server_id ==
           util::local_server_ip::ip) {
        master_config_server_id = util::local_server_ip::ip;
        return;
    }
    //we got 2 configserver here
    //uint64_t syncConfigServer = 0;
    master_config_server_id = config_server_info_list[0]->server_id;
    return;
}

// implementaion Runnable interface
void server_conf_thread::run(tbsys::CThread *thread, void *arg) {
    uint64_t sync_config_server_id = 0;
    uint64_t tmp_master_config_server_id = master_config_server_id;
    if (tmp_master_config_server_id == util::local_server_ip::ip) {
        tmp_master_config_server_id = get_slave_server_id();
    }
    if (tmp_master_config_server_id)
        sync_config_server_id =
                get_master_config_server(tmp_master_config_server_id, 1);
    if (sync_config_server_id) {
        log_info("syncConfigServer: %s",
                 tbsys::CNetUtil::addrToString(sync_config_server_id).
                         c_str());
    }

    // groupFile
    const char *group_file_name =
            TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
    if (group_file_name == NULL) {
        log_error("not found %s:%s ", CONFSERVER_SECTION, TAIR_GROUP_FILE);
        return;
    }

    uint32_t config_version_from_file = get_file_time(group_file_name);
    //bool rebuild_group = true;
    bool rebuild_this_group = false;
    load_group_file(group_file_name, config_version_from_file,
                    sync_config_server_id);
    //if (syncConfigServer != 0) {
    //    rebuild_group = false;
    //}

    //set myself is master
    if (config_server_info_list.size() == 2U &&
        (config_server_info_list[0]->server_id == util::local_server_ip::ip
         || (sync_config_server_id == 0
             && config_server_info_list[1]->server_id ==
                util::local_server_ip::ip))) {
        master_config_server_id = util::local_server_ip::ip;
        log_info("set MASTER_CONFIG: %s",
                 tbsys::CNetUtil::addrToString(master_config_server_id).
                         c_str());
        {
            group_info_map::iterator it;
            group_info_rw_locker.rdlock();
            for (it = group_info_map_data.begin();
                 it != group_info_map_data.end(); ++it) {
                if (it->second->is_failovering())
                    continue;
                if (rebuild_this_group) {
                    it->second->set_force_rebuild();
                } else {
                    bool need_build_it = false;
                    const uint64_t *p_table = it->second->get_hash_table();
                    for (uint32_t i = 0; i < it->second->get_server_bucket_count();
                         ++i) {
                        if (p_table[i] == 0) {
                            need_build_it = true;
                            break;
                        }
                    }
                    if (need_build_it) {
                        it->second->set_force_rebuild();
                    } else {
                        it->second->set_table_builded();
                    }
                }
            }
            group_info_rw_locker.unlock();
        }
    }

    // will start loop
    uint32_t loop_count = 0;
    tzset();
    int log_rotate_time = (time(NULL) - timezone) % 86400;
    struct timespec tm;
    clock_gettime(CLOCK_MONOTONIC, &tm);
    heartbeat_curr_time = tm.tv_sec;
    is_ready = true;

    while (!_stop) {
        struct timespec tm;
        clock_gettime(CLOCK_MONOTONIC, &tm);
        heartbeat_curr_time = tm.tv_sec;
        for (size_t i = 0; i < config_server_info_list.size(); i++) {
            server_info *server_info_it = config_server_info_list[i];
            if (server_info_it->server_id == util::local_server_ip::ip)
                continue;
            if (server_info_it->status != server_info::ALIVE
                && (loop_count % 30) != 0) {
                down_slave_config_server = server_info_it->server_id;
                log_debug("local server: %s set down slave config server: %s",
                          tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str(),
                          tbsys::CNetUtil::addrToString(down_slave_config_server).c_str());
                continue;
            }
            down_slave_config_server = 0;
            request_conf_heartbeart *new_packet = new request_conf_heartbeart();
            new_packet->server_id = util::local_server_ip::ip;
            new_packet->loop_count = loop_count;
            if (easy_helper::easy_async_send(eio, server_info_it->server_id,
                                             new_packet, this, &handler) == EASY_ERROR) {
                delete new_packet;
            }
        }

        loop_count++;
        if (loop_count <= 0)
            loop_count = TAIR_SERVER_DOWNTIME + 1;
        uint32_t curver = get_file_time(group_file_name);
        if (curver > config_version_from_file) {
            log_info("groupFile: %s, curver:%d configVersion:%d",
                     group_file_name, curver, config_version_from_file);
            load_group_file(group_file_name, curver, 0);
            config_version_from_file = curver;
            send_group_file_packet();
        }

        check_config_server_status(loop_count);

        if (loop_count > TAIR_SERVER_DOWNTIME) {
            check_server_status(loop_count);
        }

        // rotate log
        log_rotate_time++;
        if (log_rotate_time % 86400 == 86340) {
            log_info("rotateLog End");
            TBSYS_LOGGER.rotateLog(NULL, "%d");
            log_info("rotateLog start");
        }
        if (log_rotate_time % 3600 == 3000) {
            log_rotate_time = (time(NULL) - timezone) % 86400;
        }

        TAIR_SLEEP(_stop, 1);
    }
    is_ready = false;
}

void server_conf_thread::find_group_host(request_get_group *req,
                                         response_get_group *resp) {
    if (is_ready == false) {
        log_error("server_conf_thread is not ready");
        return;
    }

    group_info_map::iterator it;
    group_info_rw_locker.rdlock();
    it = group_info_map_data.find(req->group_name);
    if (it == group_info_map_data.end()) {
        group_info_rw_locker.unlock();
        log_warn("group info not found, group_name : %s", req->group_name);
        return;
    }
    group_info *group_info_found = it->second;
    resp->bucket_count = group_info_found->get_server_bucket_count();
    resp->copy_count = group_info_found->get_copy_count();

    resp->config_version = group_info_found->get_client_version();
    if (req->config_version == group_info_found->get_client_version()) {
        group_info_rw_locker.unlock();
        return;
    }

    resp->config_map = *(group_info_found->get_common_map());

    resp->set_hash_table(group_info_found->
                                 get_hash_table_deflate_data(req->server_flag),
                         group_info_found->get_hash_table_deflate_size(req->
                                 server_flag));
    resp->available_server_ids =
            group_info_found->get_available_server_id();
    group_info_rw_locker.unlock();
}

void server_conf_thread::set_stat_interval_time(int stat_interval_time_v) {
    if (stat_interval_time_v > 0) {
        stat_interval_time = stat_interval_time_v;
    }
}

void server_conf_thread::
change_server_list_to_group_file(request_data_server_ctrl *req, response_data_server_ctrl *resp) {
    const char *group_file_name = TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
    resp->return_code.clear();
    vector<string> keys(req->server_id_list.size(), TAIR_STR_SERVER_LIST);
    vector<string> values;
    vector<util::file_util::OpResult> results;

    for (vector<uint64_t>::const_iterator it = req->server_id_list.begin();
         it != req->server_id_list.end(); ++it)
        values.push_back(tbsys::CNetUtil::addrToString(*it));

    {
        tbsys::CThreadGuard guard(&group_file_mutex);
        group_info_rw_locker.rdlock();
        server_info_rw_locker.rdlock();
        if (DATASERVER_CTRL_OP_DELETE == req->op_cmd || DATASERVER_CTRL_OP_ADD == req->op_cmd) {
            util::file_util::FileUtilChangeType op_type = (DATASERVER_CTRL_OP_DELETE == req->op_cmd ?
                                                           util::file_util::CHANGE_FILE_DELETE :
                                                           util::file_util::CHANGE_FILE_ADD);
            util::file_util::change_file_manipulate_kv(group_file_name, op_type, req->group_name, '=',
                                                       keys, values, results);
            for (vector<util::file_util::OpResult>::const_iterator it = results.begin(); it != results.end(); ++it) {
                switch (*it) {
                    case util::file_util::RETURN_SUCCESS:
                        resp->return_code.push_back(DATASERVER_CTRL_RETURN_SUCCESS);
                        break;
                    case util::file_util::RETURN_KEY_VALUE_NUM_NOT_EQUAL:
                        resp->return_code.push_back(DATASERVER_CTRL_RETURN_NR);
                        break;
                    case util::file_util::RETURN_OPEN_FILE_FAILED:
                        resp->return_code.push_back(DATASERVER_CTRL_RETURN_FILE_OP_FAILED);
                        break;
                    case util::file_util::RETURN_SECTION_NAME_NOT_FOUND:
                        resp->return_code.push_back(DATASERVER_CTRL_RETURN_GROUP_NOT_FOUND);
                        break;
                    case util::file_util::RETURN_KEY_VALUE_NOT_FOUND:
                        resp->return_code.push_back(DATASERVER_CTRL_RETURN_SERVER_NOT_FOUND);
                        break;
                    case util::file_util::RETURN_KEY_VALUE_ALREADY_EXIST:
                        resp->return_code.push_back(DATASERVER_CTRL_RETURN_SERVER_EXIST);
                        break;
                    case util::file_util::RETURN_SAVE_FILE_FAILD:
                        resp->return_code.push_back(DATASERVER_CTRL_RETURN_FILE_OP_FAILED);
                        break;
                    case util::file_util::RETURN_INVAL_OP_TYPE:
                        resp->return_code.push_back(DATASERVER_CTRL_RETURN_NR);
                        break;
                    default:
                        resp->return_code.push_back(DATASERVER_CTRL_RETURN_NR);
                }
            }
        } else
            resp->return_code.assign(req->server_id_list.size(), DATASERVER_CTRL_RETURN_INVAL_OP_TYPE);

        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
    }

    stringstream ss;
    ss << "manipludate " << TAIR_STR_SERVER_LIST << " to group_conf file" << endl;
    string manipulate_str;
    if (DATASERVER_CTRL_OP_ADD == req->op_cmd)
        manipulate_str = "add dataserver";
    else if (DATASERVER_CTRL_OP_DELETE == req->op_cmd)
        manipulate_str = "delete dataserver";
    else
        manipulate_str = "inval op to dataserver";

    if (UNLIKELY(values.size() != resp->return_code.size())) {
        // will never not come to here, just in case of crash
        ss << "value size is not equal to resp return code size" << endl;
        for (size_t i = 0; i < values.size(); ++i)
            ss << manipulate_str << ' ' << values[i] << endl;
        log_error("%s", ss.str().c_str());
        return;
    }

    for (size_t i = 0; i < values.size(); ++i) {
        ss << manipulate_str << ' ' << values[i] << ' ';
        switch (resp->return_code[i]) {
            case DATASERVER_CTRL_RETURN_SUCCESS:
                ss << "success";
                break;
            case DATASERVER_CTRL_RETURN_FILE_OP_FAILED:
                ss << "fail, manipulate group conf file failed";
                break;
            case DATASERVER_CTRL_RETURN_GROUP_NOT_FOUND:
                ss << "fail, group not found";
                break;
            case DATASERVER_CTRL_RETURN_SERVER_NOT_FOUND:
                ss << "fail, dataserver not found";
                break;
            case DATASERVER_CTRL_RETURN_SERVER_EXIST:
                ss << "fail, datasever already exist";
                break;
            case DATASERVER_CTRL_RETURN_INVAL_OP_TYPE:
                ss << "fail, invalid manipulate type";
                break;
            case DATASERVER_CTRL_RETURN_NR:
            default:
                ss << "fail, unkown";
        }
        ss << endl;
    }
    log_warn("%s", ss.str().c_str());
}

void server_conf_thread::
get_migrating_machines(request_get_migrate_machine *req,
                       response_get_migrate_machine *resp) {
    group_info_rw_locker.rdlock();
    server_info_rw_locker.rdlock();
    server_info_map::iterator it =
            data_server_info_map.find(req->server_id);
    if (it == data_server_info_map.end()) {
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        log_error("ger MigratingMachine can not found server %s ",
                  tbsys::CNetUtil::addrToString(req->server_id).c_str());
        return;
    }
    server_info *p_server = it->second;
    group_info *group_info_found = p_server->group_info_data;
    log_debug("find groupinfo = %p", group_info_found);
    if (group_info_found == NULL) {
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        return;
    }
    group_info_found->get_migrating_machines(resp->vec_ms);
    server_info_rw_locker.unlock();
    group_info_rw_locker.unlock();
    return;
}

group_info_map *server_conf_thread::get_group_info_map() {
    return &group_info_map_data;
}

server_info_map *server_conf_thread::get_server_info_map() {
    return &data_server_info_map;
}

void server_conf_thread::do_query_info_packet(request_query_info *req,
                                              response_query_info *resp) {
    if (is_ready == false) {
        return;
    }
    group_info_map::iterator it;
    group_info_rw_locker.rdlock();
    if (req->query_type == request_query_info::Q_GROUP_INFO) {
        for (it = group_info_map_data.begin(); it != group_info_map_data.end();
             it++) {
            if (it->first == NULL)
                continue;
            string group_name(it->first);
            log_debug("group_name is %s", group_name.c_str());
            if (it->second == NULL)
                continue;
            if (it->second->get_group_is_OK()) {
                resp->map_k_v[group_name] = "status OK";
            } else {
                resp->map_k_v[group_name] = "status error";
            }
        }
        group_info_rw_locker.unlock();
        return;
    }
    it = group_info_map_data.find(req->group_name.c_str());
    if (it == group_info_map_data.end()) {
        group_info_rw_locker.unlock();
        return;
    }
    group_info *group_info_found = it->second;
    char key[32];
    char value[32];
    switch (req->query_type) {
        case request_query_info::Q_AREA_CAPACITY: {
            const map<uint32_t, uint64_t> &capacity_info =
                    group_info_found->get_area_capacity_info();
            map<uint32_t, uint64_t>::const_iterator capacity_it =
                    capacity_info.begin();
            for (; capacity_it != capacity_info.end(); capacity_it++) {
                snprintf(key, 32, "area(%u)", capacity_it->first);
                snprintf(value, 32, "%"
                PRI64_PREFIX
                "u", capacity_it->second);
                resp->map_k_v[key] = value;
                group_info_found->set_bitmap_area(capacity_it->first);
            }
        }

            break;
        case request_query_info::Q_MIG_INFO: {
            bool is_migating = false;
            if (group_info_found->is_migrating()) {
                vector<pair<uint64_t, uint32_t> > mig_machine_info;
                group_info_found->get_migrating_machines(mig_machine_info);
                is_migating = !mig_machine_info.empty();
                for (vector<pair<uint64_t, uint32_t> >::iterator mig_it =
                        mig_machine_info.begin(); mig_it != mig_machine_info.end();
                     mig_it++) {
                    snprintf(key, 32, "%s",
                             tbsys::CNetUtil::addrToString(mig_it->first).c_str());
                    snprintf(value, 32, "%u left", mig_it->second);
                    resp->map_k_v[key] = value;
                }
            }
            if (is_migating) {
                resp->map_k_v["isMigrating"] = "true";
            } else {
                resp->map_k_v["isMigrating"] = "false";
            }
        }
            break;
        case request_query_info::Q_DATA_SERVER_INFO: {
            server_info_rw_locker.rdlock();
            node_info_set nodeInfo = group_info_found->get_node_info();
            node_info_set::iterator node_it;
            for (node_it = nodeInfo.begin(); node_it != nodeInfo.end();
                 ++node_it) {
                snprintf(key, 32, "%s",
                         tbsys::CNetUtil::addrToString((*node_it)->server->
                                 server_id).c_str());
                if (server_info::ALIVE == (*node_it)->server->status)
                    sprintf(value, "%s", "alive");
                else if (server_info::STANDBY == (*node_it)->server->status)
                    sprintf(value, "%s", "standby");
                else if (server_info::DOWN == (*node_it)->server->status)
                    sprintf(value, "%s", "dead");
                else
                    sprintf(value, "%s", "unkown");

                resp->map_k_v[key] = value;
            }
            server_info_rw_locker.unlock();
        }
            break;
        case request_query_info::Q_STAT_INFO: {
            node_stat_info node_info;
            group_info_found->get_stat_info(req->server_id, node_info);
            node_info.format_info(resp->map_k_v);
        }
            break;
        default:
            break;
    }
    group_info_rw_locker.unlock();
    return;

}

void server_conf_thread::do_get_group_non_down_dataserver_packet(request_get_group_not_down_dataserver *req,
                                                                 response_get_group_non_down_dataserver *resp) {
    if (is_ready == false) {
        log_error("server_conf_thread is not ready");
        return;
    }
    group_info_map::iterator it;
    group_info_rw_locker.rdlock();
    it = group_info_map_data.find(req->group_name);
    if (it == group_info_map_data.end()) {
        group_info_rw_locker.unlock();
        log_warn("group info not found, group_name : %s", req->group_name);
        return;
    }

    group_info *group_info_found = it->second;
    resp->config_version = group_info_found->get_client_version();
    if (group_info_found->get_client_version() == req->config_version) {
        group_info_rw_locker.unlock();
        return;
    }

    server_info_rw_locker.rdlock();
    node_info_set node_set = group_info_found->get_node_info();
    for (node_info_set::const_iterator it = node_set.begin(); it != node_set.end(); ++it) {
        if (server_info::ALIVE == (*it)->server->status || server_info::STANDBY == (*it)->server->status)
            resp->non_down_server_ids.insert((*it)->server->server_id);
    }
    server_info_rw_locker.unlock();

    group_info_rw_locker.unlock();
}

void server_conf_thread::do_heartbeat_packet(request_heartbeat *req,
                                             response_heartbeat *resp) {
    if (is_ready == false) {
        log_error("server_conf_thread is not ready");
        return;
    }
    group_info_rw_locker.rdlock();
    server_info_rw_locker.rdlock();

    server_info_map::iterator it =
            data_server_info_map.find(req->get_server_id());
    if (it == data_server_info_map.end()) {
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        return;
    }
    server_info *p_server = it->second;
    p_server->last_time = heartbeat_curr_time;
    bool is_server_ready = req->check_is_server_ready();
    if (p_server->status == server_info::DOWN) {
        if (p_server->group_info_data == NULL ||
            p_server->group_info_data->get_accept_strategy() == GROUP_DEFAULT_ACCEPT_STRATEGY) //default accept strategy
        {
            log_warn("dataserver: %s UP, accept strategy is default, set it to solitary(can not work)",
                     tbsys::CNetUtil::addrToString(req->get_server_id()).c_str());
            // this data server should not be in this group
            resp->client_version = resp->server_version = 1;
            // inc table version and force rebuild table for send server table to slave asynchronously
            if (p_server->group_info_data != NULL && p_server->group_info_data->get_send_server_table() == 0) {
                p_server->group_info_data->inc_version(server_up_inc_step);
                p_server->group_info_data->set_force_send_table();
                log_warn(
                        "ds up, set force send table. version changed. group name: %s, client version: %u, server version: %u",
                        p_server->group_info_data->get_group_name(), p_server->group_info_data->get_client_version(),
                        p_server->group_info_data->get_server_version());
            }
            server_info_rw_locker.unlock();
            group_info_rw_locker.unlock();
            return;
        } else if (p_server->group_info_data->get_accept_strategy() == GROUP_AUTO_ACCEPT_STRATEGY) {
            // hearbeat from former started server
            // for the sake of consistency, wait it to clear old data first
            // set the status to standy
            set_server_standby(p_server);
        } else {
            log_error("dataserver: %s UP, accept strategy is: %d illegal.",
                      tbsys::CNetUtil::addrToString(req->get_server_id()).c_str(),
                      p_server->group_info_data->get_accept_strategy());
            server_info_rw_locker.unlock();
            group_info_rw_locker.unlock();
            return;
        }
    } else if (p_server->status == server_info::STANDBY) {
        if (is_server_ready && req->config_version >= p_server->standby_version) {
            // standby server has already received current table
            struct timespec tm;
            clock_gettime(CLOCK_MONOTONIC, &tm);
            if (tm.tv_sec - p_server->standby_time >= TAIR_SERVER_STANDBYTIME) {
                // wait TAIR_SERVER_STANDBYTIME after standby server update current table
                set_server_alive(p_server);
            }
        }
    } else if (false == is_server_ready && p_server->status == server_info::ALIVE) {
        p_server->status = server_info::STANDBY;
        if (p_server->group_info_data != NULL) {
            p_server->group_info_data->set_force_rebuild();
            // try to failover this down ds.
            if (master_config_server_id == util::local_server_ip::ip)
                p_server->group_info_data->pending_failover_server(p_server->server_id);
        }
    }

    group_info *group_info_founded = p_server->group_info_data;
    if (group_info_founded == NULL) {
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        return;
    }
    if (group_info_founded->get_group_status() == false) {
        log_error("group:%s status is abnorm. set ds: %s to solitary(can not work)",
                  group_info_founded->get_group_name(), tbsys::CNetUtil::addrToString(req->get_server_id()).c_str());
        resp->client_version = resp->server_version = 1;
        server_info_rw_locker.unlock();
        group_info_rw_locker.unlock();
        return;
    }

    tair_stat *stat = req->get_stat();
    if (stat) {
        log_debug("have stat info");
        node_stat_info node_stat;
        node_stat.set_last_update_time(heartbeat_curr_time);
        for (int i = 0; i < STAT_LENGTH; i++) {
            stat_info_detail statDetail;
            if (stat[i].get_count()) {
                statDetail.set_unit_value(stat_info_detail::GETCOUNT,
                                          stat[i].get_count());
                //log_debug("stat[i].getCount() =%d",stat[i].getCount());
            }
            if (stat[i].put_count()) {
                statDetail.set_unit_value(stat_info_detail::PUTCOUNT,
                                          stat[i].put_count());
                //log_debug("stat[i].putCount =%d",stat[i].putCount());
            }
            if (stat[i].evict_count()) {
                statDetail.set_unit_value(stat_info_detail::EVICTCOUNT,
                                          stat[i].evict_count());
                //log_debug("stat[i].evictCount() =%d",stat[i].evictCount());
            }
            if (stat[i].remove_count()) {
                statDetail.set_unit_value(stat_info_detail::REMOVECOUNT,
                                          stat[i].remove_count());
                //log_debug("stat[i].removeCount() =%d",stat[i].removeCount());
            }
            if (stat[i].hit_count()) {
                statDetail.set_unit_value(stat_info_detail::HITCOUNT,
                                          stat[i].hit_count());
                //log_debug("stat[i].hitCount() =%d",stat[i].hitCount());
            }
            if (stat[i].data_size()) {
                statDetail.set_unit_value(stat_info_detail::DATASIZE,
                                          stat[i].data_size());
                //log_debug("stat[i].dataSize() =%d",stat[i].dataSize());
            }
            if (stat[i].use_size()) {
                statDetail.set_unit_value(stat_info_detail::USESIZE,
                                          stat[i].use_size());
                //log_debug("stat[i].useSize() =%d",stat[i].useSize());
            }
            if (stat[i].item_count()) {
                statDetail.set_unit_value(stat_info_detail::ITEMCOUNT,
                                          stat[i].item_count());
                //log_debug("stat[i].itemCount() =%d",stat[i].itemCount());
            }
            if (statDetail.get_unit_size()) {
                node_stat.insert_stat_detail(i, statDetail);
            }
        }
        group_info_founded->set_stat_info(req->get_server_id(), node_stat);
    }

    //TODO interval of next stat report
    //heartbeat_curr_time will be set as last update time
    log_debug("@@ hb %d <> %d", req->config_version, group_info_founded->get_server_version());
    resp->client_version = group_info_founded->get_client_version();
    resp->server_version = group_info_founded->get_server_version();
    resp->down_slave_config_server = down_slave_config_server;
    resp->transition_info.reset(group_info_founded->get_transition_version(),
                                group_info_founded->get_data_need_move() != 0,
                                group_info_founded->is_failovering(),
                                group_info_founded->is_migrating());
    resp->recovery_version = group_info_founded->get_recovery_version();

    if (req->config_version != group_info_founded->get_server_version() || // server version change
        (group_info_founded->is_transition_status() &&                     // transition version change
         (req->transition_info.transition_version != group_info_founded->get_transition_version() ||
          req->recovery_version != group_info_founded->get_recovery_version()))) { // recovery version changed

        log_debug("local sv(%d), tv(%d), rv(%d)", group_info_founded->get_server_version(),
                  group_info_founded->get_transition_version(), group_info_founded->get_recovery_version());
        log_debug("remote sv(%d), tv(%d), rv(%d) of dataserver(%s)", req->config_version,
                  req->transition_info.transition_version, req->recovery_version,
                  tbsys::CNetUtil::addrToString(req->get_server_id()).c_str());
        // set the groupname into the packet
        int len = strlen(group_info_founded->get_group_name()) + 1;
        if (len > 64)
            len = 64;
        memcpy(resp->group_name, group_info_founded->get_group_name(), len);
        resp->group_name[63] = '\0';
        resp->bucket_count = group_info_founded->get_server_bucket_count();
        resp->copy_count = group_info_founded->get_copy_count();
        resp->set_hash_table(group_info_founded->
                                     get_hash_table_deflate_data(req->server_flag),
                             group_info_founded->
                                     get_hash_table_deflate_size(req->server_flag));
        log_info("version diff, return hash table size: %d",
                 resp->get_hash_table_size());
        // only in case that transistion version changed, response with the recovery ds
        if (group_info_founded->is_failovering() &&
            req->transition_info.transition_version != group_info_founded->get_transition_version()) {
            group_info_founded->get_recovery_ds(resp->recovery_ds);
        }
    } else {
        if (master_config_server_id == util::local_server_ip::ip) {
            group_info_founded->do_proxy_report(*req);
        }
    }

    if (req->plugins_version != group_info_founded->get_plugins_version()) {
        resp->set_plugins_names(group_info_founded->get_plugins_info());
    }
    resp->area_capacity_version =
            group_info_founded->get_area_capacity_version();
    if (req->area_capacity_version != resp->area_capacity_version) {
        resp->set_area_capacity(group_info_founded->get_area_capacity_info(),
                                group_info_founded->get_copy_count(),
                                group_info_founded->get_available_server_id().
                                        size());
    }
    resp->plugins_version = group_info_founded->get_plugins_version();
    if (req->pull_migrated_info != 0) {
        const uint64_t *p_m_table = group_info_founded->get_hash_table(1);
        const uint64_t *p_s_table = group_info_founded->get_hash_table(0);
        for (uint32_t i = 0; i < group_info_founded->get_server_bucket_count();
             ++i) {
            if (req->get_server_id() == p_s_table[i]) {
                resp->migrated_info.push_back(i);
                for (uint32_t j = 0; j < group_info_founded->get_copy_count(); ++j) {
                    resp->migrated_info.
                            push_back(p_m_table
                                      [i +
                                       j *
                                       group_info_founded->get_server_bucket_count()]);
                }
            }

        }

    }
    server_info_rw_locker.unlock();
    group_info_rw_locker.unlock();
}

// do_conf_heartbeat_packet
void server_conf_thread::
do_conf_heartbeat_packet(request_conf_heartbeart *req) {
    server_info_map::iterator it =
            config_server_info_map.find(req->server_id);
    if (it == config_server_info_map.end()) {
        return;
    }
    it->second->last_time = heartbeat_curr_time;
}

int server_conf_thread::do_finish_migrate_packet(request_migrate_finish *
req) {
    int ret = 0;
    log_info("receive migrate finish packet from [%s]",
             tbsys::CNetUtil::addrToString(req->server_id).c_str());
    if (is_ready == false) {
        return 1;
    }
    if (master_config_server_id != util::local_server_ip::ip) {
        //this is a slve config server. no response
        ret = 1;
    }
    uint64_t slave_server_id = get_slave_server_id();
    group_info_rw_locker.wrlock();
    server_info_rw_locker.rdlock();
    server_info_map::iterator it =
            data_server_info_map.find(req->server_id);
    if (it != data_server_info_map.end()
        && it->second->group_info_data != NULL) {
        if (it->second->group_info_data->
                do_finish_migrate(req->server_id, req->version, req->bucket_no,
                                  slave_server_id) == false) {
            if (ret == 0) {
                ret = -1;
            }
        }
    }
    server_info_rw_locker.unlock();
    group_info_rw_locker.unlock();
    return ret;
}

int server_conf_thread::do_finish_recovery_packet(request_recovery_finish *req) {
    int ret = 0;
    log_info("receive recovery finish packet from [%s], transition version is %d",
             tbsys::CNetUtil::addrToString(req->server_id).c_str(), req->transition_version);
    if (is_ready == false) {
        return 1;
    }
    if (master_config_server_id != util::local_server_ip::ip) {
        // this is a slve config server. no response
        // just ignore finish request
        return 1;
    }
    group_info_rw_locker.wrlock();
    server_info_rw_locker.rdlock();
    server_info_map::iterator it =
            data_server_info_map.find(req->server_id);
    if (it != data_server_info_map.end()
        && it->second->group_info_data != NULL) {
        if (it->second->group_info_data->
                do_finish_recovery(req->server_id, req->version, req->transition_version, req->bucket_no) == false) {
            if (ret == 0) {
                ret = -1;
            }
        }
    }
    server_info_rw_locker.unlock();
    group_info_rw_locker.unlock();
    return ret;
}

uint64_t server_conf_thread::get_master_config_server(uint64_t id,
                                                      int value) {
    uint64_t master_id = 0;
    request_set_master *packet = (request_set_master *) packet_factory::create_packet(TAIR_REQ_SETMASTER_PACKET);
    packet->value = value;

    base_packet *bp = NULL;
    bp = (base_packet *) easy_helper::easy_sync_send(eio, id, packet, this, &handler);
    if (bp != NULL && bp->getPCode() == TAIR_RESP_RETURN_PACKET) {
        if (((response_return *) bp)->get_code() == TAIR_RETURN_SUCCESS) {
            master_id = id;
        }
        delete bp;
    }
    log_info("get_master_config_server id is [%"PRI64_PREFIX"d], ip=%s",
             master_id, tbsys::CNetUtil::addrToString(master_id).c_str());
    return master_id;
}

void server_conf_thread::get_server_table(uint64_t sync_config_server_id,
                                          const char *group_name,
                                          int type) {
    bool ret = false;
    uint32_t hashcode = 0;
    request_get_server_table *packet = new request_get_server_table();
    packet->config_version = type;
    packet->set_group_name(group_name);
    log_info("begin syncserver table with [%s] for group [%s]",
             tbsys::CNetUtil::addrToString(sync_config_server_id).c_str(), group_name);
    base_packet *bp = NULL;
    bp = (base_packet *) easy_helper::easy_sync_send(eio, sync_config_server_id, packet, this, &handler);
    if (bp == NULL) {
        log_error("Send RequestGetServerInfoPacket to %s failure.",
                  tbsys::CNetUtil::addrToString(sync_config_server_id).
                          c_str());
    } else if (bp != NULL && bp->getPCode() == TAIR_RESP_GET_SVRTAB_PACKET) {
        response_get_server_table *resp = (response_get_server_table *) bp;
        if (resp->type == 0) {
            if (resp->size > 0) {        //write to file
                char file_name[256];
                const char *sz_data_dir =
                        TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_DATA_DIR,
                                               TAIR_DEFAULT_DATA_DIR);
                snprintf(file_name, 256, "%s/%s_server_table", sz_data_dir,
                         group_name);
                ret =
                        backup_and_write_file(file_name, resp->data, resp->size,
                                              resp->modified_time);
                if (ret) {
                    hashcode =
                            util::string_util::mur_mur_hash(resp->data, resp->size);
                }
            }
        } else {
            const char *file_name =
                    TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE,
                                           NULL);
            if (file_name != NULL) {
                ret =
                        backup_and_write_file(file_name, resp->data, resp->size,
                                              resp->modified_time);
                if (ret) {
                    hashcode =
                            util::string_util::mur_mur_hash(resp->data, resp->size);
                }
            }
        }
        delete resp;
    }

    if (type == GROUP_DATA) {
        log_info("%s, load server_table%s, hashcode: %u",
                 group_name, (ret ? "ok" : "error"), hashcode);
    } else {
        log_info("load groupFile%s, hashcode: %u", (ret ? "ok" : "error"),
                 hashcode);
    }
}

// do_set_master_packet
bool server_conf_thread::do_set_master_packet(request_set_master *req, uint64_t server_id) {
    uint64_t old_master_config_server_id = master_config_server_id;
    bool ret = true;
    if (master_config_server_id == 0 ||
        master_config_server_id != util::local_server_ip::ip) {
        ret = false;
    }
    if (req == NULL)
        return ret;

    if (req->value == 1 && config_server_info_list.size() > 0U &&
        config_server_info_list[0]->server_id != util::local_server_ip::ip) {
        master_config_server_id = 0;
    }
    server_info_map::iterator it = config_server_info_map.find(server_id);
    if (it != config_server_info_map.end()) {
        it->second->last_time = heartbeat_curr_time;
    }

    log_info("I am %s MASTER_CONFIG", (ret ? "" : "not"));
    if (old_master_config_server_id != master_config_server_id) {
        group_info_rw_locker.wrlock();
        group_info_map::iterator it = group_info_map_data.begin();
        for (; it != group_info_map_data.end(); it++) {
            it->second->inc_version();

        }
        group_info_rw_locker.unlock();
    }

    return ret;
}

void server_conf_thread::do_op_cmd(request_op_cmd *req, response_op_cmd *resp) {
    int rc = TAIR_RETURN_SUCCESS;
    // only master can op cmd
    if (master_config_server_id != util::local_server_ip::ip) {
        rc = TAIR_RETURN_FAILED;
    } else {
        const char *group_file_name = TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        ServerCmdType cmd = req->cmd;
        switch (cmd) {
            case TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER: {
                rc = get_group_config_value(resp, req->params, group_file_name, TAIR_TMP_DOWN_SERVER, "");
                break;
            }
            case TAIR_SERVER_CMD_GET_GROUP_STATUS: {
                rc = get_group_config_value(resp, req->params, group_file_name, TAIR_GROUP_STATUS, "off");
                break;
            }
            case TAIR_SERVER_CMD_SET_GROUP_STATUS: {
                rc = set_group_status(resp, req->params, group_file_name);
                break;
            }
            case TAIR_SERVER_CMD_GET_AREA_STATUS: {
                rc = get_group_config_value_list(resp, req->params, group_file_name, TAIR_AREA_STATUS, "off");
                break;
            }
            case TAIR_SERVER_CMD_SET_AREA_STATUS: {
                rc = set_area_status(resp, req->params, group_file_name);
                break;
            }
            case TAIR_SERVER_CMD_RESET_DS: {
                rc = do_reset_ds_packet(resp, req->params);
                break;
            }
            case TAIR_SERVER_CMD_ALLOC_AREA: {
                rc = do_allocate_area(resp, req->params, group_file_name);
                break;
            }
            case TAIR_SERVER_CMD_SET_QUOTA: {
                rc = do_set_quota(resp, req->params, group_file_name);
                break;
            }
            case TAIR_SERVER_CMD_MIGRATE_BUCKET: {
                rc = do_force_migrate_bucket(resp, req->params);
                break;
            }
            case TAIR_SERVER_CMD_SWITCH_BUCKET: {
                rc = do_force_switch_bucket(resp, req->params);
                break;
            }
            case TAIR_SERVER_CMD_ACTIVE_FAILOVER_OR_RECOVER: {
                rc = do_active_failover_or_recover(resp, req->params);
                break;
            }
            case TAIR_SERVER_CMD_REPLACE_DS: {
                rc = do_force_replace_ds(resp, req->params);
                break;
            }
            case TAIR_SERVER_CMD_URGENT_OFFLINE: {
                rc = do_urgent_offline(resp, req->params);
                break;
            }
            default: {
                log_error("unknown command %d received", cmd);
                rc = TAIR_RETURN_FAILED;
                break;
            }
        }
    }
    resp->code = rc;
    resp->setChannelId(req->getChannelId());
}

int server_conf_thread::load_conf(const char *group_file_name, tbsys::CConfig &config) {
    if (group_file_name == NULL) {
        log_error("group_file_name is NULL");
        return TAIR_RETURN_FAILED;
    }
    if (config.load((char *) group_file_name) == EXIT_FAILURE) {
        log_error("load group file %s failed", group_file_name);
        return TAIR_RETURN_FAILED;
    }
    return TAIR_RETURN_SUCCESS;
}

int server_conf_thread::get_group_config_value(response_op_cmd *resp,
                                               const vector<string> &params, const char *group_file_name,
                                               const char *config_key, const char *default_value) {
    if (group_file_name == NULL) {
        log_error("group_file_name is NULL");
        return TAIR_RETURN_FAILED;
    }
    if (config_key == NULL) {
        log_error("get config value but key is NULL");
        return TAIR_RETURN_FAILED;
    }
    tbsys::CConfig config;
    if (TAIR_RETURN_SUCCESS != load_conf(group_file_name, config)) {
        log_error("load group file %s failed", group_file_name);
        return TAIR_RETURN_FAILED;
    }

    for (size_t i = 0; i < params.size(); ++i) {
        const char *config_value = config.getString(params[i].c_str(), config_key, default_value);
        if (config_value == NULL) {
            config_value = "none";
        }
        char buf[128];
        snprintf(buf, sizeof(buf), "%s: %s=%s", params[i].c_str(), config_key, config_value);
        resp->infos.push_back(string(buf));
    }

    return TAIR_RETURN_SUCCESS;
}

int server_conf_thread::get_group_config_value_list(response_op_cmd *resp,
                                                    const vector<string> &params, const char *group_file_name,
                                                    const char *config_key, const char *default_value) {
    if (config_key == NULL) {
        log_error("get config value but key is NULL");
        return TAIR_RETURN_FAILED;
    }
    tbsys::CConfig config;
    if (config.load((char *) group_file_name) == EXIT_FAILURE) {
        log_error("load group file %s failed", group_file_name);
        return TAIR_RETURN_FAILED;
    }

    for (size_t i = 0; i < params.size(); ++i) {
        vector<const char *> config_values = config.getStringList(params[i].c_str(), config_key);
        if (config_values.size() != 0) {
            char buf[128];
            for (vector<const char *>::iterator it = config_values.begin(); it != config_values.end(); it++) {
                log_warn("%s: %s=%s", params[i].c_str(), config_key, *it);
                snprintf(buf, sizeof(buf), "%s: %s=%s", params[i].c_str(), config_key, *it);
                resp->infos.push_back(string(buf));
            }
        }
    }
    return TAIR_RETURN_SUCCESS;
}

int server_conf_thread::set_area_quota(const char *area_key, const char *quota, const char *group_name,
                                       const char *group_file_name, bool new_area, uint32_t *new_area_num) {
    int rc = TAIR_RETURN_FAILED;
    group_info_map::iterator mit;
    tbsys::CThreadGuard guard(&group_file_mutex);
    group_info_rw_locker.wrlock();
    mit = group_info_map_data.find(group_name);
    if (mit == group_info_map_data.end()) {
        log_error("group not found");
        return TAIR_RETURN_OP_GROUP_NOT_FOUND;
    } else {
        uint32_t area_num;
        if (new_area) {
            int ret = mit->second->get_area_num(area_key, area_num);
            if (TAIR_RETURN_SUCCESS != ret)
                return ret;
            rc = mit->second->add_area_capacity_list(group_file_name, group_name, area_key, area_num, quota);
            if (TAIR_RETURN_SUCCESS == rc) {
                rc = mit->second->add_area_key_list(group_file_name, group_name, area_key, area_num);
                *new_area_num = area_num;
            } else if (TAIR_RETURN_AREA_NOT_EXISTED == rc) {
                rc = TAIR_RETURN_SUCCESS;
            }
        } else {
            int ret = mit->second->get_area_num(area_key, area_num);
            if (TAIR_RETURN_SUCCESS != ret)
                return TAIR_RETURN_AREA_NOT_EXISTED;
            rc = mit->second->update_area_capacity_list(group_file_name, group_name, area_num, quota);
        }
    }
    group_info_rw_locker.unlock();

    return rc;
}

int server_conf_thread::do_allocate_area(response_op_cmd *resp,
                                         const vector<string> &params, const char *group_file_name) {
    if (group_file_name == NULL) {
        log_error("group_file_name is NULL");
        return TAIR_RETURN_OP_GROUP_NOT_FOUND;
    }
    if (params.size() != 3) {
        log_error("set quota but have no area/capacity_ size:%zd", params.size());
        return TAIR_RETURN_OP_INVALID_PARAM;
    }
    log_warn("set area quota: groupname:%s = area:%s, quota:%s", params[2].c_str(), params[0].c_str(),
             params[1].c_str());
    uint32_t area;
    int rc = set_area_quota(params[0].c_str(), params[1].c_str(), params[2].c_str(), group_file_name, true, &area);
    char buf[64];
    snprintf(buf, sizeof(buf), "%u", area);
    resp->infos.push_back(buf);
    return rc;
}

int server_conf_thread::do_set_quota(response_op_cmd *resp,
                                     const vector<string> &params, const char *group_file_name) {
    if (group_file_name == NULL) {
        log_error("group_file_name is NULL");
        return TAIR_RETURN_OP_GROUP_NOT_FOUND;
    }
    if (params.size() != 3) {
        log_error("set quota but have no area/capacity_");
        return TAIR_RETURN_OP_INVALID_PARAM;
    }
    log_warn("set area quota: groupname:%s = area:%s, quota:%s", params[2].c_str(), params[0].c_str(),
             params[1].c_str());
    return set_area_quota(params[0].c_str(), params[1].c_str(), params[2].c_str(), group_file_name, false, NULL);
}

int server_conf_thread::set_group_status(response_op_cmd *resp,
                                         const vector<string> &params, const char *group_file_name) {
    if (group_file_name == NULL) {
        log_error("group_file_name is NULL");
        return TAIR_RETURN_FAILED;
    }
    if (params.size() != 2) {
        log_error("set group status but have no group/status");
        return TAIR_RETURN_FAILED;
    }

    log_info("set group status: %s = %s", params[0].c_str(), params[1].c_str());
    return util::file_util::change_conf(group_file_name, params[0].c_str(), TAIR_GROUP_STATUS, params[1].c_str());
}

int server_conf_thread::set_area_status(response_op_cmd *resp,
                                        const vector<string> &params, const char *group_file_name) {
    char key[128];
    if (group_file_name == NULL) {
        log_error("group_file_name is NULL");
        return TAIR_RETURN_FAILED;
    }
    if (params.size() != 3) {
        log_error("set group status but have no group/status");
        return TAIR_RETURN_FAILED;
    }
    snprintf(key, sizeof(key), TAIR_AREA_STATUS
    "=%s", params[1].c_str());

    log_warn("set group : %s, area:%s, status:%s", params[0].c_str(), params[1].c_str(), params[2].c_str());
    //return util::file_util::change_conf(group_file_name, params[0].c_str(), TAIR_GROUP_STATUS, params[1].c_str());
    return util::file_util::change_conf(group_file_name, params[0].c_str(), key, params[2].c_str(),
                                        util::file_util::CHANGE_FILE_MODIFY, ',');
}

int server_conf_thread::do_reset_ds_packet(response_op_cmd *resp, const vector<string> &params) {
    int ret = TAIR_RETURN_SUCCESS;
    if (params.size() <= 1) {
        log_error("reset ds cmd but no group parameter");
        return TAIR_RETURN_FAILED;
    }

    const char *cmd_group = params[0].c_str();
    group_info_map::iterator mit = group_info_map_data.find(cmd_group);
    if (mit == group_info_map_data.end()) {
        log_error("rest ds cmd but invalid group name: %s", cmd_group);
        return TAIR_RETURN_FAILED;
    }

    vector<uint64_t> ds_ids;
    vector<string>::const_iterator it = params.begin();
    ++it;
    ++it;
    log_info("resetds group: %s, requeset resetds size: %lu", cmd_group, params.size() - 1);
    for (; it != params.end(); it++) {
        log_info("reset ds: %s", it->c_str());
        uint64_t ds_id = tbsys::CNetUtil::strToAddr(it->c_str(), 0);
        // check ds's status: must be alive
        server_info_rw_locker.rdlock();
        server_info_map::iterator mit =
                data_server_info_map.find(ds_id);
        if (mit == data_server_info_map.end()) {
            server_info_rw_locker.unlock();
            log_error("can not find reset ds: %s", it->c_str());
            return TAIR_RETURN_FAILED;
        } else if (mit->second->status != server_info::ALIVE) {
            server_info_rw_locker.unlock();
            log_error("reset ds: %s status is: %d illegal", it->c_str(), mit->second->status);
            return TAIR_RETURN_FAILED;
        }
        server_info_rw_locker.unlock();
        ds_ids.push_back(ds_id);
    }
    group_info_rw_locker.wrlock();
    mit->second->clear_down_server(ds_ids);
    mit->second->inc_version();
    mit->second->set_force_send_table();
    group_info_rw_locker.unlock();
    return ret;
}

int server_conf_thread::do_force_migrate_bucket(response_op_cmd *resp, const vector<string> &params) {
    //params
    //group_name bucket_no copy_no src_ds_addr dest_ds_addr
    int ret = TAIR_RETURN_SUCCESS;
    if (params.size() != 5) {
        log_error("force migrate bucket cmd, but parameter is illegal");
        ret = TAIR_RETURN_FAILED;
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        const char *cmd_group = params[0].c_str();
        group_info_map::iterator mit = group_info_map_data.find(cmd_group);
        if (mit == group_info_map_data.end()) {
            log_error("force migrate bucket cmd, group name: %s", cmd_group);
            ret = TAIR_RETURN_FAILED;
        } else {
            int bucket_no = atoi(params[1].c_str());
            int copy_no = atoi(params[2].c_str());
            uint64_t src_ds_addr = tbsys::CNetUtil::strToAddr(params[3].c_str(), 0);
            uint64_t dest_ds_addr = tbsys::CNetUtil::strToAddr(params[4].c_str(), 0);

            ret = mit->second->force_migrate_bucket(bucket_no, copy_no, src_ds_addr, dest_ds_addr,
                                                    &group_info_rw_locker, &server_info_rw_locker);
        }
    }
    return ret;
}

int server_conf_thread::do_force_switch_bucket(response_op_cmd *resp, const vector<string> &params) {
    //params
    //group_name bucket_nos
    int ret = TAIR_RETURN_SUCCESS;
    if (params.size() != 2) {
        log_error("force switch bucket cmd, but parameter is illegal");
        ret = TAIR_RETURN_FAILED;
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        const char *cmd_group = params[0].c_str();
        group_info_map::iterator mit = group_info_map_data.find(cmd_group);
        if (mit == group_info_map_data.end()) {
            log_error("force switch bucket cmd, group name: %s", cmd_group);
            ret = TAIR_RETURN_FAILED;
        } else {
            // init buckets
            set<uint32_t> bucket_nos;
            std::vector<std::string> bucket_strs;
            tair::util::string_util::split_str(params[1].c_str(), ", ", bucket_strs);

            for (size_t i = 0; i < bucket_strs.size(); ++i) {
                errno = 0;
                char *endptr = NULL;
                uint32_t bucket_no = strtol(bucket_strs[i].c_str(), &endptr, 10);
                if (0 == errno && endptr != bucket_strs[i].c_str()) {
                    bucket_nos.insert(bucket_no);
                } else {
                    log_error("invalid bucket_no : %s", bucket_strs[i].c_str());
                    ret = TAIR_RETURN_FAILED;
                    break;
                }
            }

            if (TAIR_RETURN_SUCCESS == ret) {
                ret = mit->second->force_switch_bucket(bucket_nos,
                                                       &group_info_rw_locker, &server_info_rw_locker);
            }
        }
    }
    return ret;
}

int server_conf_thread::do_active_failover_or_recover(response_op_cmd *resp, const std::vector<std::string> &params) {
    // params : group_name server_id type
    int ret = TAIR_RETURN_SUCCESS;
    if (params.size() != 3) {
        log_error("active_failover_or_recover cmd, but parameter is illegal");
        ret = TAIR_RETURN_FAILED;
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        const char *cmd_group = params[0].c_str();
        group_info_map::iterator mit = group_info_map_data.find(cmd_group);
        if (mit == group_info_map_data.end()) {
            log_error("active_failover_or_recover cmd, group name: %s not found", cmd_group);
            ret = TAIR_RETURN_FAILED;
        } else {
            uint64_t server_id = tbsys::CNetUtil::strToAddr(params[1].c_str(), TAIR_SERVER_DEFAULT_PORT);
            int type = atoi(params[2].c_str());

            log_info("active_failover_or_recover cmd, group_name(%s), ds(%s), type(%d)",
                     cmd_group, tbsys::CNetUtil::addrToString(server_id).c_str(), type);

            switch (type) {
                case ACTIVE_FAILOVER: {
                    log_error("recv active_failover req, try to failover %s",
                              tbsys::CNetUtil::addrToString(server_id).c_str());
                    ret = mit->second->active_failover_ds(server_id, &group_info_rw_locker, &server_info_rw_locker);
                }
                    break;
                case ACTIVE_RECOVER: {
                    log_error("recv active_recover req, try to recover %s",
                              tbsys::CNetUtil::addrToString(server_id).c_str());
                    ret = mit->second->active_recover_ds(server_id, &group_info_rw_locker, &server_info_rw_locker);
                }
                    break;
                default: {
                    log_error("active_failover_or_recover, unknown type: %d", type);
                    ret = TAIR_RETURN_FAILED;
                }
                    break;
            }
        }
    }
    return ret;
}

int server_conf_thread::do_force_replace_ds(response_op_cmd *resp, const std::vector<std::string> &params) {
    // params : group_name repalce_ds_cnt src_ds dest_ds
    int ret = TAIR_RETURN_SUCCESS;
    if (params.size() != 4) {
        log_error("force_replace_ds cmd, but parameter is illegal");
        ret = TAIR_RETURN_FAILED;
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        const char *cmd_group = params[0].c_str();
        group_info_map::iterator mit = group_info_map_data.find(cmd_group);
        if (mit == group_info_map_data.end()) {
            log_error("force_replace_ds cmd, group name: %s not found", cmd_group);
            ret = TAIR_RETURN_FAILED;
        } else {
            std::map<uint64_t, uint64_t> replace_ds_pairs;
            if (check_replace_ds_params(replace_ds_pairs, params)) {
                ret = mit->second->force_replace_ds(replace_ds_pairs, &group_info_rw_locker, &server_info_rw_locker);
            } else {
                log_error("force_replace_ds cmd, but parameter is illegal");
                ret = TAIR_RETURN_FAILED;
            }
        }
    }
    return ret;
}

bool server_conf_thread::check_replace_ds_params(std::map<uint64_t, uint64_t> &replace_ds_pairs,
                                                 const std::vector<std::string> &params) {
    uint32_t ds_cnt = atoi(params[1].c_str());

    std::vector<std::string> src_ds_strs;
    tair::util::string_util::split_str(params[2].c_str(), ",", src_ds_strs);
    std::vector<std::string> dst_ds_strs;
    tair::util::string_util::split_str(params[3].c_str(), ",", dst_ds_strs);

    bool valid = ds_cnt == src_ds_strs.size() && ds_cnt == dst_ds_strs.size();
    if (valid) {
        std::set<uint64_t> ds_set;
        for (uint32_t i = 0; i < src_ds_strs.size(); i++) {
            uint64_t src_server_id = tbsys::CNetUtil::strToAddr(src_ds_strs[i].c_str(), TAIR_SERVER_DEFAULT_PORT);
            uint64_t dst_server_id = tbsys::CNetUtil::strToAddr(dst_ds_strs[i].c_str(), TAIR_SERVER_DEFAULT_PORT);

            if (ds_set.insert(src_server_id).second && ds_set.insert(dst_server_id).second) {
                replace_ds_pairs.insert(std::pair<uint64_t, uint64_t>(src_server_id, dst_server_id));
            } else {
                ds_set.clear();
                replace_ds_pairs.clear();
                valid = false;
                break;
            }
        }
    }

    return valid;
}

int server_conf_thread::do_urgent_offline(response_op_cmd *resp, const std::vector<std::string> &params) {
    // params : urgent_offline group src_ds dst_ds
    int ret = TAIR_RETURN_SUCCESS;
    if (params.size() != 3) {
        log_error("urgent_offline cmd, but parameter is illegal");
        ret = TAIR_RETURN_FAILED;
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        const char *cmd_group = params[0].c_str();
        group_info_map::iterator mit = group_info_map_data.find(cmd_group);
        if (mit == group_info_map_data.end()) {
            log_error("urgent_offline cmd, group name: %s not found", cmd_group);
            ret = TAIR_RETURN_FAILED;
        } else {
            uint64_t offline_id = tbsys::CNetUtil::strToAddr(params[1].c_str(), 0);
            set<uint64_t> replace_ids;

            std::vector<std::string> replace_id_strs;
            tair::util::string_util::split_str(params[2].c_str(), ", ", replace_id_strs);

            for (size_t i = 0; i < replace_id_strs.size(); ++i) {
                uint64_t replace_id = tbsys::CNetUtil::strToAddr(replace_id_strs[i].c_str(), 0);
                if (0 == replace_id || !replace_ids.insert(replace_id).second) {
                    ret = TAIR_RETURN_FAILED;
                    break;
                }
            }

            if (0 == offline_id || replace_ids.find(offline_id) != replace_ids.end()) {
                ret = TAIR_RETURN_FAILED;
            }

            if (TAIR_RETURN_SUCCESS == ret) {
                ret = mit->second->urgent_offline(offline_id, replace_ids,
                                                  &group_info_rw_locker, &server_info_rw_locker);
            } else {
                replace_ids.clear();
                log_error("urgent_offline cmd, but parameter is illegal");
            }
        }
    }
    return ret;
}

void server_conf_thread::
read_group_file_to_packet(response_get_server_table *resp) {
    resp->type = GROUP_CONF;
    const char *file_name =
            TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
    if (file_name == NULL)
        return;
    file_mapper mmap_file;
    mmap_file.open_file(file_name);
    char *data = (char *) mmap_file.get_data();
    if (data == NULL)
        return;
    resp->set_data(data, mmap_file.get_size());
    resp->modified_time = mmap_file.get_modify_time();
    log_info("load groupfile, hashcode: %u",
             util::string_util::mur_mur_hash(resp->data, resp->size));
}

uint64_t server_conf_thread::get_slave_server_id() {
    uint64_t slave_server_id = 0;
    if (config_server_info_list.size() == 2U &&
        config_server_info_list[0]->server_id == util::local_server_ip::ip &&
        config_server_info_list[1]->status == server_info::ALIVE) {
        slave_server_id = config_server_info_list[1]->server_id;
    }
    return slave_server_id;
}

void server_conf_thread::send_group_file_packet() {
    uint64_t slave_server_id = get_slave_server_id();
    if (slave_server_id == 0)
        return;

    response_get_server_table *packet = new response_get_server_table();
    read_group_file_to_packet(packet);

    log_info("send groupFile to %s: hashcode: %u",
             tbsys::CNetUtil::addrToString(slave_server_id).c_str(),
             util::string_util::mur_mur_hash(packet->data, packet->size));

    if (easy_helper::easy_async_send(eio, slave_server_id, packet, this, &handler) == EASY_ERROR) {
        log_error("Send ResponseGetServerTablePacket %s failure.",
                  tbsys::CNetUtil::addrToString(slave_server_id).c_str());
        delete packet;
    }
}

// do_get_server_table_packet
void server_conf_thread::
do_get_server_table_packet(request_get_server_table *req,
                           response_get_server_table *resp) {
    if (req->config_version == GROUP_CONF) {
        read_group_file_to_packet(resp);
        return;
    }

    group_info_map::iterator it;
    group_info_rw_locker.rdlock();
    it = group_info_map_data.find(req->group_name);
    if (it == group_info_map_data.end()) {
        group_info_rw_locker.unlock();
        return;
    }
    group_info *group_info_founded = it->second;
    const char *data = group_info_founded->get_server_table_data();
    uint32_t size = group_info_founded->get_server_table_size();
    if (size == group_info_founded->get_SVR_table_size()) {
        resp->set_data(data, size);
        log_info("%s get servertable, hashcode: %u",
                 req->group_name,
                 util::string_util::mur_mur_hash(resp->data, resp->size));
    }
    group_info_rw_locker.unlock();
}

// do_set_server_table_packet
bool server_conf_thread::
do_set_server_table_packet(response_get_server_table *packet) {
    if (NULL == packet) {
        log_error("do set server table packet is null");
        return false;
    }

    // if config server is default master and now is master role, discard this packet.
    // if config server is not default master and now is master role, accept this packet.
    if (master_config_server_id == util::local_server_ip::ip) {
        string peer_ip = tbsys::CNetUtil::addrToString(packet->peer_id);
        size_t peer_pos = peer_ip.find(":");
        string default_m_ip = tbsys::CNetUtil::addrToString(config_server_info_list[0]->server_id);
        size_t default_m_pos = default_m_ip.find(":");
        log_warn("master conflict. local id: %s, peer id: %s, pos: %lu, ip: %s, default id: %s, pos: %lu, ip: %s",
                 tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str(),
                 peer_ip.c_str(), peer_pos, peer_ip.substr(0, peer_pos).c_str(),
                 default_m_ip.c_str(), default_m_pos, default_m_ip.substr(0, default_m_pos).c_str());
        if (peer_ip.substr(0, peer_pos) != default_m_ip.substr(0, default_m_pos)) {
            log_error("master conflict, master ip: %s, local ip: %s, default ip: %s, "
                              " this packet is not from default master, discard set server table packet",
                      tbsys::CNetUtil::addrToString(master_config_server_id).c_str(),
                      tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str(),
                      tbsys::CNetUtil::addrToString(config_server_info_list[0]->server_id).c_str());
            return false;
        }
    }

    group_info_rw_locker.wrlock();
    if (packet->type == GROUP_CONF) {
        bool ret = false;
        uint32_t hashcode = 0;
        const char *file_name =
                TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_GROUP_FILE, NULL);
        if (file_name != NULL) {
            ret =
                    backup_and_write_file(file_name, packet->data, packet->size,
                                          packet->modified_time);
            if (ret) {
                hashcode =
                        util::string_util::mur_mur_hash(packet->data, packet->size);
            }
        }
        group_info_rw_locker.unlock();
        log_info("receive groupFile%s, hashcode: %u", (ret ? "ok" : "error"),
                 hashcode);
        return ret;
    }

    group_info_map::iterator it;
    it = group_info_map_data.find(packet->group_name);
    if (it == group_info_map_data.end()) {
        group_info_rw_locker.unlock();
        return false;
    }
    group_info *group_info_founded = it->second;
    server_info_rw_locker.rdlock();
    bool retv =
            group_info_founded->write_server_table_packet(packet->data,
                                                          packet->size);
    server_info_rw_locker.unlock();
    group_info_rw_locker.unlock();
    return retv;
}

// do_group_names_packet
void server_conf_thread::do_group_names_packet(response_group_names *
resp) {
    //tbsys::CThreadGuard guard(&_mutex);
    group_info_rw_locker.rdlock();

    group_info_map::iterator sit;
    resp->status = (is_ready ? 1 : 0);
    for (sit = group_info_map_data.begin(); sit != group_info_map_data.end();
         ++sit) {
        resp->add_group_name(sit->first);
    }
    group_info_rw_locker.unlock();
}

bool server_conf_thread::backup_and_write_file(const char *file_name,
                                               const char *data, int size,
                                               int modified_time) {
    bool ret = false;
    if (data == NULL)
        return ret;
    if (1) {
        file_mapper mmap_file;
        mmap_file.open_file(file_name);
        char *tdata = (char *) mmap_file.get_data();
        int tsize = mmap_file.get_size();
        //same, not need to write
        if (tdata != NULL && tsize == size && memcmp(tdata, data, size) == 0) {
            log_info("%s not changed, set modify time  to %d", file_name,
                     modified_time);
            if (modified_time > 0) {
                struct utimbuf times;
                times.actime = modified_time;
                times.modtime = modified_time;
                utime(file_name, &times);
            }
            return true;
        }
    }
    //back up first
    char backup_file_name[256];
    time_t t;
    time(&t);
    struct tm *tm = ::localtime((const time_t *) &t);
    snprintf(backup_file_name, 256, "%s.%04d%02d%02d%02d%02d%02d.backup",
             file_name, tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
             tm->tm_hour, tm->tm_min, tm->tm_sec);
    rename(file_name, backup_file_name);
    int fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (write(fd, data, size) == size) {
        ret = true;
    }
    close(fd);
    if (ret == false) {
        rename(backup_file_name, file_name);
    } else if (modified_time > 0) {
        struct utimbuf times;
        times.actime = modified_time;
        times.modtime = modified_time;
        if (utime(file_name, &times)) {
            log_warn("can not change modified timestamp: %s", file_name);
        }
    }
    if (ret) {
        log_info("back up file  %s to  %s, new file modify time: %d",
                 file_name, backup_file_name, modified_time);
    }
    return ret;
}

server_conf_thread::table_builder_thread::
table_builder_thread(server_conf_thread *serveconf) {
    p_server_conf = serveconf;
    start();
}

server_conf_thread::table_builder_thread::~table_builder_thread() {
    stop();
    wait();

}

void server_conf_thread::table_builder_thread::run(tbsys::CThread *
thread, void *arg) {
    while (!_stop) {
        vector<group_info *> tmp;
        {
            tbsys::CThreadGuard guard(&(p_server_conf->mutex_grp_need_build));
            tmp = group_need_build;
            group_need_build.clear();
        }
        build_table(tmp);
        sleep(1);
    }
}

void server_conf_thread::table_builder_thread::build_table(vector<
        group_info *
> &groupe_will_builded) {
    for (vector<group_info *>::iterator it = groupe_will_builded.begin();
         it != groupe_will_builded.end(); ++it) {
        (*it)->rebuild(p_server_conf->get_slave_server_id(),
                       &(p_server_conf->group_info_rw_locker),
                       &(p_server_conf->server_info_rw_locker));
    }
}

void server_conf_thread::set_server_standby(server_info *p_server) {
    log_warn("dataserver(%s) change status: DOWN => STANDBY",
             tbsys::CNetUtil::addrToString(p_server->server_id).c_str());
    struct timespec tm;
    clock_gettime(CLOCK_MONOTONIC, &tm);
    p_server->status = server_info::STANDBY;
    p_server->standby_version = p_server->group_info_data->get_server_version();
    p_server->standby_time = tm.tv_sec;
}

void server_conf_thread::set_server_alive(server_info *p_server) {
    log_warn("dataserver(%s) change status: %s => ALIVE",
             tbsys::CNetUtil::addrToString(p_server->server_id).c_str(),
             server_info::DOWN == p_server->status ? "DOWN" : "STANDBY");
    log_warn("dataserver: %s UP, accept strategy is auto, we will rebuild the table.",
             tbsys::CNetUtil::addrToString(p_server->server_id).c_str());
    // accept ds automatically
    struct timespec tm;
    clock_gettime(CLOCK_MONOTONIC, &tm);
    p_server->last_time = tm.tv_sec;
    p_server->status = server_info::ALIVE;
    // pending recover this server
    if (master_config_server_id == util::local_server_ip::ip) {
        p_server->group_info_data->pending_recover_server(p_server->server_id);
    }
    p_server->group_info_data->set_force_rebuild();
}
}
}
