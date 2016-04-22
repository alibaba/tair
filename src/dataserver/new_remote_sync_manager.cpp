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

#include "new_remote_sync_manager.hpp"
#include "common/define.hpp"
#include "common/result.hpp"
#include "common/record.hpp"
#include "common/record_logger.hpp"
#include "tair_manager.hpp"
#include "client/tair_client_api_impl.hpp"

#include "common/cJSON.h"
#include "common/httpclient.hpp"

#include <time.h>
#include <sched.h>
#include <sstream>

#include <stdio.h>

namespace tair {
/* ====================== RsyncConfig ==================== */
RsyncConfig::RsyncConfig() {
    source_ = NULL;
}

RsyncConfig::~RsyncConfig() {
    delete source_;
    for (size_t i = 0; i < distinations_.size(); i++) {
        delete distinations_[i];
    }
    distinations_.clear();
}

RsyncConfig *RsyncConfig::from_string(std::string &str) {
    cJSON *json = cJSON_Parse(str.c_str());
    if (json == NULL || json->type != cJSON_Object) {
        cJSON_Delete(json);
        return NULL;
    }
    cJSON *code = cJSON_GetObjectItem(json, "code");
    if (code != NULL && (code->type != cJSON_Number || code->valueint != 0)) {
        cJSON_Delete(json);
        return NULL;
    }

    log_info("rsync config: %s", str.c_str());

    cJSON *source_json = cJSON_GetObjectItem(json, "source");
    if (source_json == NULL) {
        cJSON_Delete(json);
        return NULL;
    }

    RsyncConfig *config = new RsyncConfig();

    config->source_ = ClusterHandler::Config::from_json(source_json);
    if (config->source_ == NULL) {
        cJSON_Delete(json);
        delete config;
        return NULL;
    }

    cJSON *distinations_json = cJSON_GetObjectItem(json, "distinations");
    if (distinations_json != NULL && distinations_json->type == cJSON_Array) {
        int dist_arr_len = cJSON_GetArraySize(distinations_json);
        for (int i = 0; i < dist_arr_len; i++) {
            cJSON *distination_json = cJSON_GetArrayItem(distinations_json, i);
            ClusterHandler::Config *c = ClusterHandler::Config::from_json(distination_json);
            if (c != NULL) {
                config->distinations_.push_back(c);
            }
        }
    }

    cJSON_Delete(json);
    return config;
}

std::string RsyncConfig::to_string() const {
    cJSON *json = cJSON_CreateObject();
    if (source_ != NULL) {
        cJSON *source_json = source_->to_json();
        cJSON_AddItemToObject(json, "source", source_json);
    }
    cJSON *distinations_json = cJSON_CreateArray();
    for (size_t i = 0; i < distinations_.size(); i++) {
        if (distinations_[i] != NULL) {
            cJSON *distination_json = distinations_[i]->to_json();
            if (distinations_json != NULL) {
                cJSON_AddItemToArray(distinations_json, distination_json);
            }
        }
    }
    cJSON_AddItemToObject(json, "distinations", distinations_json);
    char *ptr = cJSON_Print(json);
    std::string ret = ptr;
    delete ptr;
    cJSON_Delete(json);
    return ret;
}

/* ============================= End ====================== */

NewRemoteSyncManager::NewRemoteSyncManager(tair_manager *manager)
        : IRemoteSyncManager(IRemoteSyncManager::NEW_VERSION) {
    tair_manager_ = manager;
    last_time_ = time(NULL);
    is_paused_ = false;
    wait_us_ = 0;
    instance_count_ = 0;
    config_version_ = 0;
    last_check_delete_log_file_ = NULL;
    local_cluster_handler_ = new ClusterHandler();
    remote_cluster_handlers_ = new CLUSTER_HANDLER_MAP();
    lazy_release_cluster_handler_ = local_cluster_handler_;
    sem_init(&stop_sem_, 0, 0);
    System::Net::HttpGobal::Initialize();
}

NewRemoteSyncManager::~NewRemoteSyncManager() {
    stop();
    asm volatile("":: : "memory");
    sem_post(&stop_sem_);
    wait();
    sem_destroy(&stop_sem_);

    // clean garbages before than release local,remote handler
    clean_garbages(true);

    delete local_cluster_handler_;
    lazy_release_cluster_handler_ = NULL;

    assert(remote_cluster_handlers_ != NULL);
    CLUSTER_HANDLER_MAP::iterator it;
    for (it = remote_cluster_handlers_->begin(); it != remote_cluster_handlers_->end(); it++) {
        delete it->second;
    }
    delete remote_cluster_handlers_;
    delete last_check_delete_log_file_;
    System::Net::HttpGobal::Destroy();
}

bool NewRemoteSyncManager::is_master_node(int bucket) {
    assert(bucket >= 0);
    // TODO. we consider all data is due to this node now, all will be ok when emitting do_proxy()
    return tair_manager_->get_table_manager()->is_master(bucket, TAIR_SERVERFLAG_CLIENT) ||
           tair_manager_->get_table_manager()->is_master(bucket, TAIR_SERVERFLAG_PROXY);
}

void NewRemoteSyncManager::callback(int ret, void *arg) {
    RemoteClusterHandler::CallbackArg *cb = (RemoteClusterHandler::CallbackArg *) arg;
    cb->handler_->put_result(common::Result<RemoteClusterHandler::CallbackArg *>(cb, ret));
}

static int subfilter(std::set<int> *forbidden_namespaces,
                     std::set<int> *allowed_namespaces, KVRecord *record) {
    int ns = record->key_->get_area();
    if (forbidden_namespaces != NULL && forbidden_namespaces->size() > 0) {
        if (forbidden_namespaces->find(ns) == forbidden_namespaces->end()) {
            return TAIR_RETURN_SUCCESS;
        } else {
            return TAIR_RETURN_FAILED;
        }
    } else {
        if (allowed_namespaces->find(ns) == allowed_namespaces->end()) {
            return TAIR_RETURN_FAILED;
        } else {
            return TAIR_RETURN_SUCCESS;
        }
    }
}

common::Result<common::Record *> NewRemoteSyncManager::filter(Record *record, RemoteClusterHandler *handler,
                                                              NewRemoteSyncManager *manager) {
    set<int> *forbidden_namespaces = handler->get_forbidden_namespaces();
    set<int> *allowed_namespaces = handler->get_allowed_namespaces();
    if ((forbidden_namespaces == NULL || forbidden_namespaces->size() == 0) &&
        (allowed_namespaces == NULL || allowed_namespaces->size() == 0)) {
        return Result<Record *>(record, TAIR_RETURN_SUCCESS);
    }

    if (record->type_ == KeyValue) {
        int ret = subfilter(forbidden_namespaces, allowed_namespaces, (KVRecord *) record);
        return Result<Record *>(record, ret);
    }

    assert(record->type_ == BatchKeyValue);
    BatchBucketRecord *bbr = (BatchBucketRecord *) record;
    std::vector<Record *> &v = *(bbr->value_);
    std::queue<Record *> q;
    for (size_t i = 0; i < v.size(); i++) {
        int ret = subfilter(forbidden_namespaces, allowed_namespaces, (KVRecord *) v[i]);
        if (ret == TAIR_RETURN_FAILED) {
            delete v[i];
        } else {
            q.push(v[i]);
        }
        v[i] = NULL;
    }
    if (q.empty()) {
        return Result<Record *>(record, TAIR_RETURN_FAILED);
    } else {
        size_t index = 0;
        v.resize(q.size());
        while (!q.empty()) {
            v[index++] = q.front();
            q.pop();
        }
        return Result<Record *>(record, TAIR_RETURN_SUCCESS);
    }
}

std::string NewRemoteSyncManager::get_rsync_info() {
    std::ostringstream ss;
    std::vector<std::pair<std::string, std::string> > v;
    for (int i = 0; i < instance_count_; i++) {
        v = get_rsync_info(i);
        for (size_t j = 0; j < v.size(); j++) {
            ss << i << "#" << v[j].first << " => " << v[j].second << std::endl;
        }
    }
    return ss.str();
}

std::vector<std::pair<std::string, std::string> > NewRemoteSyncManager::get_rsync_info(int instance) {
    std::vector<std::pair<std::string, std::string> > v;
    CLUSTER_HANDLER_MAP::iterator iter;
    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    if (m == NULL) return v;
    for (iter = m->begin(); iter != m->end(); iter++) {
        string info = iter->second->get_min_position(instance).to_string();
        if (iter->second->paused()) {
            info = "Paused!" + info;
        } else {
            info = "NotPaused!" + info;
        }
        v.push_back(std::make_pair<std::string, std::string>(iter->second->sid(), info));
    }
    return v;
}

int NewRemoteSyncManager::get_rsync_config_update_interval() {
    return rsync_config_update_interval_;
}

void NewRemoteSyncManager::set_rsync_config_update_interval(int interval) {
    rsync_config_update_interval_ = interval;
}

// thread safe based on delay delete
std::string NewRemoteSyncManager::get_cluster_info() {
    cJSON *json = cJSON_CreateObject();
    ClusterHandler *handler = local_cluster_handler_;
    if (handler != NULL) {
        cJSON *source_json = handler->get_cluster_info();
        if (source_json != NULL) {
            cJSON_AddItemToObject(json, "source", source_json);
        }
    }

    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    if (m != NULL && m->empty() == false) {
        cJSON *distinations_json = cJSON_CreateArray();
        for (CLUSTER_HANDLER_MAP::iterator iter = m->begin(); iter != m->end(); iter++) {
            cJSON *distination_json = iter->second->get_cluster_info();
            if (distinations_json != NULL) {
                cJSON_AddItemToArray(distinations_json, distination_json);
            }
        }
        cJSON_AddItemToObject(json, "distinations", distinations_json);
    }

    char *ptr = cJSON_Print(json);
    std::string ret = ptr;
    delete ptr;
    cJSON_Delete(json);
    return ret;
}

std::string NewRemoteSyncManager::connection_stat() {
    std::stringstream ss;
    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    ss << "Remote Cluster Connection Statistics:" << std::endl;
    if (m != NULL && m->empty() == false) {
        for (CLUSTER_HANDLER_MAP::iterator iter = m->begin(); iter != m->end(); iter++) {
            ss << iter->second->sid() << " => " << iter->second->connection_stat() << std::endl;
        }
    }
    ss << "Local Cluster Connection Statistics:" << std::endl;
    ClusterHandler *tch = local_cluster_handler_;
    if (tch != NULL) {
        ss << tch->sid() << " => " << tch->connection_stat() << std::endl;
    }
    return ss.str();
}

int NewRemoteSyncManager::init() {
//    if ((ret = init_sync_conf()) != TAIR_RETURN_SUCCESS) {
//      log_error("init remote sync config fail: %d", ret);
//      return ret;
//    }
    remote_url_ = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, "rsync_config_service", "");
    if (remote_url_.empty()) {
        log_error("please set rsync config service !!");
        return TAIR_RETURN_FAILED;
    }

    rsync_config_update_interval_ = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, "rsync_config_update_interval", 600);

    instance_count_ = tair_manager_->get_storage_manager()->get_instance_count();
    last_check_delete_log_file_ = new time_t[instance_count_];
    time_t tm = time(NULL);
    for (int i = 0; i < instance_count_; i++) {
        last_check_delete_log_file_[i] = tm;
    }

    // zero is update config thread
    setThreadCount(MAX_WORKER_THREAD_NUM + 1);

    start();

    return TAIR_RETURN_SUCCESS;
}

void NewRemoteSyncManager::pause(bool pause) {
    CLUSTER_HANDLER_MAP::iterator iter;
    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    if (m == NULL) return;
    for (iter = m->begin(); iter != m->end(); iter++) {
        iter->second->pause(pause);
        std::pair<std::string, std::string> p = iter->second->get_unit_and_group();
        log_warn("sync data to %s:%s -> %s", p.first.c_str(), p.second.c_str(), (pause ? "pause" : "resume"));
    }
}

int NewRemoteSyncManager::pause(bool pause,
                                const std::string unit, const std::string group) {
    CLUSTER_HANDLER_MAP::iterator iter;
    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    if (m != NULL) {
        for (iter = m->begin(); iter != m->end(); iter++) {
            std::pair<std::string, std::string> p = iter->second->get_unit_and_group();
            if (p.first == unit && p.second == group) {
                iter->second->pause(pause);
                log_warn("sync data to %s:%s -> %s", unit.c_str(), group.c_str(), (pause ? "pause" : "resume"));
                return TAIR_RETURN_SUCCESS;
            }
        }
    }
    log_warn("can't find cluster handler for group: %s at unit: %s", group.c_str(), unit.c_str());
    return TAIR_RETURN_FAILED;
}

int NewRemoteSyncManager::watch(const std::string unit,
                                const std::string group) {
    CLUSTER_HANDLER_MAP::iterator iter;
    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    if (m != NULL) {
        if (unit == "all") {
            for (iter = m->begin(); iter != m->end(); iter++) {
                assert(iter->second != NULL);
                iter->second->watch();
            }
            return TAIR_RETURN_SUCCESS;
        } else {
            for (iter = m->begin(); iter != m->end(); iter++) {
                std::pair<std::string, std::string> p = iter->second->get_unit_and_group();
                if (p.first == unit && p.second == group) {
                    iter->second->watch();
                    return TAIR_RETURN_SUCCESS;
                }
            }
        }
    }
    log_warn("can't find cluster handler for group: %s at unit: %s", group.c_str(), unit.c_str());
    return TAIR_RETURN_FAILED;
}

int NewRemoteSyncManager::statistics(const std::string unit,
                                     const std::string group) {
    CLUSTER_HANDLER_MAP::iterator iter;
    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    if (m != NULL) {
        if (unit == "all") {
            for (iter = m->begin(); iter != m->end(); iter++) {
                assert(iter->second != NULL);
                iter->second->statistics();
            }
            return TAIR_RETURN_SUCCESS;
        } else {
            for (iter = m->begin(); iter != m->end(); iter++) {
                std::pair<std::string, std::string> p = iter->second->get_unit_and_group();
                if (p.first == unit && p.second == group) {
                    iter->second->statistics();
                    return TAIR_RETURN_SUCCESS;
                }
            }
        }
    }
    log_warn("can't find cluster handler for group: %s at unit: %s", group.c_str(), unit.c_str());
    return TAIR_RETURN_FAILED;
}

std::string NewRemoteSyncManager::options(const std::string unit, const std::string group) {
    ostringstream os;
    CLUSTER_HANDLER_MAP::iterator iter;
    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    if (m != NULL) {
        if (unit == "all") {
            for (iter = m->begin(); iter != m->end(); iter++) {
                assert(iter->second != NULL);
                os << iter->second->sid() << " => " << iter->second->options() << std::endl;
            }
        } else {
            for (iter = m->begin(); iter != m->end(); iter++) {
                assert(iter->second != NULL);
                std::pair<std::string, std::string> p = iter->second->get_unit_and_group();
                if (p.first == unit && p.second == group) {
                    os << iter->second->sid() << " => " << iter->second->options() << std::endl;
                    break;
                }
            }
        }
    }
    return os.str();
}

int NewRemoteSyncManager::options(const std::string unit,
                                  const std::string group, std::map<std::string, int> &mp) {
    CLUSTER_HANDLER_MAP::iterator iter;
    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    if (m != NULL) {
        if (unit == "all") {
            for (iter = m->begin(); iter != m->end(); iter++) {
                assert(iter->second != NULL);
                iter->second->options(mp);
            }
            return TAIR_RETURN_SUCCESS;
        } else {
            for (iter = m->begin(); iter != m->end(); iter++) {
                std::pair<std::string, std::string> p = iter->second->get_unit_and_group();
                if (p.first == unit && p.second == group) {
                    iter->second->options(mp);
                    return TAIR_RETURN_SUCCESS;
                }
            }
        }
    }
    log_warn("can't find cluster handler for group: %s at unit: %s", group.c_str(), unit.c_str());
    return TAIR_RETURN_FAILED;
}

void NewRemoteSyncManager::set_wait_us(int64_t us) {
    wait_us_ = us;
}

int64_t NewRemoteSyncManager::get_wait_us() {
    return wait_us_;
}

std::string NewRemoteSyncManager::get_rsync_config_service() {
    return remote_url_;
}

void NewRemoteSyncManager::set_rsync_config_service(const std::string &address) {
    remote_url_ = address;
}

RsyncConfig *NewRemoteSyncManager::get_json_format_config() {
    System::Net::HttpWebRequest *req = System::Net::HttpWebRequest::Create(remote_url_);
    System::Net::HttpWebResponse *resp = req->GetResponse();
    std::string str = resp->GetResponseStream().str();
    req->Destroy();
    resp->Destroy();

    if (str.empty()) {
        log_warn("connect to rsync config web service %s fail", remote_url_.c_str());
        return NULL;
    }

    RsyncConfig *config = RsyncConfig::from_string(str);
    if (config == NULL) {
        log_warn("error format rsync config:\n%s", str.c_str());
    }
    return config;
}

RsyncConfig *NewRemoteSyncManager::get_rsync_config() {
    return get_json_format_config();
}

RsyncConfig *NewRemoteSyncManager::check_rsync_config(RsyncConfig *config) {
    if (config->source_ == NULL) { // remotes_ can be empty mean, not do any rsync include local rsync
        delete config;
        return NULL;
    }
    if (config->source_->valid() == false) {
        delete config;
        return NULL;
    }

    size_t size = config->distinations_.size();
    for (size_t i = 0; i < size; i++) {
        if (config->distinations_[i]->valid() == false ||
            config->distinations_[i]->type_ == RSYNC_SELF) {
            delete config;
            return NULL;
        }
    }

    return config;
}

void NewRemoteSyncManager::update_thread(int id) {
    while (!_stop) {
        // TODO get rsync config
        RsyncConfig *config = get_rsync_config();
        if (config != NULL) {
            // check rsync config
            config = check_rsync_config(config);
            // update rsync config
            if (config != NULL) {
                log_warn("== Now Config Passed All Valid Check, Go To Apply It ! ==");
                update_rsync_config(config);
                delete config;
                config = NULL;
            } else {
                log_warn("config from rsync config service has some errors, please check your config");
            }
        }

        {
            int s;
            struct timespec timeout;
            timeout.tv_sec = time(NULL) + rsync_config_update_interval_;
            timeout.tv_nsec = 0;
            while ((s = sem_timedwait(&stop_sem_, &timeout)) == -1 && errno == EINTR) {
                sched_yield();
            }
            asm volatile("":: : "memory");
            if (s > 0 && _stop == true) break;
        }
    }
}

void NewRemoteSyncManager::work_thread(int id) {
    // at start wait all handler be inited, be started as well
    while (!_stop &&
           local_cluster_handler_->get_status() == ClusterHandler::UNINIT) {
        log_error("%d tell: local cluster handler is not init", id);
        sleep(1);
    }

    if (!_stop) {
        do_remote_sync();
    }
}

void NewRemoteSyncManager::run(tbsys::CThread *thread, void *args) {
    // index == 0 => update config thread
    // index > 0  => worker thread
    // now wo just have one update config thread, one worker thread
    int32_t index = static_cast<int32_t>(reinterpret_cast<int64_t>(args));
    if (index == 0) {
        log_warn("I am update thread");
        update_thread(index);
    } else {
        log_warn("I am work thread");
        // TODO at future may need to use thread pool to do work thread
        work_thread(index);
    }
    log_warn("%s %d say: exit, bye",
             index == 0 ? "update thread" : "work thread", index);
}

void NewRemoteSyncManager::do_remote_sync() {
    uint64_t need_wait_us = 0;
    while (!_stop) {
        bool has_one_success = false;
        int64_t loop_times = need_wait_us / BASE_WAIT_US;
        do {
            CLUSTER_HANDLER_MAP::iterator iter;
            CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
            for (iter = m->begin(); iter != m->end(); iter++) {
                RemoteClusterHandler *handler = iter->second;
                //int count = 2;  // TODO to be dynamic configure
                while (!handler->no_results()/* && (count--)*/) {
                    common::Result<RemoteClusterHandler::CallbackArg *> result = handler->get_result();
                    RemoteClusterHandler::CallbackArg *value = result.value_;
                    bool success = (result.code_ == TAIR_RETURN_SUCCESS ||
                                    result.code_ == TAIR_RETURN_DATA_NOT_EXIST);
                    if (!success && result.code_ != TAIR_RETURN_TIMEOUT) {
                        // if timeout will print error log at tair_client_api_impl.cpp:3310
                        log_warn("result code: %d", result.code_);
                    } else if (success) {
                        has_one_success = true;
                    }
                    handler->update(value->instance_, value->bucket_, success);
                    delete value;
                }
            }

            if (_stop == true) return;

            if (has_one_success == true) {
                // if has one success back, mean must have one can try get record,
                // in that case, need no wait more
                break;
            }

            if (loop_times > 0) {
                // log_warn("need sleep time: %" PRI64_PREFIX "d" , need_wait_us);
                usleep(BASE_WAIT_US);
                loop_times--;
            }
        } while (loop_times);

        need_wait_us = wait_us_;

        if (is_paused_) {
            need_wait_us = DEFAULT_WAIT_US;
            continue;
        }

        // each loop reget local_cluster_handler_, the object event if it is deleted,
        // its memory will release after one day, here is safe
        lazy_release_cluster_handler_ = local_cluster_handler_;
        // only can be two status, inited or started, for one local cluster handler object,
        // can't from started status to inited status
        // inited => mean update thread create a new local cluster handler, but start client fail,
        //           we need try start it util it is started
        // started => mean update thread do no update or create a new local cluster handler prefectly
        // for easy to deal with status, now we just make sure below logic running under cluster handler
        // is started
        if (lazy_release_cluster_handler_->get_status() != ClusterHandler::STARTED) {
            int status = lazy_release_cluster_handler_->start();
            if (status != TAIR_RETURN_SUCCESS) {
                log_error("lazy release cluster handler start fail, status %d", status);
                need_wait_us = DEFAULT_WAIT_US;
                continue;
            } else {
                log_warn("== lazy release cluster handler start OK ==");
            }
        }

        int ret = TAIR_RETURN_DATA_NOT_EXIST;
        for (int i = 0; i < instance_count_; i++) {
            int code = do_remote_for_instance(i);
            // TAIR_RETURN_SUCCESS
            // TAIR_RETURN_DATA_NOT_EXIST
            if (code == TAIR_RETURN_SUCCESS) {
                // equals or greater than one success return success
                ret = code;
            }
        }

        if (ret == TAIR_RETURN_DATA_NOT_EXIST) {
            if (wait_us_ > REST_WAIT_US) {
                need_wait_us = 3 * wait_us_ / 2;
            } else {
                need_wait_us = REST_WAIT_US;
            }
        }
    }
}

void NewRemoteSyncManager::try_clean_useless_log_file(int instance) {
    if (last_check_delete_log_file_[instance] + CHECK_INTERVAL < time(NULL)) {
        last_check_delete_log_file_[instance] = time(NULL);
        common::RecordPosition min_position;
        CLUSTER_HANDLER_MAP::iterator iter;
        CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
        for (iter = m->begin(); iter != m->end(); iter++) {
            common::RecordPosition position = iter->second->get_min_position(instance);
            if (position.valid()) {
                if (min_position.valid() == false || position < min_position) {
                    min_position = position;
                }
            }
        }
        if (min_position.valid()) {
            m->begin()->second->delete_log_file(instance, min_position.filenumber_);
        }
    }
}

int NewRemoteSyncManager::do_remote_for_instance(int instance) {
    int ret = TAIR_RETURN_DATA_NOT_EXIST;
    CLUSTER_HANDLER_MAP::iterator iter;
    CLUSTER_HANDLER_MAP *m = remote_cluster_handlers_;
    for (iter = m->begin(); iter != m->end(); iter++) {
        assert(iter->second != NULL && iter->second->get_status() != ClusterHandler::UNINIT);
        if (LIKELY((iter->second->get_status() == ClusterHandler::STARTED))) {
            int code = do_remote_for_cluster(instance, iter->second, filter);
            // TAIR_RETURN_SUCCESS
            // TAIR_RETURN_DATA_NOT_EXIST
            if (code == TAIR_RETURN_SUCCESS) {
                // equals or greater than one success return success
                ret = code;
            }
        } else {
            iter->second->start();
        }
    }

    // Note: Dangerous
    // clean useless log file will remove local log file, call this
    // function, please make sure no one will hold old log file whose filenumber
    // smaller than min_position, now work thread is single thread, operating log file
    // only happen in this thread, so here is safe
    try_clean_useless_log_file(instance);

    return ret;
}

int NewRemoteSyncManager::do_remote_for_cluster(int instance,
                                                RemoteClusterHandler *handler, FilterRecordFunc filter) {
    int ret = TAIR_RETURN_SUCCESS;
    common::Result<common::Record *> result = handler->get_record(instance);
    if (result.code_ != TAIR_RETURN_SUCCESS) return result.code_;  /* data not exists */

    result = filter(result.value_, handler, this);
    if (result.code_ != TAIR_RETURN_SUCCESS) {
        handler->update(instance, result.value_->bucket_, true);
        return result.code_;
    }

    bool is_master = is_master_node(result.value_->origin_bucket_);

    if (is_master == false) {
        result = do_get_from_local_cluster(result.value_);
        ret = result.code_;
    }

    if (ret == TAIR_RETURN_SUCCESS) {
        if (result.value_->type_ == BatchKeyValue) {
            common::BatchBucketRecord *br = static_cast<common::BatchBucketRecord *>(result.value_);
            std::vector<common::Record *> &v = *(br->value_);
            if (handler->get_type() == RSYNC_INTERUNIT) {
                std::vector<int> droped_index;
                for (size_t i = 0; i < v.size(); i++) {
                    if (v[i]->type_ == KeyValue) {
                        common::KVRecord *kvrecord = static_cast<common::KVRecord *>(v[i]);
                        if (is_meta_flag_unit(kvrecord->key_->data_meta.flag) == true) {
                            droped_index.push_back(i);
                        } else {
                            set_meta_flag_sending_unit(kvrecord->key_->data_meta.flag);
                        }
                    }
                }

                for (size_t i = droped_index.size(); i > 0; i--) {
                    common::Record *tmp = v[droped_index[i - 1]];
                    v.erase(v.begin() + droped_index[i - 1]);
                    delete tmp;;
                }
            }

            if (v.size() == 0) {
                handler->update(instance, result.value_->bucket_, true);
                return TAIR_RETURN_SUCCESS;
            }

            for (size_t i = 0; i < v.size(); i++) {
                if (v[i]->type_ == KeyValue) {
                    common::KVRecord *kvrecord = static_cast<common::KVRecord *>(v[i]);
                    attach_info_to_key(kvrecord->key_, kvrecord->value_, handler->get_type());
                }
            }
            ret = do_remote_sync_op(instance, result.value_, handler);
        } else {
            log_error("ignore unexpected type %d", result.value_->type_);
            handler->update(instance, result.value_->bucket_, true);
            return TAIR_RETURN_SUCCESS;
        }
    } else {
        handler->update(instance, result.value_->bucket_, false);
        log_warn("get data from local cluster fail");
    }

    return ret;
}

int NewRemoteSyncManager::do_remote_sync_op(int instance, Record *record, RemoteClusterHandler *handler) {
    log_debug("do remote sync op");

    int ret = TAIR_RETURN_SUCCESS;

    RemoteClusterHandler::CallbackArg *arg =
            new RemoteClusterHandler::CallbackArg(instance, record->bucket_, handler);

    switch (record->operator_type_) {
        case TAIR_REMOTE_SYNC_TYPE_BATCH: {
            std::vector<common::Record *> *v = static_cast<BatchBucketRecord *>(record)->value_;
            ret = handler->client()->sync(record->bucket_, v, false,
                                          &NewRemoteSyncManager::callback, arg);
        }
            break;
        default:
            log_error("please check code %d, must where is error", record->operator_type_);
    }

    if (ret != TAIR_RETURN_SUCCESS) {
        // maybe ret == TAIR_RETURN_REMOTE_SLOW
        handler->update(instance, record->bucket_, false);
        delete arg;

        if (last_time_ != time(NULL)) {
            last_time_ = time(NULL);
            log_warn("send request to slave cluster, get ret: %d", ret);
        }
    }

    return ret;
}

common::Result<common::Record *> NewRemoteSyncManager::do_get_from_local_cluster(common::Record *record) {
    int ret = TAIR_RETURN_SUCCESS;
    std::vector<size_t> not_need_send;
    if (record->type_ == BatchKeyValue) {
        common::BatchBucketRecord *br = static_cast<common::BatchBucketRecord *>(record);
        std::vector<common::Record *> &v = *(br->value_);
        for (size_t i = 0; i < v.size(); i++) {
            assert(v[i]->type_ == KeyValue);
            common::KVRecord *kvrecord = static_cast<common::KVRecord *>(v[i]);

            data_entry *value = NULL;
            ret = lazy_release_cluster_handler_->client()->get_hidden(kvrecord->key_->get_area(),
                                                                      *(kvrecord->key_), value);
            if (ret == TAIR_RETURN_HIDDEN) {
                ret = TAIR_RETURN_SUCCESS;
            }
            if (kvrecord->operator_type_ == TAIR_REMOTE_SYNC_TYPE_PUT) {
                if (ret == TAIR_RETURN_SUCCESS) {
                    if (kvrecord->value_ != NULL) {
                        delete kvrecord->value_;
                        kvrecord->value_ = NULL;
                    }
                    kvrecord->value_ = value;
                } else if (ret == TAIR_RETURN_DATA_NOT_EXIST) {
                    delete value;
                    kvrecord->operator_type_ = TAIR_REMOTE_SYNC_TYPE_DELETE;
                } else {
                    delete value;
                    return Result<common::Record *>(record, ret);
                }
            } else if (kvrecord->operator_type_ == TAIR_REMOTE_SYNC_TYPE_DELETE) {
                delete value;
                if (ret == TAIR_RETURN_SUCCESS) {
                    not_need_send.push_back(i);
                } else if (ret != TAIR_RETURN_DATA_NOT_EXIST) {
                    return Result<common::Record *>(record, ret);
                }
            }
        }
        for (size_t i = not_need_send.size(); i > 0; i--) {
            common::Record *r = v[not_need_send[i - 1]];
            v.erase(v.begin() + not_need_send[i - 1]);
            delete r;
        }
    }
    return Result<common::Record *>(record, TAIR_RETURN_SUCCESS);
}

void NewRemoteSyncManager::attach_info_to_key(data_entry *key, data_entry *value, int type) {
    if (value != NULL) {
        key->data_meta.cdate = value->data_meta.cdate;
        key->data_meta.edate = value->data_meta.edate;
        key->data_meta.mdate = value->data_meta.mdate;
        key->data_meta.version = value->data_meta.version;
    }

    assert(type != RSYNC_SELF);

    if (type == RSYNC_INNERUNIT) {
        key->server_flag = TAIR_SERVERFLAG_RSYNC;
    } else {
        // sync data from A unit to B unit, for B unit, A just a simple client
        key->server_flag = TAIR_SERVERFLAG_CLIENT;
    }
    // key->data_meta.flag = 0;
}

void NewRemoteSyncManager::throw_garbage(void *data,
                                         garbage_destory_func func) {
    NewRemoteSyncManager::Garbage *garbage = new NewRemoteSyncManager::Garbage();
    garbage->data_ = data;
    garbage->destroy_ = func;
    garbage->dead_time_ = time(NULL) + TIME_OF_DAY;
    garbages_.push(garbage);
}

void NewRemoteSyncManager::clean_garbages(bool force) {
    while (!garbages_.empty()) {
        NewRemoteSyncManager::Garbage *garbage = garbages_.front();
        if (force == true || garbage == NULL || garbage->dead_time_ < time(NULL)) {
            garbages_.pop();
            if (garbage != NULL) {
                garbage->destroy_(garbage->data_);
            }
            delete garbage;
        } else {
            break;
        }
    }
}

void NewRemoteSyncManager::destroy_cluster_map(void *m) {
    CLUSTER_HANDLER_MAP *chm = (CLUSTER_HANDLER_MAP *) m;
    for (CLUSTER_HANDLER_MAP::iterator iter = chm->begin();
         iter != chm->end(); iter++) {
        if (iter->second->GetRef() == 0) {
            delete iter->second;
        }
    }
    delete chm;
}

void NewRemoteSyncManager::update_detail_cluster_config(ClusterHandler *handler, ClusterHandler::Config *config) {
    handler->set_time_out(config->timeout_ms_);
    handler->set_queue_limit(config->queue_limit_);
    handler->set_unit(config->unit_);
    handler->set_type(config->type_);
    // update handler type or not ?
    assert(handler->get_allowed_namespaces() != NULL); // fuck

    std::set<int> *namespaces = config->allowed_namespaces_;
    tair::common::Result<std::set<int> *> result = handler->cas_allowed_namespaces(namespaces);
    if (result.code_ == TAIR_RETURN_SUCCESS) {
        // here result.value_ need lazy destroy, maybe some thread hold it
        config->allowed_namespaces_ = NULL;
        throw_garbage(result.value_, ClusterHandler::Config::destroy_namespaces);
    }

    // close compiler re-order optimizing
    // cas allowed must before cas forbidden, because forbidden_namespaces
    asm volatile("":: : "memory");

    assert(handler->get_forbidden_namespaces() != NULL); // fuck

    namespaces = config->forbidden_namespaces_;
    result = handler->cas_forbidden_namespaces(namespaces);
    if (result.code_ == TAIR_RETURN_SUCCESS) {
        // here result.value_ need lazy destroy, maybe some thread hold it
        config->forbidden_namespaces_ = NULL;
        throw_garbage(result.value_, ClusterHandler::Config::destroy_namespaces);
    }
}

void NewRemoteSyncManager::update_rsync_self_config(ClusterHandler::Config *&lconfig) {
    if (local_cluster_handler_->equals(lconfig)) {
        update_detail_cluster_config(local_cluster_handler_, lconfig);
    } else {
        // instead of local_cluster_handler_
        // start new handler
        ClusterHandler *handler = new ClusterHandler();
        int status = TAIR_RETURN_FAILED;
        status = handler->prepare_and_start(lconfig);
        lconfig = NULL; // set lconfig to be NULL, keep config in handler
        if (status != TAIR_RETURN_SUCCESS) {
            log_error("update thread say: local cluster %s start fail => %d", handler->id().c_str(), status);
        }
        // instead of old handler
        throw_garbage(local_cluster_handler_, ClusterHandler::destroy);
        local_cluster_handler_ = handler;
    }
}

bool NewRemoteSyncManager::is_rsync_remote_config_equals(std::vector<ClusterHandler::Config *> &remotes) {
    if (remote_cluster_handlers_->size() != remotes.size()) return false;
    for (size_t i = 0; i < remotes.size(); i++) {
        if (remote_cluster_handlers_->find(remotes[i]->info_) == remote_cluster_handlers_->end()) {
            return false;
        }
    }
    return true;
}

// TODO optimizing
void NewRemoteSyncManager::update_rsync_remote_config(std::vector<ClusterHandler::Config *> &remotes) {
    if (is_rsync_remote_config_equals(remotes) == true) {
        // may need update cluster config
        for (size_t i = 0; i < remotes.size(); i++) {
            CLUSTER_HANDLER_MAP::iterator iter = remote_cluster_handlers_->find(remotes[i]->info_);
            assert(iter != remote_cluster_handlers_->end());
            update_detail_cluster_config(iter->second, remotes[i]);
        }
    } else {
        // add or remote cluster config
        CLUSTER_HANDLER_MAP *m = new CLUSTER_HANDLER_MAP();
        for (size_t i = 0; i < remotes.size(); i++) {
            ClusterHandler::Config *rconfig = remotes[i];
            CLUSTER_HANDLER_MAP::iterator iter = m->find(rconfig->info_);
            if (iter == m->end()) {
                // start new handler
                RemoteClusterHandler *handler = new RemoteClusterHandler();
                int ret = handler->prepare_and_start(rconfig,
                                                     tair_manager_->get_storage_manager()->get_remote_sync_logger(1));
                remotes[i] = NULL; // keep rconfig in handler
                if (ret != TAIR_RETURN_SUCCESS) {
                    log_error("update thread say: cluster %s start fail => %d", handler->id().c_str(), ret);
                }
                int ref = handler->Ref(); //incr ref
                UNUSED(ref);
                assert(ref == 1);
                m->insert(std::make_pair<std::string, RemoteClusterHandler *>(handler->get_info(), handler));
            } else {
                int ref = iter->second->Ref(); // incr ref
                UNUSED(ref);
                assert(ref == 2);
                m->insert(std::make_pair<std::string, RemoteClusterHandler *>(iter->first, iter->second));
                // maybe need update cluster config
                update_detail_cluster_config(iter->second, rconfig);
            }
        }

        // instead of old map
        for (CLUSTER_HANDLER_MAP::iterator iter = remote_cluster_handlers_->begin();
             iter != remote_cluster_handlers_->end(); iter++) {
            iter->second->UnRef(); // decr ref
        }
        throw_garbage(remote_cluster_handlers_, NewRemoteSyncManager::destroy_cluster_map);
        remote_cluster_handlers_ = m;
    }
}

// not thread-safe
void NewRemoteSyncManager::update_rsync_config(RsyncConfig *config) {
    clean_garbages();

    update_rsync_self_config(config->source_);
    update_rsync_remote_config(config->distinations_);
}
}
