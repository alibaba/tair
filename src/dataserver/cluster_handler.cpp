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

#include "cluster_handler.hpp"
#include "common/define.hpp"
#include "common/util.hpp"
#include "common/result.hpp"
#include "common/record.hpp"
#include "common/cJSON.h"
#include "client/tair_client_api_impl.hpp"
#include "record_logger.hpp"
#include "storage/storage_manager.hpp"

namespace tair {

/* =======================Config===================== */
ClusterHandler::Config::Config() :
        info_(""), master_cs_addr_(""), slave_cs_addr_(""), group_name_(""), unit_("") {
    queue_limit_ = DEFAULT_QUEUE_LIMIT;
    timeout_ms_ = DEFAULT_TIMEOUT;
    type_ = RSYNC_SELF;
    distance_ = 0;
    forbidden_namespaces_ = NULL;
    allowed_namespaces_ = NULL;
}

ClusterHandler::Config::~Config() {
    delete forbidden_namespaces_;
    delete allowed_namespaces_;
}

bool ClusterHandler::Config::valid() {
    if (master_cs_addr_ == "" ||
        group_name_ == "") {
        return false;
    }
    return true;
}

// thread-unsafe
cJSON *ClusterHandler::Config::to_json() const {
    /*
     *      {
     *        "master": "xxx.xxx.xxx.xxx:5199",
     *        "slave": "xxx.xxx.xxx.xxx:5199",
     *        "group": "group3",
     *        "queuelimit": 800,
     *        "timeout": 2000,
     *        "type": "remote",
     *        "distance": 20,
     *        "unit": "Shanghai",
     *        "allows": [0, 1, 2, 4]
     *      }
     */
    cJSON *json = cJSON_CreateObject();
    if (master_cs_addr_.empty() == false) {
        cJSON *master_json = cJSON_CreateString(master_cs_addr_.c_str());
        cJSON_AddItemToObject(json, "master", master_json);
    }
    if (slave_cs_addr_.empty() == false) {
        cJSON *slave_json = cJSON_CreateString(slave_cs_addr_.c_str());
        cJSON_AddItemToObject(json, "slave", slave_json);
    }
    if (group_name_.empty() == false) {
        cJSON *group_json = cJSON_CreateString(group_name_.c_str());
        cJSON_AddItemToObject(json, "group", group_json);
    }

    cJSON *queuelimit_json = cJSON_CreateNumber(queue_limit_);
    cJSON_AddItemToObject(json, "queuelimit", queuelimit_json);

    cJSON *timeout_json = cJSON_CreateNumber(timeout_ms_);
    cJSON_AddItemToObject(json, "timeout", timeout_json);

    if (type_ == RSYNC_SELF) {
        cJSON *type_json = cJSON_CreateString("self");
        cJSON_AddItemToObject(json, "type", type_json);
    } else if (type_ == RSYNC_INNERUNIT) {
        cJSON *type_json = cJSON_CreateString("innerunit");
        cJSON_AddItemToObject(json, "type", type_json);
    } else if (type_ == RSYNC_INTERUNIT) {
        cJSON *type_json = cJSON_CreateString("interunit");
        cJSON_AddItemToObject(json, "type", type_json);
    }

    cJSON *distance_json = cJSON_CreateNumber(distance_);
    cJSON_AddItemToObject(json, "distance", distance_json);

    if (unit_.empty() == false) {
        cJSON *unit_json = cJSON_CreateString(unit_.c_str());
        cJSON_AddItemToObject(json, "unit", unit_json);
    }

    std::set<int> *namespaces = allowed_namespaces_;
    if (namespaces != NULL && namespaces->empty() == false) {
        int count = 0;
        int *arr = new int[namespaces->size()];
        for (std::set<int>::iterator it = namespaces->begin();
             it != namespaces->end(); it++) {
            arr[count++] = *it;
        }
        cJSON *allows_json = cJSON_CreateIntArray(arr, count);
        cJSON_AddItemToObject(json, "allows", allows_json);
        delete arr;
    }

    namespaces = forbidden_namespaces_;
    if (namespaces != NULL && namespaces->empty() == false) {
        int count = 0;
        int *arr = new int[namespaces->size()];
        for (std::set<int>::iterator it = namespaces->begin();
             it != namespaces->end(); it++) {
            arr[count++] = *it;
        }
        cJSON *allows_json = cJSON_CreateIntArray(arr, count);
        cJSON_AddItemToObject(json, "forbiddens", allows_json);
        delete arr;
    }

    return json;
}

ClusterHandler::Config *ClusterHandler::Config::from_json(cJSON *json) {
    /*
     *      {
     *        "master": "xxx.xxx.xxx.xxx:5199",
     *        "slave": "xxx.xxx.xxx.xxx:5199",
     *        "group": "group3",
     *        "queuelimit": 800,
     *        "timeout": 2000,
     *        "type": "remote",
     *        "distance": 20,
     *        "unit": "Shanghai",
     *        "allows": [0, 1, 2, 4]
     *      }
     */
    ClusterHandler::Config *config = new ClusterHandler::Config();
    cJSON *master = cJSON_GetObjectItem(json, "master");
    if (master == NULL || master->type != cJSON_String) {
        delete config;
        return NULL;
    }
    config->master_cs_addr_ = master->valuestring;

    cJSON *slave = cJSON_GetObjectItem(json, "slave");
    if (slave == NULL || slave->type != cJSON_String) {
        config->slave_cs_addr_ = "";
    } else {
        config->slave_cs_addr_ = slave->valuestring;
    }

    cJSON *group = cJSON_GetObjectItem(json, "group");
    if (group == NULL || master->type != cJSON_String) {
        delete config;
        return NULL;
    }
    config->group_name_ = group->valuestring;

    cJSON *queuelimit = cJSON_GetObjectItem(json, "queuelimit");
    if (queuelimit == NULL || queuelimit->type != cJSON_Number || queuelimit->valueint <= 0) {
        config->queue_limit_ = ClusterHandler::Config::DEFAULT_QUEUE_LIMIT;
    } else {
        config->queue_limit_ = queuelimit->valueint;
    }

    cJSON *timeout = cJSON_GetObjectItem(json, "timeout");
    if (timeout == NULL || timeout->type != cJSON_Number || timeout->valueint <= 0) {
        config->timeout_ms_ = ClusterHandler::Config::DEFAULT_TIMEOUT;
    } else {
        config->timeout_ms_ = timeout->valueint;
    }


    cJSON *type = cJSON_GetObjectItem(json, "type");
    if (type == NULL || type->type != cJSON_String) {
        config->type_ = RSYNC_SELF;
    } else {
        if (strcmp("innerunit", type->valuestring) == 0) {
            config->type_ = RSYNC_INNERUNIT;
        } else if (strcmp("interunit", type->valuestring) == 0) {
            config->type_ = RSYNC_INTERUNIT;
        } else {
            config->type_ = RSYNC_SELF;
        }
    }

    cJSON *distance = cJSON_GetObjectItem(json, "distance");
    if (distance == NULL || distance->type != cJSON_Number) {
        config->distance_ = 0;
    } else {
        config->distance_ = distance->valueint;
    }

    cJSON *unit = cJSON_GetObjectItem(json, "unit");
    if (unit == NULL || unit->type != cJSON_String) {
        delete config;
        return NULL;
    } else {
        config->unit_ = unit->valuestring;
    }

    cJSON *allownamespaces = cJSON_GetObjectItem(json, "allows");
    config->allowed_namespaces_ = new std::set<int>();
    if (allownamespaces != NULL && allownamespaces->type == cJSON_Array) {
        int length = cJSON_GetArraySize(allownamespaces);
        for (int i = 0; i < length; i++) {
            cJSON *ns = cJSON_GetArrayItem(allownamespaces, i);
            if (ns != NULL && ns->type == cJSON_Number) {
                config->allowed_namespaces_->insert(ns->valueint);
            }
        }
    }

    cJSON *forbiddennamespaces = cJSON_GetObjectItem(json, "forbiddens");
    config->forbidden_namespaces_ = new std::set<int>();
    if (forbiddennamespaces != NULL && forbiddennamespaces->type == cJSON_Array) {
        int length = cJSON_GetArraySize(forbiddennamespaces);
        for (int i = 0; i < length; i++) {
            cJSON *ns = cJSON_GetArrayItem(forbiddennamespaces, i);
            if (ns != NULL && ns->type == cJSON_Number) {
                config->forbidden_namespaces_->insert(ns->valueint);
            }
        }
    }

    config->encode_info();

    return config;
}

void ClusterHandler::Config::destroy_namespaces(void *namespaces) {
    if (namespaces != NULL) {
        delete (std::set<int> *) namespaces;
    }
}

// not thread-safe
static tair::common::Result<std::set<int> *> compare_and_swap_namespaces(std::set<int> *&oldnamespaces,
                                                                         std::set<int> *&newnamespaces) {
    assert(oldnamespaces != NULL && newnamespaces != NULL);
    if (oldnamespaces == newnamespaces || (*oldnamespaces) == (*newnamespaces)) {
        return tair::common::Result<std::set<int> *>(newnamespaces, TAIR_RETURN_FAILED);
    }
    std::set<int> *tmp = oldnamespaces;
    oldnamespaces = newnamespaces;
    return tair::common::Result<std::set<int> *>(tmp, TAIR_RETURN_SUCCESS);
}

tair::common::Result<std::set<int> *>
ClusterHandler::Config::cas_forbidden_namespaces(std::set<int> *newnamespaces) {
    return compare_and_swap_namespaces(this->forbidden_namespaces_, newnamespaces);
}

tair::common::Result<std::set<int> *>
ClusterHandler::Config::cas_allowed_namespaces(std::set<int> *newnamespaces) {
    return compare_and_swap_namespaces(this->allowed_namespaces_, newnamespaces);
}

bool ClusterHandler::Config::equals(const ClusterHandler::Config *config) {
    return info_ == config->info_;
}

void ClusterHandler::Config::encode_info() {
    char buf[sizeof(uint64_t) * 2];
    tair::util::coding_util::
    encode_fixed64(buf,
                   tbsys::CNetUtil::strToAddr(master_cs_addr_.c_str(), TAIR_CONFIG_SERVER_DEFAULT_PORT));
    tair::util::coding_util::
    encode_fixed64(buf + sizeof(uint64_t),
                   tbsys::CNetUtil::strToAddr(slave_cs_addr_.c_str(), TAIR_CONFIG_SERVER_DEFAULT_PORT));
    info_.clear();
    info_.reserve(sizeof(buf) + group_name_.size());
    info_.append(buf, sizeof(buf));
    info_.append(group_name_);
}

int ClusterHandler::Config::decode_info() {
    int ret = TAIR_RETURN_FAILED;
    if (info_.size() > sizeof(uint64_t) * 2) {
        master_cs_addr_ = tbsys::CNetUtil::addrToString(tair::util::coding_util::decode_fixed64(info_.c_str()));
        slave_cs_addr_ =
                tbsys::CNetUtil::addrToString(
                        tair::util::coding_util::decode_fixed64(info_.c_str() + sizeof(uint64_t)));
        group_name_ = info_.substr(sizeof(uint64_t) * 2);
        ret = TAIR_RETURN_SUCCESS;
    }
    return ret;
}

/* ===================End================= */
/* ===================ClusterHandler================= */
ClusterHandler::ClusterHandler() {
    client_ = NULL;
    rsync_ = false;
    status_ = ClusterHandler::UNINIT;
    config_ = NULL;
}

ClusterHandler::~ClusterHandler() {
    destroy_client();
    delete config_;
}

void ClusterHandler::destroy_client() {
    delete client_;
    client_ = NULL;
}

void ClusterHandler::destroy(void *handler) {
    delete (ClusterHandler *) handler;
}

bool ClusterHandler::conf_inited() {
    return status_ != ClusterHandler::UNINIT;
}

int ClusterHandler::init_conf(std::vector<std::string> &config) {
    int ret = TAIR_RETURN_FAILED;
    if (config.size() == 5) {
        if (config_ == NULL) {
            config_ = new ClusterHandler::Config();
        }
        config_->master_cs_addr_ = config[0];
        config_->slave_cs_addr_ = config[1];
        config_->group_name_ = config[2];
        config_->timeout_ms_ = atoi(config[3].c_str());
        config_->queue_limit_ = atoi(config[4].c_str());

        config_->encode_info();

        ret = TAIR_RETURN_SUCCESS;
        status_ = ClusterHandler::INITED;
    }
    return ret;
}

int ClusterHandler::init_conf(const std::string &info, int32_t timeout_ms, int32_t queue_limit) {
    if (config_ == NULL) {
        config_ = new ClusterHandler::Config();
    }
    config_->info_ = info;
    config_->timeout_ms_ = timeout_ms;
    config_->queue_limit_ = queue_limit;
    int ret = config_->decode_info();
    status_ = (ret == TAIR_RETURN_SUCCESS ?
               ClusterHandler::INITED : ClusterHandler::UNINIT);
    return ret;
}

// not thread-safe, one time can have update thread but just one
int ClusterHandler::init_conf(ClusterHandler::Config *config) {
    assert(config_ == NULL);
    assert(status_ == ClusterHandler::UNINIT);
    config_ = config;
    status_ = ClusterHandler::INITED;

    return TAIR_RETURN_SUCCESS;
}

int ClusterHandler::start() {
    if (client_ == NULL) {
        client_ = new tair_client_impl();
        client_->set_timeout(timeout());
        client_->set_queue_limit(queuelimit());
    }

    client_->set_rsync_mode(rsync_);

    bool ret = client_->startup(master().c_str(), slave().c_str(), group_name().c_str());

    if (ret == false) {
        log_error("init cluster %s config fail: %d", id().c_str(), ret);
        assert(status_ == ClusterHandler::INITED);
    } else {
        status_ = ClusterHandler::STARTED;
    }

    return (ret == false ? TAIR_RETURN_FAILED : TAIR_RETURN_SUCCESS);
}

// thread-unsafe
int ClusterHandler::prepare_and_start(ClusterHandler::Config *config) {
    assert(config_ == NULL);
    assert(status_ == ClusterHandler::UNINIT);
    int ret = init_conf(config);
    UNUSED(ret);
    assert(ret == TAIR_RETURN_SUCCESS);
    assert(status_ == ClusterHandler::INITED);
    return start();
}

void ClusterHandler::set_queue_limit(int limit) {
    assert(config_ != NULL);
    if (limit <= 0) {
        config_->queue_limit_ = ClusterHandler::Config::DEFAULT_QUEUE_LIMIT;
    }
    config_->queue_limit_ = limit;
    if (client_ != NULL) {
        client_->set_queue_limit(config_->queue_limit_);
    }
}

void ClusterHandler::set_time_out(int timeout) {
    assert(config_ != NULL);
    if (timeout <= 0) {
        config_->timeout_ms_ = ClusterHandler::Config::DEFAULT_TIMEOUT;
    }
    config_->timeout_ms_ = timeout;
    if (client_ != NULL) {
        client_->set_timeout(config_->timeout_ms_);
    }
}

void ClusterHandler::set_unit(std::string unit) {
    assert(config_ != NULL);
    config_->unit_ = unit;
}

void ClusterHandler::set_type(int type) {
    assert(config_ != NULL);
    config_->type_ = type;
}

tair_client_impl *ClusterHandler::client() const { return client_; }

std::string ClusterHandler::get_info() const { return info(); }

int ClusterHandler::get_queue_limit() const {
    if (client_ != NULL) {
        return client_->get_queue_limit();
    }
    return queuelimit();
}

int ClusterHandler::get_time_out() const {
    return timeout();
}

const std::string ClusterHandler::connection_stat() {
    if (client_ != NULL) {
        return client_->stat();
    }
    return "";
}

//  void ClusterHandler::encode_info() {
//    char buf[sizeof(uint64_t)*2];
//    tair::util::coding_util::
//      encode_fixed64(buf,
//          tbsys::CNetUtil::strToAddr(master_cs_addr_.c_str(), TAIR_CONFIG_SERVER_DEFAULT_PORT));
//    tair::util::coding_util::
//      encode_fixed64(buf+sizeof(uint64_t),
//          tbsys::CNetUtil::strToAddr(slave_cs_addr_.c_str(), TAIR_CONFIG_SERVER_DEFAULT_PORT));
//    info_.clear();
//    info_.reserve(sizeof(buf) + group_name_.size());
//    info_.append(buf, sizeof(buf));
//    info_.append(group_name_);
//  }
//
//  int ClusterHandler::decode_info() {
//    int ret = TAIR_RETURN_FAILED;
//    if (info_.size() > sizeof(uint64_t) * 2)
//    {
//      master_cs_addr_ = tbsys::CNetUtil::addrToString(tair::util::coding_util::decode_fixed64(info_.c_str()));
//      slave_cs_addr_ =
//        tbsys::CNetUtil::addrToString(tair::util::coding_util::decode_fixed64(info_.c_str() + sizeof(uint64_t)));
//      group_name_ = info_.substr(sizeof(uint64_t)*2);
//      ret = TAIR_RETURN_SUCCESS;
//    }
//    return ret;
//  }

std::string ClusterHandler::id() const { return master() + "#" + group_name() + "@" + unit(); }

std::string ClusterHandler::sid() const { return group_name() + "@" + unit(); }

int ClusterHandler::get_type() const { return type(); }

std::string ClusterHandler::get_unit() const { return unit(); }

std::string ClusterHandler::get_group_name() const { return group_name(); }

std::string ClusterHandler::debug_string() const {
    char tmp[32];
    snprintf(tmp, sizeof(tmp), "%d,%d", timeout(), queuelimit());
    return std::string("[") + master() + "," + slave() + "," + group_name() + "," + tmp + "]";
}

cJSON *ClusterHandler::get_cluster_info() const {
    return config_ == NULL ? NULL : config_->to_json();
}

void ClusterHandler::set_rsync_mode(bool rsync) { rsync_ = rsync; }

bool ClusterHandler::equals(const ClusterHandler::Config *config) {
    return config_ == NULL ? false : config_->equals(config);
}

tair::common::Result<std::set<int> *> ClusterHandler::cas_forbidden_namespaces(std::set<int> *newnamespaces) {
    return config_ != NULL ?
           config_->cas_forbidden_namespaces(newnamespaces) :
           tair::common::Result<std::set<int> *>(NULL, TAIR_RETURN_FAILED);
}

tair::common::Result<std::set<int> *> ClusterHandler::cas_allowed_namespaces(std::set<int> *newnamespaces) {
    return config_ != NULL ?
           config_->cas_allowed_namespaces(newnamespaces) :
           tair::common::Result<std::set<int> *>(NULL, TAIR_RETURN_FAILED);
}

std::set<int> *ClusterHandler::get_forbidden_namespaces() {
    return config_ != NULL ? config_->forbidden_namespaces_ : NULL;
}

std::set<int> *ClusterHandler::get_allowed_namespaces() {
    return config_ != NULL ? config_->allowed_namespaces_ : NULL;
}

std::string ClusterHandler::info() const { return config_ == NULL ? "" : config_->info_; }

std::string ClusterHandler::master() const { return config_ == NULL ? "" : config_->master_cs_addr_; }

std::string ClusterHandler::slave() const { return config_ == NULL ? "" : config_->slave_cs_addr_; }

std::string ClusterHandler::group_name() const { return config_ == NULL ? "" : config_->group_name_; }

std::string ClusterHandler::unit() const { return config_ == NULL ? "" : config_->unit_; }

int ClusterHandler::timeout() const {
    return config_ == NULL ?
           ClusterHandler::Config::DEFAULT_TIMEOUT : config_->timeout_ms_;
}

int ClusterHandler::queuelimit() const {
    return config_ == NULL ?
           ClusterHandler::Config::DEFAULT_QUEUE_LIMIT : config_->queue_limit_;
}

int ClusterHandler::type() const { return config_ == NULL ? RSYNC_SELF : config_->type_; }

/* ===================End================= */

/* ===================RemoteClusterHandler================= */
RemoteClusterHandler::RemoteClusterHandler()
        : ClusterHandler() {
    set_rsync_mode(true);
    logger_ = NULL;
    ref_ = 0;
    pause_ = false;
}

RemoteClusterHandler::~RemoteClusterHandler() {
    // we must make sure client release before queue clear, otherwise maybe cause memory leak
    ClusterHandler::destroy_client();
    // now ldb_manager don't manager RecordLogger,
    // each RemoteClusterHandler manager its logger, so here need to
    // release logger
    delete logger_;
    logger_ = NULL;
}

void RemoteClusterHandler::set_logger(common::RecordLogger *logger) {
    logger_ = logger;
}

Result<Record *> RemoteClusterHandler::get_record(int instance) {
    if (pause_) {
        return Result<Record *>(NULL, TAIR_RETURN_PAUSED);
    }
    return logger_->get_record(instance);
}

void RemoteClusterHandler::update(int instance, int bucket, bool status) {
    logger_->update(instance, bucket, status);
}

void RemoteClusterHandler::delete_log_file(int instance, uint64_t filenumber) {
    logger_->delete_log_file(instance, filenumber);
}

common::RecordPosition RemoteClusterHandler::get_min_position(int instance) {
    return logger_->get_min_position(instance);
}

void RemoteClusterHandler::pause(bool p) {
    pause_ = p;
    if (pause_ == false) {
        restart();
    }
}

bool RemoteClusterHandler::paused() const {
    return pause_;
}

void RemoteClusterHandler::restart() {
    logger_->restart();
}

void RemoteClusterHandler::watch() {
    logger_->watch();
}

void RemoteClusterHandler::statistics() {
    logger_->statistics();
}

std::string RemoteClusterHandler::options() {
    return logger_->options();
}

void RemoteClusterHandler::options(const std::map<std::string, int> &m) {
    logger_->options(m);
}

common::Result<RemoteClusterHandler::CallbackArg *> RemoteClusterHandler::get_result() {
    return queue_.get();
}

void RemoteClusterHandler::put_result(common::Result<RemoteClusterHandler::CallbackArg *> result) {
    return queue_.put(result);
}

bool RemoteClusterHandler::no_results() {
    return queue_.empty();
}

std::pair<std::string, std::string> RemoteClusterHandler::get_unit_and_group() {
    return make_pair<std::string, std::string>(get_unit(), get_group_name());
}

int RemoteClusterHandler::start() {
    int ret = ClusterHandler::start();
    if (ret == TAIR_RETURN_SUCCESS && logger_ != NULL) {
        int max_dest_bucket_count = client()->get_max_bucket_count();
        logger_->set_max_bucket_count(max_dest_bucket_count);
    }
    return ret;
}

int RemoteClusterHandler::prepare_and_start(ClusterHandler::Config *config,
                                            common::RecordLogger *logger) {
    int ret = ClusterHandler::prepare_and_start(config);
    set_logger(logger);
    assert(logger_ != NULL);
    if (ret == TAIR_RETURN_SUCCESS) {
        int max_dest_bucket_count = client()->get_max_bucket_count();
        logger_->set_max_bucket_count(max_dest_bucket_count);
    }
    return ret;
}

int RemoteClusterHandler::Ref() { return ++ref_; }

int RemoteClusterHandler::UnRef() { return --ref_; }

int RemoteClusterHandler::GetRef() { return ref_; }

/* ===================End================= */


RemoteClusterHandler::ResponseQueue::~ResponseQueue() {
    while (!queue_.Empty()) {
        common::Result<RemoteClusterHandler::CallbackArg *> result(NULL, TAIR_RETURN_DATA_NOT_EXIST);
        queue_.Get(result);
        delete result.value_;
    }
}

common::Result<RemoteClusterHandler::CallbackArg *> RemoteClusterHandler::ResponseQueue::get() {
    common::Result<RemoteClusterHandler::CallbackArg *> result(NULL, TAIR_RETURN_DATA_NOT_EXIST);
    queue_.TryGet(result);
    return result;
}

void RemoteClusterHandler::ResponseQueue::put(common::Result<RemoteClusterHandler::CallbackArg *> result) {
    queue_.Put(result);
}

bool RemoteClusterHandler::ResponseQueue::empty() {
    return queue_.Empty();
}
}
