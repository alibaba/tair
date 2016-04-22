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

#include "configuration_manager.h"
#include "configuration_flowcontrol.hpp"
#include "configuration_namespace.hpp"
#include "snappy/snappy.h"
#include <sys/prctl.h>

namespace tair {

const std::string configuration_manager::NAMESPACE_CID = "namespace";
const std::string configuration_manager::FLOWCONTROL_CID = "flowcontrol";

configuration::configuration()
        : version_(0), config_change_(false) {
    pthread_mutex_init(&mutex_, NULL);
}

configuration::~configuration() {
    pthread_mutex_destroy(&mutex_);
}

void configuration::set_cid(const std::string &cid) {
    this->cid_ = cid;
}

void configuration::set_version(int64_t v) {
    this->version_ = v;
}

const std::string &configuration::cid() const {
    return cid_;
}

int64_t configuration::version() const {
    return version_;
}

void configuration::lock() const {
    pthread_mutex_lock(&mutex_);
}

void configuration::unlock() const {
    pthread_mutex_unlock(&mutex_);
}

int configuration::get_conf_from_configfile(std::map<std::string, std::string> &conf,
                                            const std::vector<std::string> &lines) {
    int64_t start = 0, end = 0;
    configuration_manager::find_section_pos(section_name_, start, end, lines);
    for (; start < end; ++start) {
        const char *p = lines[start].c_str();
        while (*p && (*p == ' ' || *p == '\t')) ++p;
        if (*p == '#')
            continue;
        std::vector<std::string> key_value;
        char *saveptr = NULL;
        for (char *s = strtok_r(const_cast<char *>(p), " ", &saveptr); s != NULL; s = strtok_r(NULL, " ", &saveptr)) {
            key_value.push_back(s);
        }
        if (key_value.size() < 2) {
            log_error("line:%ld format error", start + 1);
            return TAIR_RETURN_FAILED;
        }
        std::string key = key_value[0];
        std::string value = key_value[1];
        conf[key] = value;
    }
    return TAIR_RETURN_SUCCESS;
}

int64_t configuration::process(request_admin_config *request, std::map<std::string, std::string> &configs) {
    if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO) {
        log_info("Process Config Request %s request opt %s",
                  request->cid().c_str(), request->optstr().c_str());
        const std::map<std::string, std::string> &request_configs = request->configs();
        for (std::map<std::string, std::string>::const_iterator iter = request_configs.begin();
             iter != request_configs.end(); ++iter) {
            log_info("Config Key:%s Val:%s", iter->first.c_str(), iter->second.c_str());
        }
    }
    int64_t curt_version = -1;
    lock();
    if (request->opt() == request_admin_config::kSync) {
        if (update_sync(request->configs()))
            curt_version = request->next_version();
        if (request->configs().size() != 0) {
            configs_key_value_ = request->configs();
            config_change_ = true;
        }
        set_version(request->next_version());
    } else if (request->opt() == request_admin_config::kGet) {
        curt_version = version();
        configs = request->configs();
        get_config(configs);
    } else if (request->last_version() != version() ||
               request->opt() == request_admin_config::kCheck) {
        curt_version = version();
    } else if (request->opt() == request_admin_config::kSet) {
        if (update_set(request->configs()))
            curt_version = request->next_version();
        set_version(request->next_version());
        std::map<std::string, std::string>::const_iterator it = request->configs().begin();
        for (; it != request->configs().end(); ++it) {
            configs_key_value_[it->first] = it->second;
        }
        config_change_ = true;
    } else if (request->opt() == request_admin_config::kDel) {
        if (update_del(request->configs()))
            curt_version = request->next_version();
        std::map<std::string, std::string>::const_iterator it = request->configs().begin();
        for (; it != request->configs().end(); ++it) {
            configs_key_value_.erase(it->first);
        }
        config_change_ = true;
        set_version(request->next_version());
    } else {
        log_info("unknow %s request opt %d", request->cid().c_str(),
                  request->opt());
        curt_version = -1;
    }
    unlock();
    return curt_version;
}

void configuration_manager::write_conf() {
    bool change = false;
    std::map<std::string, configuration *>::const_iterator it = configs_.begin();
    for (; it != configs_.end(); ++it) {
        it->second->lock();
        change |= it->second->config_change();
        it->second->set_config_change(false);
        it->second->unlock();
    }
    if (!change)
        return;
    char tmp[128];
    snprintf(tmp, sizeof(tmp), "%s.%d", admin_file_.c_str(), getpid());
    do {
        FILE *fd = fopen(tmp, "wb+");
        if (fd == NULL) {
            log_error("open admin conf file %s failed: %s", admin_file_.c_str(), strerror(errno));
            break;
        }
        std::string write_string;
        write_string.reserve(5242880);// 5M
        it = configs_.begin();
        for (; it != configs_.end(); ++it) {
            std::string section;
            section += '[';
            section += it->second->section_name();
            section += "]\n";
            write_string += section;
            std::map<std::string, std::string> kv;
            it->second->configs_key_value(kv);
            if (kv.size() == 0) continue;
            std::map<std::string, std::string>::const_iterator iter = kv.begin();
            for (; iter != kv.end(); ++iter) {
                std::string key_value;
                key_value.reserve(
                        iter->first.length() + iter->second.length() + 2); //~ plus key_delimiter, '\n' and '\t'
                key_value += iter->first;
                key_value += ' ';
                key_value += iter->second;
                key_value += '\n';
                write_string += key_value;
            }
        }
        char *output = new char[snappy::MaxCompressedLength(write_string.length())];
        size_t output_length;
        snappy::RawCompress(write_string.c_str(), write_string.length(), output, &output_length);
        int len = fwrite(output, sizeof(char), output_length, fd);
        if (len != (int) output_length) {
            log_error("write admin conf error, file:%s, real write:%d", admin_file_.c_str(), len);
        }
        delete[]output;
        fclose(fd);
    } while (0);
    if (rename(tmp, admin_file_.c_str()) != 0) {
        log_error("rename from %s to %s failed", tmp, admin_file_.c_str());
    }
}

int configuration_manager::do_load_admin_conf() const {
    if (!(admin_config_flag_ & TAIR_ADMIN_CONFIG_FLAG_NAMESPACE) &&
        !(admin_config_flag_ & TAIR_ADMIN_CONFIG_FLAG_FLOWCONTROL)) {
        return TAIR_RETURN_SUCCESS;
    }
    FILE *fd = fopen(admin_file_.c_str(), "rb+");
    if (fd == NULL) {
        log_error("open conf file %s failed: %s", admin_file_.c_str(), strerror(errno));
        return TAIR_RETURN_SUCCESS;
    }
    fseek(fd, 0L, SEEK_END);
    int64_t len = ftell(fd);
    rewind(fd);// fseek(fd, 0L, SEEK_SET);
    char *buf = new char[len];
    assert(fread(buf, sizeof(char), len, fd) == (size_t) len);
    size_t output_length = 0;
    if (!snappy::GetUncompressedLength(buf, len, &output_length)) {
        log_error("snappy decompress err while GetUncompressedLength");
        return TAIR_RETURN_SUCCESS;
    }
    char *output = new char[output_length];
    if (!snappy::RawUncompress(buf, len, output)) {
        log_error("snappy decompress err while RawUncompress");
        delete[]buf;
        delete[]output;
        return TAIR_RETURN_SUCCESS;
    }
    log_debug("admin conf:%s", output);
    std::vector<std::string> lines;
    char *saveptr = NULL;
    for (char *p = strtok_r(output, "\r\n", &saveptr); p != NULL; p = strtok_r(NULL, "\r\n", &saveptr)) {
        lines.push_back(p);
    }
    delete[]buf;
    delete[]output;
    fclose(fd);
    std::map<std::string, configuration *>::const_iterator iter = configs_.begin();
    for (; iter != configs_.end(); ++iter) {
        std::map<std::string, std::string> conf;
        if (admin_config_flag_ & iter->second->config_flag()) {
            if (iter->second->get_conf_from_configfile(conf, lines) == TAIR_RETURN_FAILED) {
                log_error("parse configfile error. configid:%s, file:%s", iter->first.c_str(), admin_file_.c_str());
                return TAIR_RETURN_FAILED;
            }
            if (conf.size() == 0) continue;
            if (iter->second->update_sync(conf) == false) {
                log_error("load conf error. configid:%s, file:%s", iter->first.c_str(), admin_file_.c_str());
                return TAIR_RETURN_FAILED;
            }
        }
    }
    return TAIR_RETURN_SUCCESS;
}

void configuration_manager::RunSyncConfig() {
    time_t now = 0, last_run = 0;
    int WRITE_INTERVAL = 5 * 60; // Every 5 minutes write file
    while (stop_ == false) {
        now = time(NULL);
        if (WRITE_INTERVAL > (now - last_run)) {
            sleep(1);
            continue;
        }
        if (stop_ == true)
            break;
        write_conf();
        last_run = now;
    }
}

void *configuration_manager::SyncConfigThread(void *arg) {
    prctl(PR_SET_NAME, "sync-config");
    configuration_manager *cm = reinterpret_cast<configuration_manager *>(arg);
    cm->RunSyncConfig();
    return NULL;
}

void configuration_manager::start() {
    stop_ = false;
    pthread_create(&pid_, NULL, configuration_manager::SyncConfigThread, this);
}

void configuration_manager::stop() {
    stop_ = true;
}

void configuration_manager::wait() {
    pthread_join(pid_, NULL);
}

int64_t configuration_manager::process(request_admin_config *request, std::map<std::string, std::string> &configs) {
    configuration_iterator iter = configs_.find(request->cid());
    int64_t return_value;
    if (iter != configs_.end())
        return_value = iter->second->process(request, configs);
    else
        return_value = -1;

    // once set true, is_all_configuration_has_verion never change again.
    if (UNLIKELY(false == if_all_configuration_has_verion)) {
        for (iter = configs_.begin(); iter != configs_.end(); ++iter) {
            if (iter->second->version() == 0)
                break;
        }
        if (configs_.end() == iter)
            if_all_configuration_has_verion = true;
    }

    return return_value;
}

void configuration_manager::register_configuration(configuration *conf) {
    assert(conf->version() == 0);
    assert(configs_.find(conf->cid()) == configs_.end());
    configs_[conf->cid()] = conf;
    log_info("Register configuration:%s", conf->cid().c_str());
}

void configuration_manager::register_configuration_flowcontrol(tair::stat::FlowController *flow_ctrl,
                                                               const char *section_name) {
    configuration_flowcontrol *conf = new configuration_flowcontrol();
    conf->set_cid(FLOWCONTROL_CID);
    conf->set_flow_controller(flow_ctrl);
    conf->set_conf_flag(TAIR_ADMIN_CONFIG_FLAG_FLOWCONTROL);
    conf->set_section_name(section_name);
    register_configuration(conf);
}

void configuration_manager::register_configuration_namespace(tair_manager *mgr, const char *section_name) {
    configuration_namespace *conf = new configuration_namespace();
    conf->set_cid(NAMESPACE_CID);
    conf->set_tair_manager(mgr);
    conf->set_conf_flag(TAIR_ADMIN_CONFIG_FLAG_NAMESPACE);
    conf->set_section_name(section_name);
    register_configuration(conf);
}

int configuration_manager::find_section_pos(const std::string &section, int64_t &start, int64_t &end,
                                            const std::vector<std::string> &lines) {
    size_t i = 0;
    for (; i < lines.size(); ++i) {
        const char *p = lines[i].c_str();
        while (*p && (*p == ' ' || *p == '\t')) ++p;
        if (*p == '#')
            continue;
        if (strstr(p, section.c_str()) == NULL) {
            continue;
        }
        break;
    }
    if (i == lines.size()) {
        log_error("cann't find section: %s", section.c_str());
        return TAIR_RETURN_FAILED; //~ no such section
    }
    ++i;
    size_t j = i;
    for (; j < lines.size(); ++j) {
        const char *p = lines[j].c_str();
        while (*p && (*p == ' ' || *p == '\t')) ++p;
        if (*p == '#')
            continue;
        if (*p == '[')
            break;
    }
    start = i;
    end = j;
    return TAIR_RETURN_SUCCESS;
}

configuration_manager::configuration_manager()
        : if_all_configuration_has_verion(false) {
    stop_ = false;
}

configuration_manager::~configuration_manager() {
    configuration_iterator iter = configs_.begin();
    configuration_iterator iter_end = configs_.end();
    for (; iter != iter_end; ++iter) {
        delete iter->second;
        iter->second = NULL;
    }
    configs_.clear();
}
} // end namespace tair
