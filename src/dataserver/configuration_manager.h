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

#ifndef TAIR_CONFIGURATION_MANAGER_H_
#define TAIR_CONFIGURATION_MANAGER_H_

#include "admin_config_packet.hpp"

namespace tair {

namespace stat {
class FlowController;
}

class configuration;

class tair_manager;

class configuration_manager {
public:
    configuration_manager();

    ~configuration_manager();

    int64_t process(request_admin_config *request, std::map <std::string, std::string> &configs);

    void register_configuration_namespace(tair_manager *mgr, const char *section_name);

    void register_configuration_flowcontrol(tair::stat::FlowController *flow_ctrl, const char *section_name);

    void register_configuration(configuration *conf);

    void start();

    void stop();

    void wait();

    static void *SyncConfigThread(void *arg);

    void RunSyncConfig();

    static int find_section_pos(const std::string &section, int64_t &start, int64_t &end,
                                const std::vector <std::string> &lines);

    void write_conf();

    void set_admin_file(const char *file) {
        admin_file_ = file;
    }

    void set_ns_load() {
        admin_config_flag_ |= TAIR_ADMIN_CONFIG_FLAG_NAMESPACE;
    }

    void set_flow_load() {
        admin_config_flag_ |= TAIR_ADMIN_CONFIG_FLAG_FLOWCONTROL;
    }

    int do_load_admin_conf() const;

    bool if_all_configuration_inited() {
        return if_all_configuration_has_verion;
    }

public:
    static const std::string NAMESPACE_CID;
    static const std::string FLOWCONTROL_CID;

private:
    typedef std::map<std::string, configuration *>::iterator configuration_iterator;
    std::map<std::string, configuration *> configs_;
    std::string admin_file_;
    bool stop_;
    pthread_t pid_;
    int admin_config_flag_;
    // set true if the version() of all configuration is not 0, once set true, never changed.
    bool if_all_configuration_has_verion;
};

class configuration {
public:
    configuration();

    virtual ~configuration();

    void set_cid(const std::string &cid);

    inline const std::string &cid() const;

    inline int64_t version() const;

    inline void set_version(int64_t v);

    int64_t process(request_admin_config *request, std::map <std::string, std::string> &configs);

    virtual bool update_set(const std::map <std::string, std::string> &configs) = 0;

    virtual bool update_del(const std::map <std::string, std::string> &configs) = 0;

    virtual bool update_sync(const std::map <std::string, std::string> &configs) = 0;

    virtual void get_config(std::map <std::string, std::string> &configs) = 0;

    virtual void set_conf_flag(int flag) {
        config_flag_ = flag;
    }

    virtual int config_flag() {
        return config_flag_;
    }

    void configs_key_value(std::map <std::string, std::string> &ckv) {
        lock();
        ckv = configs_key_value_;
        unlock();
    }

    virtual void set_section_name(const std::string &name) {
        section_name_ = name;
    }

    virtual std::string &section_name() {
        return section_name_;
    }

    int get_conf_from_configfile(std::map <std::string, std::string> &conf, const std::vector <std::string> &lines);

    bool config_change() { return config_change_; }

    void set_config_change(bool change) { config_change_ = change; }

    void lock() const;

    void unlock() const;

protected:
    bool parse_ns(const std::string &key, int32_t &ns) {
        ns = 0;
        std::stringstream ss(key);
        ss >> ns;
        if (ss.eof() && !ss.fail() && ns >= 0 && ns < TAIR_MAX_AREA_COUNT) {
            return true;
        }
        return false;
    }

private:
    int64_t version_;
    bool config_change_;
    std::string cid_;
    mutable pthread_mutex_t mutex_;
    std::string section_name_;
    std::map <std::string, std::string> configs_key_value_;
    int config_flag_;
};

} // end namespace tair

#endif
