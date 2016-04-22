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

#ifndef TAIR_CONFIGURATION_NAMESPACE_HPP_
#define TAIR_CONFIGURATION_NAMESPACE_HPP_

#include "configuration_manager.h"
#include "tair_manager.hpp"

namespace tair {

class configuration_namespace : public configuration {
public:
    configuration_namespace() : tair_mgr(NULL) {}

    virtual void get_config(std::map<std::string, std::string> &configs) {
        std::map<std::string, std::string>::iterator iter = configs.begin();
        std::map<std::string, std::string>::iterator iter_end = configs.end();
        for (; iter != iter_end; ++iter) {
            int32_t ns = 0;
            if (parse_ns(iter->first, ns) == false)
                continue;
            int64_t quota = tair_mgr->get_area_quota(ns);
            std::stringstream ss;
            ss << quota;
            iter->second = ss.str();
        }
    }

    virtual bool update_sync(const std::map<std::string, std::string> &configs) {
        bool all_success = true;
        std::map<int32_t, int64_t> quotas;
        std::map<std::string, std::string>::const_iterator iter = configs.begin();
        std::map<std::string, std::string>::const_iterator iter_end = configs.end();
        for (; iter != iter_end; ++iter) {
            int32_t ns = 0;
            int64_t quota = 0;
            if (parse_ns_quota(iter->first, iter->second, ns, quota) == false) {
                all_success = false;
                continue;
            }
            quotas[ns] = quota;
        }
        tair_mgr->set_all_area_total_quota(quotas);
        return all_success;
    }

    virtual bool update_del(const std::map<std::string, std::string> &configs) {
        bool all_success = true;
        std::map<std::string, std::string>::const_iterator iter = configs.begin();
        std::map<std::string, std::string>::const_iterator iter_end = configs.end();
        for (; iter != iter_end; ++iter) {
            int32_t ns = 0;
            if (parse_ns(iter->first, ns) == false) {
                all_success = false;
                continue;
            }
            tair_mgr->clear(ns);
            tair_mgr->set_area_total_quota(ns, 0);
        }
        return all_success;
    }

    virtual bool update_set(const std::map<std::string, std::string> &configs) {
        bool all_success = true;
        std::map<std::string, std::string>::const_iterator iter = configs.begin();
        std::map<std::string, std::string>::const_iterator iter_end = configs.end();
        for (; iter != iter_end; ++iter) {
            int32_t ns = 0;
            int64_t quota = 0;
            if (parse_ns_quota(iter->first, iter->second, ns, quota) == false) {
                all_success = false;
                continue;
            }
            tair_mgr->set_area_total_quota(ns, quota);
        }
        return all_success;
    }

    void set_tair_manager(tair_manager *mgr) {
        this->tair_mgr = mgr;
    }

private:
    bool parse_ns_quota(const std::string &key, const std::string &val,
                        int32_t &ns, int64_t &quota) {
        if (parse_ns(key, ns) == false) return false;
        quota = 0;
        std::stringstream quota_ss(val);
        quota_ss >> quota;
        if (quota_ss.eof() && !quota_ss.fail() && quota > 0) {
            return true;
        }
        log_error("%s parameter namespace:%s quota:%s format error",
                  __FUNCTION__, key.c_str(), val.c_str());
        return false;
    }

private:
    tair_manager *tair_mgr;
};

}

#endif

