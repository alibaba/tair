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

#ifndef TAIR_CONFIGURATION_FLOWCONTROL_HPP_
#define TAIR_CONFIGURATION_FLOWCONTROL_HPP_

#include <string.h>
#include "configuration_manager.h"
#include "flow_controller.h"

namespace tair {

class configuration_flowcontrol : public configuration {
public:
    configuration_flowcontrol() : flow_ctrl(NULL) {}

    virtual void get_config(std::map<std::string, std::string> &configs) {
        std::map<std::string, std::string>::iterator iter = configs.begin();
        std::map<std::string, std::string>::iterator iter_end = configs.end();
        for (; iter != iter_end; ++iter) {
            int32_t ns = 0;
            if (parse_ns(iter->first, ns) == false)
                continue;

            tair::stat::FlowLimit in_limit = flow_ctrl->GetFlowLimit(ns, tair::stat::IN);
            tair::stat::FlowLimit out_limit = flow_ctrl->GetFlowLimit(ns, tair::stat::OUT);
            tair::stat::FlowLimit ops_limit = flow_ctrl->GetFlowLimit(ns, tair::stat::OPS);
            std::stringstream ss;
            ss << in_limit.lower / kOneMB << "," << in_limit.upper / kOneMB << ","
               << out_limit.lower / kOneMB << "," << out_limit.upper / kOneMB << ","
               << ops_limit.lower << "," << ops_limit.upper;
            iter->second = ss.str();
        }
    }

    virtual bool update_sync(const std::map<std::string, std::string> &configs) {
        bool all_success = true;
        std::map<int, tair::stat::AllFlowLimit> flow_limits;
        std::map<std::string, std::string>::const_iterator iter = configs.begin();
        std::map<std::string, std::string>::const_iterator iter_end = configs.end();
        for (; iter != iter_end; ++iter) {
            int32_t ns = 0;
            std::vector<int64_t> control;
            if (parse_ns_control(iter, ns, control) == false) {
                all_success = false;
                continue;
            }
            tair::stat::AllFlowLimit &limit = flow_limits[ns];
            limit.in.lower = control[0];
            limit.in.upper = control[1];
            limit.out.lower = control[2];
            limit.out.upper = control[3];
            limit.ops.lower = control[4];
            limit.ops.upper = control[5];
        }
        flow_ctrl->SyncFlowLimit(flow_limits);
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
            flow_ctrl->SetDefaultLimit(ns);
        }
        return all_success;
    }

    virtual bool update_set(const std::map<std::string, std::string> &configs) {
        bool all_success = true;
        std::map<std::string, std::string>::const_iterator iter = configs.begin();
        std::map<std::string, std::string>::const_iterator iter_end = configs.end();
        for (; iter != iter_end; ++iter) {
            int32_t ns = 0;
            std::vector<int64_t> control;
            if (parse_ns_control(iter, ns, control) == false) {
                all_success = false;
                continue;
            }
            flow_ctrl->SetFlowLimit(ns, tair::stat::IN, control[0], control[1]);
            flow_ctrl->SetFlowLimit(ns, tair::stat::OUT, control[2], control[3]);
            flow_ctrl->SetFlowLimit(ns, tair::stat::OPS, control[4], control[5]);
        }
        return all_success;
    }

    void set_flow_controller(tair::stat::FlowController *ctrl) {
        this->flow_ctrl = ctrl;
    }

private:
    bool parse_ns_control(std::map<std::string, std::string>::const_iterator &iter,
                          int32_t &ns, vector <int64_t> &control) {
        if (parse_ns(iter->first, ns) == false) return false;
        bool format_error = true;
        char *saveptr = NULL;
        char *buffer = new char[iter->second.length() + 1];
        memcpy(buffer, iter->second.c_str(), iter->second.length());
        buffer[iter->second.length()] = '\0';
        for (char *p = strtok_r(buffer, " ,", &saveptr); p != NULL; p = strtok_r(NULL, " , ", &saveptr)) {
            int64_t control_num = 0;
            std::stringstream ss(p);
            ss >> control_num;
            if (ss.eof() && !ss.fail()) {
                format_error = false;
                control.push_back(control_num);
                continue;
            }
            format_error = true;
            break;
        }
        delete[] buffer;
        if (!format_error && control.size() == 6
            && control[0] <= control[1]
            && control[2] <= control[3]
            && control[4] <= control[5]) {
            for (size_t i = 0; i < 4; ++i) control[i] = control[i] * kOneMB;
        } else {
            format_error = true;
        }
        if (format_error) {
            log_error("%s parameter namespace:%s control:%s format error",
                      __FUNCTION__, iter->first.c_str(), iter->second.c_str());
        }
        return format_error == false;
    }

private:
    tair::stat::FlowController *flow_ctrl;
    const static int64_t kOneMB = 1024 * 1024;
};

}

#endif
