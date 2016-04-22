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

#ifndef DIAMOND_MANAGER
#define DIAMOND_MANAGER

#include "cluster_controller.hpp"

#include <DiamondClient.h>

#include <vector>
#include <string>
#include <string.h>

namespace tair {
using namespace std;

class diamond_manager {
public:
    diamond_manager(cluster_controller *a_controller, int tair_info_check_interval_second);

    ~diamond_manager();

    void set_net_device_name(const string &a_net_device_name);

    // true -- success
    // false -- fail
    bool connect(const string &a_data_id, DIAMOND::SubscriberListener *a_listener);

    void close();

    const string &get_config_id();

    const string &get_rule_group_id();

    const string &get_data_group_id();

    bool update_data_group_id(const string &rule_group_content);

    bool update_config(const string &json);

private:
    void get_route_rule(const string &group_rule, vector<string> &rule_ip, vector<string> &rule_dest);

    bool get_local_ip(vector<string> &ip);

    bool search_data_group(const vector<string> &local_ip, const vector<string> &rule_ip,
                           const vector<string> &rule_dest, string &data_group_id);

private:
    DIAMOND::DiamondConfig config;
    DIAMOND::DiamondClient *diamond_client;
    DIAMOND::SubscriberListener *listener;

    cluster_controller *controller;

    string net_device_name;
    string config_id;
    string rule_group_id;
    string data_group_id;
};
}
#endif
