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

#ifndef TAIR_SERVER_INFO_H
#define TAIR_SERVER_INFO_H

#include <algorithm>
#include <tbsys.h>
#include <set>
#include "mmap_file.hpp"


namespace tair {
namespace config_server {
using namespace std;

class node_info;

class group_info;

#pragma pack(1)

struct server_info {
    friend class node_info;

public:
    enum {
        ALIVE = 0,
        DOWN,
        STANDBY
    };

    server_info();

    ~server_info();

    void print();

public:
    uint64_t server_id;        // ip + port
    uint32_t last_time;
    int8_t status;                // 0 - alive, 1 - down
    uint32_t standby_version;
    uint32_t standby_time;
    group_info *group_info_data;
    node_info *node_info_data;
};

#pragma pack()

typedef __gnu_cxx::hash_map<uint64_t, server_info *, __gnu_cxx::hash < int> >
server_info_map;

class node_info {
public:
    explicit node_info(server_info *m);

    ~node_info();

    bool operator<(const node_info &node) const;

public:
    server_info *server;
};

class node_info_compare {
public:
    bool operator()(const node_info *a, const node_info *b) {
        return ((*a) < (*b));
    }
};

typedef std::set<node_info *, node_info_compare> node_info_set;

}
}
#endif
