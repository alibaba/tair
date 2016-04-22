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

#ifndef SERVER_INFO_ALLOCATOR_H
#define SERVER_INFO_ALLOCATOR_H

#include "server_info_file_mapper.hpp"
#include "vector"

struct easy_io_t;
namespace tair {
namespace config_server {
using namespace std;

class server_info_allocator {
public:
    ~server_info_allocator();

    server_info *new_server_info(group_info *group_info_belongs,
                                 uint64_t id, easy_io_t *eio, bool is_cs_server_info = false);

    static server_info_allocator server_info_allocator_instance;
private:
    server_info_allocator();

    vector<server_info_file_mapper *> vec_server_infos;
    uint32_t use_size;

};
}
}
#endif
