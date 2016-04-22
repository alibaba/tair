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

#include "conf_server_table_manager.hpp"

using namespace tair;
using namespace
tair::config_server;

int main(int argc, char **argv) {
    if (argc != 2) {
        printf("cstMonitot serverTableName\n");
        return 0;
    }
    conf_server_table_manager cstm;
    if (cstm.open(argv[1]) == false) {
        printf("can not open file %s\n", argv[1]);
        return 0;
    }
    cstm.print_table();

    return 0;
}
