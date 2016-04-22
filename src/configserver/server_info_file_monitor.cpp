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

#include "server_info_file_mapper.hpp"

using namespace tair;
using namespace config_server;

int main(int argc, char **argv) {
    if (argc != 2) {
        printf("sifMonitot fileName\n");
        return 0;
    }
    server_info_file_mapper cstm;
    if (cstm.open(argv[1], true) == false) {
        printf("can not open file %s\n", argv[1]);
        return 0;
    }
    cstm.print_head();
    for (int i = 0; i < *cstm.getp_used_counter(); ++i) {
        cstm.print_cell(i);
    }

    return 0;
}
