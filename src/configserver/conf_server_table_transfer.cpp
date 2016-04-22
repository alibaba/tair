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
    if (argc != 3) {
        printf("transfer txtfile binaryfile\n");
        return 0;
    }
    conf_server_table_manager cstm;
    if (cstm.translate_from_txt2binary(argv[1], argv[2]) == false) {
        printf("transfer error\n");
        return 0;
    }
    printf("transfer ok\n");

    return 0;
}
