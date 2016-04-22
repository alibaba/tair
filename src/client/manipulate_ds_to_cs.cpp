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

#include <getopt.h>

#include <stdio.h>
#include <string>

#include <tbnetutil.h>
#include "tair_client_api_impl.hpp"
#include <query_info_packet.hpp>

using namespace std;

void print_result(const vector<uint64_t> &server_id_list, const vector<DataserverCtrlReturnType> &return_code) {
    if (server_id_list.size() != return_code.size())
        return;

    for (size_t i = 0; i < server_id_list.size(); ++i) {
        printf("%-20s ", tbsys::CNetUtil::addrToString(server_id_list[i]).c_str());
        switch (return_code[i]) {
            case DATASERVER_CTRL_RETURN_SUCCESS:
                printf("%s\n", "success");
                break;
            case DATASERVER_CTRL_RETURN_FILE_OP_FAILED:
                printf("%s\n", "fail, manipulate group conf file failed");
                break;
            case DATASERVER_CTRL_RETURN_SEND_FAILED:
                printf("%s\n", "fail, send packet failed");
                break;
            case DATASERVER_CTRL_RETURN_TIMEOUT:
                printf("%s\n", "fail, wait response timeout");
                break;
            case DATASERVER_CTRL_RETURN_SERVER_NOT_FOUND:
                printf("%s\n", "fail, dataserver not found");
                break;
            case DATASERVER_CTRL_RETURN_SERVER_EXIST:
                printf("%s\n", "fail, dataserver already exist");
                break;
            case DATASERVER_CTRL_RETURN_GROUP_NOT_FOUND:
                printf("%s\n", "fail, group not found");
                break;
            case DATASERVER_CTRL_RETURN_INVAL_OP_TYPE:
                printf("%s\n", "fail, invalid manipulate type");
                break;
            case DATASERVER_CTRL_RETURN_NO_CONFIGSERVER:
                printf("%s\n", "fail, master configserver not found");
            default:
                printf("%s\n", "fail, unkown");
        }
    }
}

void print_usage(const char *prog_name) {
    fprintf(stderr,
            "%s -c configserver:port -g group_name -t|--type op_type dataserver0:port0 [dataserver1:port1 ...]\n"
                    "options:\n"
                    "   -c, --configserver  cs addr\n"
                    "   -g, --group_name    group_name\n"
                    "   -t, --type          manipulate type, add -- add ds to cs\n"
                    "                                        delete -- del ds to cs\n"
                    "   -h, --help\n"
                    "argument:\n"
                    "    dataserver0:port0 [dataserver1:port1 ... ]  ds to manipulate\n", prog_name);
}

int main(int argc, char *argv[]) {
    string master_cs;
    string group;
    vector<uint64_t> server_id_list;
    DataserverCtrlOpType type = DATASERVER_CTRL_OP_NR;

    struct option long_option[] =
            {
                    {"configserver", required_argument, NULL, 'c'},
                    {"groupname",    required_argument, NULL, 'g'},
                    {"type",         required_argument, NULL, 't'},
                    {"help",         no_argument,       NULL, 'h'},
                    {NULL, 0,                           NULL, 0}
            };
    int ch;
    while ((ch = getopt_long(argc, argv, "c:g:t:h", long_option, NULL)) != -1) {
        switch (ch) {
            case 'c':
                master_cs = optarg;
                break;
            case 'g':
                group = optarg;
                break;
            case 't':
                if (strcmp(optarg, "add") == 0)
                    type = DATASERVER_CTRL_OP_ADD;
                else if (strcmp(optarg, "delete") == 0)
                    type = DATASERVER_CTRL_OP_DELETE;
                else {
                    fprintf(stderr, "invalid op type info, -t|--type\n");
                    print_usage(argv[0]);
                    exit(1);
                }
                break;
            case 'h':
            default:
                print_usage(argv[0]);
                exit(1);
        }
    }

    if (master_cs.empty()) {
        fprintf(stderr, "miss configserver addr, -c|--configserver\n");
        print_usage(argv[0]);
        exit(1);
    }

    if (type == DATASERVER_CTRL_OP_NR) {
        fprintf(stderr, "miss manipulate type info, -t|--type\n");
        print_usage(argv[0]);
        exit(1);
    }

    if (optind >= argc) {
        fprintf(stderr, "miss dataserver list\n");
        print_usage(argv[0]);
        exit(1);
    }

    for (int32_t index = optind; index < argc; ++index) {
        string server_str = argv[index];
        string::size_type pos = server_str.find_first_of(':');
        if (string::npos == pos) {
            fprintf(stderr, "dataserver format invalid\n");
            print_usage(argv[0]);
            exit(1);
        }
        string server_ip(server_str, 0, pos - 0);
        int port = atoi(server_str.substr(pos + 1).c_str());
        uint64_t server_id = tbsys::CNetUtil::strToAddr(server_ip.c_str(), port);
        server_id_list.push_back(server_id);
    }

    tair::tair_client_impl tair_client;
    TBSYS_LOGGER.setLogLevel("ERROR");
    if (tair_client.startup(master_cs.c_str(), NULL, group.c_str()) == false) {
        fprintf(stderr, "startup failed\n");
        exit(2);
    }

    vector<DataserverCtrlReturnType> master_return_code;
    tair_client.manipulate_ds_to_cs(server_id_list, type, master_return_code);

    assert(server_id_list.size() == master_return_code.size());
    print_result(server_id_list, master_return_code);

    return 0;
}
