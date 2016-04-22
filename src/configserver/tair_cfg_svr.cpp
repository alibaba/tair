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

#include "tair_cfg_svr.hpp"
#include "easy_helper.hpp"
#include "response_return_packet.hpp"
#include "util.hpp"

namespace tair {
namespace config_server {
tair_config_server *tair_config_server::_this = NULL;

tair_config_server::tair_config_server() : my_server_conf_thread(&eio) {
    stop_flag = 0;
    _this = this;
    memset(&eio, 0, sizeof(eio));
}

tair_config_server::~tair_config_server() {
}

void tair_config_server::start() {
    if (initialize() != EXIT_SUCCESS) {
        return;
    }
    int port = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
    easy_listen_t *listen = easy_io_add_listen(&eio, NULL, port, &handler);
    if (listen == NULL) {
      log_error("listen port %d error", port);
    } else {
      log_info("listen tcp port: %d", port);
    }

    if (listen) {
        log_info("---- program stated PID: %d ----", getpid());
        easy_io_start(&eio);
        my_server_conf_thread.load_config_server();
        my_server_conf_thread.start();
    } else {
        stop();
    }

    my_server_conf_thread.wait();
    easy_io_wait(&eio);

    destroy();
}

void tair_config_server::stop() {
    if (stop_flag == 0) {
        stop_flag = 1;
        log_warn("will stop eio");
        easy_io_stop(&eio);
        log_warn("will stop server_conf_thread");
        my_server_conf_thread.stop();
    }
}

int tair_config_server::initialize() {
    const char *dev_name = TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_DEV_NAME, NULL);
    uint32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name);
    if (local_ip == 0U) {
        log_error("local ip is 0.0.0.0, please check your dev_name");
        return EXIT_FAILURE;
    }
    int port = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
    util::local_server_ip::ip = tbsys::CNetUtil::ipToAddr(local_ip, port);

    vector<const char *> str_list = TBSYS_CONFIG.getStringList(TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
    uint64_t id;
    int server_index = 0;
    for (uint32_t i = 0; i < str_list.size(); i++) {
        id = tbsys::CNetUtil::strToAddr(str_list[i], port);
        if (id == 0)
            continue;
        if (id == util::local_server_ip::ip) {
            server_index = 0;
            break;
        }
        server_index++;
        if (server_index >= 2)
            break;
    }
    if (server_index != 0) {
        log_error("my Ip %s is not in the list of config_server check it out.",
                 tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str());
        return EXIT_FAILURE;
    } else {
        log_info("my IP is: %s", tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str());
    }

    int task_thread_count = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PROCESS_THREAD_COUNT, 8);
    int io_thread_count = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_IO_PROCESS_THREAD_COUNT, 2);

    //~ create eio
    if (!easy_io_create(&eio, io_thread_count)) {
        log_error("create eio failed");
        return EXIT_FAILURE;
    }
    eio.do_signal = 0;
    eio.no_redispatch = 0;
    eio.listen_all = 1;
    eio.no_reuseport = 1;
    eio.tcp_nodelay = 1;
    eio.tcp_cork = 0;
    eio.affinity_enable = 1;

    //~ initialize eio handler
    easy_helper::init_handler(&handler, this);
    handler.process = easy_io_packet_handler_cb;

    //~ create thread pool
    task_queue = easy_request_thread_create(&eio, task_thread_count, packet_handler_cb, NULL);

    return EXIT_SUCCESS;
}

int tair_config_server::destroy() {
    easy_io_destroy(&eio);
    return EXIT_SUCCESS;
}

int tair_config_server::easy_io_packet_handler(easy_request_t *r) {
    base_packet *packet = (base_packet *) r->ipacket;
    if (packet == NULL || !packet->isRegularPacket()) {
        log_error("ControlPacket, timeout");
        return EASY_OK;
    }

    packet->set_direction(DIRECTION_RECEIVE);
    // set hash_code is 0, libeasy will rehash it by addr of struct connection_t
    easy_thread_pool_push(task_queue, r, 0);
    return EASY_AGAIN;
}

int tair_config_server::packet_handler(easy_request_t *r) {
    base_packet *packet = (base_packet *) r->ipacket;
    int pcode = packet->getPCode();

    bool send_ret = true;
    int ret = EXIT_SUCCESS;
    const char *msg = "";
    switch (pcode) {
        case TAIR_REQ_GET_GROUP_PACKET: {
            request_get_group *req = (request_get_group *) packet;
            response_get_group *resp = new response_get_group();

            my_server_conf_thread.find_group_host(req, resp);

            resp->setChannelId(packet->getChannelId());
            r->opacket = resp;
            send_ret = false;
            break;
        }
        case TAIR_REQ_HEARTBEAT_PACKET: {
            request_heartbeat *req = (request_heartbeat *) packet;
            response_heartbeat *resp = new response_heartbeat();

            my_server_conf_thread.do_heartbeat_packet(req, resp);
            resp->setChannelId(packet->getChannelId());
            r->opacket = resp;
            send_ret = false;
            break;
        }
        case TAIR_REQ_QUERY_INFO_PACKET: {
            request_query_info *req = (request_query_info *) packet;
            response_query_info *resp = new response_query_info();

            my_server_conf_thread.do_query_info_packet(req, resp);
            resp->setChannelId(packet->getChannelId());
            r->opacket = resp;
            send_ret = false;
            break;
        }
        case TAIR_REQ_CONFHB_PACKET: {
            my_server_conf_thread.do_conf_heartbeat_packet((request_conf_heartbeart *) packet);
            break;
        }
        case TAIR_REQ_SETMASTER_PACKET: {
            if (my_server_conf_thread.
                    do_set_master_packet((request_set_master *) packet, easy_helper::convert_addr(r->ms->c->addr)) == false) {
                ret = EXIT_FAILURE;
            }
            break;
        }
        case TAIR_REQ_GET_SVRTAB_PACKET: {
            request_get_server_table *req = (request_get_server_table *) packet;
            response_get_server_table *resp = new response_get_server_table();

            my_server_conf_thread.do_get_server_table_packet(req, resp);
            resp->setChannelId(packet->getChannelId());
            r->opacket = resp;
            send_ret = false;
            break;
        }
        case TAIR_RESP_GET_SVRTAB_PACKET: {
            if (my_server_conf_thread.
                    do_set_server_table_packet((response_get_server_table *) packet) == false) {
                ret = EXIT_FAILURE;
            }
            break;
        }
        case TAIR_REQ_GET_GROUP_NON_DOWN_DATASERVER_PACKET: {
            request_get_group_not_down_dataserver *req = (request_get_group_not_down_dataserver *) packet;
            response_get_group_non_down_dataserver *resp = new response_get_group_non_down_dataserver();

            my_server_conf_thread.do_get_group_non_down_dataserver_packet(req, resp);
            resp->setChannelId(packet->getChannelId());
            r->opacket = resp;
            send_ret = false;
            break;
        }
        case TAIR_REQ_MIG_FINISH_PACKET: {
            ret =
                    my_server_conf_thread.
                            do_finish_migrate_packet((request_migrate_finish *) packet);
            if (ret == 1) {
                send_ret = false;
            }
            if (ret == -1) {
                ret = EXIT_FAILURE;
            } else {
                ret = EXIT_SUCCESS;
            }
            break;
        }
            // add for recovery
        case TAIR_REQ_REC_FINISH_PACKET: {
            ret = my_server_conf_thread.do_finish_recovery_packet((request_recovery_finish *) packet);
            if (ret == 1) {
                send_ret = false;
            }
            if (ret == -1) {
                ret = EXIT_FAILURE;
            } else {
                ret = EXIT_SUCCESS;
            }
            break;
        }
        case TAIR_REQ_GROUP_NAMES_PACKET: {
            response_group_names *resp = new response_group_names();
            my_server_conf_thread.do_group_names_packet(resp);
            resp->setChannelId(packet->getChannelId());
            r->opacket = resp;
            send_ret = false;
            break;
        }
        case TAIR_REQ_DATASERVER_CTRL_PACKET: {
            // force add or delete some dataserver;
            request_data_server_ctrl *req = (request_data_server_ctrl *) packet;
            response_data_server_ctrl *resp = new response_data_server_ctrl();
            my_server_conf_thread.change_server_list_to_group_file(req, resp);
            resp->setChannelId(packet->getChannelId());
            r->opacket = resp;
            send_ret = false;
        }
            break;
        case TAIR_REQ_GET_MIGRATE_MACHINE_PACKET: {
            response_get_migrate_machine *resp = new response_get_migrate_machine();
            request_get_migrate_machine *req = (request_get_migrate_machine *) packet;

            my_server_conf_thread.get_migrating_machines(req, resp);

            resp->setChannelId(packet->getChannelId());
            r->opacket = resp;
            send_ret = false;
            break;
        }
        case TAIR_REQ_OP_CMD_PACKET: {
            response_op_cmd *resp = new response_op_cmd();
            request_op_cmd *req = (request_op_cmd *) packet;
            my_server_conf_thread.do_op_cmd(req, resp);
            r->opacket = resp;
            send_ret = false;
            break;
        }
        case TAIR_REQ_PING_PACKET: {
            response_return *resp = new response_return(packet->getChannelId(), ret, msg, 0);
            r->opacket = resp;
            send_ret = false;
            break;
        }
        default:
            log_error("unknow packet pcode: %d", pcode);
    }
    if (send_ret && packet->get_direction() == DIRECTION_RECEIVE) {
        response_return *resp = new response_return(packet->getChannelId(), ret, msg, 0);
        r->opacket = resp;
    }
    return EASY_OK;
}
}
} // namespace end

////////////////////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////////////////////
tair::config_server::tair_config_server *tair_cfg_svr = NULL;

void
sign_handler(int sig) {
    switch (sig) {
        case SIGTERM:
        case SIGINT:
            if (tair_cfg_svr != NULL) {
                log_warn("will stop tair_cfg_svr");
                tair_cfg_svr->stop();
            }
            break;
        case 40:
            TBSYS_LOGGER.checkFile();
            break;
        case 41:
        case 42:
            if (sig == 41) {
                tair::easy_helper::easy_set_log_level(++TBSYS_LOGGER._level);
            } else {
                tair::easy_helper::easy_set_log_level(--TBSYS_LOGGER._level);
            }
            log_error("TBSYS_LOGGER._level: %d", TBSYS_LOGGER._level);
            break;
    }
}

void
print_usage(char *prog_name) {
    fprintf(stderr, "%s -f config_file\n"
            "    -f, --config_file  config file\n"
            "    -h, --help         this help\n"
            "    -V, --version      version\n\n", prog_name);
}

char *
parse_cmd_line(int argc, char *const argv[]) {
    int opt;
    const char *opt_string = "hVf:";
    struct option longopts[] = {
            {"config_file", 1, NULL, 'f'},
            {"help",        0, NULL, 'h'},
            {"version",     0, NULL, 'V'},
            {0,             0, 0,    0}
    };

    char *config_file = NULL;
    while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
        switch (opt) {
            case 'f':
                config_file = optarg;
                break;
            case 'V':
                fprintf(stderr, "BUILD_TIME: %s %s\nGIT: %s\n", __DATE__, __TIME__, TAIR_GIT_INFO);
                exit(1);
            case 'h':
                print_usage(argv[0]);
                exit(1);
        }
    }
    return config_file;
}

int main(int argc, char *argv[]) {
    // parse cmd
    char *config_file = parse_cmd_line(argc, argv);
    if (config_file == NULL) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    if (TBSYS_CONFIG.load(config_file)) {
        fprintf(stderr, "load file %s error\n", config_file);
        return EXIT_FAILURE;
    }

    const char *sz_pid_file = TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_PID_FILE, "cfgsvr.pid");
    const char *sz_log_file = TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_LOG_FILE, "cfgsvr.log");
    char *p, dir_path[256];
    sprintf(dir_path, "%s", TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_DATA_DIR, TAIR_DEFAULT_DATA_DIR));
    if (!tbsys::CFileUtil::mkdirs(dir_path)) {
      fprintf(stderr, "create dir %s error\n", dir_path);
      return EXIT_FAILURE;
    }
    sprintf(dir_path, "%s", sz_pid_file);
    p = strrchr(dir_path, '/');
    if (p != NULL)
      *p = '\0';
    if (p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
      fprintf(stderr, "create dir %s error\n", dir_path);
      return EXIT_FAILURE;
    }
    sprintf(dir_path, "%s", sz_log_file);
    p = strrchr(dir_path, '/');
    if (p != NULL)
      *p = '\0';
    if (p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
      fprintf(stderr, "create dir %s error\n", dir_path);
      return EXIT_FAILURE;
    }

    int pid;
    if ((pid = tbsys::CProcess::existPid(sz_pid_file))) {
        fprintf(stderr, "program has been exist: pid=%d\n", pid);
        return EXIT_FAILURE;
    }

    const char * sz_log_level = TBSYS_CONFIG.getString(CONFSERVER_SECTION, TAIR_LOG_LEVEL, "info");
    TBSYS_LOGGER.setLogLevel(sz_log_level);

    if (tbsys::CProcess::startDaemon(sz_pid_file, sz_log_file) == 0) {
        signal(SIGPIPE, SIG_IGN);
        signal(SIGHUP, SIG_IGN);
        signal(SIGINT, sign_handler);
        signal(SIGTERM, sign_handler);
        signal(40, sign_handler);
        signal(41, sign_handler);
        signal(42, sign_handler);

        tair_cfg_svr = new tair::config_server::tair_config_server();
        tair_cfg_svr->start();

        // ignore signal when destroy, cause sig_handler may use tair_cfg_svr between delete and set it to NULL.
        signal(SIGINT, SIG_IGN);
        signal(SIGTERM, SIG_IGN);

        delete tair_cfg_svr;
        tair_cfg_svr = NULL;

        log_info("exit program.");
    }

    return EXIT_SUCCESS;
}

////////////////////////////////END
