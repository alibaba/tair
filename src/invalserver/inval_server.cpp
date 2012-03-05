#include "inval_server.hpp"

namespace tair {
  InvalServer::InvalServer() {
    _stop = false;
    ignore_zero_area = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_IGNORE_ZERO_AREA, 0);
  }

  InvalServer::~InvalServer() {
  }

  void InvalServer::start() {
    //~ initialization
    if (!init()) {
      return ;
    }

    //~ start thread that retrieves the group infos.
    invalid_loader.start();
    //~ start the retry threads.
    retry_thread.start();
    //~ start the threads handling the packets received from clients.
    task_queue_thread.start();
    //~ start the async thread
    async_thread.start();

    char spec[32];
    bool ret = true;
    //~ establish the server socket.
    if (ret) {
      int port = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_PORT, TAIR_INVAL_SERVER_DEFAULT_PORT);
      snprintf(spec, sizeof(spec), "tcp::%d", port);
      if (transport.listen(spec, &streamer, this) == NULL) {
        log_error("listen on port %d failure.", port);
        ret = false;
      } else {
        log_info("listen on port %d.", port);
      }
    }
    //~ start the network components.
    if (ret) {
      log_info("invalid server start running, pid: %d", getpid());
      transport.start();
    } else {
      stop();
    }

    //~ wait threads to complete.
    invalid_loader.wait();
    task_queue_thread.wait();
    retry_thread.wait();
    async_thread.wait();
    transport.wait();

    destroy();
  }

  void InvalServer::stop() {
    //~ stop threads.
    if (!_stop) {
      _stop = true;
      invalid_loader.stop();
      log_warn("stopping invalid_loader");
      task_queue_thread.stop();
      log_warn("stopping task_queue_thread");
      async_thread.stop();
      log_warn("stopping async_thread");
      retry_thread.stop();
      log_warn("stopping retry_thread");
      transport.stop();
      log_warn("stopping transport");
    }
  }

  //~ the callback interface of IServerAdapter
  tbnet::IPacketHandler::HPRetCode InvalServer::handlePacket(tbnet::Connection *connection,
      tbnet::Packet *packet) {
    if (!packet->isRegularPacket()) {
      log_error("ControlPacket, cmd: %d", ((tbnet::ControlPacket*)packet)->getCommand());
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }
    base_packet *bp = (base_packet*)packet;
    bp->set_connection(connection);
    bp->set_direction(DIRECTION_RECEIVE);
    //~ push regular packet to task queue
    if (!task_queue_thread.push(bp)) {
      log_error("push packet to 'task_queue_thread' failed.");
    }

    return tbnet::IPacketHandler::FREE_CHANNEL;
  }

  //~ the callback interface of IPacketQueueHandler
  bool InvalServer::handlePacketQueue(tbnet::Packet *packet, void *arg) {
    base_packet *bp = (base_packet*)packet;
    int pcode = bp->getPCode();

    bool send_ret = true;
    int ret = TAIR_RETURN_SUCCESS;
    const char *msg = "";

    switch (pcode) {
      case TAIR_REQ_INVAL_PACKET:
        {
          request_invalid *req = dynamic_cast<request_invalid*>(bp);
          if (req != NULL) {
            if (req->is_sync == SYNC_INVALID) {
              ret = do_invalid(req);
            } else if (req->is_sync == ASYNC_INVALID) {
              ret = async_thread.add_packet(new request_invalid(*req)) ?
                TAIR_RETURN_SUCCESS : TAIR_RETURN_QUEUE_OVERFLOWED;
            }
          } else {
            log_error("[FATAL ERROR] packet could not be casted to request_invalid packet.");
            ret = TAIR_RETURN_FAILED;
          }
        }
        break;
      case TAIR_REQ_HIDE_BY_PROXY_PACKET:
        {
          request_hide_by_proxy *req = dynamic_cast<request_hide_by_proxy*>(bp);
          if (req != NULL) {
            if (req->is_sync == SYNC_INVALID) {
              ret = do_hide(req);
            } else if (req->is_sync == ASYNC_INVALID) {
              ret = async_thread.add_packet(new request_hide_by_proxy(*req)) ?
                TAIR_RETURN_SUCCESS : TAIR_RETURN_QUEUE_OVERFLOWED;
            }
          } else {
            log_error("[FATAL ERROR] packet could not be casted to request_hide_by_proxy packet.");
            ret = TAIR_RETURN_FAILED;
          }
        }
        break;
      case TAIR_REQ_PING_PACKET:
        {
          request_ping *req = dynamic_cast<request_ping*>(bp);
          if (req != NULL) {
            log_info("ping packet received, config_version: %u, value: %d", req->config_version, req->value);
            ret = TAIR_RETURN_SUCCESS;
          } else {
            ret = TAIR_RETURN_FAILED;
          }
        }
        break;
      default:
        {
          log_error("[FATAL ERROR] packet not recognized, pcode: %d.", pcode);
          ret = TAIR_RETURN_FAILED;
        }
    }
    //~ set and send the general return_packet.
    if (send_ret && bp->get_direction() == DIRECTION_RECEIVE) {
      tair_packet_factory::set_return_packet(bp, ret, msg, 0);
    }
    //~ do not let 'tbnet' delete this 'packet'
    if (bp) delete bp;
    return false;
  }
  //~ would not delete req
  int InvalServer::do_invalid(request_invalid *req) {
    int ret = TAIR_RETURN_SUCCESS;

    if (ignore_zero_area && req->area == 0) {
      log_info("ignoring packet of area 0");
    } else {
      std::vector<tair_client_api*> *clients = invalid_loader.get_client_list(req->group_name);
      if (clients != NULL) {
        //~ 'set' to collect keys that failed
        tair_dataentry_set key_set;

        for (int i = 0; i < clients->size(); ++i) {
          tair_client_api *client = (*clients)[i];
          //~ single key
          if (req->key_count == 1) {
            if ((ret = client->remove(req->area, *(req->key))) != TAIR_RETURN_SUCCESS) {
              if (ret != TAIR_RETURN_DATA_NOT_EXIST) {
                std::vector<std::string> servers;
                client->get_server_with_key(*(req->key), servers);
                log_error("[FATAL ERROR] RemoveFailure: Group %s, DataServer %s.",
                    req->group_name, servers[0].c_str());

                data_entry *key = new data_entry;
                key->clone(*(req->key));
                pair<tair_dataentry_set::iterator, bool> result = key_set.insert(key);
                if (!result.second) {
                  delete key;
                }
              }
            }
          } else if (req->key_count > 1) {
            //~ multiple keys
            for (tair_dataentry_set::iterator it = (*req->key_list).begin();
                it != (*req->key_list).end(); ++it) {
              if ((ret = client->remove(req->area, **it)) != TAIR_RETURN_SUCCESS) {
                if (ret != TAIR_RETURN_DATA_NOT_EXIST) {
                  std::vector<std::string> servers;
                  client->get_server_with_key(**it, servers);
                  log_error("[FATAL ERROR] RemoveFailure: Group %s, DataServer %s.",
                      req->group_name, servers[0].c_str());
                  data_entry *key = new data_entry;
                  key->clone(**it);
                  pair<tair_dataentry_set::iterator, bool> result = key_set.insert(key);
                  if (!result.second) {
                    delete key;
                  }
                }
              }
            }
          } //~ end if
        } //~ end for

        for (tair_dataentry_set::iterator it = key_set.begin();
            it != key_set.end(); ++it) {
          request_invalid *packet = new request_invalid;
          packet->add_key((**it).get_data(), (**it).get_size());
          packet->area = req->area;
          packet->set_group_name(req->group_name);
          retry_thread.add_packet(packet, 0);
          log_warn("add packet to 'RetryThread' 0");
        }
        ret = TAIR_RETURN_SUCCESS;
      } else {
        log_error("[FATAL ERROR] cannot find clients, maybe Group %s not added, or invalid_loader still loading.",
            req->group_name);
        ret = TAIR_RETURN_INVAL_CONN_ERROR;
      }
    }
    return ret;
  }
  //~ would not delete req
  int InvalServer::do_hide(request_hide_by_proxy *req) {
    int ret = TAIR_RETURN_SUCCESS;

    if (ignore_zero_area && req->area == 0) {
      log_info("ignoring packet of area 0");
    } else {
      std::vector<tair_client_api*> *clients = invalid_loader.get_client_list(req->group_name);
      if (clients != NULL) {
        //~ 'set' to collect keys that failed
        tair_dataentry_set key_set;

        for (int i = 0; i < clients->size(); ++i) {
          tair_client_api *client = (*clients)[i];
          //~ single key
          if (req->key_count == 1) {
            if ((ret = client->hide(req->area, *(req->key))) != TAIR_RETURN_SUCCESS) {
              if (ret != TAIR_RETURN_DATA_NOT_EXIST) {
                std::vector<std::string> servers;
                client->get_server_with_key(*(req->key), servers);
                log_error("[FATAL ERROR] HideFailure: Group %s, DataServer %s.",
                    req->group_name, servers[0].c_str());

                data_entry *key = new data_entry;
                key->clone(*(req->key));
                pair<tair_dataentry_set::iterator, bool> result = key_set.insert(key);
                if (!result.second) {
                  delete key;
                }
              }
            }
          } else if (req->key_count > 1) {
            //~ multiple keys
            for (tair_dataentry_set::iterator it = (*req->key_list).begin();
                it != (*req->key_list).end(); ++it) {
              if ((ret = client->hide(req->area, **it)) != TAIR_RETURN_SUCCESS) {
                if (ret != TAIR_RETURN_DATA_NOT_EXIST) {
                  std::vector<std::string> servers;
                  client->get_server_with_key(**it, servers);
                  log_error("[FATAL ERROR] HideFailure: Group %s, DataServer %s.",
                      req->group_name, servers[0].c_str());
                  data_entry *key = new data_entry;
                  key->clone(**it);
                  pair<tair_dataentry_set::iterator, bool> result = key_set.insert(key);
                  if (!result.second) {
                    delete key;
                  }
                }
              }
            }
          } //~ end if
        } //~ end for

        for (tair_dataentry_set::iterator it = key_set.begin();
            it != key_set.end(); ++it) {
          request_hide_by_proxy *packet = new request_hide_by_proxy();
          packet->add_key((**it).get_data(), (**it).get_size());
          packet->area = req->area;
          packet->set_group_name(req->group_name);
          retry_thread.add_packet(packet, 0);
          log_warn("add packet to 'RetryThread' 0");
        }
        ret = TAIR_RETURN_SUCCESS;
      } else {
        log_error("[FATAL ERROR] cannot find clients, maybe Group %s not added, or invalid_loader still loading.",
            req->group_name);
        ret = TAIR_RETURN_INVAL_CONN_ERROR;
      }
    }
    return ret;
  }

  bool InvalServer::init() {
    //~ get local address
    const char *dev_name = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_DEV_NAME, "eth0");
    uint32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
    int port = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_PORT, TAIR_INVAL_SERVER_DEFAULT_PORT);
    util::local_server_ip::ip = tbsys::CNetUtil::ipToAddr(ip, port);
    log_info("address: %s", tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str());

    //~ set packet factory for packet streamer.
    streamer.setPacketFactory(&packet_factory);
    int thread_count = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_PROCESS_THREAD_COUNT, 4);
    //~ set the number of threads to handle the requests.
    task_queue_thread.setThreadParameter(thread_count, this, NULL);
    retry_thread.setThreadParameter(&invalid_loader);
    thread_count = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, "async_thread_num", 4);
    async_thread.setThreadParameter(&invalid_loader, &retry_thread, thread_count);
    return true;
  }

  bool InvalServer::destroy() {
    return true;
  }
} //~ end of namespace tair

tair::InvalServer * invalid_server = NULL;
uint64_t tair::util::local_server_ip::ip = 0;

//~ signal handler
void sig_handler(int sig) {
  switch (sig) {
    case SIGTERM:
    case SIGINT:
      if (invalid_server != NULL) {
        invalid_server->stop();
      }
      break;
    case 40:
      TBSYS_LOGGER.checkFile();
      break;
    case 41:
    case 42:
      if (sig == 41) {
        TBSYS_LOGGER._level++;
      } else {
        TBSYS_LOGGER._level--;
      }
      log_error("log level changed to %d.", TBSYS_LOGGER._level);
      break;
  }
}
//~ print the help information
void print_usage(char *prog_name) {
  fprintf(stderr, "%s -f config_file\n"
      "    -f, --config_file  config file\n"
      "    -h, --help         show this info\n"
      "    -V, --version      show build time\n\n",
      prog_name);
}
//~ parse the command line
char *parse_cmd_line(int argc, char *const argv[]) {
  int opt;
  const char *optstring = "hVf:";
  struct option longopts[] = {
    {"config_file", 1, NULL, 'f'},
    {"help", 0, NULL, 'h'},
    {"version", 0, NULL, 'V'},
    {0, 0, 0, 0}
  };

  char *configFile = NULL;
  while ((opt = getopt_long(argc, argv, optstring, longopts, NULL)) != -1) {
    switch (opt) {
      case 'f':
        configFile = optarg;
        break;
      case 'V':
        fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
        exit (1);
      case 'h':
        print_usage(argv[0]);
        exit (1);
    }
  }
  return configFile;
}

int main(int argc, char **argv) {
  const char *configfile = parse_cmd_line(argc, argv);
  if (configfile == NULL) {
    print_usage(argv[0]);
    return 1;
  }
  if (TBSYS_CONFIG.load(configfile)) {
    fprintf(stderr, "load ConfigFile %s failed.\n", configfile);
    return 1;
  }

  const char *pidFile = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_PID_FILE, "inval.pid");
  const char *logFile = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_LOG_FILE, "inval.log");

  if (1) {
    char *p, dirpath[256];
    snprintf(dirpath, sizeof(dirpath), "%s", pidFile);
    p = strrchr(dirpath, '/');
    if (p != NULL) *p = '\0';
    if (p != NULL && !tbsys::CFileUtil::mkdirs(dirpath)) {
      fprintf(stderr, "mkdirs %s failed.\n", dirpath);
      return 1;
    }
    snprintf(dirpath, sizeof(dirpath), "%s", logFile);
    p = strrchr(dirpath, '/');
    if (p != NULL) *p = '\0';
    if (p != NULL && !tbsys::CFileUtil::mkdirs(dirpath)) {
      fprintf(stderr, "mkdirs %s failed.\n", dirpath);
      return 1;
    }
  }
  //~ check if one process is already running.
  int pid;
  if ((pid = tbsys::CProcess::existPid(pidFile))) {
    fprintf(stderr, "process already running, pid:%d\n", pid);
    return 1;
  }

  const char *logLevel = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_LOG_LEVEL, "info");
  TBSYS_LOGGER.setLogLevel(logLevel);

  //~ run as daemon process
  if (tbsys::CProcess::startDaemon(pidFile, logFile) == 0) {
    //~ register signal handlers.
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(40, sig_handler);
    signal(41, sig_handler);
    signal(42, sig_handler);

    //~ function starts.
    invalid_server = new tair::InvalServer();
    log_info("starting invalid server.");
    invalid_server->start();

    delete invalid_server;
    log_info("process exit.");
  }
  return 0;
}
