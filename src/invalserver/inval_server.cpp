#include "inval_server.hpp"

namespace tair {
  InvalServer::InvalServer() : processor(&invalid_loader) {
    _stop = false;
    ignore_zero_area = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_IGNORE_ZERO_AREA, 0);
    sync_task_thread_count = 0;
    async_task_thread_count = 0;
  }

  InvalServer::~InvalServer() {
    if(request_storage != NULL) {
      delete request_storage;
      request_storage = NULL;
    }
  }

  void InvalServer::start() {
    //~ initialization
    if (!init()) {
      return ;
    }

    //~ start the thread to save the request packets.
    request_storage->start();
    //~ start thread that retrieves the group infos.
    invalid_loader.start();
    //~ start the retry threads.
    retry_thread.start();
    //~ start the threads handling the packets received from clients.
    task_queue_thread.start();
    //~ start the async thread
    async_thread.start();
    //~ start the stat thread
    TAIR_INVAL_STAT.start();

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
    TAIR_INVAL_STAT.wait();
    request_storage->wait();
    transport.wait();

    destroy();
  }

  void InvalServer::stop() {
    //~ stop threads.
    if (!_stop) {
      _stop = true;
      transport.stop();
      log_warn("stopping transport");
      task_queue_thread.stop();
      log_warn("stopping task_queue_thread");
      async_thread.stop();
      log_warn("stopping async_thread");
      retry_thread.stop();
      log_warn("stopping retry_thread");
      invalid_loader.stop();
      log_warn("stopping invalid_loader");
      TAIR_INVAL_STAT.stop();
      log_warn("stopping stat_helper");
      request_storage->stop();
      log_warn("stopping request_storage");
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
              request_invalid *async_packet = new request_invalid();
              async_packet->swap(*req);
              async_packet->set_group_name(req->group_name);
              ret = async_thread.add_packet(async_packet) ?
                TAIR_RETURN_SUCCESS : TAIR_RETURN_QUEUE_OVERFLOWED;
            }
          } else {
            log_error("[FATAL ERROR] packet could not be casted to request_invalid packet.");
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
      case TAIR_REQ_HIDE_BY_PROXY_PACKET:
        {
          request_hide_by_proxy *req = dynamic_cast<request_hide_by_proxy*>(bp);
          if (req != NULL) {
            if (req->is_sync == SYNC_INVALID) {
              ret = do_hide(req);
            } else if (req->is_sync == ASYNC_INVALID) {
              request_hide_by_proxy *async_packet = new request_hide_by_proxy();
              async_packet->swap(*req);
              async_packet->set_group_name(req->group_name);
              ret = async_thread.add_packet(async_packet) ?
                TAIR_RETURN_SUCCESS : TAIR_RETURN_QUEUE_OVERFLOWED;
            }
          } else {
            log_error("[FATAL ERROR] packet could not be casted to request_hide_by_proxy packet.");
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
      case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
        {
          request_prefix_hides_by_proxy *req = dynamic_cast<request_prefix_hides_by_proxy*>(bp);
          if (req != NULL) {
            if (req->is_sync == SYNC_INVALID) {
              ret = do_prefix_hides(req);
            } else if (req->is_sync == ASYNC_INVALID) {
              request_prefix_hides_by_proxy *async_packet = new request_prefix_hides_by_proxy();
              async_packet->swap(*req);
              async_packet->set_group_name(req->group_name);
              ret = async_thread.add_packet(async_packet) ?
                TAIR_RETURN_SUCCESS : TAIR_RETURN_QUEUE_OVERFLOWED;
            }
          } else {
            log_error("[FATAL ERROR] packet could not be casted to request_hides_by_proxy packet.");
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
      case TAIR_REQ_PREFIX_INVALIDS_PACKET:
        {
          request_prefix_invalids *req = dynamic_cast<request_prefix_invalids*>(bp);
          if (req != NULL) {
            if (req->is_sync == SYNC_INVALID) {
              ret = do_prefix_invalids(req);
            } else if (req->is_sync == ASYNC_INVALID) {
              request_prefix_invalids *async_packet = new request_prefix_invalids();
              async_packet->swap(*req);
              async_packet->set_group_name(req->group_name);
              ret = async_thread.add_packet(async_packet) ?
                TAIR_RETURN_SUCCESS : TAIR_RETURN_QUEUE_OVERFLOWED;
            }
          } else {
            log_error("[FATAL ERROR] packet could not be casted to request_hide_by_proxy packet.");
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
      case TAIR_REQ_PING_PACKET:
        {
          if (invalid_loader.is_loading()) {
            log_info("ping packet received, but clients are still not ready");
            ret = TAIR_RETURN_FAILED;
            msg = "iv not ready";
            break;
          }
          request_ping *req = dynamic_cast<request_ping*>(bp);
          if (req != NULL) {
            log_info("ping packet received, config_version: %u, value: %d", req->config_version, req->value);
            ret = TAIR_RETURN_SUCCESS;
          } else {
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
      case TAIR_REQ_INVAL_STAT_PACKET:
        {
          request_inval_stat *req = dynamic_cast<request_inval_stat*>(bp);
          response_inval_stat *resp = new response_inval_stat();
          if (req != NULL) {
            ret = do_request_stat(req, resp);
            if (ret == TAIR_RETURN_SUCCESS)
              send_ret = false;
          }
          else {
            log_error("[FATAL ERROR] the request should be request_stat.");
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
      case TAIR_REQ_RETRY_ALL_PACKET:
        {
          request_retry_all *retry_all = dynamic_cast<request_retry_all*>(bp);
          if (retry_all != NULL) {
            request_retry_all *async_retry_all = new request_retry_all(*retry_all);
            ret = async_thread.add_packet(async_retry_all) ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED ;
          }
          else {
            log_error("[FATAL ERROR] packet could not be casted to request_retry_all packet.");
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
      case TAIR_REQ_OP_CMD_PACKET:
        {
          request_op_cmd *op_cmd = dynamic_cast<request_op_cmd*>(bp);
          if (op_cmd != NULL) {
            if (do_debug_support(op_cmd) == TAIR_RETURN_SUCCESS) {
              //had sent the packet
              send_ret = false;
            }
          }
          else
          {
            log_error("[FATAL ERROR] packet could not be casted to requet_op_cmd packet.");
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
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

  int InvalServer::do_invalid(request_invalid *req) {
    int ret = TAIR_RETURN_SUCCESS;
    if (ignore_zero_area && req->area == 0) {
      log_info("ignoring packet of area 0");
    } else {
      request_invalid *post_req = NULL;
      ret = processor.process(req, post_req);
      TAIR_INVAL_STAT.statistcs(inval_stat_helper::INVALID,
          std::string(req->group_name), req->area, inval_area_stat::FIRST_EXEC);
      if (post_req != NULL) {
        log_error("add invalid packet to RetryThread 0");
        retry_thread.add_packet(post_req, 0);
      }
    }
    return ret;
  }

  int InvalServer::do_hide(request_hide_by_proxy *req) {
    int ret = TAIR_RETURN_SUCCESS;
    if (ignore_zero_area && req->area == 0) {
      log_info("ignoring packet of area 0");
    } else {
      request_hide_by_proxy *post_req = NULL;
      ret = processor.process(req, post_req);
      TAIR_INVAL_STAT.statistcs(inval_stat_helper::HIDE,
          std::string(req->group_name), req->area, inval_area_stat::FIRST_EXEC);
      if (post_req != NULL) {
        log_error("add hide packet to RetryThread 0");
        retry_thread.add_packet(post_req, 0);
      }
    }
    return ret;
  }

  int InvalServer::do_prefix_hides(request_prefix_hides_by_proxy *req) {
    int ret = TAIR_RETURN_SUCCESS;
    if (ignore_zero_area && req->area == 0) {
      log_info("ignoring packet of area 0");
    } else {
      request_prefix_hides_by_proxy *post_req = NULL;
      ret = processor.process(req, post_req);
      TAIR_INVAL_STAT.statistcs(inval_stat_helper::PREFIX_HIDE,
          std::string(req->group_name), req->area, inval_area_stat::FIRST_EXEC);
      if (post_req != NULL) {
        log_error("add prefix hides packet to RetryThread 0");
        retry_thread.add_packet(post_req, 0);
      }
    }
    return ret;
  }

  int InvalServer::do_prefix_invalids(request_prefix_invalids *req) {
    int ret = TAIR_RETURN_SUCCESS;
    if (ignore_zero_area && req->area == 0) {
      log_info("ignoring packet of area 0");
    } else {
      request_prefix_invalids *post_req = NULL;
      ret = processor.process(req, post_req);
      TAIR_INVAL_STAT.statistcs(inval_stat_helper::PREFIX_INVALID,
          std::string(req->group_name), req->area, inval_area_stat::FIRST_EXEC);
      if (post_req != NULL) {
        log_error("add prefix invalids packet to RetryThread 0");
        retry_thread.add_packet(post_req, 0);
      }
    }
    return ret;
  }

  int InvalServer::do_request_stat(request_inval_stat *req, response_inval_stat *resp) {
    int ret = TAIR_RETURN_SUCCESS;
    if (ignore_zero_area && req->area == 0) {
      log_info("ignoring packet of area 0.");
    }
    else {
      unsigned long buffer_size = 0;
      unsigned long uncompressed_data_size = 0;
      int group_count = 0;
      char *buffer = 0;
      TAIR_INVAL_STAT.get_stat_buffer(buffer, buffer_size, uncompressed_data_size, group_count);
      if (buffer_size == 0) {
        log_error("NONE stat info at all");
        delete resp;
        ret = TAIR_RETURN_FAILED;
      }
      std::string key("inval_stats");
      resp->key = new data_entry(key.c_str());
      resp->stat_value = new data_entry(buffer, buffer_size, true); //alloc the new memory
      resp->uncompressed_data_size = uncompressed_data_size;
      resp->group_count = group_count;
      resp->setChannelId(req->getChannelId());
      if (req->get_connection()->postPacket(resp) == false) {
        log_error("[FATAL ERROR] fail to send stat info to client.");
        delete resp;
        ret = TAIR_RETURN_FAILED;
      }
      delete [] buffer; // alloced by `inval_stat_helper
      buffer = NULL;
    }
    return ret;
  }

  int InvalServer::parse_params(const std::vector<std::string>& params,
      std::string &group_name, int32_t& area, int32_t& add_request_storage)
  {
    int ret = TAIR_RETURN_SUCCESS;
    std::string key;
    std::string value;
    group_name.clear();
    area = -1;
    add_request_storage = -1;
    for (int i = 0; i < params.size(); ++i) {
      std::string param = params[i];
      size_t equ_pos = param.find_first_of('=');
      if (equ_pos != std::string::npos && equ_pos < param.size()) {
        key = param.substr(0, equ_pos);
        value = param.substr(equ_pos + 1, param.size());
        if (key == "group_name") {
          group_name = value;
        }
        else if (key == "area") {
          area = atoi(value.c_str());
        }
        else if (key == "add_request_storage") {
          add_request_storage = atoi(value.c_str());
        }
        else {
          log_error("undefined parameters <key, value>: <%s, %s>", key.c_str(), value.c_str());
        }
      }
      else {
        log_error("can't parse params from: %s", param.c_str());
      }
    }
    if (group_name.size() == 0 || area < 0 || add_request_storage < 0) {
      log_error("the parameters are illegal, group_name: %s, area: %d, add_request_storage: %d",
          group_name.c_str(), area, add_request_storage);
      ret = TAIR_RETURN_FAILED;
    }
    return ret;
  }

  void InvalServer::construct_debug_infos(std::vector<std::string>& infos)
  {
    infos.clear();
    int32_t packet_count = request_storage->get_packet_count();
    int32_t packet_count_on_disk = request_storage->get_packet_count_on_disk();
    char buffer[256];
    memset(buffer,'\0', 256);
    sprintf(buffer, "packet_count=%8d", packet_count);
    infos.push_back(buffer);
    memset(buffer, '\0', 256);
    sprintf(buffer, "packet_count_on_disk=%8d", packet_count_on_disk);
    infos.push_back(buffer);
    memset(buffer, '\0', 256);
    sprintf(buffer, "packet_count_in_queue=%8d", packet_count - packet_count_on_disk);
    infos.push_back(buffer);
    memset(buffer, '\0', 256);
    sprintf(buffer, "task_queue_size=%8d", task_queue_thread.size());
    infos.push_back(buffer);
    memset(buffer, '\0', 256);
    sprintf(buffer, "memory_queue_max_size=%8d", request_storage->get_queue_max_size());
    infos.push_back(buffer);
    memset(buffer, '\0', 256);
    sprintf(buffer, "read_threshold=%8d", request_storage->get_read_threshold());
    infos.push_back(buffer);
    memset(buffer, '\0', 256);
    sprintf(buffer, "write_threshold=%8d", request_storage->get_write_threshold());
    infos.push_back(buffer);
    for (int i = 0; i < InvalRetryThread::RETRY_COUNT; ++i) {
      memset(buffer, '\0', 256);
      sprintf(buffer, "retry_queue_size#%d=%8d", i, retry_thread.retry_queue_size(i));
      infos.push_back(buffer);
    }
  }

  int InvalServer::do_debug_support(request_op_cmd *req)
  {
    int ret = TAIR_RETURN_SUCCESS;
    std::vector<std::string> &params = req->params;
    if (params.empty() == false) {
      std::string group_name;
      int32_t area = -1;
      int32_t add_request_storage = -1;
      if (parse_params(params, group_name, area, add_request_storage) == TAIR_RETURN_SUCCESS) {
        log_debug("add %d packet to the request_storage.", add_request_storage);
        for (int i = 0; i < add_request_storage; ++i) {
          request_hide_by_proxy *invalid = new request_hide_by_proxy();
          invalid->set_group_name(group_name.c_str());
          invalid->area = area;
          char * key = "TEST_KEY";
          invalid->add_key(key, strlen(key));
          if (invalid != NULL) {
            request_storage->write_request(invalid);
          }
        }
      }
      else {
        log_error("failed to parse parameters.");
      }
    }
    //response
    response_op_cmd *resp = new response_op_cmd();
    if (resp != NULL) {
      //query debug infos
      construct_debug_infos(resp->infos);
      resp->code = TAIR_RETURN_SUCCESS;
      resp->setChannelId(req->getChannelId());
      if (req->get_connection()->postPacket(resp) == false) {
        log_error("[FATAL ERROR] fail to send stat info to client.");
        delete resp;
        ret = TAIR_RETURN_FAILED;
      }
    }
    else {
      log_error("failed to create the response packet.");
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

    request_storage = new inval_request_storage();
    const char *data_dir = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_INVAL_DATA_DIR,
        TAIR_INVAL_DEFAULT_DATA_DIR);
    log_info("invalid server data path: %s", data_dir);
    char *queue_name = "disk_queue";
    //the packet's count default value is 10,000.
    const int cached_packet_count = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_INVAL_CACHED_PACKET_COUNT,
        TAIR_INVAL_DEFAULT_CACHED_PACKET_COUNT);
    log_info("invalid server cached packet's count: %d", cached_packet_count);
    request_storage->setThreadParameter(data_dir, queue_name, 0.2, 0.8, cached_packet_count, &packet_factory);
    //~ set packet factory for packet streamer.
    streamer.setPacketFactory(&packet_factory);
    int thread_count = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_PROCESS_THREAD_COUNT, 4);
    //~ set the number of threads to handle the requests.
    task_queue_thread.setThreadParameter(thread_count, this, NULL);
    retry_thread.setThreadParameter(&invalid_loader, &processor, request_storage);
    thread_count = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, "async_thread_num", 8);
    async_thread.setThreadParameter(&invalid_loader, &retry_thread,
        &processor, thread_count, request_storage);
    sync_task_thread_count = thread_count;
    async_task_thread_count = thread_count;
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
        fprintf(stderr, "BUILD_TIME: %s %s\nSVN: %s\n", __DATE__, __TIME__, TAIR_SVN_INFO);
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
  TBSYS_LOGGER.setMaxFileSize(1<<23);

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
