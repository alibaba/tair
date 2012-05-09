/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * tair_server daemon impl
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "tair_server.hpp"

char g_tair_home[129];

namespace tair {

   tair_server::tair_server()
   {
      is_stoped = 0;
      tair_mgr = NULL;
      req_processor = NULL;
      conn_manager = NULL;
      thread_count = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PROCESS_THREAD_COUNT, -1);
      task_queue_size = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION,TAIR_TASK_QUEUE_SIZE,100);
      if (thread_count < 0) thread_count = sysconf(_SC_NPROCESSORS_CONF);
      if (thread_count > 0) {
         setBatchPushPacket(true);
      }
      if (task_queue_size < 0){
         task_queue_size = 100;
      }
   }

   tair_server::~tair_server()
   {
   }

   void tair_server::start()
   {
      if (!initialize()) {
         return;
      }

      if (thread_count > 0) {
         task_queue_thread.start();
         duplicate_task_queue_thread.start();
      }
      heartbeat.start();
      async_task_queue_thread.start();
      TAIR_STAT.start();

      char spec[32];
      bool ret = true;
      if (ret) {
         int port = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PORT, TAIR_SERVER_DEFAULT_PORT);
         sprintf(spec, "tcp::%d", port);
         if (transport.listen(spec, &streamer, this) == NULL) {
            log_error("listen on port %d failed", port);
            ret = false;
         } else {
            log_info("listened on port %d", port);
         }
      }

      if (ret) {
         log_info("---- tair_server started, pid: %d ----", getpid());
      } else {
         stop();
      }

      if (thread_count > 0) {
         task_queue_thread.wait();
         duplicate_task_queue_thread.wait();
      }
      heartbeat.wait();
      async_task_queue_thread.wait();
      transport.wait();

      destroy();
   }

   void tair_server::stop()
   {
      if (is_stoped == 0) {
         is_stoped = 1;
         log_info("will stop transport");
         transport.stop();
         if (thread_count > 0) {
            log_info("will stop taskQueue");
            task_queue_thread.stop();
            duplicate_task_queue_thread.stop();
         }
         log_info("will stop heartbeatThread");
         heartbeat.stop();
         log_info("will stop yncTaskQueue");
         async_task_queue_thread.stop();
         log_info("will stop TAIR_STAT");
         TAIR_STAT.stop();
      }
   }

   bool tair_server::initialize()
   {

      const char *dev_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_DEV_NAME, NULL);
      uint32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name);
      int port = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PORT, TAIR_SERVER_DEFAULT_PORT);
      local_server_ip::ip =  tbsys::CNetUtil::ipToAddr(local_ip, port);

      // packet_streamer
      streamer.setPacketFactory(&factory);

      if (thread_count>0) {
         task_queue_thread.setThreadParameter(thread_count, this, NULL);
         duplicate_task_queue_thread.setThreadParameter(thread_count, this, NULL);
         // m_duplicateTaskQueueThread should have m_threadCount * (server_copy_count-1) thread, but we do not know server_copy_count here
         // m_threadCount just ok.
      }
      async_task_queue_thread.setThreadParameter(1, this, NULL);

      conn_manager = new tbnet::ConnectionManager(&transport, &streamer, this);
      conn_manager->setDefaultQueueLimit(0, 500);

      transport.start();

      // m_tairManager
      tair_mgr = new tair_manager();
      bool init_rc = tair_mgr->initialize(&transport, &streamer);
      if (!init_rc) {
         return false;
      }

      heartbeat.set_thread_parameter(this, &streamer, tair_mgr);

      req_processor = new request_processor(tair_mgr, &heartbeat, conn_manager);

      return true;
   }

   bool tair_server::destroy()
   {
      if (req_processor != NULL) {
         delete req_processor;
         req_processor = NULL;
      }

      if (tair_mgr != NULL) {
         log_info("will destroy m_tairManager");
         delete tair_mgr;
         tair_mgr = NULL;
      }
      if (conn_manager != NULL) {
         log_info("will destroy m_connmgr");
         delete conn_manager;
         conn_manager = NULL;
      }
      return true;
   }

   bool tair_server::handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue)
   {
      heartbeat.request_count += packetQueue.size();

      tbnet::Packet *list = packetQueue.getPacketList();
      while (list != NULL) {
         base_packet *bp = (base_packet*) list;
         bp->set_connection(connection);
         bp->set_direction(DIRECTION_RECEIVE);

         if ((TBSYS_LOG_LEVEL_DEBUG<=TBSYS_LOGGER._level)) {
            bp->request_time = tbsys::CTimeUtil::getTime();
         }

         if (thread_count == 0) {
            handlePacketQueue(bp, NULL);
         }

         list = list->getNext();
      }

      if (thread_count > 0) {
         task_queue_thread.pushQueue(packetQueue);
      } else {
         packetQueue.clear();
      }

      return true;
   }

   tbnet::IPacketHandler::HPRetCode tair_server::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
   {
      if (!packet->isRegularPacket()) {
         log_error("ControlPacket, cmd:%d", ((tbnet::ControlPacket*)packet)->getCommand());
         return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      base_packet *bp = (base_packet*) packet;
      bp->set_connection(connection);
      bp->set_direction(DIRECTION_RECEIVE);

      if ((TBSYS_LOG_LEVEL_DEBUG<=TBSYS_LOGGER._level)) {
         bp->request_time = tbsys::CTimeUtil::getTime();
      }

      if (thread_count == 0) {
         if(handlePacketQueue(bp, NULL))
         delete packet;
      } else {
         int pcode = packet->getPCode();
         if (TAIR_REQ_DUPLICATE_PACKET == pcode) {
            duplicate_task_queue_thread.push(packet);
         }else {
            if ( !task_queue_thread.push(packet,task_queue_size,false) ){
               bp->free();
               return tbnet::IPacketHandler::KEEP_CHANNEL;
            }
         }
      }
      heartbeat.request_count ++;

      return tbnet::IPacketHandler::FREE_CHANNEL;
   }

   bool tair_server::handlePacketQueue(tbnet::Packet *apacket, void *args)
   {
      base_packet *packet = (base_packet*)apacket;
      int pcode = packet->getPCode();

      bool send_return = true;
      int ret = TAIR_RETURN_SUCCESS;
      const char *msg = "";
      switch (pcode) {
         case TAIR_REQ_PUT_PACKET:
         {
            request_put *npacket = (request_put*)packet;
            ret = req_processor->process(npacket, send_return);
            break;
         }
         case TAIR_REQ_GET_PACKET:
         {
            request_get *npacket = (request_get*)packet;
            ret = req_processor->process(npacket, send_return);
            send_return = false;
            break;
         }
         case TAIR_REQ_HIDE_PACKET:
         {
             request_hide *npacket = (request_hide*) packet;
             ret = req_processor->process(npacket, send_return);
             break;
         }
         case TAIR_REQ_GET_HIDDEN_PACKET:
         {
             request_get_hidden *npacket = (request_get_hidden*) packet;
             ret = req_processor->process(npacket, send_return);
             send_return = false;
             break;
         }
         case TAIR_REQ_REMOVE_PACKET:
         {
            request_remove *npacket = (request_remove*)packet;
            ret = req_processor->process(npacket, send_return);
            log_debug("end remove,prepare to send return packet");
            break;
         }

         case TAIR_REQ_REMOVE_AREA_PACKET:
         {
            request_remove_area *npacket = (request_remove_area*)packet;
            if (npacket->get_direction() == DIRECTION_RECEIVE) {
               async_task_queue_thread.push(new request_remove_area(*npacket));
            } else {
               if (tair_mgr->clear(npacket->area) == false) {
                  ret = EXIT_FAILURE;
               }
            }
            break;
         }
         case TAIR_REQ_LOCK_PACKET:
         {
            request_lock *npacket = (request_lock*)packet;
            ret = req_processor->process(npacket, send_return);
            break;
         }
         case TAIR_REQ_PING_PACKET:
         {
            ret = ((request_ping*)packet)->value;
            break;
         }
         case TAIR_REQ_DUMP_PACKET:
         {
            request_dump *npacket = (request_dump*)packet;
            if (npacket->get_direction() == DIRECTION_RECEIVE) {
               async_task_queue_thread.push(new request_dump(*npacket));
            } else {
               tair_mgr->do_dump(npacket->info_set);
            }
            break;
         }

         case TAIR_REQ_DUMP_BUCKET_PACKET:
         {
            // request_dump_bucket *npacket = (request_dump_bucket*)packet;
            // if (m_tairManager->dump_bucket(npacket->dbid, npacket->path) == false ) {
            ret = EXIT_FAILURE;
            // }
            break;
         }

         case TAIR_REQ_INCDEC_PACKET:
         {
            request_inc_dec *npacket = (request_inc_dec*)packet;
            ret = req_processor->process(npacket, send_return);
            break;
         }
         case TAIR_REQ_DUPLICATE_PACKET:
         {
            request_duplicate *dpacket = (request_duplicate *)packet;
            ret = req_processor->process(dpacket, send_return);
            if (ret == TAIR_RETURN_SUCCESS) send_return = false;
            break;
         }
         case TAIR_REQ_ADDITEMS_PACKET:
         {
            request_add_items *dpacket = (request_add_items *)(packet);
            ret = req_processor->process(dpacket, send_return);
            break;
         }
         case TAIR_REQ_GETITEMS_PACKET:
         {
            request_get_items *dpacket = (request_get_items *)(packet);
            ret = req_processor->process(dpacket, send_return);
            if (ret == TAIR_RETURN_SUCCESS) send_return = false;
            break;
         }
         case TAIR_REQ_REMOVEITEMS_PACKET:
         {
            request_remove_items *dpacket = (request_remove_items *)(packet);
            ret = req_processor->process(dpacket, send_return);
            break;
         }
         case TAIR_REQ_GETANDREMOVEITEMS_PACKET:
         {
            request_get_and_remove_items *dpacket = (request_get_and_remove_items *)(packet);
            ret = req_processor->process(dpacket, send_return);
            if (ret == TAIR_RETURN_SUCCESS) send_return = false;
            break;
         }
         case TAIR_REQ_GETITEMSCOUNT_PACKET:
         {
            request_get_items_count *dpacket = (request_get_items_count *)(packet);
            ret = req_processor->process(dpacket,send_return);
            break;
         }
         case TAIR_REQ_MUPDATE_PACKET:
         {
            request_mupdate *mpacket = (request_mupdate *)(packet);
            ret = req_processor->process(mpacket, send_return);
            break;
         }
         case TAIR_REQ_PREFIX_PUTS_PACKET:
         {
           request_prefix_puts *ppacket = (request_prefix_puts*)packet;
           ret = req_processor->process(ppacket, send_return);
           break;
         }
         case TAIR_REQ_PREFIX_REMOVES_PACKET:
         {
           request_prefix_removes *rpacket = (request_prefix_removes*)packet;
           ret = req_processor->process(rpacket, send_return);
           break;
         }
         case TAIR_REQ_PREFIX_INCDEC_PACKET:
         {
           request_prefix_incdec *ipacket = (request_prefix_incdec*)packet;
           ret = req_processor->process(ipacket, send_return);
           break;
         }
         case TAIR_REQ_PREFIX_GETS_PACKET:
         {
           request_prefix_gets *npacket = (request_prefix_gets*)packet;
           ret = req_processor->process(npacket, send_return);
           break;
         }
         case TAIR_REQ_PREFIX_HIDES_PACKET:
         {
           request_prefix_hides *hpacket = (request_prefix_hides*)packet;
           ret = req_processor->process(hpacket, send_return);
           break;
         }
         case TAIR_REQ_PREFIX_GET_HIDDENS_PACKET:
         {
           request_prefix_get_hiddens *ipacket = (request_prefix_get_hiddens*)packet;
           ret = req_processor->process(ipacket, send_return);
           break;
         }
         default:
         {
            ret = EXIT_FAILURE;
            log_warn("unknow packet, pcode: %d", pcode);
         }
      }

      if (ret == TAIR_RETURN_PROXYED )//||TAIR_DUP_WAIT_RSP==ret || TAIR_RETURN_DUPLICATE_BUSY== ret)
	  {
		// request is proxyed
		//or wait dup_response,don't rsp to client unlit dup_rsp arrive or timeout.
		return false;
	  }
      if (TAIR_DUP_WAIT_RSP==ret )
	  {
		send_return =false;
		return true;
	  }

      if (send_return && packet->get_direction() == DIRECTION_RECEIVE) {
         log_debug("send return packet, return code: %d", ret);
         tair_packet_factory::set_return_packet(packet, ret, msg, heartbeat.get_client_version());
      }

      if ((TBSYS_LOG_LEVEL_DEBUG<=TBSYS_LOGGER._level)) {
         int64_t now = tbsys::CTimeUtil::getTime();
         if (packet->get_direction() == DIRECTION_RECEIVE && now-packet->request_time>100000LL) {
            log_warn("Slow, pcode: %d, %ld us", pcode, now-packet->request_time);
         }
      }

      return true;
   }

   tbnet::IPacketHandler::HPRetCode tair_server::handlePacket(tbnet::Packet *packet, void *args)
   {
      base_packet *resp = (base_packet*)packet;
      base_packet *req = (base_packet*)args;
      if (!packet->isRegularPacket()) {
         tbnet::ControlPacket *cp = (tbnet::ControlPacket*)packet;
         log_warn("ControlPacket, cmd:%d", cp->getCommand());
         if (cp->getCommand() == tbnet::ControlPacket::CMD_DISCONN_PACKET) {
            return tbnet::IPacketHandler::FREE_CHANNEL;
         }
         resp = NULL;
      }

      if (req != NULL) {
         if (resp == NULL) {
            if (req->getPCode() == TAIR_REQ_GET_PACKET) {
               response_get *p = new response_get();
               p->config_version = heartbeat.get_client_version();
               resp = p;
            } else {
               response_return *p = new response_return();
               p->config_version = heartbeat.get_client_version();
               resp = p;
            }
         }
         resp->setChannelId(req->getChannelId());
         if (req->get_connection()->postPacket(resp) == false) {
            delete resp;
         }

         if ((TBSYS_LOG_LEVEL_DEBUG<=TBSYS_LOGGER._level)) {
            int64_t now = tbsys::CTimeUtil::getTime();
            if (now-req->request_time>100000LL) {
               log_warn("Slow, migrate, pcode: %d, %ld us", req->getPCode(), now-req->request_time);
            }
         }
         delete req;
      } else if (packet != NULL) {
         packet->free();
      }
      return tbnet::IPacketHandler::KEEP_CHANNEL;
   }


} // namespace end

////////////////////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////////////////////
tair::tair_server *tair_server = NULL;
uint64_t tair::common::local_server_ip::ip = 0;
tbsys::CThreadMutex mutex;

void sign_handler(int sig)
{
   switch (sig) {
      case SIGTERM:
      case SIGINT:
         if (tair_server != NULL) {
            log_info("will stop tairserver");
            tair_server->stop();
         }
         break;
      case 40:
         TBSYS_LOGGER.checkFile();
         break;
      case 41:
      case 42:
         if (sig==41) {
            TBSYS_LOGGER._level ++;
         } else {
            TBSYS_LOGGER._level --;
         }
         log_error("TBSYS_LOGGER._level: %d", TBSYS_LOGGER._level);
         break;
      case 43:
         PROFILER_SET_STATUS((tbsys::util::Profiler::m_profiler.status + 1) % 2);
         log_error("profiler is %s", (tbsys::util::Profiler::m_profiler.status == 1) ? "enabled" : "disabled");
         break;
      case 44: //remove expired
         mutex.lock();
         if (tair_server != NULL) {
           tair_server->get_tair_manager()->clear(-1);
         }
         mutex.unlock();
         break;
      case 45: //balance
         mutex.lock();
         if (tair_server != NULL) {
           tair_server->get_tair_manager()->clear(-2);
         }
         mutex.unlock();
         break;
      case 46: //clear all
         mutex.lock();
         if (tair_server != NULL){
           tair_server->get_tair_manager()->clear(-3);
         }
      default:
         log_error("sig: %d", sig);
   }
}

void print_usage(char *prog_name)
{
   fprintf(stderr, "%s -f config_file\n"
           "    -f, --config_file  config file name\n"
           "    -h, --help         display this help and exit\n"
           "    -V, --version      version and build time\n\n",
           prog_name);
}

char *parse_cmd_line(int argc, char *const argv[])
{
   int opt;
   const char *opt_string = "hVf:";
   struct option long_opts[] = {
      {"config_file", 1, NULL, 'f'},
      {"help", 0, NULL, 'h'},
      {"version", 0, NULL, 'V'},
      {0, 0, 0, 0}
   };

   char *config_file = NULL;
   while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
      switch (opt) {
         case 'f':
            config_file = optarg;
            break;
         case 'V':
            fprintf(stderr, "BUILD_TIME: %s %s\n", __DATE__, __TIME__);
            return NULL;
         case 'h':
            print_usage(argv[0]);
            return NULL;
      }
   }
   return config_file;
}

int main(int argc, char *argv[])
{
   // parse cmd
   char *config_file = parse_cmd_line(argc, argv);
   if (config_file == NULL) {
      print_usage(argv[0]);
      return EXIT_FAILURE;
   }

   char *pathName=getenv("TAIR_HOME");
   if(pathName!=NULL)
   {
     strncpy(g_tair_home,pathName,128);
   }
   else
   {
#ifdef TAIR_DEBUG
     strcpy(g_tair_home,"./");
#else
     strcpy(g_tair_home,"/home/admin/tair_bin/");
#endif
   }


   if (TBSYS_CONFIG.load(config_file)) {
      fprintf(stderr, "load config file (%s) failed\n", config_file);
      return EXIT_FAILURE;
   }

   int pid;
   const char *pid_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_PID_FILE, "server.pid");
   const char *log_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_LOG_FILE, "server.log");
   if (1) {
      char *p, dir_path[256];
      sprintf(dir_path, "%s", pid_file_name);
      p = strrchr(dir_path, '/');
      if (p != NULL) *p = '\0';
      if (p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
         fprintf(stderr, "mkdir failed: %s\n", dir_path);
         return EXIT_FAILURE;
      }
      sprintf(dir_path, "%s", log_file_name);
      p = strrchr(dir_path, '/');
      if (p != NULL) *p = '\0';
      if (p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
         fprintf(stderr, "mkdir failed: %s\n", dir_path);
         return EXIT_FAILURE;
      }
      const char *se_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_SENGINE, NULL);
      if (se_name == NULL || (strcmp(se_name, "mdb") == 0)) {
      } else if (strcmp(se_name, "fdb") == 0){
         sprintf(dir_path, "%s", TBSYS_CONFIG.getString(TAIRFDB_SECTION, FDB_DATA_DIR, FDB_DEFAULT_DATA_DIR));
      } else if (strcmp(se_name, "kdb") == 0){
         sprintf(dir_path, "%s", TBSYS_CONFIG.getString(TAIRKDB_SECTION, KDB_DATA_DIR, KDB_DEFAULT_DATA_DIR));
      }

      if(!tbsys::CFileUtil::mkdirs(dir_path)) {
         fprintf(stderr, "mkdir failed: %s\n", dir_path);
         return EXIT_FAILURE;
      }
      const char *dump_path = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_DUMP_DIR, TAIR_DEFAULT_DUMP_DIR);
      sprintf(dir_path, "%s", dump_path);
      if (!tbsys::CFileUtil::mkdirs(dir_path)) {
         fprintf(stderr, "create dump directory {%s} failed.", dir_path);
         return EXIT_FAILURE;
      }

   }

   if ((pid = tbsys::CProcess::existPid(pid_file_name))) {
      fprintf(stderr, "tair_server already running: pid=%d\n", pid);
      return EXIT_FAILURE;
   }

   const char *log_level = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_LOG_LEVEL, "info");
   TBSYS_LOGGER.setLogLevel(log_level);

   // disable profiler by default
   PROFILER_SET_STATUS(0);
   // set the threshold
   int32_t profiler_threshold = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PROFILER_THRESHOLD, 10000);
   PROFILER_SET_THRESHOLD(profiler_threshold);

   if (tbsys::CProcess::startDaemon(pid_file_name, log_file_name) == 0) {
      for (int i=0; i<64; i++) {
         if (i==9 || i==SIGINT || i==SIGTERM || i==40) continue;
         signal(i, SIG_IGN);
      }
      signal(SIGINT, sign_handler);
      signal(SIGTERM, sign_handler);
      signal(40, sign_handler);
      signal(41, sign_handler);
      signal(42, sign_handler);
      signal(43, sign_handler); // for switch profiler enable/disable status
      signal(44, sign_handler); // remove expired item
      signal(45, sign_handler); // remove all item

      log_error("profiler disabled by default, threshold has been set to %d", profiler_threshold);

      tair_server = new tair::tair_server();
      tair_server->start();

      // ignore signal when destroy, cause sig_handler may use tair_server between delete and set it to NULL.
      signal(SIGINT, SIG_IGN);
      signal(SIGTERM, SIG_IGN);

      delete tair_server;
      tair_server = NULL;

      log_info("tair_server exit.");
   }

   return EXIT_SUCCESS;
}
////////////////////////////////END
