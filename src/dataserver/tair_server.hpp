/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * tair_server daemon
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_SERVER_H
#define TAIR_SERVER_H

#include <string>
#include <ext/hash_map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>

#include <tbsys.h>
#include <tbnet.h>

#include "packet_factory.hpp"
#include "remove_area_packet.hpp"
#include "response_return_packet.hpp"
#include "dump_packet.hpp"
#include "heartbeat_thread.hpp"
#include "tair_manager.hpp"
#include "wait_object.hpp"
#include "request_processor.hpp"
#include "stat_helper.hpp"

using namespace __gnu_cxx;

namespace tair {

   class tair_server : public tbnet::IServerAdapter, public tbnet::IPacketQueueHandler, public tbnet::IPacketHandler {
   public:
      tair_server();
      ~tair_server();
      void start();
      void stop();
      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
      bool handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue);
      bool handlePacketQueue(tbnet::Packet *packet, void *args);
      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet *packet, void *args);

   private:
      int is_stoped;
      tair_packet_factory factory;
      tbnet::DefaultPacketStreamer streamer;

      tbnet::Transport transport;
      tbnet::ConnectionManager *conn_manager;

      tbnet::PacketQueueThread task_queue_thread;
      tbnet::PacketQueueThread duplicate_task_queue_thread;
      tbnet::PacketQueueThread async_task_queue_thread;
      tair_manager *tair_mgr;
      heartbeat_thread heartbeat;
      int thread_count;
      int task_queue_size;
      wait_object_manager wait_object_mgr;
      request_processor *req_processor;

   private:
      inline bool initialize();
      inline bool destroy();
   };
}

#endif
///////////////////////END
