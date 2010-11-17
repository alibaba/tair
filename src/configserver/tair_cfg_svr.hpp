/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef TAIR_CONFIG_SERVER_H
#define TAIR_CONFIG_SERVER_H
#include <string>
#include <ext/hash_map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <tbsys.h>
#include <tbnet.h>
#include "server_conf_thread.hpp"
#include "packet_factory.hpp"

using namespace std;
using namespace __gnu_cxx;

namespace tair {
  namespace config_server {
    class tair_config_server:public tbnet::IServerAdapter,
      public tbnet::IPacketQueueHandler {
    public:
      tair_config_server();
      ~tair_config_server();
      void start();
      void stop();
      // iserveradapter interface
        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *
                                                      connection,
                                                      tbnet::Packet * packet);
      // IPacketQueueHandler interface
      bool handlePacketQueue(tbnet::Packet * packet, void *args);

    private:
      int stop_flag;
      tair_packet_factory packet_factory;
        tbnet::DefaultPacketStreamer packet_streamer;
        tbnet::Transport packet_transport;
        tbnet::Transport heartbeat_transport;        //

        tbnet::PacketQueueThread task_queue_thread;
      server_conf_thread my_server_conf_thread;

    private:
        inline int initialize();
      inline int destroy();
    };
  }
}
#endif
///////////////////////END
