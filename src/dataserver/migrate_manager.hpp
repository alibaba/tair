/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * migrate manager
 *
 * Version: $Id$
 *
 * Authors:
 *   fanggang <fanggang@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_MIGRATE_HPP
#define TAIR_MIGRATE_HPP

#include <stdio.h>
#include <vector>
#include <list>
#include <unistd.h>
#include <ext/hash_map>
#include <tbsys.h>
#include <tbnet.h>
#include "wait_object.hpp"
#include "data_file_reader.hpp"
#include "heartbeat_thread.hpp"
#include "duplicate_base.hpp"
#include "update_log.hpp"
#include "tair_manager.hpp"
#include "storage_manager.hpp"
#include "base_migrate.hpp"
#include "mupdate_packet.hpp"
#include "response_return_packet.hpp"
#include "migrate_finish_packet.hpp"

namespace tair {
   using namespace std;
   using namespace tair::storage::fdb;
   const static uint MIGRATE_LOCK_LOG_LEN = 1 * 1024 * 1024;
   const static uint MUPDATE_PACKET_HEADER_LEN = 8 + 4;
   const static int MISECONDS_WAITED_FOR_WRITE = 500000;
   class heartbeat_thread;

   class migrate_manager: public base_migrate, public tbsys::CDefaultRunnable, public tbnet::IPacketHandler {
   public:
      migrate_manager(tbnet::Transport *transport, tair_packet_streamer *streamer, base_duplicator *this_duplicator, tair_manager *tair_mgr, tair::storage::storage_manager *storage_mgr);
      ~migrate_manager();
      void set_log(update_log *log);
      void set_migrate_server_list(bucket_server_map migrate_server_list, uint32_t version);
      void run(tbsys::CThread *thread, void *arg);
      void do_run();
      void do_migrate();
      void do_migrate_one_bucket(int bucket_number, vector<uint64_t> dest_servers);
      void do_server_list_changed();
      void signal();
      void stop();
      bool is_bucket_available(int bucket_number);
      bool is_bucket_migrating(int bucket_number);
      bool is_migrating();
      vector<uint64_t> get_migrated_buckets();
      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet *packet, void *args);


   private:
      bool migrate_data_file(int bucket_number, vector<uint64_t> dest_servers);
      bool migrate_log(int bucket_number, vector<uint64_t> dest_servers, lsn_type start_lsn, lsn_type end_lsn);

      bool send_packet(vector<uint64_t> dest_servers, request_mupdate *packet, int db_id);
      void finish_migrate_bucket(int bucket_number);
      bucket_server_map migrate_servers;
      bucket_server_map temp_servers;
      vector<uint64_t> migrated_buckets;
      tbsys::CThreadMutex get_data_mutex;
      tbsys::CThreadMutex get_migrating_buckets_mutex;
      volatile int current_locked_bucket;
      uint32_t version;
      volatile int current_migrating_bucket;
      base_duplicator *duplicator;
      tair_manager *tair_mgr;
      tair::storage::storage_manager *storage_mgr;
      uint timeout;
      vector<uint64_t> config_server_list;

      tbnet::ConnectionManager* conn_mgr;
      wait_object_manager wait_object_mgr;

      update_log *log;

      tbsys::CThreadCond        condition;
      bool      is_stopped;
      int       is_running;
      bool      is_signaled;
      bool is_alive;
      tbsys::CThreadMutex mutex;
   };
}
#endif
