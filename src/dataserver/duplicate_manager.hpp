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
#ifndef DUPLICATE_MANAGER_H
#define DUPLICATE_MANAGER_H
#include <tbsys.h>
#include <tbnet.h>
#include "table_manager.hpp"
#include "boost/shared_ptr.hpp"
#include "duplicate_base.hpp"
#include "duplicate_packet.hpp"
#include <ext/hash_map>
#include <queue>
#include <map>
namespace tair {
   using namespace std;
   class duplicate_sender_manager;
   class bucket_waiting_queue {
   public:
      typedef boost::shared_ptr<request_duplicate> request_duplicate_packet;
      typedef queue<request_duplicate_packet> packets_queue_type;

      bucket_waiting_queue(duplicate_sender_manager* psm, uint32_t bucket_number);

      void push(request_duplicate_packet& p_packet, uint64_t server_id);
      bool pop(uint32_t packet_id, uint64_t server_id);
      bool send(int64_t now);
      size_t size() ;
      uint64_t get_bucket_number() const {
         return bucket_number;
      }
      void do_server_table_change(const set<uint64_t>&);
      ~bucket_waiting_queue();
   private:
      void pop(uint64_t server_id);
   private:
      uint32_t bucket_number;
      tbsys::CThreadMutex mutex;
      map<uint64_t, packets_queue_type> packets_queue;
      map<uint64_t, int64_t> last_send_time;  // will set this to 0 when receive the response
      duplicate_sender_manager* psm;
      atomic_t packet_id_creater;

   };
   class duplicate_sender_manager : public base_duplicator, public tbsys::CDefaultRunnable, public tbnet::IPacketHandler {
   public:

      duplicate_sender_manager( tbnet::Transport *transport,
                                tbnet::DefaultPacketStreamer *streamer, table_manager* table_mgr);
      ~duplicate_sender_manager();
      void do_hash_table_changed();
      void set_max_queue_size(uint32_t max_queue_size) {
         this->max_queue_size = max_queue_size;
      }

      bool is_bucket_available(uint32_t bucket_id);


      void duplicate_data(int area, const data_entry* key, const data_entry* value,
                          int bucket_number, const vector<uint64_t>& des_server_ids);

      bool has_bucket_duplicate_done(int bucket_number);

      void do_duplicate_response(uint32_t bucket_id, uint64_t d_server_id, uint32_t packet_id);

      void run(tbsys::CThread *thread, void *arg);

      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet *packet, void *args);

      friend class bucket_waiting_queue;
   private:
      table_manager* table_mgr;
      tbnet::ConnectionManager* conn_mgr;
      map<uint32_t, bucket_waiting_queue> packets_mgr;
      tbsys::CRWSimpleLock packages_mgr_mutex;
      volatile int have_data_to_send;
      uint32_t max_queue_size;
   };
}
#endif
