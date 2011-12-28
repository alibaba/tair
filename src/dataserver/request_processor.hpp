/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * request dispatcher header
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "data_entry.hpp"
#include "tair_manager.hpp"
#include "heartbeat_thread.hpp"

#include "put_packet.hpp"
#include "get_packet.hpp"
#include "remove_packet.hpp"
#include "inc_dec_packet.hpp"
#include "duplicate_packet.hpp"
#include "items_packet.hpp"
#include "mupdate_packet.hpp"
#include "remote_sync_manager.hpp"
#include "lock_packet.hpp"

namespace tair {
   class request_processor {
   public:
      request_processor(tair_manager *tair_mgr, heartbeat_thread *heart_beat, tbnet::ConnectionManager *connection_mgr,remote_sync_manager  *remote_mgr);
      ~request_processor();

      int process(request_put *request, bool &send_return);
      int process(request_get *request, bool &send_return);
      int process(request_remove *request, bool &send_return);
      int process(request_inc_dec *request, bool &send_return);
      int process(request_duplicate *request, bool &send_return);
      int process(request_add_items *request, bool &send_return);
      int process(request_get_items *request, bool &send_return);
      int process(request_remove_items *request, bool &send_return);
      int process(request_get_and_remove_items *request, bool &send_return);
      int process(request_get_items_count *request,bool &send_return);
      int process(request_mupdate *request,bool &send_return);
      int process(request_lock* request, bool &send_return);

   private:
      bool do_proxy(uint64_t target_server_id, base_packet *proxy_packet, base_packet *packet);
 
   private:
      tair_manager *tair_mgr;
      heartbeat_thread *heart_beat;
      tbnet::ConnectionManager *connection_mgr;
      remote_sync_manager  *remote_mgr;
   };
}
