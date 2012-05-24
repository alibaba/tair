/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * packet code and base packet are defined here
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKETS_BASE_H
#define TAIR_PACKETS_BASE_H
#include <string>
#include <map>
#include <set>
#include <vector>
#include <tbsys.h>
#include <tbnet.h>
#include <stdint.h>
#include <zlib.h>
#include "define.hpp"
#include "data_entry.hpp"
#include "util.hpp"
#include "log.hpp"

using namespace std;
namespace tair {
   using namespace common;

   enum {
      TAIR_SIMPLE_STREAM_PACKET = 0,
      TAIR_REQ_PUT_PACKET = 1,
      TAIR_REQ_GET_PACKET,
      TAIR_REQ_REMOVE_PACKET,
      TAIR_REQ_REMOVE_AREA_PACKET,
      TAIR_REQ_STAT_PACKET,
      TAIR_REQ_PING_PACKET,
      TAIR_REQ_DUMP_PACKET,
      TAIR_REQ_PARAM_PACKET,
      TAIR_REQ_HEARTBEAT_PACKET,
      TAIR_REQ_INCDEC_PACKET = 11,
      TAIR_REQ_MUPDATE_PACKET = 13,
      TAIR_REQ_LOCK_PACKET = 14,

      TAIR_REQ_HIDE_PACKET = 20,
      TAIR_REQ_HIDE_BY_PROXY_PACKET = 21,
      TAIR_REQ_GET_HIDDEN_PACKET = 22,
      TAIR_REQ_INVAL_PACKET = 23,

      TAIR_REQ_PREFIX_PUTS_PACKET = 24,
      TAIR_REQ_PREFIX_REMOVES_PACKET = 25,
      TAIR_REQ_PREFIX_INCDEC_PACKET = 26,
      TAIR_RESP_PREFIX_INCDEC_PACKET = 27,
      TAIR_RESP_MRETURN_PACKET = 28,
      TAIR_REQ_PREFIX_GETS_PACKET = 29,
      TAIR_RESP_PREFIX_GETS_PACKET = 30,
      TAIR_REQ_PREFIX_HIDES_PACKET = 31,
      TAIR_REQ_PREFIX_INVALIDS_PACKET = 32,
      TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET = 33,
      TAIR_REQ_PREFIX_GET_HIDDENS_PACKET = 34,

      TAIR_RESP_RETURN_PACKET = 101,
      TAIR_RESP_GET_PACKET,
      TAIR_RESP_STAT_PACKET,
      TAIR_RESP_HEARTBEAT_PACKET,
      TAIR_RESP_INCDEC_PACKET,

      TAIR_REQ_GET_GROUP_PACKET   =1002,
      TAIR_REQ_GET_SVRTAB_PACKET  =1003,
      TAIR_REQ_CONFHB_PACKET      =1004,
      TAIR_REQ_SETMASTER_PACKET   =1005,
      TAIR_REQ_GROUP_NAMES_PACKET =1006,
      TAIR_REQ_QUERY_INFO_PACKET  =1009,

      TAIR_RESP_GET_GROUP_PACKET  =1102,
      TAIR_RESP_GET_SVRTAB_PACKET =1103,
      TAIR_RESP_GROUP_NAMES_PACKET=1104,
      TAIR_RESP_QUERY_INFO_PACKET =1106,

      TAIR_REQ_DUMP_BUCKET_PACKET = 1200,
      TAIR_REQ_MIG_FINISH_PACKET,

      TAIR_REQ_DUPLICATE_PACKET = 1300,
      TAIR_RESP_DUPLICATE_PACKET,
      TAIR_RESP_GET_MIGRATE_MACHINE_PACKET,

      //items
      TAIR_REQ_ADDITEMS_PACKET = 1400,
      TAIR_REQ_GETITEMS_PACKET,
      TAIR_REQ_REMOVEITEMS_PACKET,
      TAIR_REQ_GETANDREMOVEITEMS_PACKET,
      TAIR_REQ_GETITEMSCOUNT_PACKET,
      TAIR_RESP_GETITEMS_PACKET,

      TAIR_REQ_DATASERVER_CTRL_PACKET = 1500,
      TAIR_REQ_GET_MIGRATE_MACHINE_PACKET,

      TAIR_STAT_CMD_VIEW      = 9000,
      TAIR_FLOW_CONTROL       = 9001,
      TAIR_FLOW_CONTROL_SET   = 9002,
      TAIR_REQ_FLOW_VIEW      = 9003,
      TAIR_RESP_FLOW_VIEW     = 9004,
      TAIR_FLOW_CHECK         = 9005,
   };

   enum {
      DIRECTION_RECEIVE = 1,
      DIRECTION_SEND
   };

   enum {
      TAIR_STAT_TOTAL = 1,
      TAIR_STAT_SLAB = 2,
      TAIR_STAT_HASH = 3,
      TAIR_STAT_AREA = 4,
      TAIR_STAT_GET_MAXAREA = 5,
      TAIR_STAT_ONEHOST = 256
   };

   class base_packet : public tbnet::Packet {
   public:
      base_packet()
      {
         connection = NULL;
         direction = DIRECTION_SEND;
         no_free = false;
         server_flag = 0;
         request_time = 0;
         fixed_size = 0;
      }

      virtual size_t size()
      {
        return 0;
      }

      virtual uint16_t ns()
      {
        return 0;
      }

      virtual ~base_packet()
      {
      }

      // Connection
      tbnet::Connection *get_connection()
      {
         return connection;
      }

      // connection
      void set_connection(tbnet::Connection *connection)
      {
         this->connection = connection;
      }

      // direction
      void set_direction(int direction)
      {
         this->direction = direction;
      }

      // direction
      int get_direction()
      {
         return direction;
      }

      void free()
      {
         if (!no_free) {
            delete this;
         }
      }

      void set_no_free()
      {
         no_free = true;
      }

   private:
      base_packet& operator = (const base_packet&);

      tbnet::Connection *connection;
      int direction;
      bool no_free;
   public:
      uint8_t server_flag;
      int64_t request_time;
   protected:
      size_t fixed_size;
   };

}
#endif
