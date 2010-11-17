/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * packet factory creates packet according to packet code
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "packet_factory.hpp"
#include "base_packet.hpp"
#include "conf_heartbeat_packet.hpp"
#include "data_server_ctrl_packet.hpp"
#include "dump_bucket_packet.hpp"
#include "dump_packet.hpp"
#include "duplicate_packet.hpp"
#include "get_group_packet.hpp"
#include "get_migrate_machine.hpp"
#include "get_packet.hpp"
#include "get_server_table_packet.hpp"
#include "group_names_packet.hpp"
#include "heartbeat_packet.hpp"
#include "inc_dec_packet.hpp"
#include "items_packet.hpp"
#include "migrate_finish_packet.hpp"
#include "mupdate_packet.hpp"
#include "ping_packet.hpp"
#include "put_packet.hpp"
#include "query_info_packet.hpp"
#include "remove_area_packet.hpp"
#include "remove_packet.hpp"
#include "response_return_packet.hpp"
#include "server_hash_table_packet.hpp"
#include "set_master_packet.hpp"
namespace tair {
   tbnet::Packet *tair_packet_factory::createPacket(int pcode)
   {
      tbnet::Packet *packet = NULL;
      switch (pcode) {
         case TAIR_REQ_PUT_PACKET:
            packet = new tair::request_put();
            break;
         case TAIR_REQ_MUPDATE_PACKET:
            packet = new tair::request_mupdate();
            break;
         case TAIR_REQ_GET_PACKET:
            packet = new request_get();
            break;
         case TAIR_REQ_QUERY_INFO_PACKET:
            packet = new request_query_info();
            break;
         case TAIR_REQ_REMOVE_PACKET:
            packet = new request_remove();
            break;
         case TAIR_REQ_REMOVE_AREA_PACKET:
            packet = new request_remove_area();
            break;
         case TAIR_REQ_PING_PACKET:
            packet = new request_ping();
            break;
         case TAIR_REQ_DUMP_PACKET:
            packet = new request_dump();
            break;
         case TAIR_REQ_HEARTBEAT_PACKET:
            packet = new request_heartbeat();
            break;
         case TAIR_REQ_INCDEC_PACKET:
            packet = new request_inc_dec();
            break;
         case TAIR_RESP_RETURN_PACKET:
            packet = new response_return();
            break;
         case TAIR_RESP_GET_PACKET:
            packet = new response_get();
            break;
         case TAIR_RESP_QUERY_INFO_PACKET:
            packet = new response_query_info();
            break;
         case TAIR_RESP_HEARTBEAT_PACKET:
            packet = new response_heartbeat();
            break;
         case TAIR_RESP_INCDEC_PACKET:
            packet = new response_inc_dec();
            break;
         case TAIR_REQ_GET_GROUP_PACKET:
            packet = new request_get_group();
            break;
         case TAIR_REQ_GET_SVRTAB_PACKET:
            packet = new request_get_server_table();
            break;
         case TAIR_REQ_CONFHB_PACKET:
            packet = new request_conf_heartbeart();
            break;
         case TAIR_REQ_SETMASTER_PACKET:
            packet = new request_set_master();
            break;
         case TAIR_REQ_GROUP_NAMES_PACKET:
            packet = new request_group_names();
            break;
         case TAIR_RESP_GET_GROUP_PACKET:
            packet = new response_get_group();
            break;
         case TAIR_RESP_GET_SVRTAB_PACKET:
            packet = new response_get_server_table();
            break;
         case TAIR_RESP_GROUP_NAMES_PACKET:
            packet = new response_group_names();
            break;
         case TAIR_REQ_DUPLICATE_PACKET :
            packet = new request_duplicate();
            break;
         case TAIR_RESP_DUPLICATE_PACKET :
            packet = new response_duplicate();
            break;
         case TAIR_REQ_MIG_FINISH_PACKET:
            packet = new request_migrate_finish();
            break;
         case TAIR_REQ_ADDITEMS_PACKET:
            packet = new request_add_items();
            break;
         case TAIR_REQ_GETITEMS_PACKET:
            packet = new request_get_items();
            break;
         case TAIR_REQ_REMOVEITEMS_PACKET:
            packet = new request_remove_items();
            break;
         case TAIR_REQ_GETANDREMOVEITEMS_PACKET:
            packet = new request_get_and_remove_items();
            break;
         case TAIR_REQ_GETITEMSCOUNT_PACKET:
            packet = new request_get_items_count();
            break;
         case TAIR_RESP_GETITEMS_PACKET:
            packet = new response_get_items();
            break;
         case TAIR_REQ_DATASERVER_CTRL_PACKET:
            packet = new request_data_server_ctrl();
            break;
         case TAIR_REQ_GET_MIGRATE_MACHINE_PACKET:
            packet = new request_get_migrate_machine();
            break;
         case TAIR_RESP_GET_MIGRATE_MACHINE_PACKET:
            packet = new response_get_migrate_machine();
            break;
         default:
            log_error("createpacket error: pcode=%d", pcode);
            break;
      }
      if (packet) {
         assert(pcode == packet->getPCode());
      }
      return packet;
   }
   tair_packet_factory::~tair_packet_factory()
   {
   }

   int tair_packet_factory::set_return_packet(base_packet *packet, int code,const char *msg, uint32_t version)
   {
      response_return *return_packet = new response_return(packet->getChannelId(), code, msg);
      return_packet->config_version = version;
      if (packet->get_connection()->postPacket(return_packet) == false) {
         log_warn("send ReturnPacket failure, request pcode: %d", packet->getPCode());
         delete return_packet;
      }
      return EXIT_SUCCESS;
   }


}
