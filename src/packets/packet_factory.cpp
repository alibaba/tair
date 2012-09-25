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
#include "flow_control_packet.hpp"
#include "lock_packet.hpp"
#include "query_info_packet.hpp"
#include "remove_area_packet.hpp"
#include "remove_packet.hpp"
#include "response_return_packet.hpp"
#include "server_hash_table_packet.hpp"
#include "set_master_packet.hpp"
#include "invalid_packet.hpp"
#include "hide_packet.hpp"
#include "hide_by_proxy_packet.hpp"
#include "get_hidden_packet.hpp"
#include "stat_cmd_packet.hpp"
#include "flow_view.hpp"
#include "get_range_packet.hpp"
#include "op_cmd_packet.hpp"
#include "expire_packet.hpp"

#include "prefix_puts_packet.hpp"
#include "prefix_incdec_packet.hpp"
#include "prefix_removes_packet.hpp"
#include "prefix_gets_packet.hpp"
#include "response_mreturn_packet.hpp"
#include "prefix_hides_packet.hpp"
#include "prefix_invalids_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "prefix_get_hiddens_packet.hpp"

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
        case TAIR_REQ_MPUT_PACKET:
          packet = new tair::request_mput();
          break;
        case TAIR_REQ_OP_CMD_PACKET:
          packet = new tair::request_op_cmd();
          break;
        case TAIR_RESP_OP_CMD_PACKET:
          packet = new response_op_cmd();
          break;
        case TAIR_REQ_GET_PACKET:
          packet = new request_get();
          break;
        case TAIR_REQ_HIDE_PACKET:
          packet = new request_hide();
          break;
        case TAIR_REQ_HIDE_BY_PROXY_PACKET:
          packet = new request_hide_by_proxy();
          break;
        case TAIR_REQ_GET_HIDDEN_PACKET:
          packet = new request_get_hidden();
          break;
        case TAIR_REQ_INVAL_PACKET:
          packet = new request_invalid();
          break;
        case TAIR_REQ_LOCK_PACKET:
          packet = new request_lock();
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
        case TAIR_STAT_CMD_VIEW:
          packet = new stat_cmd_view_packet();
          break;
        case TAIR_FLOW_CONTROL:
          packet = new flow_control();
          break;
        case TAIR_FLOW_CHECK:
          packet = new flow_check();
          break;
        case TAIR_FLOW_CONTROL_SET:
          packet = new flow_control_set();
          break;
        case TAIR_REQ_FLOW_VIEW:
          packet = new flow_view_request();
          break;
        case TAIR_RESP_FLOW_VIEW:
          packet = new flow_view_response();
          break;
        case TAIR_REQ_PREFIX_PUTS_PACKET:
          packet = new request_prefix_puts();
          break;
        case TAIR_REQ_PREFIX_REMOVES_PACKET:
          packet = new request_prefix_removes();
          break;
        case TAIR_REQ_PREFIX_INCDEC_PACKET:
          packet = new request_prefix_incdec();
          break;
        case TAIR_RESP_PREFIX_INCDEC_PACKET:
          packet = new response_prefix_incdec();
          break;
        case TAIR_REQ_PREFIX_GETS_PACKET:
          packet = new request_prefix_gets();
          break;
        case TAIR_RESP_PREFIX_GETS_PACKET:
          packet = new response_prefix_gets();
          break;
        case TAIR_RESP_MRETURN_PACKET:
          packet = new response_mreturn();
          break;
        case TAIR_RESP_MRETURN_DUP_PACKET:
          packet = new response_mreturn_dup();
          break;
        case TAIR_REQ_PREFIX_HIDES_PACKET:
          packet = new request_prefix_hides();
          break;
        case TAIR_REQ_PREFIX_INVALIDS_PACKET:
          packet = new request_prefix_invalids();
          break;
        case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
          packet = new request_prefix_hides_by_proxy();
          break;
        case TAIR_REQ_PREFIX_GET_HIDDENS_PACKET:
          packet = new request_prefix_get_hiddens();
          break;
        case TAIR_REQ_EXPIRE_PACKET:
          packet = new request_expire();
          break;
         case TAIR_REQ_GET_RANGE_PACKET:
            packet = new request_get_range();
            break;
         case TAIR_RESP_GET_RANGE_PACKET:
            packet = new response_get_range();
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

    int tair_packet_factory::set_return_packet(base_packet *packet, int code,const char *msg, uint32_t version, uint32_t &resp_size)
    {
      response_return *return_packet = new response_return(packet->getChannelId(), code, msg);
      return_packet->config_version = version;
      tbnet::Connection *conn=packet->get_connection();
      resp_size = return_packet->size() + 16;
      if (!conn || conn->postPacket(return_packet) == false) {
        log_warn("ReturnPacket fail, req: %d, code: %d, dst: %s", packet->getPCode(), code,
                 conn != NULL ? tbsys::CNetUtil::addrToString(conn->getServerId()).c_str() : "null");
        delete return_packet;
        resp_size = 0;
      }       
      return EXIT_SUCCESS;
    }
    
    int tair_packet_factory::set_return_packet(tbnet::Connection *conn,uint32_t chid,int cmd_id,
        int code,const char *msg,uint32_t version)
    {
      response_return *return_packet = new response_return(chid,code, msg);
      return_packet->config_version = version;
      if (!conn || conn->postPacket(return_packet) == false)
      {
        log_warn("ReturnPacket fail, req: %d, code: %d, dst: %s", cmd_id, code,
                 conn != NULL ? tbsys::CNetUtil::addrToString(conn->getServerId()).c_str() : "null");
        delete return_packet;
      }
      return EXIT_SUCCESS;
    }
  }

