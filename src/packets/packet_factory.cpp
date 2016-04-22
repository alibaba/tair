/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#include "packet_factory.hpp"
#include "easy_helper.hpp"
#include "base_packet.hpp"
#include "admin_config_packet.hpp"
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
#include "get_group_non_down_dataserver_packet.hpp"
#include "heartbeat_packet.hpp"
#include "inc_dec_packet.hpp"
#include "migrate_finish_packet.hpp"
#include "mupdate_packet.hpp"
#include "ping_packet.hpp"
#include "simple_prefix_get_packet.hpp"
#include "put_packet.hpp"
#include "flow_control_packet.hpp"
#include "lock_packet.hpp"
#include "query_info_packet.hpp"
#include "remove_area_packet.hpp"
#include "remove_packet.hpp"
#include "uniq_remove_packet.hpp"
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
#include "sync_packet.hpp"
#include "expire_packet.hpp"
#include "statistics_packet.hpp"

#include "prefix_puts_packet.hpp"
#include "prefix_incdec_packet.hpp"
#include "prefix_removes_packet.hpp"
#include "prefix_gets_packet.hpp"
#include "response_mreturn_packet.hpp"
#include "prefix_hides_packet.hpp"
#include "prefix_invalids_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "prefix_get_hiddens_packet.hpp"

#include "retry_all_packet.hpp"
#include "inval_stat_packet.hpp"
#include "mc_ops_packet.hpp"
#include "inc_dec_bounded_packet.hpp"
#include "prefix_incdec_bounded_packet.hpp"
#include "recovery_finish_packet.hpp"
#include "rt_packet.hpp"

namespace tair {
easy_atomic32_t packet_factory::chid = 100;

void *packet_factory::decode_cb(easy_message_t *m) {
    if (m->input->last - m->input->pos < (int) (4 * sizeof(int))) {
        log_debug("packet header not complete");
        return NULL;
    }

    assert(m->pool->tlock <= 1 && m->pool->flags <= 1);
    DataBuffer input(m->input);
    int flag = input.readInt32();
    int chid = input.readInt32();
    int pcode = input.readInt32();
    int len = input.readInt32();
    if (flag != TAIR_PACKET_FLAG || len < 0 || len > (1 << 26) /* 64M */) {
        log_error("decoding failed: flag=%d, len=%d, m->status=%d, pcode=%d",
                  flag, len, m->status, pcode);
        m->status = EASY_ERROR;
        return NULL;
    }
    // set one packet end offset
    // end postion = now postion + len
    input.setLastReadMark(len);

    if (m->input->last - m->input->pos < len) {
        //~ data not enough
        log_debug("data in buffer not enough for a whole packet: data_len=%d, buf_len=%lu, pcode=%d",
                  len, m->input->last - m->input->pos, pcode);
        m->next_read_len = len - (m->input->last - m->input->pos);
        m->input->pos -= 4 * sizeof(int); //~ restore buffer
        return NULL;
    }

    base_packet *bp = create_packet(pcode);
    if (bp == NULL) {
        m->status = EASY_ERROR;
        log_error("create packet failed: pcode=%d", pcode);
        return NULL;
    }
    bp->_packetHeader._chid = chid;
    bp->_packetHeader._pcode = pcode;
    bp->_packetHeader._dataLen = len;
    bp->peer_id = easy_helper::convert_addr(m->c->addr);
    if (bp->_packetHeader._pcode == TAIR_FLOW_CONTROL) {
        flow_control *control = dynamic_cast<flow_control *>(bp);
        control->set_user_data(m->c->handler->user_data);
        control->set_addr(m->c->addr);
    }

    uint8_t *start = (uint8_t *) m->input->pos;
    if (!bp->decode(&input, &bp->_packetHeader)) {
        log_error("decoding packet failed: pcode=%d", pcode);
        delete bp;
        m->status = EASY_ERROR;
        return NULL;
    }

    if (start + len != (uint8_t *) m->input->pos) {
        m->status = EASY_ERROR;
        delete bp;
        log_error("len not match");
        return NULL;
    }

    assert(m->pool->tlock <= 1 && m->pool->flags <= 1);
    return bp;
}

int packet_factory::encode_cb(easy_request_t *r, void *packet) {
    base_packet *bp = (base_packet *) packet;
    bool flip = true; //! only should client send one packet one time
    while (bp != NULL) {
        if (!flip) {
            log_warn("not allowed for client to send multiple packets once");
            break;
        }
        if (r->ms->c->type == EASY_TYPE_CLIENT) {
            bp->_packetHeader._chid = (uint32_t) ((easy_session_t *) r->ms)->packet_id;
            flip = false;
        } else if (r->ms->c->type == EASY_TYPE_SERVER) {
            if (((easy_message_t *) r->ms)->status == EASY_MESG_DESTROY) {
                return EASY_ERROR;
            }
            if (r->retcode == EASY_ABORT) {
                r->ms->c->pool->ref--;
                easy_atomic_dec(&r->ms->pool->ref);
                r->retcode = EASY_OK;
            }
        }

        easy_list_t list;
        easy_list_init(&list);
        DataBuffer output(r->ms->pool, &list);

        output.reserve(bp->size() + sizeof(bp->_packetHeader));

        output.writeInt32(TAIR_PACKET_FLAG);
        output.writeInt32(bp->_packetHeader._chid);
        output.writeInt32(bp->_packetHeader._pcode);

        output.writeInt32(0); //~ placeholder for data length
        easy_buf_t *b = easy_list_get_last(&list, easy_buf_t, node);
        uint32_t *len_pos = (uint32_t *) (b->last - 4);

        if (!bp->encode(&output)) {
            return EASY_ERROR;
        }

        int len = 0;
        b = NULL;
        easy_list_for_each_entry(b, &list, node)
        {
            len += b->last - b->pos;
        }
        len -= TAIR_PACKET_HEADER_SIZE;
        *len_pos = bswap_32(len);

        easy_request_addbuf_list(r, &list); //~ add buffers into `request'
        bp = (base_packet *) bp->_next;
    }
    return EASY_OK;
}

uint64_t packet_factory::get_packet_id_cb(easy_connection_t *c, void *packet) {
    int32_t packet_id = 0;
    if (packet != NULL) {
        base_packet *bp = (base_packet *) packet;
        packet_id = bp->getChannelId();
    }
    if (packet_id == 0) {
        while (packet_id == 0 || packet_id == -1) {
            packet_id = (int32_t) easy_atomic32_add_return(&chid, 1);
        }
    }
    return packet_id;
}

base_packet *packet_factory::create_packet(int pcode) {
    base_packet *packet = NULL;
    bool control_cmd = false;
    switch (pcode) {
        case TAIR_REQ_SIMPLE_GET_PACKET:
            packet = new tair::request_simple_get();
            break;
        case TAIR_RESP_SIMPLE_GET_PACKET:
            packet = new tair::response_simple_get();
            break;
        case TAIR_REQ_PUT_PACKET:
            packet = new tair::request_put();
            break;
        case TAIR_REQ_MUPDATE_PACKET:
            packet = new tair::request_mupdate();
            break;
        case TAIR_REQ_MPUT_PACKET:
            packet = new tair::request_mput();
            break;
        case TAIR_REQ_SYNC_PACKET:
            packet = new tair::request_sync();
            break;
        case TAIR_REQ_OP_CMD_PACKET:
            packet = new tair::request_op_cmd();
            control_cmd = true;
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
        case TAIR_REQ_UNIQ_REMOVE_PACKET:
            packet = new request_uniq_remove();
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
        case TAIR_REQ_GET_GROUP_NON_DOWN_DATASERVER_PACKET:
            packet = new request_get_group_not_down_dataserver();
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
        case TAIR_RESP_GET_GROUP_NON_DOWN_DATASERVER_PACKET:
            packet = new response_get_group_non_down_dataserver();
            break;
        case TAIR_REQ_BATCH_DUPLICATE_PACKET:
            packet = new request_batch_duplicate();
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
        case TAIR_REQ_REC_FINISH_PACKET:
            packet = new request_recovery_finish();
            break;
        case TAIR_REQ_DATASERVER_CTRL_PACKET:
            packet = new request_data_server_ctrl();
            break;
        case TAIR_RESP_DATASERVER_CTRL_PACKET:
            packet = new response_data_server_ctrl();
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
            control_cmd = true;
            break;
        case TAIR_FLOW_CHECK:
            packet = new flow_check();
            control_cmd = true;
            break;
        case TAIR_FLOW_CONTROL_SET:
            packet = new flow_control_set();
            control_cmd = true;
            break;
        case TAIR_REQ_FLOW_VIEW:
            packet = new flow_view_request();
            control_cmd = true;
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
        case TAIR_REQ_RETRY_ALL_PACKET:
            packet = new request_retry_all();
            break;
        case TAIR_REQ_INVAL_STAT_PACKET:
            packet = new request_inval_stat();
            break;
        case TAIR_RESP_INVAL_STAT_PACKET:
            packet = new response_inval_stat();
            break;
        case TAIR_REQ_MC_OPS_PACKET:
            packet = new request_mc_ops();
            break;
        case TAIR_RESP_MC_OPS_PACKET:
            packet = new response_mc_ops();
            break;
        case TAIR_REQ_INCDEC_BOUNDED_PACKET:
            packet = new request_inc_dec_bounded();
            break;
        case TAIR_RESP_INCDEC_BOUNDED_PACKET:
            packet = new response_inc_dec_bounded();
            break;
        case TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET:
            packet = new request_prefix_incdec_bounded();
            break;
        case TAIR_RESP_PREFIX_INCDEC_BOUNDED_PACKET:
            packet = new response_prefix_incdec_bounded();
            break;
        case TAIR_REQ_STATISTICS_PACKET:
            packet = new request_statistics();
            break;
        case TAIR_RESP_STATISTICS_PACKET:
            packet = new response_statistics();
            break;
        case TAIR_REQ_RT_PACKET:
            packet = new request_rt;
            break;
        case TAIR_RESP_RT_PACKET:
            packet = new response_rt;
            break;
        case TAIR_REQ_ADMIN_CONFIG_PACKET:
            packet = new request_admin_config();
            break;
        default:
            log_error("create packet failed: pcode=%d", pcode);
            break;
    }
    if (packet != NULL) {
        assert(pcode == packet->getPCode());
    }
    if (control_cmd) {
        ((base_packet *) packet)->control_cmd = true;
    }
    return packet;
}

base_packet *packet_factory::create_dup_packet(base_packet *ipacket) {
    int pcode = ipacket->getPCode();
    base_packet *bp = NULL;
    switch (pcode) {
        case TAIR_REQ_PREFIX_PUTS_PACKET:
            bp = new request_prefix_puts(*(request_prefix_puts *) ipacket);
            break;
        case TAIR_REQ_PREFIX_REMOVES_PACKET:
            bp = new request_prefix_removes(*(request_prefix_removes *) ipacket);
            break;
        case TAIR_REQ_PREFIX_HIDES_PACKET:
            bp = new request_prefix_hides(*(request_prefix_hides *) ipacket);
            break;
        case TAIR_REQ_PREFIX_INCDEC_PACKET:
            bp = new request_prefix_incdec(*(request_prefix_incdec *) ipacket);
            break;
        case TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET :
            bp = new request_prefix_incdec_bounded(*(request_prefix_incdec_bounded *) ipacket);
            break;
        case TAIR_REQ_MC_OPS_PACKET:
            bp = new request_mc_ops(*(request_mc_ops *) ipacket);
            break;
        default:
            log_error("create duplicate packet failed: pcode=%d", pcode);
            return NULL;
            break;
    }
    bp->setChannelId(0);
    bp->server_flag = TAIR_SERVERFLAG_DUPLICATE;
    return bp;
}

base_packet *packet_factory::create_response(const base_packet *p) {
    switch (p->getPCode()) {
        default:
            return NULL;
        case TAIR_REQ_PUT_PACKET:
        case TAIR_REQ_REMOVE_PACKET:
        case TAIR_REQ_PING_PACKET:
        case TAIR_REQ_LOCK_PACKET:
        case TAIR_REQ_HIDE_PACKET:
        case TAIR_REQ_EXPIRE_PACKET:
            return new response_return;
        case TAIR_REQ_GET_PACKET:
        case TAIR_REQ_GET_HIDDEN_PACKET:
            return new response_get;
        case TAIR_REQ_INCDEC_PACKET:
            return new response_inc_dec;
        case TAIR_REQ_GET_RANGE_PACKET:
            return new response_get_range;
        case TAIR_REQ_PREFIX_PUTS_PACKET:
        case TAIR_REQ_PREFIX_REMOVES_PACKET:
        case TAIR_REQ_PREFIX_HIDES_PACKET:
            return new response_mreturn;
        case TAIR_REQ_PREFIX_INCDEC_PACKET:
            return new response_prefix_incdec;
        case TAIR_REQ_PREFIX_GETS_PACKET:
        case TAIR_REQ_PREFIX_GET_HIDDENS_PACKET:
            return new response_prefix_gets;
        case TAIR_REQ_SIMPLE_GET_PACKET:
            return new response_simple_get;
        case TAIR_REQ_INCDEC_BOUNDED_PACKET:
            return new response_inc_dec_bounded;
        case TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET:
            return new response_prefix_incdec_bounded;
        case TAIR_REQ_MC_OPS_PACKET:
            return new response_mc_ops;
        case TAIR_REQ_OP_CMD_PACKET:
            return new response_op_cmd;
            /*
             * The ideal model we expect is
             * to let each type of request create its own response.
             * But for some annoying reasons, I cannot make it for now.
             * Pardon me, my lord.
             */
    }
}

}
