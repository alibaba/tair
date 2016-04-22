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

#include "dup_sync_manager.hpp"
#include "syncproc.hpp"
#include "packet_factory.hpp"
#include "inc_dec_packet.hpp"
#include "storage_manager.hpp"
#include "response_return_packet.hpp"
#include "put_packet.hpp"
#include "prefix_incdec_packet.hpp"
#include "duplicate_packet.hpp"
#include "prefix_puts_packet.hpp"
#include "prefix_removes_packet.hpp"
#include "prefix_hides_packet.hpp"
#include "response_mreturn_packet.hpp"
#include "tair_server.hpp"

extern tair::tair_server *tair_server;
namespace tair {
dup_sync_sender_manager::dup_sync_sender_manager(tair::stat::FlowController *flow_ctrl, RTCollector *rtc,
                                                 uint32_t ioth_count) {
    easy_helper::init_handler(&handler, this);
    handler.process = packet_handler_cb;
    memset(&eio, 0, sizeof(eio));
    io_thread_count = ioth_count;
    easy_io_create(&eio, io_thread_count);
    eio.do_signal = 0;
    eio.no_redispatch = 1;
    eio.tcp_nodelay = 1;
    eio.tcp_cork = 0;
    easy_io_start(&eio);
    easy_eio_set_uthread_start(&eio, easy_helper::easy_set_thread_name < dup_sync_sender_manager > ,
                               const_cast<char *>("dup_io"));
    atomic_set(&packet_id_creater, 0);
    this->flow_ctrl = flow_ctrl;
    this->rtc = rtc;
}

dup_sync_sender_manager::~dup_sync_sender_manager() {
    this->stop();
    this->wait();
    easy_io_stop(&eio);
    easy_io_wait(&eio);
    easy_io_destroy(&eio);
}

void dup_sync_sender_manager::do_hash_table_changed() {
    return;
}

bool dup_sync_sender_manager::is_bucket_available(uint32_t bucket_number) {
    return true;
}

int dup_sync_sender_manager::duplicate_data(int32_t bucket,
                                            const std::vector<common::Record *> *records,
                                            const std::vector<uint64_t> &des_server_ids,
                                            easy_request_t *r) {
    if (des_server_ids.empty() || r == NULL || r->ipacket == NULL) {
        return TAIR_RETURN_SUCCESS;
    }

    unsigned int _copy_count = des_server_ids.size();
    uint32_t max_packet_id = get_chid();

    if (_copy_count > 1) {
        log_error("`dup_sync` mode only works with `copy_count=2`, but now is %d", _copy_count + 1);
        return TAIR_RETURN_DUPLICATE_BUSY;
    }

    int ret = direct_send(r, bucket, records, des_server_ids, max_packet_id);
    if (TAIR_RETURN_SUCCESS != ret) {
        log_error("direct send fialed, ret = %d", ret);
        return ret;
    }

    return TAIR_DUP_WAIT_RSP;
}

//xinshu. add new function for directy duplicate.
int dup_sync_sender_manager::duplicate_data(int area,
                                            const data_entry *key,
                                            const data_entry *value,
                                            int bucket_number,
                                            const vector<uint64_t> &des_server_ids,
                                            easy_request_t *r) {
    if (des_server_ids.empty() || r == NULL || r->ipacket == NULL) {
        return 0;
    }
    unsigned int _copy_count = des_server_ids.size();
    uint32_t max_packet_id = get_chid();

    if (_copy_count > 1) {
        log_error("`dup_sync' mode only works with `copy_count=2', but now is %d", _copy_count + 1);
        return TAIR_RETURN_DUPLICATE_BUSY;
    }

    int ret = direct_send(r, area, key, value, bucket_number, des_server_ids, max_packet_id);
    if (TAIR_RETURN_SUCCESS != ret) {
        log_error("direct_send failed, ret=%d", ret);
        return ret;
    }

    return TAIR_DUP_WAIT_RSP;
}

int dup_sync_sender_manager::duplicate_data(int32_t bucket_number,
                                            easy_request_t *r,
                                            vector<uint64_t> &des_server_ids) {
    if (des_server_ids.empty() || r == NULL || r->ipacket == NULL) {
        return 0;
    }
    unsigned int _copy_count = des_server_ids.size();

    if (_copy_count > 1) {
        //NOTE we have to maintain a `duplicate_count' to tell whether all slaves respond.
        //     for the present, simply only support `copy_count=2'
        log_error("`dup_sync' mode only works with `copy_count=2', but now is %d", _copy_count + 1);
        return TAIR_RETURN_DUPLICATE_BUSY;
    }
    uint32_t ioth_index = get_ioth_index((base_packet *) r->ipacket);
    base_packet *tmp_packet = packet_factory::create_dup_packet((base_packet *) r->ipacket);
    if (tmp_packet == NULL) {
        return TAIR_RETURN_DUPLICATE_BUSY;
    }

    if (easy_helper::easy_async_send(&eio,
                                     easy_helper::convert_addr(des_server_ids[0], ioth_index),
                                     tmp_packet,
                                     r, &handler, dup_timeout) != EASY_OK) {
        delete tmp_packet;
        return TAIR_RETURN_DUPLICATE_BUSY;
    }
    return TAIR_DUP_WAIT_RSP;
}

int dup_sync_sender_manager::direct_send(easy_request_t *r,
                                         int32_t bucket,
                                         const std::vector<common::Record *> *records,
                                         const std::vector<uint64_t> &des_server_ids,
                                         uint32_t max_packet_id) {
    request_batch_duplicate *packet = new request_batch_duplicate();
    packet->packet_id = max_packet_id;
    packet->bucket = bucket;

    std::vector<common::Record *> *rhs = new std::vector<common::Record *>();
    std::vector<common::Record *>::const_iterator iter;
    for (iter = records->begin(); iter != records->end(); iter++) {
        common::Record *record = (*iter)->clone();
        // rewrite server_flag to be duplicate
        record->key_->server_flag = TAIR_SERVERFLAG_DUPLICATE;
        rhs->push_back(record);
    }
    packet->records = rhs;

    if (easy_helper::easy_async_send(&eio, des_server_ids[0], packet,
                                     r, &handler, dup_timeout) != EASY_OK) {
        log_error("batch duplicate packet failed, packet_id: %u, slave: %s",
                  packet->packet_id, tbsys::CNetUtil::addrToString(des_server_ids[0]).c_str());
        delete packet;
        return TAIR_RETURN_DUPLICATE_BUSY;
    }

    return TAIR_RETURN_SUCCESS;
}

int dup_sync_sender_manager::direct_send(easy_request_t *r,
                                         int area,
                                         const data_entry *key,
                                         const data_entry *value,
                                         int bucket_number,
                                         const vector<uint64_t> &des_server_ids,
                                         uint32_t max_packet_id) {
    max_packet_id = get_chid();

    request_duplicate *tmp_packet = new request_duplicate();
    tmp_packet->packet_id = max_packet_id;
    tmp_packet->area = area;
    tmp_packet->key.clone(*key);
    tmp_packet->key.server_flag = TAIR_SERVERFLAG_DUPLICATE;

    if (value != NULL) {
        tmp_packet->data = *value;
        if (tmp_packet->data.is_alloc() == false) {
            tmp_packet->data.set_data(value->get_data(), value->get_size(), true);
        }
        uint32_t pcode = ((base_packet *) r->ipacket)->getPCode();
        if (TAIR_REQ_INCDEC_PACKET == pcode || TAIR_REQ_INCDEC_BOUNDED_PACKET == pcode) {
            int32_t *v = (int32_t *) (value->get_data() + ITEM_HEAD_LENGTH); //for inc_dec
            request_inc_dec *request = (request_inc_dec *) r->ipacket;
            request->result_value = *v; //keep for inc_dec .because the inc_dec dup response doesn't have it.
        }
    } else {
        tmp_packet->data.data_meta.flag = TAIR_ITEM_FLAG_DELETED;
    }
    tmp_packet->bucket_number = bucket_number;

    uint32_t ioth_index = get_ioth_index(key);
    if (easy_helper::easy_async_send(&eio,
                                     easy_helper::convert_addr(des_server_ids[0], ioth_index),
                                     tmp_packet,
                                     r, &handler, dup_timeout) != EASY_OK) {
        log_error("duplicate packet failed, packet_id: %d, slave: %s",
                  tmp_packet->packet_id, tbsys::CNetUtil::addrToString(des_server_ids[0]).c_str());
        delete tmp_packet;
        return TAIR_RETURN_DUPLICATE_BUSY;
    }
    return TAIR_RETURN_SUCCESS;
}

int dup_sync_sender_manager::duplicate_batch_data(int bucket_number,
                                                  const mput_record_vec *record_vec,
                                                  const std::vector<uint64_t> &des_server_ids,
                                                  base_packet *request) {
    unsigned _copy_count = des_server_ids.size();
    uint32_t max_packet_id = get_chid();

    if (_copy_count > 1) {
        log_error("`dup_sync' mode only works with `copy_count=2', but now is %d", _copy_count + 1);
        return TAIR_RETURN_DUPLICATE_BUSY;
    }

    request_mput *tmp_packet = new request_mput();
    tmp_packet->swap(*(dynamic_cast<request_mput *>(request)));
    tmp_packet->server_flag = TAIR_SERVERFLAG_DUPLICATE;
    //tmp_packet->bucket_number = bucket_number;
    tmp_packet->packet_id = max_packet_id;
    if (tmp_packet->compressed != 0) {
        tmp_packet->compressed = 0;
        if (tmp_packet->packet_data != NULL) {
            delete tmp_packet->packet_data;
        }
        tmp_packet->compress();
    }
    //and send it to slave
    log_debug("duplicate packet %d sent: %s", tmp_packet->packet_id,
              tbsys::CNetUtil::addrToString(des_server_ids[0]).c_str());
    if (easy_helper::easy_async_send(&eio, des_server_ids[0], tmp_packet, request->r, &handler, dup_timeout) !=
        EASY_OK) {
        log_error("duplicate packet failed, packet_id: %d, slave: %s",
                  tmp_packet->packet_id, tbsys::CNetUtil::addrToString(des_server_ids[0]).c_str());
        delete tmp_packet;
        return TAIR_RETURN_DUPLICATE_BUSY;
    }

    return TAIR_DUP_WAIT_RSP;
}

base_packet *dup_sync_sender_manager::create_return_packet(base_packet *bp, int heartbeat_version) {
    int pcode = bp->getPCode();
    uint32_t chid = bp->getChannelId();
    const char *result_msg = "";

    base_packet *packet = NULL;
    switch (pcode) {
        case TAIR_REQ_PUT_PACKET:
        case TAIR_REQ_REMOVE_PACKET:
        case TAIR_REQ_LOCK_PACKET:
        case TAIR_REQ_EXPIRE_PACKET:
        case TAIR_REQ_HIDE_PACKET:
        case TAIR_REQ_SYNC_PACKET: {
            packet = new response_return(chid, 0, result_msg, heartbeat_version);
            break;
        }
        case TAIR_REQ_INCDEC_PACKET:
        case TAIR_REQ_INCDEC_BOUNDED_PACKET: {
            response_inc_dec *resp = NULL;
            if (pcode == TAIR_REQ_INCDEC_PACKET) {
                resp = new response_inc_dec();
            } else {
                resp = new response_inc_dec_bounded();
            }
            resp->value = ((request_inc_dec *) bp)->result_value;
            resp->config_version = heartbeat_version;
            resp->setChannelId(chid);
            packet = resp;
            break;
        }
        case TAIR_REQ_MC_OPS_PACKET: {
            response_mc_ops *resp = new response_mc_ops;
            request_mc_ops *req = (request_mc_ops *) bp;
            resp->mc_opcode = req->mc_opcode;
            resp->config_version = heartbeat_version;
            resp->setChannelId(chid);
            resp->code = TAIR_RETURN_SUCCESS;
            /*
             * The following judgement is bound to the logic in tair_manager::mc_ops.
             * Be Careful with the `padding' field!
             */
            resp->version = *(uint16_t *) (req->padding + 8);
            if (resp->mc_opcode == 0x05 /* INCR */
                || resp->mc_opcode == 0x06 /* DECR */) {
                resp->value.set_alloced_data(req->padding,
                                             sizeof(uint64_t));// first 8 bytes is value, last 2 bytes is version
                req->padding = NULL;
            }

            packet = resp;
            break;
        }
        default: {
            log_warn("unknow pcode =%d!", pcode);
            packet = new response_return(chid, TAIR_RETURN_FAILED, result_msg, heartbeat_version);
            break;
        }
    }
    return packet;
}

bool dup_sync_sender_manager::has_bucket_duplicate_done(int bucketNumber) {
    return true;
}

int dup_sync_sender_manager::do_response(easy_request_t *cr, /* request from client */
                                         easy_request_t *sr, /* request from slave ds */
                                         int heartbeat_version) {
    base_packet *dresp = (base_packet *) sr->ipacket;

    if (!dresp) return TAIR_RETURN_DUPLICATE_BUSY; //should never happen.

    uint32_t chid = ((base_packet *) cr->ipacket)->getChannelId();
    switch (dresp->getPCode()) {
        case TAIR_RESP_DUPLICATE_PACKET: {
            base_packet *bp = create_return_packet((base_packet *) cr->ipacket,
                                                   heartbeat_version);
            cr->opacket = bp;
            break;
        }
        case TAIR_RESP_RETURN_PACKET: {
            response_return *resp = new response_return();
            resp->code = static_cast<response_return *>(dresp)->code;
            resp->setChannelId(chid);
            resp->config_version = heartbeat_version;
            cr->opacket = resp;
            break;
        }
        case TAIR_RESP_MRETURN_DUP_PACKET: {
            response_mreturn *resp = new response_mreturn;
            resp->swap(*(dynamic_cast<response_mreturn_dup *>(dresp)));
            resp->setChannelId(chid);
            resp->config_version = heartbeat_version;
            cr->opacket = resp;
            break;
        }
        case TAIR_RESP_PREFIX_INCDEC_PACKET:
        case TAIR_RESP_PREFIX_INCDEC_BOUNDED_PACKET: {
            response_prefix_incdec *resp = NULL;
            if (dresp->getPCode() == TAIR_RESP_PREFIX_INCDEC_PACKET) {
                resp = dynamic_cast<response_prefix_incdec *>(dresp);
            } else {
                resp = dynamic_cast<response_prefix_incdec_bounded *>(dresp);
            }
            if (resp == NULL) {
                log_error("invalid duplicate response received while dealing with pcode %d", dresp->getPCode());
                cr->opacket = NULL;
            } else {
                resp->server_flag = TAIR_SERVERFLAG_CLIENT;
                resp->setChannelId(chid);
                resp->config_version = heartbeat_version;

                sr->ipacket = NULL;
                cr->opacket = resp;
            }
            break;
        }
        default: {
            cr->opacket = NULL;
            log_warn("unknown duplicate rsponse pcode : %d", dresp->getPCode());
        }
    }

    uint16_t ns = (uint16_t) ((base_packet *) cr->ipacket)->ns();
    // add flow control here
    if (flow_ctrl != NULL && flow_ctrl->ShouldFlowControl(ns)) {
        flow_control *ctrl_packet = new flow_control();
        ctrl_packet->set_status(flow_ctrl->IsOverflow(ns));
        ctrl_packet->set_ns(ns);
        ctrl_packet->setChannelId(-1);
        ctrl_packet->_next = (Packet *) cr->opacket;
        cr->opacket = ctrl_packet;
        log_warn("flow control happened, ns: %d", ns);
    }

    cr->retcode = EASY_OK;
    return TAIR_RETURN_SUCCESS;
}

void dup_sync_sender_manager::run(tbsys::CThread *thread, void *arg) {
}

int dup_sync_sender_manager::packet_handler(easy_request_t *r) {
    int rc = r->ipacket != NULL ? EASY_OK : EASY_ERROR;
    easy_request_t *cr = (easy_request_t *) r->args; //the request from client.
    if (rc == EASY_ERROR) {
        int pcode = -1, ns = -1;
        if (NULL != cr && NULL != cr->ipacket) {
            pcode = ((base_packet *) cr->ipacket)->getPCode();
            ns = ((base_packet *) cr->ipacket)->ns();
        }
        log_error("duplicate to %s timeout, pcode : %d, ns : %d", easy_helper::easy_connection_to_str(r->ms->c).c_str(),
                  pcode, ns);
    } else {
        if (cr != NULL) {
            do_response(cr, r, ::tair_server->get_heartbeat_version());
        }
    }
    if (cr != NULL) {
        rtc->rt_end(cr);
        easy_request_wakeup(cr);
    }
    easy_session_destroy(r->ms);
    return EASY_OK;
}

uint32_t dup_sync_sender_manager::get_ioth_index(const data_entry *key) {
    uint32_t hv = key == NULL ? 0 : key->get_hashcode();
    return hv % io_thread_count;
}

uint32_t dup_sync_sender_manager::get_ioth_index(const base_packet *p) {
    uint32_t index = 0;
    switch (p->getPCode()) {
        case TAIR_REQ_PREFIX_PUTS_PACKET: {
            request_prefix_puts *req = (request_prefix_puts *) p;
            index = get_ioth_index(req->kvmap->begin()->first);
            break;
        }
        case TAIR_REQ_PREFIX_INCDEC_PACKET: {
            request_prefix_incdec *req = (request_prefix_incdec *) p;
            index = get_ioth_index(req->key_counter_map->begin()->first);
            break;
        }
        case TAIR_REQ_PREFIX_REMOVES_PACKET: {
            request_prefix_removes *req = (request_prefix_removes *) p;
            data_entry *key = req->key;
            if (req->key_list != NULL) {
                key = *req->key_list->begin();
            }
            index = get_ioth_index(key);
            break;
        }
        default:
            break;
    }

    return index;
}

}
