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

#include "tair_stat.hpp"
#include "tair_server.hpp"
#include "flow_control_packet.hpp"
#include "duplicate_packet.hpp"
#include "rt_packet.hpp"
#include "feedback_packet.hpp"
#include "op_killer.hpp"
#include "configuration_manager.h"
#include "rt_profiler.hpp"

char g_tair_home[129];

namespace tair {
tair_server::tair_server() {
    is_stoped = 0;
    task_queue = NULL;
    async_task_queue = NULL;
    slow_task_queue = NULL;
    control_task_queue = NULL;
    tair_mgr = NULL;
    req_processor = NULL;

    task_thread_count = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PROCESS_THREAD_COUNT, -1);
    
    int ncpu = sysconf(_SC_NPROCESSORS_CONF);
    if (task_thread_count < 0) {
        task_thread_count = ncpu;
    } else if (task_thread_count == 0) {
        //! let it be, to disable task threads.
    }

    io_thread_count = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_IO_PROCESS_THREAD_COUNT, ncpu);
    stat_mgr = tair::stat::StatManager::NewInstance();
    flow_ctrl = tair::stat::FlowController::NewInstance();
    configuration_mgr = new configuration_manager();
    stat_prc = NULL;
    bzero(&eio, sizeof(easy_io_t));
    bzero(last_flow_control_log_time_, sizeof(last_flow_control_log_time_));

    readonly_ns = new tair::util::StaticBitMap(TAIR_MAX_AREA_COUNT);

    rtc = new RTCollector;
    hotk = new HotKey;
    opkiller = new OPKiller;
    cur_queued_mem = 0;
    const char *max_queued_mem_str = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_MAX_QUEUED_MEM, "1073741824");
    max_queued_mem = strtoll(max_queued_mem_str, NULL, 10);
}

tair_server::~tair_server() {
    delete stat_mgr;
    delete flow_ctrl;
    delete stat_prc;
    delete readonly_ns;
    delete rtc;
    delete hotk;
    delete opkiller;
    delete configuration_mgr;
}

void tair_server::start() {
    if (!initialize()) {
        return;
    }

    heartbeat.start();
    TAIR_STAT.start();

    bool ret = true;
    if (ret) {
        int port = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PORT, TAIR_SERVER_DEFAULT_PORT);
        easy_listen_t *listen = easy_io_add_listen(&eio, NULL /* local host */, port, &eio_handler);
        if (listen == NULL) {
            log_error("listen on port %d failed", port);
            ret = false;
        } else {
            log_info("listened on port %d", port);
        }
    }

    if (ret && easy_io_start(&eio) == EASY_OK && rsync_start()) {
        log_info("---- tair_server started, pid: %d ----", getpid());
    } else {
        log_info("start eio failed");
        stop();
    }

    heartbeat.wait();
    easy_io_wait(&eio);
    rsync_wait();
    destroy();
    configuration_mgr->wait();
}

void tair_server::stop() {
    if (is_stoped == 0) {
        is_stoped = 1;
        log_warn("will stop eio");
        easy_io_stop(&eio);
        rsync_stop();
        log_info("will stop heartbeatThread");
        heartbeat.stop();
        log_info("will stop TAIR_STAT");
        TAIR_STAT.stop();
    }
    configuration_mgr->stop();
}

bool tair_server::initialize() {
    if (stat_mgr->Initialize() == false || flow_ctrl->Initialize() == false)
        return false;

    if (!rtc->load_config(config_file.c_str())) {
        return false;
    }

    easy_io_initialize();
    const char *dev_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_DEV_NAME, NULL);
    uint32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name);
    if (local_ip == 0U) {
        log_error("local ip:0.0.0.0, check your `dev_name', `bond0' is preferred if present");
        return false;
    }
    int port = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PORT, TAIR_SERVER_DEFAULT_PORT);
    local_server_ip::ip = tbsys::CNetUtil::ipToAddr(local_ip, port);

    memset(&eio, 0, sizeof(eio));
    if (!easy_io_create(&eio, io_thread_count)) {
        log_error("create eio failed");
        return false;
    }
    eio.do_signal = 0;
    eio.no_redispatch = 0;
    eio.no_reuseport = 1;
    eio.listen_all = 1;
    eio.tcp_nodelay = 1;
    eio.tcp_cork = 0;
    eio.affinity_enable = 1;

    if (task_thread_count > 0) {
        task_queue = easy_request_thread_create(&eio, task_thread_count, packet_handler_cb, this);
    }

    if (!rsync_initialize()) {
        log_error("rsync init failed");
        return false;
    }

    //~ set the thread count of the slow task queue, based on the regular one
    int slow_task_thread_count = (task_thread_count >> 2);
    if (slow_task_thread_count == 0) {
        slow_task_thread_count = 1;
    }
    slow_task_queue = easy_request_thread_create(&eio, slow_task_thread_count, packet_handler_cb, this);

    control_task_queue = easy_request_thread_create(&eio, 1, packet_handler_cb, this);

    // m_tairManager
    tair_mgr = new tair_manager(&eio);
    bool init_rc = tair_mgr->initialize(flow_ctrl, rtc);
    if (!init_rc) {
        return false;
    }
    configuration_mgr->register_configuration_namespace(tair_mgr, ADMINCONF_NAMESPACE_SECTION);
    configuration_mgr->register_configuration_flowcontrol(flow_ctrl, ADMINCONF_FLOWCONTROL_SECTION);
    if (configuration_mgr->do_load_admin_conf() == TAIR_RETURN_FAILED) {
        log_error("do_load_admin_conf error");
        /* Don't return false to exit */
    }
    configuration_mgr->start();
    server_stat = tair_mgr->get_stat_manager();

    heartbeat.set_thread_parameter(tair_mgr, configuration_mgr);
    // set if need health check before send_heartbeat
    // depends on type of storage engine now
    heartbeat.set_health_check(tair_mgr->get_storage_manager()->need_health_check());

    req_processor = new request_processor(tair_mgr, &heartbeat, this);
    stat_prc = new stat_processor(tair_mgr, stat_mgr, flow_ctrl);

    return true;
}

void tair_server::easy_io_initialize() {
    easy_helper::init_handler(&eio_handler, this);
    eio_handler.process = tair_server::easy_io_handler_cb;

    easy_helper::init_handler(&eio_proxy_handler, this);
    eio_proxy_handler.process = tair_server::proxy_packet_handler_cb;
}

bool tair_server::rsync_initialize() {
    rsync_enabled = (bool) TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, "rsync_listen", 1);
    if (rsync_enabled) {
        rsync_port = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PORT, TAIR_SERVER_DEFAULT_PORT) + 1;
        rsync_io_thread_num = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_RSYNC_IO_THREAD_NUM, 1);
        if (rsync_io_thread_num != 1) {
            log_error("only `one` io thread supported for now, rsync_io_thread_num: %d", rsync_io_thread_num);
            return false;
        }

        rsync_task_thread_num = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_RSYNC_TASK_THREAD_NUM, 2);

        easy_helper::init_handler(&rsync_eio_handler, this);
        rsync_eio_handler.process = tair_server::easy_rsync_io_handler_cb;
        memset(&rsync_eio, 0, sizeof(rsync_eio));
        if (!easy_io_create(&rsync_eio, rsync_io_thread_num)) {
            log_error("create rsync_eio failed");
            return false;
        }
        rsync_eio.do_signal = 0;
        rsync_eio.no_redispatch = 0;
        rsync_eio.no_reuseport = 1;
        rsync_eio.listen_all = 1;
        rsync_eio.tcp_nodelay = 1;
        rsync_eio.tcp_cork = 0;
        rsync_eio.affinity_enable = 1;
        easy_eio_set_uthread_start(&rsync_eio, easy_helper::easy_set_thread_name < void > ,
                                   const_cast<char *>("rsync_io"));

        easy_listen_t *listen = easy_io_add_listen(&rsync_eio, NULL, rsync_port, &rsync_eio_handler);
        if (listen == NULL) {
            log_error("rsync add listen on port %d failed %m", rsync_port);
            return false;
        }
        rsync_task_queue = easy_request_thread_create(&rsync_eio, rsync_task_thread_num, packet_handler_cb, this);
        if (rsync_task_queue == NULL) {
            log_error("create rsync task thread pool failed, rsync_task_thread_num: %d", rsync_task_thread_num);
            return false;
        }
    }
    return true;
}

bool tair_server::rsync_start() {
    if (rsync_enabled) {
        if (easy_io_start(&rsync_eio) != EASY_OK) {
            log_error("start rsync eio failed");
            return false;
        }
    }
    return true;
}

void tair_server::rsync_stop() {
    if (rsync_enabled) {
        easy_io_stop(&rsync_eio);
    }
}

void tair_server::rsync_wait() {
    if (rsync_enabled) {
        easy_io_wait(&rsync_eio);
    }
}

bool tair_server::destroy() {
    if (req_processor != NULL) {
        delete req_processor;
        req_processor = NULL;
    }

    if (tair_mgr != NULL) {
        delete tair_mgr;
        tair_mgr = NULL;
    }

    easy_io_destroy(&eio);
    if (rsync_enabled) {
        easy_io_destroy(&rsync_eio);
    }

    return true;
}

int tair_server::easy_rsync_io_packet_handler(easy_request_t *r) {
    base_packet *bp = (base_packet *) r->ipacket;
    bp->r = r;
    if (bp == NULL || r->ms->status == EASY_MESG_DESTROY) {
        log_error("timeout,null pakcet,shouldn't happen\n");
        return EASY_ERROR;
    }
    bp->set_direction(DIRECTION_RECEIVE);

    if ((TBSYS_LOG_LEVEL_DEBUG <= TBSYS_LOGGER._level)) {
        bp->request_time = tbsys::CTimeUtil::getTime();
    }

    heartbeat.request_count++; //the request_count  isn't thread_safe.
    int pcode = bp->getPCode();
    uint64_t hv = 0;
    switch (pcode) {
        case TAIR_REQ_SYNC_PACKET: {
            request_sync *packet = static_cast<request_sync *>(bp);
            hv = packet->bucket;
            break;
        }
        default: {
            log_warn("get unsupport packet %d", pcode);
            return EASY_ERROR;
        }
    }
    // add mem stat
    easy_atomic_add(&cur_queued_mem, (int64_t)bp->size());
    easy_thread_pool_push(rsync_task_queue, r, hv);
    return EASY_AGAIN;
}

int tair_server::easy_io_packet_handler(easy_request_t *r) {
    base_packet *bp = (base_packet *) r->ipacket;
    bp->r = r;
    if (bp == NULL || r->ms->status == EASY_MESG_DESTROY) {
        log_error("timeout,null pakcet,shouldn't happen\n");
        return EASY_ERROR;
    }
    bp->set_direction(DIRECTION_RECEIVE);

    /*
     * beginning of rt, with the end in the end of `this->packet_handler',
     * or `dup_sync_sender::do_response'
     */
    rtc->rt_beg(r);

    if (!(bp->server_flag & TAIR_SERVERFLAG_DUPLICATE)) {
        hotk->hot(bp);
        hotk->tag_if_hot(bp);

        if (!opkiller->test(bp)) {
            base_packet *resp = packet_factory::create_response(bp);
            if (resp != NULL) {
                resp->set_code(TAIR_RETURN_REFUSED);
                resp->setChannelId(bp->getChannelId());
                r->opacket = resp;
                return EASY_OK;
            } else {
                log_info("op[%d] slipped off from OPKiller", bp->getPCode());
            }
        }
    }

    if (bp->control_cmd) {
        easy_thread_pool_push(control_task_queue, r, easy_hash_key((uint64_t) (long) r));
        return EASY_AGAIN;
    } else {
      // add mem stat
      easy_atomic_add(&cur_queued_mem, (int64_t)bp->size());
    }

    switch (bp->getPCode()) {
        //~ potential slow requests
        case TAIR_REQ_STATISTICS_PACKET:
        case TAIR_REQ_GET_RANGE_PACKET:
        case TAIR_REQ_RT_PACKET: //~ RT_RELOAD may be a little bit slow
        case TAIR_REQ_ADMIN_CONFIG_PACKET:
            easy_thread_pool_push(slow_task_queue, r, easy_hash_key((uint64_t) (long) r));
            return EASY_AGAIN;
        default:
            break;
    }

    if (task_thread_count > 0) {
        easy_thread_pool_push(task_queue, r, easy_hash_key((uint64_t) (long) r));
        return EASY_AGAIN;
    }
    return this->packet_handler(r);
}

int tair_server::packet_handler(easy_request_t *r) {
    base_packet *bp = (base_packet *) r->ipacket;
    uint16_t ns = bp->ns();

    static time_t last_log_mem_exceed_time = 0;
    // test if the memory exceed
    if (!bp->control_cmd && easy_atomic_add_return(&cur_queued_mem, -(int64_t) bp->size()) >= max_queued_mem) {
        time_t time_now = time(NULL);
        if (time_now > last_log_mem_exceed_time) {
          log_error("memory exceed! cur_queued_men: %"PRI64_PREFIX"d max_queued_mem: %"PRI64_PREFIX"d",
              cur_queued_mem, max_queued_mem);
          last_log_mem_exceed_time = time_now;
        }
        return EASY_OK;
    }

    tair::stat::StatRecord stat;
    stat.ip = (uint32_t) (easy_helper::convert_addr(r->ms->c->addr) & 0xffffffff);
    stat.ns = ns;
    stat.op = (uint16_t) bp->getPCode();
    stat.in = (bp->get_type() == base_packet::REQ_SPECIAL ? 0 : (uint32_t) bp->size());
    stat.out = 0;

    int pcode = bp->getPCode();

    if (bp->server_flag == TAIR_SERVERFLAG_CLIENT) {
        // ns 0 is reserved ns, like all cmd based op_cmd, ns set 0
        if ((bp->get_type() == base_packet::REQ_WRITE ||
             bp->get_type() == base_packet::REQ_MIX) && readonly_ns->test(ns)) {
            if (pcode == TAIR_REQ_PREFIX_PUTS_PACKET ||
                pcode == TAIR_REQ_PREFIX_REMOVES_PACKET ||
                pcode == TAIR_REQ_PREFIX_HIDES_PACKET) {
                response_mreturn *mreturn_packet = new response_mreturn();
                mreturn_packet->set_code(TAIR_RETURN_READ_ONLY);
                mreturn_packet->set_message("your namespace has been set readonly");
                mreturn_packet->config_version = heartbeat.get_client_version();
                mreturn_packet->setChannelId(bp->getChannelId());
                r->opacket = mreturn_packet;
            } else if (pcode == TAIR_REQ_PREFIX_INCDEC_PACKET) {
                response_prefix_incdec *resp = new response_prefix_incdec();
                resp->set_code(TAIR_RETURN_READ_ONLY);
                resp->config_version = heartbeat.get_client_version();
                resp->setChannelId(bp->getChannelId());
                r->opacket = resp;
            } else if (pcode == TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET) {
                response_prefix_incdec_bounded *resp = new response_prefix_incdec_bounded();
                resp->set_code(TAIR_RETURN_READ_ONLY);
                resp->config_version = heartbeat.get_client_version();
                resp->setChannelId(bp->getChannelId());
                r->opacket = resp;
            } else {
                response_return *return_packet = new response_return(bp->getChannelId(), TAIR_RETURN_READ_ONLY,
                                                  "your namespace has been set readonly", heartbeat.get_client_version());
                r->opacket = return_packet;
            }
            return EASY_OK;
        }
    }

    bool send_return = true;
    int ret = TAIR_RETURN_SUCCESS;
    const char *msg = "";
    uint32_t weight = 1;
    switch (pcode) {
        case TAIR_REQ_SYNC_PACKET: {
            request_sync *packet = (request_sync *) bp;
            send_return = false;
            ret = req_processor->process(packet, send_return);
            break;
        }
        case TAIR_REQ_SIMPLE_GET_PACKET: {
            request_simple_get *packet = (request_simple_get *) bp;
            send_return = false;
            ret = req_processor->process(packet, stat.out, weight);
            break;
        }
        case TAIR_REQ_PUT_PACKET: {
            request_put *packet = (request_put *) bp;
            ret = req_processor->process(packet, send_return, stat.out);
            break;
        }
        case TAIR_REQ_GET_PACKET: {
            request_get *packet = (request_get *) bp;
            ret = req_processor->process(packet, send_return, stat.out, weight);
            send_return = false;
            break;
        }
        case TAIR_REQ_STATISTICS_PACKET: {
            request_statistics *packet = (request_statistics *) bp;
            ret = req_processor->process(packet, send_return);
            break;
        }
        case TAIR_REQ_HIDE_PACKET: {
            request_hide *packet = (request_hide *) bp;
            ret = req_processor->process(packet, send_return, stat.out);
            break;
        }
        case TAIR_REQ_GET_HIDDEN_PACKET: {
            request_get_hidden *packet = (request_get_hidden *) bp;
            ret = req_processor->process(packet, send_return, stat.out);
            send_return = false;
            break;
        }
        case TAIR_REQ_GET_RANGE_PACKET: {
            request_get_range *packet = (request_get_range *) bp;
            ret = req_processor->process(packet, send_return, stat.out, weight);
            break;
        }
        case TAIR_REQ_REMOVE_PACKET: {
            request_remove *packet = (request_remove *) bp;
            ret = req_processor->process(packet, send_return, stat.out);
            log_debug("end remove,prepare to send return bp");
            break;
        }
        case TAIR_REQ_UNIQ_REMOVE_PACKET: {
            request_uniq_remove *packet = (request_uniq_remove *) bp;
            ret = req_processor->process(packet, send_return);
            log_debug("end uniq remove, prepare to send return bp");
            break;
        }
        case TAIR_REQ_MPUT_PACKET: {
            request_mput *npacket = (request_mput *) bp;
            // decompress here
            npacket->decompress();
            ret = req_processor->process(npacket, send_return, stat.out);
            break;
        }
        case TAIR_REQ_REMOVE_AREA_PACKET: {
            request_remove_area *packet = (request_remove_area *) bp;
            ret = tair_mgr->clear(packet->area);
            break;
        }
        case TAIR_REQ_LOCK_PACKET: {
            request_lock *packet = (request_lock *) bp;
            ret = req_processor->process(packet, send_return, stat.out);
            break;
        }
        case TAIR_REQ_PING_PACKET: {
            ret = ((request_ping *) bp)->value;
            break;
        }
        case TAIR_REQ_DUMP_PACKET: {
            request_dump *packet = (request_dump *) bp;
            tair_mgr->do_dump(packet->info_set);
            break;
        }
        case TAIR_REQ_DUMP_BUCKET_PACKET: {
            ret = EXIT_FAILURE;
            break;
        }
        case TAIR_REQ_INCDEC_PACKET:
        case TAIR_REQ_INCDEC_BOUNDED_PACKET: {
            request_inc_dec *packet = (request_inc_dec *) bp;
            ret = req_processor->process(packet, send_return, stat.out);
            break;
        }
        case TAIR_REQ_DUPLICATE_PACKET: {
            request_duplicate *packet = (request_duplicate *) bp;
            ret = req_processor->process(packet, send_return, stat.out);
            if (ret == TAIR_RETURN_SUCCESS) send_return = false;
            break;
        }
        case TAIR_REQ_BATCH_DUPLICATE_PACKET: {
            request_batch_duplicate *packet = (request_batch_duplicate *) bp;
            ret = req_processor->process(packet, send_return);
            if (ret == TAIR_RETURN_SUCCESS) send_return = false;
            break;
        }
        case TAIR_REQ_MUPDATE_PACKET: {
            request_mupdate *packet = (request_mupdate * )(bp);
            ret = req_processor->process(packet, send_return);
            break;
        }
        case TAIR_STAT_CMD_VIEW: {
            stat_cmd_view_packet *packet = (stat_cmd_view_packet * )(bp);
            stat.out = stat_prc->process(packet);
            send_return = false;
            break;
        }
        case TAIR_FLOW_CONTROL_SET: {
            flow_control_set *packet = dynamic_cast<flow_control_set *>(bp);
            stat.out = stat_prc->process(packet);
            send_return = false;
            break;
        }
        case TAIR_REQ_FLOW_VIEW: {
            flow_view_request *packet = dynamic_cast<flow_view_request *>(bp);
            stat.out = stat_prc->process(packet);
            send_return = false;
            break;
        }
        case TAIR_FLOW_CHECK: {
            flow_check *packet = (flow_check * )(bp);
            stat.out = stat_prc->process(packet);
            send_return = false;
            break;
        }
        case TAIR_REQ_PREFIX_PUTS_PACKET: {
            request_prefix_puts *packet = (request_prefix_puts *) bp;
            ret = req_processor->process(packet, send_return, stat.out, weight);
            break;
        }
        case TAIR_REQ_PREFIX_REMOVES_PACKET: {
            request_prefix_removes *packet = (request_prefix_removes *) bp;
            ret = req_processor->process(packet, send_return, stat.out, weight);
            break;
        }
        case TAIR_REQ_PREFIX_INCDEC_PACKET:
        case TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET: {
            request_prefix_incdec *packet = (request_prefix_incdec *) bp;
            ret = req_processor->process(packet, send_return, stat.out, weight);
            break;
        }
        case TAIR_REQ_PREFIX_GETS_PACKET: {
            request_prefix_gets *packet = (request_prefix_gets *) bp;
            ret = req_processor->process(packet, send_return, stat.out, weight);
            break;
        }
        case TAIR_REQ_PREFIX_HIDES_PACKET: {
            request_prefix_hides *packet = (request_prefix_hides *) bp;
            ret = req_processor->process(packet, send_return, stat.out, weight);
            break;
        }
        case TAIR_REQ_PREFIX_GET_HIDDENS_PACKET: {
            request_prefix_get_hiddens *packet = (request_prefix_get_hiddens *) bp;
            ret = req_processor->process(packet, send_return, stat.out, weight);
            break;
        }
        case TAIR_REQ_OP_CMD_PACKET: {
            request_op_cmd *packet = (request_op_cmd * )(bp);
            if (packet->cmd == TAIR_SERVER_CMD_NS_READ_ONLY_ON) {
                ret = operate_read_only_switch(packet, true, send_return);
            } else if (packet->cmd == TAIR_SERVER_CMD_NS_READ_ONLY_OFF) {
                ret = operate_read_only_switch(packet, false, send_return);
            } else if (packet->cmd == TAIR_SERVER_CMD_GET_NS_READ_ONLY_STATUS) {
                ret = get_read_only_status(packet, send_return);
            } else if (packet->cmd == TAIR_SERVER_CMD_OPKILL
                       || packet->cmd == TAIR_SERVER_CMD_OPKILL_DISABLE) {
                ret = do_op_kill(packet, send_return);
            } else if (packet->cmd == TAIR_SERVER_CMD_SET_FLOW_POLICY) {
                ret = do_set_flow_policy(packet, send_return);
            } else {
                ret = req_processor->process(packet, send_return);
            }
            break;
        }
        case TAIR_REQ_EXPIRE_PACKET: {
            request_expire *packet = (request_expire * )(bp);
            ret = req_processor->process(packet, send_return, stat.out);
            break;
        }
        case TAIR_REQ_MC_OPS_PACKET: {
            request_mc_ops *packet = (request_mc_ops * )(bp);
            ret = req_processor->process(packet, send_return, stat.out);
            break;
        }
        case TAIR_REQ_RT_PACKET: {
            response_rt *resp = new response_rt;
            rtc->do_req_rt((request_rt *) bp, resp);
            ret = TAIR_RETURN_SUCCESS;
            resp->set_code(ret);
            resp->setChannelId(bp->getChannelId());
            r->opacket = resp;
            send_return = false;
            break;
        }
        case TAIR_REQ_ADMIN_CONFIG_PACKET: {
            send_return = false;
            request_admin_config *request = (request_admin_config * )(bp);
            if (bp != NULL) {
                response_admin_config *response = new response_admin_config();
                int64_t version = configuration_mgr->process(request, response->configs());
                response->set_version(version);
                response->setChannelId(request->getChannelId());
                request->r->opacket = response;
                stat.out = response->size();
                ret = EASY_OK;
            } else
                ret = EASY_ERROR;
            break;
        }
        default: {
            ret = EXIT_FAILURE;
            log_warn("unknow packet, pcode: %d", pcode);
        }
    }

    int easy_ret = EASY_OK;
    if (ret == TAIR_RETURN_PROXYED || ret == TAIR_DUP_WAIT_RSP) {
        /*
         * two copy, do rt in dup_sync_manager
         */
        easy_ret = EASY_ABORT;
    } else if (send_return && bp->get_direction() == DIRECTION_RECEIVE) {
        log_debug("send return packet, return code: %d", ret);
        response_return *return_packet = new response_return(bp->getChannelId(), ret, msg, heartbeat.get_client_version());
        r->opacket = return_packet;
        stat.out += return_packet->size();
    }

    stat_mgr->AddUp(stat.ip, stat.ns, stat.op, stat.in, stat.out);
    flow_ctrl->AddUp(stat.ns, stat.in, stat.out, weight);

    // update flowrate stat, only in/out
    server_stat->update_flow(stat.ns, stat.in, stat.out);
    if (EASY_OK == easy_ret) {
        if (flow_ctrl->ShouldFlowControl(stat.ns)) {
            flow_control *ctrl_packet = new flow_control();
            ctrl_packet->set_status(flow_ctrl->IsOverflow(stat.ns));
            ctrl_packet->set_ns(stat.ns);
            ctrl_packet->setChannelId(-1);
            ctrl_packet->_next = (Packet *) r->opacket;
            r->opacket = ctrl_packet;
            if (is_flow_control_log_time(stat.ns)) {
                int64_t lower = 0;
                int64_t upper = 0;
                int64_t current = 0;
                flow_ctrl->GetCurrOverFlowRecord(stat.ns, lower, upper, current);
                log_warn("flow control happened, ns: %d, overflow type: %s, lower: %ld upper: %ld current: %ld",
                         stat.ns, flow_ctrl->GetCurrOverFlowTypeStr(stat.ns), lower, upper, current);
            }
        }

        /*
         * rt of single copy
         */
        rtc->rt_end(r);

        if (r->ipacket != NULL && ((base_packet *) r->ipacket)->is_hot()) {
            base_packet *req = (base_packet *) r->ipacket;
            base_packet *resp = (base_packet *) r->opacket;
            response_feedback *feedback = new response_feedback;
            feedback->set_feedback_type(response_feedback::FEEDBACK_HOT_KEY);
            feedback->set_ns(req->ns());
            feedback->set_key(req->pick_key()); //~ may be NULL
            feedback->setChannelId(-1);
            feedback->_next = resp;
            r->opacket = feedback;
            log_warn("send response_feedback to %s, ns: %u", tbsys::CNetUtil::addrToString(req->peer_id).c_str(), req->ns());
        }
    }

    return easy_ret;
}

bool tair_server::is_flow_control_log_time(int ns) {
    time_t time_now = time(NULL);
    if (time_now - last_flow_control_log_time_[ns] > 10) {
        last_flow_control_log_time_[ns] = time_now;
        return true;
    }

    return false;
}

int tair_server::proxy_packet_handler(easy_request_t *r) {
    easy_request_t *old_r = (easy_request_t *) r->args; //~ the `proxied' request from client
    base_packet *resp = (base_packet *) r->ipacket; //~ response from another data server
    base_packet *bp = (base_packet *) old_r->ipacket;

    if (resp != NULL) {
        log_debug("send proxy response");
        r->ipacket = NULL;
        resp->setChannelId(bp->getChannelId());
        old_r->opacket = resp;
    } else {
        if (bp->getPCode() == TAIR_REQ_GET_PACKET) {
            response_get *p = new response_get();
            p->config_version = heartbeat.get_client_version();
            p->set_code(TAIR_RETURN_PROXYED_ERROR);
            resp = p;
        } else {
            response_return *p = new response_return();
            p->config_version = heartbeat.get_client_version();
            p->set_code(TAIR_RETURN_PROXYED_ERROR);
            resp = p;
        }
        resp->setChannelId(bp->getChannelId());
        old_r->opacket = resp;
    }
    easy_request_wakeup(old_r);
    easy_session_destroy(r->ms);
    return EASY_OK;
}

int tair_server::get_valid_command_operating_ns(std::vector<std::string> &params, int &ns) {
    if (params.size() < 1) {
        return TAIR_RETURN_FAILED;
    }

    if (params[0] == "all") {
        ns = -1;
        return TAIR_RETURN_SUCCESS;
    }

    ns = atoi(params[0].c_str());
    if (ns < 0 || ns >= readonly_ns->size()) {
        return TAIR_RETURN_FAILED;
    }

    log_info("Notice: you are operating namespace %d", ns);
    return TAIR_RETURN_SUCCESS;
}

int tair_server::operate_read_only_switch(request_op_cmd *request, bool on, bool &send_return) {
    send_return = true;
    int ns = 0;
    int ret = get_valid_command_operating_ns(request->params, ns);
    if (ret == TAIR_RETURN_SUCCESS && ns >= 0) {
        send_return = false;

        if (on == true) {
            readonly_ns->on(ns);
        } else {
            readonly_ns->off(ns);
        }

        char msg[128];
        snprintf(msg, 128, "you succeed to turn namespace %d readony %s", ns, (on == true ? "on" : "off"));
        response_op_cmd *resp = new response_op_cmd();
        resp->infos.push_back(msg);
        resp->setChannelId(request->getChannelId());
        resp->set_code(ret);
        request->r->opacket = resp;
    }
    return ret;
}

int tair_server::get_read_only_status(request_op_cmd *request, bool &send_return) {
    send_return = true;
    int ns = 0;
    int ret = get_valid_command_operating_ns(request->params, ns);
    if (ret == TAIR_RETURN_SUCCESS) {
        char msg[64];
        send_return = false;
        response_op_cmd *resp = new response_op_cmd();
        if (ns >= 0) {
            bool on = readonly_ns->test(ns);
            snprintf(msg, 64, "ns %d readonly %s", ns, (on == true ? "on" : "off"));
            resp->infos.push_back(msg);
        } else {
            int start = 0, end = 0;
            bool on = readonly_ns->test(0);
            int size = readonly_ns->size();
            for (int i = 1; i < size; i++) {
                if (readonly_ns->test(i) == on) {
                    end = i;
                } else {
                    snprintf(msg, 64, "ns %d - %d readonly %s", start, end, (on == true ? "on" : "off"));
                    resp->infos.push_back(msg);
                    start = i;
                    end = start;
                    on = !on;
                }
            }
            if (start < size) {
                snprintf(msg, 64, "ns %d - %d readonly %s", start, end, (on == true ? "on" : "off"));
                resp->infos.push_back(msg);
            }
        }
        resp->setChannelId(request->getChannelId());
        resp->set_code(ret);
        request->r->opacket = resp;
    }
    return ret;
}

int tair_server::do_op_kill(request_op_cmd *p, bool &send_return) {
    send_return = false;
    response_op_cmd *resp = (response_op_cmd *) packet_factory::create_response(p);
    assert(resp != NULL);
    resp->setChannelId(p->getChannelId());
    resp->code = TAIR_RETURN_SUCCESS;
    if (p->cmd == TAIR_SERVER_CMD_OPKILL_DISABLE) {
        opkiller->disable();
    } else if (p->cmd == TAIR_SERVER_CMD_OPKILL) {
        int area = atoi(p->params[0].c_str());
        int op = atoi(p->params[1].c_str());
        log_warn("op[%d] of area[%d] would be refused", op, area);
        opkiller->add_target(area, op);
    } else {
        assert(false);
    }
    p->r->opacket = resp;
    return resp->code;
}

int tair_server::do_set_flow_policy(request_op_cmd *p, bool &send_return) {
    send_return = false;
    response_op_cmd *resp = (response_op_cmd *) packet_factory::create_response(p);
    assert(resp != NULL);
    resp->setChannelId(p->getChannelId());
    int area = atoi(p->params[0].c_str());
    bool flag = (0 == atoi(p->params[1].c_str())) ? false : true;
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
        resp->code = TAIR_RETURN_FAILED;
    } else {
        flow_ctrl->SetAreaFlowPolicy(area, flag);
        resp->code = TAIR_RETURN_SUCCESS;
    }
    p->r->opacket = resp;
    return resp->code;
}

void tair_server::load_max_queued_mem() {
    tbsys::CConfig config;
    if (config.load(config_file.c_str()) == EXIT_FAILURE) {
        log_error("load_config file %s err", config_file.c_str());
        return;
    }
    const char *max_queued_mem_str = config.getString(TAIRSERVER_SECTION, TAIR_MAX_QUEUED_MEM, "5368709120");
    max_queued_mem = strtoll(max_queued_mem_str, NULL, 10);
    log_warn("max_queued_mem is %"PRI64_PREFIX"d", max_queued_mem);
}

} // namespace end

////////////////////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////////////////////
tair::tair_server *tair_server = NULL;
tbsys::CThreadMutex mutex;

void sign_handler(int sig) {
    switch (sig) {
        case SIGTERM:
        case SIGINT:
            if (tair_server != NULL) {
                log_info("will stop tairserver");
                tair_server->stop();
            }
            break;
        case 40:
            TBSYS_LOGGER.checkFile();
            break;
        case 41:
        case 42:
            if (sig == 41) {
                tair::easy_helper::easy_set_log_level(++TBSYS_LOGGER._level);
            } else {
                tair::easy_helper::easy_set_log_level(--TBSYS_LOGGER._level);
            }
            log_error("TBSYS_LOGGER._level: %d", TBSYS_LOGGER._level);
            break;
        case 43:
            PROFILER_SET_STATUS((tbsys::util::Profiler::m_profiler.status + 1) % 2);
            log_error("profiler is %s", (tbsys::util::Profiler::m_profiler.status == 1) ? "enabled" : "disabled");
            break;
        case 47: // reload config
            if (tair_server != NULL) {
                tair_server->reload_config();
            }
            break;
        case 48:
            tair::util::MM::malloc_stats();
            break;
        case 49:
            tair::util::MM::release_free_memory();
            break;
        case 50:
            tair_server->hot_key();
            break;
        case 51:
            tair::storage::g_rt_profiler.do_switch();
            break;
        case 52:
            tair::storage::g_rt_profiler.dump();
            break;
        case 53:
            tair::storage::g_rt_profiler.reset();
            break;
        case 54:
            tair_server->set_heartbeat_update(false);
            log_warn("shutdown hearbeat update");
            break;
        case 55:
            tair_server->set_heartbeat_update(true);
            log_warn("openup hearbeat update");
            break;
        case 56:
            log_warn("cur used mem is %"PRI64_PREFIX"u", tair_server->get_cur_queued_mem());
            break;
        case 57:
            // load mem used mem
            tair_server->load_max_queued_mem();
            break;
        default:
            log_error("sig: %d", sig);
    }
}

void print_usage(char *prog_name) {
    fprintf(stderr, "%s -f config_file\n"
                    "    -f, --config_file  config file name\n"
                    "    -h, --help         display this help and exit\n"
                    "    -V, --version      version and build time\n\n",
            prog_name);
}

char *parse_cmd_line(int argc, char *const argv[]) {
    int opt;
    const char *opt_string = "hVf:";
    struct option long_opts[] = {
            {"config_file", 1, NULL, 'f'},
            {"help",        0, NULL, 'h'},
            {"version",     0, NULL, 'V'},
            {0,             0, 0,    0}
    };

    char *config_file = NULL;
    while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
        switch (opt) {
            case 'f':
                config_file = optarg;
                break;
            case 'V':
                fprintf(stderr, "BUILD_TIME: %s %s\nGIT: %s\n", __DATE__, __TIME__, TAIR_GIT_INFO);
                exit(1);
            case 'h':
                print_usage(argv[0]);
                exit(1);
        }
    }
    return config_file;
}

int main(int argc, char *argv[]) {
    // parse cmd
    char *config_file = parse_cmd_line(argc, argv);
    if (config_file == NULL) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    char *pathName = getenv("TAIR_HOME");
    if (pathName != NULL) {
        strncpy(g_tair_home, pathName, 128);
    } else {
#ifdef TAIR_DEBUG
        strcpy(g_tair_home,"./");
#else
        strcpy(g_tair_home, "/home/admin/tair/");
#endif
    }

    if (TBSYS_CONFIG.load(config_file)) {
        fprintf(stderr, "load config file (%s) failed\n", config_file);
        return EXIT_FAILURE;
    }

    const char *pid_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_PID_FILE, "server.pid");
    const char *log_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_LOG_FILE, "server.log");

    char *p, dir_path[256];
    sprintf(dir_path, "%s", pid_file_name);
    p = strrchr(dir_path, '/');
    if (p != NULL) *p = '\0';
    if (p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
      fprintf(stderr, "mkdir failed: %s\n", dir_path);
      return EXIT_FAILURE;
    }
    sprintf(dir_path, "%s", log_file_name);
    p = strrchr(dir_path, '/');
    if (p != NULL) *p = '\0';
    if (p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
      fprintf(stderr, "mkdir failed: %s\n", dir_path);
      return EXIT_FAILURE;
    }

    if (!tbsys::CFileUtil::mkdirs(dir_path)) {
      fprintf(stderr, "mkdir failed: %s\n", dir_path);
      return EXIT_FAILURE;
    }
    const char *dump_path = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_DUMP_DIR, TAIR_DEFAULT_DUMP_DIR);
    sprintf(dir_path, "%s", dump_path);
    if (!tbsys::CFileUtil::mkdirs(dir_path)) {
      fprintf(stderr, "create dump directory {%s} failed.", dir_path);
      return EXIT_FAILURE;
    }

    int pid;
    if ((pid = tbsys::CProcess::existPid(pid_file_name))) {
        fprintf(stderr, "tair_server already running: pid=%d\n", pid);
        return EXIT_FAILURE;
    }

    const char *log_level = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_LOG_LEVEL, "info");
    TBSYS_LOGGER.setLogLevel(log_level);

    // disable profiler by default
    PROFILER_SET_STATUS(0);
    // set the threshold
    int32_t profiler_threshold = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PROFILER_THRESHOLD, 10000);
    PROFILER_SET_THRESHOLD(profiler_threshold);

    // admin config manager
    int is_namespace_load = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_IS_NAMESPACE_LOAD, 0);
    int is_flowcontrol_load = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_IS_FLOWCONTROL_LOAD, 0);
    const char *admin_file = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_ADMIN_FILE, "admin.conf");
    if (tbsys::CProcess::startDaemon(pid_file_name, log_file_name) == 0) {
        for (int i = 0; i < 64; i++) {
            if (i == 9 || i == SIGINT || i == SIGTERM || i == 40) continue;
            signal(i, SIG_IGN);
        }
        signal(SIGINT, sign_handler);
        signal(SIGTERM, sign_handler);
        signal(40, sign_handler);
        signal(41, sign_handler);
        signal(42, sign_handler);
        signal(43, sign_handler); // for switch profiler enable/disable status
        signal(47, sign_handler); // reload config
        signal(48, sign_handler); // print out memory stats
        signal(49, sign_handler); // release free memory in TCMalloc
        signal(50, sign_handler); // hot key
        signal(51, sign_handler); // RTProfiler::do_switch in ldb
        signal(52, sign_handler); // RTProfiler::dump in ldb
        signal(53, sign_handler); // RTProfiler::reset in ldb
        signal(54, sign_handler); // update false
        signal(55, sign_handler); // update true
        signal(56, sign_handler); // get cur used mem
        signal(57, sign_handler); // load max used mem

        log_error("profiler disabled by default, threshold has been set to %d", profiler_threshold);

        tair_server = new tair::tair_server();
        tair_server->set_config_file(config_file);
        tair_server->set_admin_file(admin_file);
        if (is_namespace_load == 1) {
            tair_server->set_ns_load();
        }
        if (is_flowcontrol_load == 1) {
            tair_server->set_flow_load();
        }
        tair_server->start();

        // ignore signal when destroy, cause sig_handler may use tair_server between delete and set it to NULL.
        signal(SIGINT, SIG_IGN);
        signal(SIGTERM, SIG_IGN);

        delete tair_server;
        tair_server = NULL;
        log_warn("tair_server exit.");
    }

    return EXIT_SUCCESS;
}
////////////////////////////////END
