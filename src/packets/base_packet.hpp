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

#ifndef TAIR_PACKETS_BASE_H
#define TAIR_PACKETS_BASE_H

#include <string>
#include <map>
#include <set>
#include <vector>
#include <tbsys.h>
#include <stdint.h>
#include <zlib.h>
#include "define.hpp"
#include "data_entry.hpp"
#include "packet.hpp"
#include "databuffer.hpp"
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
    TAIR_REQ_MPUT_PACKET,
    TAIR_REQ_OP_CMD_PACKET,
    TAIR_RESP_OP_CMD_PACKET,
    TAIR_REQ_GET_RANGE_PACKET = 18,
    TAIR_RESP_GET_RANGE_PACKET,

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
    TAIR_RESP_MRETURN_DUP_PACKET = 35,
    TAIR_REQ_SIMPLE_GET_PACKET = 36,
    TAIR_RESP_SIMPLE_GET_PACKET = 37,
    TAIR_REQ_UNIQ_REMOVE_PACKET = 38,

    TAIR_REQ_VERSION_GET_PACKET = 39,
    TAIR_RESP_VERSION_GET_PACKET = 40,
    TAIR_REQ_VERSION_PUT_PACKET = 41,
    TAIR_RESP_VERSION_PUT_PACKET = 42,
    TAIR_REQ_VERSION_PREFIX_GETS_PACKET = 43,
    TAIR_RESP_VERSION_PREFIX_GETS_PACKET = 44,
    TAIR_REQ_VERSION_PREFIX_PUTS_PACKET = 45,
    TAIR_RESP_VERSION_PREFIX_PUTS_PACKET = 46,

    TAIR_RESP_RETURN_PACKET = 101,
    TAIR_RESP_GET_PACKET,
    TAIR_RESP_STAT_PACKET,
    TAIR_RESP_HEARTBEAT_PACKET,
    TAIR_RESP_INCDEC_PACKET,

    TAIR_REQ_GET_GROUP_PACKET = 1002,
    TAIR_REQ_GET_SVRTAB_PACKET = 1003,
    TAIR_REQ_CONFHB_PACKET = 1004,
    TAIR_REQ_SETMASTER_PACKET = 1005,
    TAIR_REQ_GROUP_NAMES_PACKET = 1006,
    TAIR_REQ_QUERY_INFO_PACKET = 1009,
    TAIR_REQ_STATISTICS_PACKET = 1010,
    TAIR_REQ_ADMIN_CONFIG_PACKET = 1011,
    TAIR_REQ_GET_GROUP_NON_DOWN_DATASERVER_PACKET = 1012,

    TAIR_RESP_GET_GROUP_PACKET = 1102,
    TAIR_RESP_GET_SVRTAB_PACKET = 1103,
    TAIR_RESP_GROUP_NAMES_PACKET = 1104,
    TAIR_RESP_QUERY_INFO_PACKET = 1106,
    TAIR_RESP_STATISTICS_PACKET = 1107,
    TAIR_RESP_ADMIN_CONFIG_PACKET = 1111,
    TAIR_RESP_GET_GROUP_NON_DOWN_DATASERVER_PACKET = 1112,

    TAIR_REQ_RT_PACKET = 1121,
    TAIR_RESP_RT_PACKET = 1122,
    TAIR_RESP_FEEDBACK_PACKET = 1123,

    TAIR_REQ_DUMP_BUCKET_PACKET = 1200,
    TAIR_REQ_MIG_FINISH_PACKET,
    TAIR_REQ_REC_FINISH_PACKET,

    TAIR_REQ_BATCH_DUPLICATE_PACKET = 1299,
    TAIR_REQ_DUPLICATE_PACKET = 1300,
    TAIR_RESP_DUPLICATE_PACKET,
    TAIR_RESP_GET_MIGRATE_MACHINE_PACKET,

    TAIR_REQ_RETRY_ALL_PACKET = 1406,
    TAIR_REQ_INVAL_STAT_PACKET = 1407,
    TAIR_RESP_INVAL_STAT_PACKET = 1408,
    TAIR_DEBUG_SUPPORT_PACKET = 1409,

    TAIR_REQ_DATASERVER_CTRL_PACKET = 1500,
    TAIR_REQ_GET_MIGRATE_MACHINE_PACKET = 1501,
    // for Compatible, put TAIR_RESP_DATASERVER_CTRL_PACKET here, as 1502
            TAIR_RESP_DATASERVER_CTRL_PACKET = 1502,

    //the bounded counter
            TAIR_REQ_INCDEC_BOUNDED_PACKET = 1704,
    TAIR_RESP_INCDEC_BOUNDED_PACKET = 1705,
    TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET = 1706,
    TAIR_RESP_PREFIX_INCDEC_BOUNDED_PACKET = 1707,

    TAIR_REQ_SYNC_PACKET = 2000,

    TAIR_REQ_EXPIRE_PACKET = 2127, // compatible with java client

    TAIR_REQ_MC_OPS_PACKET = 2222,
    TAIR_RESP_MC_OPS_PACKET = 2223,

    TAIR_STAT_CMD_VIEW = 9000,
    TAIR_FLOW_CONTROL = 9001,
    TAIR_FLOW_CONTROL_SET = 9002,
    TAIR_REQ_FLOW_VIEW = 9003,
    TAIR_RESP_FLOW_VIEW = 9004,
    TAIR_FLOW_CHECK = 9005,
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

class base_packet : public Packet {
public:
    typedef enum {
        REQ_SPECIAL = 0,
        REQ_WRITE = 1, // turn readonly on, this type packet will be refused
        REQ_READ = 2,
        REQ_MIX = 3, // turn readonly on, this type packet will be refused
        RESP_COMMON = 4
    } Type;
public:
    base_packet() {
        direction = DIRECTION_SEND;
        no_free = false;
        server_flag = 0;
        request_time = 0;
        rt_granted_ = true;
        is_hot_ = false;
        control_cmd = false;
    }

    base_packet(const base_packet &bp) : Packet(bp) {
        direction = bp.direction;
        no_free = bp.no_free;
        server_flag = bp.server_flag;
        request_time = bp.request_time;
        rt_granted_ = bp.rt_granted_;
        is_hot_ = bp.is_hot_;
    }

    base_packet &operator=(const base_packet &bp) {
        if (this != &bp) {
            direction = bp.direction;
            no_free = bp.no_free;
            server_flag = bp.server_flag;
            request_time = bp.request_time;
            rt_granted_ = bp.rt_granted_;
            is_hot_ = bp.is_hot_;
        }
        return *this;
    }

    virtual size_t size() const {
        return 0;
    }

    virtual int64_t distribute_value() {
        return -1;
    }

    virtual base_packet::Type get_type() = 0;

    virtual uint16_t ns() const {
        return 0;
    }

    virtual ~base_packet() {
    }

    // direction
    void set_direction(int direction) {
        this->direction = direction;
    }

    // direction
    int get_direction() {
        return direction;
    }

    void free() {
        if (!no_free) {
            delete this;
        }
    }

    virtual void dump() const {
        fprintf(stderr,
                "op: %s(%d), packet_size: %lu, ns(MayNotSet): %u\n",
                name(getPCode()), getPCode(), size(), ns());
    }

    virtual const data_entry *pick_key() const {
        return NULL;
    }

    virtual bool failed() const {
        return false;
    }

    virtual void set_code(int code) {
        (void) &code;
    }

    void set_no_free() {
        no_free = true;
    }

    bool rt_granted() const {
        return rt_granted_;
    }

    void rt_not_grant() const {
        rt_granted_ = false;
    }

    bool is_hot() const {
        return is_hot_;
    }

    void tag_hot() const {
        is_hot_ = true;
    }


private:

    int direction;
    bool no_free;
    mutable bool rt_granted_;
    mutable bool is_hot_;
public:
    uint8_t server_flag;
    int64_t request_time;
    bool control_cmd;
public:
    static const char *name(int opcode) {
        switch (opcode) {
            default:
                return "unkown";
            case TAIR_REQ_SYNC_PACKET:
                return "req_sync";
            case TAIR_REQ_PUT_PACKET:
                return "req_put";
            case TAIR_REQ_GET_PACKET:
                return "req_get";
            case TAIR_REQ_REMOVE_PACKET:
                return "req_remove";
            case TAIR_REQ_STAT_PACKET:
                return "req_stat";
            case TAIR_REQ_PING_PACKET:
                return "req_ping";
            case TAIR_REQ_DUMP_PACKET:
                return "req_dump";
            case TAIR_REQ_PARAM_PACKET:
                return "req_param";
            case TAIR_REQ_HEARTBEAT_PACKET:
                return "req_heartbeat";
            case TAIR_REQ_INCDEC_PACKET:
                return "req_incdec";
            case TAIR_REQ_MUPDATE_PACKET:
                return "req_mupdate";
            case TAIR_REQ_LOCK_PACKET:
                return "req_lock";
            case TAIR_REQ_MPUT_PACKET:
                return "req_mput";
            case TAIR_REQ_OP_CMD_PACKET:
                return "req_op_cmd";
            case TAIR_REQ_GET_RANGE_PACKET:
                return "req_get_range";
            case TAIR_RESP_GET_RANGE_PACKET:
                return "response_get_range";
            case TAIR_REQ_HIDE_PACKET:
                return "req_hide";
            case TAIR_REQ_HIDE_BY_PROXY_PACKET:
                return "req_hide_by_proxy";
            case TAIR_REQ_GET_HIDDEN_PACKET:
                return "req_get_hidden";
            case TAIR_REQ_INVAL_PACKET:
                return "req_invalid";
            case TAIR_REQ_PREFIX_PUTS_PACKET:
                return "req_prefix_puts";
            case TAIR_REQ_PREFIX_REMOVES_PACKET:
                return "req_prefix_removes";
            case TAIR_REQ_PREFIX_INCDEC_PACKET:
                return "req_prefix_incdec";
            case TAIR_RESP_PREFIX_INCDEC_PACKET:
                return "resp_prefix_incdec";
            case TAIR_RESP_MRETURN_PACKET:
                return "resp_mreturn";
            case TAIR_REQ_PREFIX_GETS_PACKET:
                return "resp_prefix_gets";
            case TAIR_RESP_PREFIX_GETS_PACKET:
                return "resp_prefix_gets";
            case TAIR_REQ_PREFIX_HIDES_PACKET:
                return "req_prefix_hides";
            case TAIR_REQ_PREFIX_INVALIDS_PACKET:
                return "req_prefix_invalids";
            case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
                return "req_prefix_hides_by_proxy";
            case TAIR_REQ_PREFIX_GET_HIDDENS_PACKET:
                return "req_prefix_get_hiddens";
            case TAIR_RESP_MRETURN_DUP_PACKET:
                return "resp_mreturn_dup";
            case TAIR_REQ_SIMPLE_GET_PACKET:
                return "req_simple_get";
            case TAIR_RESP_SIMPLE_GET_PACKET:
                return "resp_simple_get";
            case TAIR_REQ_UNIQ_REMOVE_PACKET:
                return "req_uniq_remove";
            case TAIR_RESP_RETURN_PACKET:
                return "resp_return";
            case TAIR_RESP_GET_PACKET:
                return "resp_get";
            case TAIR_RESP_STAT_PACKET:
                return "resp_stat";
            case TAIR_RESP_HEARTBEAT_PACKET:
                return "resp_heartbeat";
            case TAIR_RESP_INCDEC_PACKET:
                return "resp_incdec";
            case TAIR_REQ_GET_GROUP_PACKET:
                return "req_get_group";
            case TAIR_REQ_GET_SVRTAB_PACKET:
                return "req_get_server_table";
            case TAIR_REQ_CONFHB_PACKET:
                return "req_conf_heartbeat";
            case TAIR_REQ_SETMASTER_PACKET:
                return "req_set_master";
            case TAIR_REQ_GROUP_NAMES_PACKET:
                return "req_group_names";
            case TAIR_REQ_QUERY_INFO_PACKET:
                return "req_query_info";
            case TAIR_REQ_STATISTICS_PACKET:
                return "req_statistics";
            case TAIR_REQ_GET_GROUP_NON_DOWN_DATASERVER_PACKET:
                return "req_get_group_non_down_ds";
            case TAIR_RESP_GET_GROUP_PACKET:
                return "resp_get_group";
            case TAIR_RESP_GET_SVRTAB_PACKET:
                return "resp_get_server_table";
            case TAIR_RESP_GROUP_NAMES_PACKET:
                return "resp_group_names";
            case TAIR_RESP_QUERY_INFO_PACKET:
                return "resp_query_info";
            case TAIR_RESP_STATISTICS_PACKET:
                return "resp_statistics";
            case TAIR_RESP_GET_GROUP_NON_DOWN_DATASERVER_PACKET:
                return "resp_get_group_non_down_ds";
            case TAIR_REQ_RT_PACKET:
                return "req_rt";
            case TAIR_RESP_RT_PACKET:
                return "resp_rt";
            case TAIR_REQ_DUMP_BUCKET_PACKET:
                return "req_dump_bucket";
            case TAIR_REQ_MIG_FINISH_PACKET:
                return "req_mig_finish";
            case TAIR_REQ_REC_FINISH_PACKET:
                return "req_rec_finish";
            case TAIR_REQ_DUPLICATE_PACKET:
                return "req_duplicate";
            case TAIR_RESP_DUPLICATE_PACKET:
                return "resp_duplicate";
            case TAIR_RESP_GET_MIGRATE_MACHINE_PACKET:
                return "resp_get_mig_server";
            case TAIR_REQ_RETRY_ALL_PACKET:
                return "req_retry_all";
            case TAIR_REQ_INVAL_STAT_PACKET:
                return "req_inval_stat";
            case TAIR_RESP_INVAL_STAT_PACKET:
                return "resp_inval_stat";
            case TAIR_DEBUG_SUPPORT_PACKET:
                return "debug_support";
            case TAIR_REQ_DATASERVER_CTRL_PACKET:
                return "req_ds_ctrl";
            case TAIR_RESP_DATASERVER_CTRL_PACKET:
                return "resp_ds_ctrl";
            case TAIR_REQ_GET_MIGRATE_MACHINE_PACKET:
                return "req_get_mig_server";
            case TAIR_REQ_INCDEC_BOUNDED_PACKET:
                return "req_incdec_bounded";
            case TAIR_RESP_INCDEC_BOUNDED_PACKET:
                return "resp_incdec_bounded";
            case TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET:
                return "req_prefix_incdec_bounded";
            case TAIR_RESP_PREFIX_INCDEC_BOUNDED_PACKET:
                return "resp_prefix_incdec_bounded";
            case TAIR_REQ_EXPIRE_PACKET:
                return "req_expire";
            case TAIR_REQ_MC_OPS_PACKET:
                return "req_mc_ops";
            case TAIR_RESP_MC_OPS_PACKET:
                return "resp_mc_ops";
            case TAIR_STAT_CMD_VIEW:
                return "stat_cmd_view";
            case TAIR_FLOW_CONTROL:
                return "flow_control";
            case TAIR_FLOW_CONTROL_SET:
                return "flow_control_set";
            case TAIR_REQ_FLOW_VIEW:
                return "req_flow_view";
            case TAIR_RESP_FLOW_VIEW:
                return "resp_flow_view";
            case TAIR_FLOW_CHECK:
                return "flow_check";
        }
    }
};

}
#endif
