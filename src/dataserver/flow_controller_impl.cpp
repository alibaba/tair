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

#include <cassert>
#include <string.h>
#include <errno.h>
#include <tbsys.h>

#include "log.hpp"
#include "flow_controller_impl.h"
#include "mutex_lock.h"

namespace tair {
namespace stat {

inline
const char *FlowTypeStr(FlowType type) {
    switch (type) {
        case IN:
            return "IN";
        case OUT:
            return "OUT";
        case OPS:
            return "OPS";
    }
    return "UNKONW";
}

inline
const char *FlowStatusStr(FlowStatus status) {
    switch (status) {
        case UP:
            return "UP";
        case DOWN:
            return "DOWN";
        case KEEP:
            return "KEEP";
    }
    return "UNKONW";
}

int FlowControllerImpl::default_net_upper = 30 * 1024 * 1024; // 30MB/s
int FlowControllerImpl::default_net_lower = 15 * 1024 * 1024; // 15MB/s
int FlowControllerImpl::default_ops_upper = 30000000; // 300000/s
int FlowControllerImpl::default_ops_lower = 20000000; // 200000/s

int FlowControllerImpl::default_total_net_upper = 75 * 1024 * 1024; // 75 MB/s
int FlowControllerImpl::default_total_net_lower = 65 * 1024 * 1024; // 65 MB/s
int FlowControllerImpl::default_total_ops_upper = 50000000; // 500000 ops
int FlowControllerImpl::default_total_ops_lower = 40000000; // 400000 ops

FlowControllerImpl::FlowControllerImpl(int ns)
        : stop_(false),
          cal_interval_second_(2),
          mutex_(new flstorage::Mutex()),
          area_flow_policy_(MAXAREA) {
    bzero(flows_, sizeof(flows_));

    default_net_upper = TBSYS_CONFIG.getInt(FLOW_CONTROL_SECTION, TAIR_DEFAULT_NET_UPPER, 300000000);
    default_net_lower = TBSYS_CONFIG.getInt(FLOW_CONTROL_SECTION, TAIR_DEFAULT_NET_LOWER, 150000000);
    default_ops_upper = TBSYS_CONFIG.getInt(FLOW_CONTROL_SECTION, TAIR_DEFAULT_OPS_UPPER, 300000);
    default_ops_lower = TBSYS_CONFIG.getInt(FLOW_CONTROL_SECTION, TAIR_DEFAULT_OPS_LOWER, 200000);

    default_total_net_upper = TBSYS_CONFIG.getInt(FLOW_CONTROL_SECTION, TAIR_DEFAULT_TOTAL_NET_UPPER, 75000000);
    default_total_net_lower = TBSYS_CONFIG.getInt(FLOW_CONTROL_SECTION, TAIR_DEFAULT_TOTAL_NET_LOWER, 65000000);
    default_total_ops_upper = TBSYS_CONFIG.getInt(FLOW_CONTROL_SECTION, TAIR_DEFAULT_TOTAL_OPS_UPPER, 500000);
    default_total_ops_lower = TBSYS_CONFIG.getInt(FLOW_CONTROL_SECTION, TAIR_DEFAULT_TOTAL_OPS_LOWER, 400000);

    SetFlowLimit(-1, IN, default_net_lower, default_net_upper);
    SetFlowLimit(-1, OUT, default_net_lower, default_net_upper);
    SetFlowLimit(-1, OPS, default_ops_lower, default_ops_upper);
    SetFlowLimit(MAXAREA, IN, default_total_net_lower, default_total_net_upper);
    SetFlowLimit(MAXAREA, OUT, default_total_net_lower, default_total_net_upper);
    SetFlowLimit(MAXAREA, OPS, default_total_ops_lower, default_total_ops_upper);
}

FlowController *FlowController::NewInstance() {
    return new FlowControllerImpl(0);
}

bool FlowControllerImpl::Initialize() {
    int ret = pthread_create(&bg_tid, NULL, BackgroundFunc, this);
    if (ret != 0) {
        log_error("create FlowController background thread faild: %s", strerror(errno));
    }
    return ret == 0;
}

FlowControllerImpl::~FlowControllerImpl() {
    mutex_->Lock();
    stop_ = true;
    mutex_->Signal();
    mutex_->UnLock();

    pthread_join(bg_tid, NULL);
    delete mutex_;
}

inline
bool FlowControllerImpl::ShouldFlowControl(int ns) {
    if (ns < 0 || ns > MAXAREA) ns = MAXAREA;
    return (flows_[ns].status != DOWN) && (area_flow_policy_.test(ns) || flows_[MAXAREA].status != DOWN);
}

inline
FlowStatus FlowControllerImpl::IsOverflow(int ns) {
    if (ns < 0 || ns > MAXAREA) ns = MAXAREA;
    return flows_[ns].status;
}

inline
FlowType FlowControllerImpl::GetCurrOverFlowType(int ns) {
    if (ns < 0 || ns > MAXAREA) ns = MAXAREA;
    return flows_[ns].overflow_type;
}

inline
char const *const FlowControllerImpl::GetCurrOverFlowTypeStr(int ns) {
    return FlowTypeStr(GetCurrOverFlowType(ns));
}

void FlowControllerImpl::GetCurrOverFlowRecord(int ns, int64_t &lower, int64_t &upper, int64_t &current) {
    AreaFlow &flow = flows_[ns];
    FlowType type = GetCurrOverFlowType(ns);
    if (type == IN) {
        lower = flow.in.lower_bound;
        upper = flow.in.upper_bound;
        current = flow.in.curt_quantity;
    } else if (type == OUT) {
        lower = flow.out.lower_bound;
        upper = flow.out.upper_bound;
        current = flow.out.curt_quantity;
    } else if (type == OPS) {
        lower = flow.ops.lower_bound;
        upper = flow.ops.upper_bound;
        current = flow.ops.curt_quantity;
    }
}

Flowrate FlowControllerImpl::GetFlowrate(int ns) {
    if (ns < 0 || ns > MAXAREA) ns = MAXAREA;
    AreaFlow &flow = flows_[ns];
    Flowrate rate = {
            flow.in.last_per_second,
            flow.in.status,
            flow.out.last_per_second,
            flow.out.status,
            flow.ops.last_per_second,
            flow.ops.status,
            flow.status
    };
    return rate;
}

/**
 * @return ture is relaxed, (flow.status == DOWN)
 */
bool FlowControllerImpl::AddUp(Flow &flow, int size) {
    int64_t curt = __sync_add_and_fetch(&flow.curt_quantity, size);
    if (curt / cal_interval_second_ >= flow.upper_bound)
        flow.status = UP;
    // curt / spend
    return flow.status == DOWN;
}

FlowStatus FlowControllerImpl::CalCurrentFlow(Flow &flow) {
    // step
    timeval val;
    gettimeofday(&val, NULL);
    int64_t last_cal_time = flow.last_cal_time;
    int now = (val.tv_sec % 100000) * 1000 + val.tv_usec / 1000; // 32bit, so mod 100000
    int spend = last_cal_time > 0 ? now - last_cal_time : cal_interval_second_ * 1000;
    int last_per_second = static_cast<int>(spend < 1000.00001 ?
                                           flow.curt_quantity :
                                           static_cast<double>(flow.curt_quantity) / spend * 1000);
    // stroe last flow for query
    flow.last_per_second = last_per_second;
    // last update time
    flow.last_cal_time = now;
    // reset quantity
    flow.curt_quantity = 0;

    if (last_per_second >= flow.upper_bound)
        flow.status = UP;
    else if (flow.status == UP && last_per_second >= flow.lower_bound)
        flow.status = KEEP;
    else if (last_per_second < flow.lower_bound)
        flow.status = DOWN;

    return flow.status;
}

void *FlowControllerImpl::BackgroundFunc(void *arg) {
    FlowControllerImpl *this_object = reinterpret_cast<FlowControllerImpl *>(arg);
    this_object->BackgroundCalFlows();
    return NULL;
}

void FlowControllerImpl::BackgroundCalFlows() {
    while (stop_ == false) {
        mutex_->Lock();
        if (stop_ == false)
            mutex_->Wait(cal_interval_second_);
        mutex_->UnLock();
        if (stop_)
            break;
        log_debug("calculating flow");
        for (size_t ns = 0; ns <= MAXAREA; ++ns) {
            // ns == MAXAREA (65536), is summary number
            if (ns == MAXAREA) {
                log_debug("cal Summary Flow rate");
            }

            AreaFlow &flow = flows_[ns];
            FlowStatus status_in = CalCurrentFlow(flow.in);
            if (status_in != DOWN) {
                log_info("Flow IN Limit: ns %lu %s", ns, FlowStatusStr(status_in));
                flow.overflow_type = IN;
            }
            FlowStatus status_out = CalCurrentFlow(flow.out);
            if (status_out != DOWN) {
                log_info("Flow OUT Limit: ns %lu %s", ns, FlowStatusStr(status_out));
                flow.overflow_type = OUT;
            }
            FlowStatus status_ops = CalCurrentFlow(flow.ops);
            if (status_ops != DOWN) {
                log_info("Flow OPS Limit: ns %lu %s", ns, FlowStatusStr(status_ops));
                flow.overflow_type = OPS;
            }

            if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG &&
                (flow.in.last_per_second != 0 ||
                 flow.out.last_per_second != 0 ||
                 flow.ops.last_per_second != 0)) {
                log_debug("Flow rate: ns %lu; in %ld %s; out %ld %s; ops %ld %s",
                          ns,
                          flow.in.last_per_second, FlowStatusStr(status_in),
                          flow.out.last_per_second, FlowStatusStr(status_out),
                          flow.ops.last_per_second, FlowStatusStr(status_ops)
                );
            }

            FlowStatus newStatus;
            newStatus = (status_in > status_out) ? status_in : status_out;
            newStatus = (newStatus > status_ops) ? newStatus : status_ops;

            // UP or KEEP -> DOWN
            if (flow.status != DOWN && newStatus == DOWN) {
                flow.is_recovery = true;
                flow.last_recovery_time = time(NULL);
            } else if (flow.status == DOWN && newStatus == DOWN && flow.is_recovery) {
                time_t time_now = time(NULL);
                if (time_now - flow.last_recovery_time > 60) {
                    flow.is_recovery = false;
                    if (ns != 65536) {
                        log_warn("flow control recovery, ns: %lu, type: %s", ns, GetCurrOverFlowTypeStr(ns));
                    }
                }
            } else {
                flow.is_recovery = false;
            }

            flow.status = newStatus;
        }
        log_debug("calculating end");
    }
}

bool FlowControllerImpl::AddUp(int ns, int in, int out, int weight) {
    if (ns < 0 || ns > MAXAREA) ns = MAXAREA;
    // AddUp total first
    bool is_relaxed = false;
    if (ns != MAXAREA) {
        is_relaxed = AddUp(MAXAREA, in, out, weight);
    }
    AreaFlow &flow = flows_[ns];
    bool limit = false;

    limit = !AddUp(flow.in, in) || limit;
    if (is_relaxed == false && limit) {
        flow.overflow_type = IN;
        log_info("Overflow happend ns:%d in lower:%ld upper:%ld current:%ld",
                 ns, flow.in.lower_bound, flow.in.upper_bound, flow.in.curt_quantity);
    }

    limit = !AddUp(flow.out, out) || limit;
    if (is_relaxed == false && limit) {
        flow.overflow_type = OUT;
        log_info("Overflow happend ns:%d out lower:%ld upper:%ld current:%ld",
                 ns, flow.out.lower_bound, flow.out.upper_bound, flow.out.curt_quantity);
    }

    limit = !AddUp(flow.ops, weight) || limit;
    if (is_relaxed == false && limit) {
        flow.overflow_type = OPS;
        log_info("Overflow happend ns:%d ops lower:%ld upper:%ld current:%ld",
                 ns, flow.ops.lower_bound, flow.ops.upper_bound, flow.ops.curt_quantity);
    }

    if (limit == true) {
        flow.status = UP;
    }
    log_debug("Add up: ns %d, in %d, out %d, status %s", ns, in, out, FlowStatusStr(flow.status));

    return is_relaxed || flow.status == DOWN;
}

FlowLimit FlowControllerImpl::GetFlowLimit(int ns, FlowType type) {
    if (ns == -1) {
        FlowLimit bound = {0, 0};
        return bound;
    }

    if (ns < 0 || ns > MAXAREA) ns = MAXAREA;
    FlowLimit bound = {0, 0};
    AreaFlow &flow = flows_[ns];
    switch (type) {
        case IN:
            bound.lower = flow.in.lower_bound;
            bound.upper = flow.in.upper_bound;
            break;

        case OUT:
            bound.lower = flow.out.lower_bound;
            bound.upper = flow.out.upper_bound;
            break;

        case OPS:
            bound.lower = flow.ops.lower_bound;
            bound.upper = flow.ops.upper_bound;
            break;
    }
    return bound;
}

bool FlowControllerImpl::SetDefaultLimit(int ns) {
    bool ret = true;
    ret &= SetFlowLimit(ns, IN, default_total_net_lower, default_total_net_upper);
    ret &= SetFlowLimit(ns, OUT, default_total_net_lower, default_total_net_upper);
    ret &= SetFlowLimit(ns, OPS, default_total_ops_lower, default_total_ops_upper);
    return ret;
}

bool FlowControllerImpl::SyncFlowLimit(const std::map<int, AllFlowLimit> &flow_limits) {
    bool ret = true;
    int last = 0;
    for (std::map<int, AllFlowLimit>::const_iterator iter = flow_limits.begin();
         iter != flow_limits.end(); ++iter, ++last) {
        for (; last < iter->first; ++last) {
            SetDefaultLimit(last);
        }
        ret &= SetFlowLimit(iter->first, IN, iter->second.in.lower, iter->second.in.upper);
        ret &= SetFlowLimit(iter->first, OUT, iter->second.out.lower, iter->second.out.upper);
        ret &= SetFlowLimit(iter->first, OPS, iter->second.ops.lower, iter->second.ops.upper);
    }
    for (; last < MAXAREA; ++last) {
        SetDefaultLimit(last);
    }
    return ret;
}

bool FlowControllerImpl::SetFlowLimit(int ns, FlowType type, int64_t lower, int64_t upper) {
    log_debug("set(or get) flow limit bound, ns %d; type %s; lower %ld; upper %ld",
              ns, FlowTypeStr(type), lower, upper);
    if (lower < -1 || upper < -1) {
        log_error("invliad flow limit parameter, lower %ld; upper %ld", lower, upper);
        return false;
    }
    if (ns == -1) {
        bool success = true;
        for (size_t i = 0; i < MAXAREA; ++i)
            if (SetFlowLimit(i, type, lower, upper) == false)
                success = false;
        return success;
    } else {
        FlowLimit limit = GetFlowLimit(ns, type);
        if (upper != -1)
            limit.upper = upper;
        if (lower != -1)
            limit.lower = lower;

        if (limit.lower > limit.upper) {
            log_error("%ld > %ld is invalid", limit.lower, limit.upper);
            return false;
        }

        if (ns < 0 || ns > MAXAREA) ns = MAXAREA;
        switch (type) {
            case IN:
                if (lower >= 0)
                    flows_[ns].in.lower_bound = lower;
                if (upper >= 0)
                    flows_[ns].in.upper_bound = upper;
                break;
            case OUT:
                if (lower >= 0)
                    flows_[ns].out.lower_bound = lower;
                if (upper >= 0)
                    flows_[ns].out.upper_bound = upper;
                break;
            case OPS:
                if (lower >= 0)
                    flows_[ns].ops.lower_bound = lower;
                if (upper >= 0)
                    flows_[ns].ops.upper_bound = upper;
                break;
            default:
                log_error("unknow type %d", type);
                break;
        }
    }
    return true;
}

void FlowControllerImpl::SetAreaFlowPolicy(int ns, bool flag) {
    area_flow_policy_.set(ns, flag);
}

}
}

