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

#ifndef FLOW_CONTROLLER_IMPL_H
#define FLOW_CONTROLLER_IMPL_H

#include <time.h>
#include <pthread.h>

#include "define.hpp"
#include "flow_controller.h"
#include "util.hpp"

#define MAXAREA TAIR_MAX_AREA_COUNT

namespace flstorage {
class Mutex;
}

namespace tair {
namespace stat {

struct Flow {
    volatile int64_t curt_quantity;
    volatile int64_t last_per_second;
    volatile int64_t last_cal_time;
    volatile int64_t lower_bound;
    volatile int64_t upper_bound;
    volatile FlowStatus status;
};

struct AreaFlow {
    Flow in;
    Flow out;
    Flow ops;
    volatile bool is_recovery;
    volatile time_t last_recovery_time;
    volatile FlowType overflow_type;
    volatile FlowStatus status;
};

class FlowControllerImpl : public FlowController {
public:
    FlowControllerImpl(int n);

    virtual ~FlowControllerImpl();

    virtual Flowrate GetFlowrate(int ns);

    virtual bool ShouldFlowControl(int ns);

    virtual FlowType GetCurrOverFlowType(int ns);

    virtual char const *const GetCurrOverFlowTypeStr(int ns);

    virtual void GetCurrOverFlowRecord(int ns, int64_t &lower, int64_t &upper, int64_t &current);

    virtual FlowStatus IsOverflow(int ns);

    virtual bool AddUp(int ns, int in, int out, int weight = 1);

    virtual bool SetFlowLimit(int ns, FlowType type, int64_t lower, int64_t upper);

    virtual bool SetDefaultLimit(int ns);

    virtual FlowLimit GetFlowLimit(int ns, FlowType type);

    virtual bool Initialize();

    virtual bool SyncFlowLimit(const std::map<int, AllFlowLimit> &flow_limits);

    virtual void SetAreaFlowPolicy(int ns, bool flag);

private:
    void BackgroundCalFlows();

    FlowStatus CalCurrentFlow(Flow &flow);

    bool AddUp(Flow &flow, int size);

    static void *BackgroundFunc(void *arg);

    AreaFlow flows_[MAXAREA + 1];
    bool stop_;
    pthread_t bg_tid;

    time_t cal_interval_second_;
    flstorage::Mutex *mutex_;

    // area flow policy
    // default is false, false means whether flow control open
    // depends on both maxarea and area flow status ;
    // true means that it only depends on area flow status
    tair::util::dynamic_bitset area_flow_policy_;

private:
    static int default_net_upper;
    static int default_net_lower;
    static int default_ops_upper;
    static int default_ops_lower;

    static int default_total_net_upper;
    static int default_total_net_lower;
    static int default_total_ops_upper;
    static int default_total_ops_lower;
};


}
}

#endif
