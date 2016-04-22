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

#ifndef FLOW_CONTROLLER_H
#define FLOW_CONTROLLER_H

#include "flowrate.h"
#include <map>

namespace tair {
namespace stat {

class FlowController {
public:
    static FlowController *NewInstance();

    virtual ~FlowController() {}

    virtual bool Initialize() = 0;

    virtual bool ShouldFlowControl(int ns) = 0;

    virtual FlowType GetCurrOverFlowType(int ns) = 0;

    virtual char const *const GetCurrOverFlowTypeStr(int ns) = 0;

    virtual void GetCurrOverFlowRecord(int ns, int64_t &lower, int64_t &upper, int64_t &current) = 0;

    virtual FlowStatus IsOverflow(int ns) = 0;

    virtual Flowrate GetFlowrate(int ns) = 0;

    virtual FlowLimit GetFlowLimit(int ns, FlowType type) = 0;

    /**
     * @return true is relaxed
     */
    virtual bool AddUp(int ns, int in, int out, int weight = 1) = 0;

    virtual bool SetFlowLimit(int ns, FlowType type, int64_t lower, int64_t upper) = 0;

    virtual bool SyncFlowLimit(const std::map<int, AllFlowLimit> &flow_limits) = 0;

    virtual bool SetDefaultLimit(int ns) = 0;

    virtual void SetAreaFlowPolicy(int ns, bool flag) = 0;
};

}
}

#endif

