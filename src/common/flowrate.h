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

#ifndef FLOW_RATE_H
#define FLOW_RATE_H

#include <cstddef>
#include <stdint.h>

namespace tair {
namespace stat {

enum FlowType {
    IN = 0, OUT = 1, OPS = 2
};

enum FlowStatus {
    DOWN = 0, KEEP = 1, UP = 2
};

struct FlowLimit {
    int64_t lower;
    int64_t upper;
};

struct AllFlowLimit {
    FlowLimit in;
    FlowLimit out;
    FlowLimit ops;
};

struct Flowrate {
    int64_t in;
    FlowStatus in_status;
    int64_t out;
    FlowStatus out_status;
    int64_t ops;
    FlowStatus ops_status;

    FlowStatus status;
};

}
}

#endif

