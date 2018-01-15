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

#include "stat_manager.h"

namespace tair {
namespace stat {

class StatManagerMock : public StatManager {

public:
    StatManagerMock() {
    }

    bool Initialize() {
        return true;
    }

    ~StatManagerMock() {
    }

    void AddUp(uint32_t ip, uint16_t ns,
               uint16_t op, uint32_t in,
               uint32_t out, uint32_t cnt) {
    }

    Flowrate Measure(uint32_t ip, uint16_t ns, uint16_t op) {
        Flowrate rate = {0, DOWN, 0, DOWN, 0, DOWN};
        return rate;
    }

    bool SwitchAndScheduleDump() {
        return true;
    }
};

StatManager *StatManager::NewInstance() {
    return new StatManagerMock();
}
}
}

