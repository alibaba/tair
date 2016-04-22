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

#ifndef I_REMOTE_SYNC_MANAGER_HPP_
#define I_REMOTE_SYNC_MANAGER_HPP_

#include "common/define.hpp"
#include "common/record.hpp"

#include <map>

namespace tair {
namespace common {
class data_entry;
}

const int64_t BASE_WAIT_US = 1000; // 1ms
const int64_t DEFAULT_WAIT_US = 1000000; // 1s
const int64_t REST_WAIT_US = 10000;      // 10ms
const int64_t SPEED_CONTROL_WAIT_US = 2000; // 2ms

class IRemoteSyncManager {
public:
    enum {
        OLD_VERSION = 0,
        NEW_VERSION = 1
    };

public:
    IRemoteSyncManager(int version) { version_ = version; }

    virtual ~IRemoteSyncManager() {}

    virtual int add_record(TairRemoteSyncType type, common::data_entry *key, common::data_entry *value) {
        return TAIR_RETURN_SUCCESS;
    }

    virtual int set_mtime_care(bool care) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

public:
    virtual std::string get_rsync_info() { return "not support get rsync info"; }

    virtual int init()=0;

    virtual void pause(bool p)=0;

    virtual int pause(bool p, std::string unit, std::string group) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int watch(const std::string unit, const std::string group) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual int statistics(const std::string unit, const std::string group) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual std::string options(const std::string unit, const std::string group) { return ""; }

    virtual int options(const std::string unit, const std::string group, std::map<std::string, int> &m) {
        return TAIR_RETURN_NOT_SUPPORTED;
    }

    virtual void set_wait_us(int64_t us)=0;

    virtual int64_t get_wait_us()=0;

    virtual std::string get_rsync_config_service() { return ""; }

    virtual void set_rsync_config_service(const std::string &uri) {}

    virtual int get_rsync_config_update_interval() { return 0; }

    virtual void set_rsync_config_update_interval(int interval) {}

    virtual std::string get_cluster_info() { return ""; }

    virtual std::string connection_stat() { return ""; }

public:
    int version_;
};
}

#endif
