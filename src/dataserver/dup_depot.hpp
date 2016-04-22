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

#ifndef TAIR_DATASERVER_DUPDEPOT_H
#define TAIR_DATASERVER_DUPDEPOT_H

#include <vector>
#include <string>

#include <tbsys.h>

namespace tair {
namespace common {
class data_entry;
}
class tair_manager;

class update_log;

class DupDepot {
public:
    explicit DupDepot(tair_manager *manager);

    ~DupDepot();

    const static char *DUP_DATA_FILE_NAME;

    int init();

    void add(tair::sn_operation_type type, tair::common::data_entry &key, tair::common::data_entry &value,
             int32_t bucket_number);

    update_log *get_failover_log();

    int start_recover();

    int stop_recover();

    int finish_recover();

    void clear();

private:
    tair_manager *manager_;
    update_log *failover_log_;
};
}
#endif
