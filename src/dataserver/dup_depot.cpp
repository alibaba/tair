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

#include "common/data_entry.hpp"

#include "tair_manager.hpp"
#include "dup_depot.hpp"
#include "update_log.hpp"

namespace tair {
//////////////////////////////// DupDepot
const char *DupDepot::DUP_DATA_FILE_NAME = "dupdata";

DupDepot::DupDepot(tair_manager *manager) :
        manager_(manager), failover_log_(NULL) {
}

DupDepot::~DupDepot() {
    if (failover_log_ != NULL) {
        failover_log_->close();
    }
}

int DupDepot::init() {
    int ret = TAIR_RETURN_SUCCESS;
    std::string data_dir = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_DUP_DEPOT_DIR, "./data/dupdepot");
    std::string tmp_dir = data_dir;
    if (!tbsys::CFileUtil::mkdirs(const_cast<char *>(tmp_dir.c_str()))) {
        log_error("make dup depot data directory fail: %s", data_dir.c_str());
        ret = TAIR_RETURN_FAILED;
    } else {
        // use update_log
        const char *log_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_ULOG_FAILOVER_BASENAME,
                                                           TAIR_ULOG_FAILOVER_DEFAULT_BASENAME);
        // use the same configuration as migrate log
        int32_t log_file_number = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_ULOG_FILENUM, TAIR_ULOG_DEFAULT_FILENUM);
        int32_t log_file_size = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_ULOG_FILESIZE, TAIR_ULOG_DEFAULT_FILESIZE);
        log_file_size *= MB_SIZE;
        failover_log_ = update_log::open(data_dir.c_str(), log_file_name, log_file_number, false, log_file_size);
    }
    return ret;
}

void DupDepot::add(tair::sn_operation_type type, tair::common::data_entry &key, tair::common::data_entry &value,
                   int32_t bucket_number) {
    failover_log_->log(type, key, value, bucket_number);
}

update_log *DupDepot::get_failover_log() {
    return failover_log_;
}

void DupDepot::clear() {
    failover_log_ = failover_log_->reset();
}

}
