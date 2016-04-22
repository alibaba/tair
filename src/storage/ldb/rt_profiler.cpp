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

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <cstring>
#include <sys/time.h>
#include "log.hpp"
#include "rt_profiler.hpp"

namespace tair {
namespace storage {

RTProfiler g_rt_profiler;

RTProfiler::RTProfiler() {
    memset(rtos_, 0, sizeof(rtos_));
    active_ = false;
    dump_file_ = "ldb_rt_dump.txt";
}

RTProfiler::~RTProfiler() {
}

const char *RTProfiler::probe_name(uint32_t seq) {
    switch (seq) {
        case PSN_LDB_MANAGER_PUT:
            return "LdbManager::put";
        case PSN_DO_PUT:
            return "LdbManager::do_put";
        case PSN_LEVELDB_PUT:
            return "leveldb::Put";
        case PSN_LEVELDB_WRITEBATCH_PUT:
            return "leveldb::WriteBatch::Put";
        case PSN_LEVELDB_WRITE:
            return "leveldb::Write";
        case PSN_LEVELDB_CV_WAIT:
            return "leveldb::Write::cv.wait";
        case PSN_LEVELDB_LOG_ADD_RECORD:
            return "leveldb::Write::log::AddRecord";
        case PSN_LEVELDB_INSERT_MMT:
            return "leveldb::Write::InsertInto";
        case PSN_LEVELDB_MAKE_ROOM_FOR_WRITE:
            return "leveldb::MakeRoomForWrite";
        case PSN_LEVELDB_MAKEROOM_SLEEP:
            return "levedb::MakeRoom::sleep";
        case PSN_LEVELDB_MAKEROOM_BGWAIT_IMM:
            return "leveldb::MakeRoom::BgWaitImm";
        case PSN_LEVELDB_MAKEROOM_BGWAIT_LEVEL0:
            return "leveldb::MakeRoom::BgWaitLevel0";
        case PSN_LDB_MANAGER_GET:
            return "LdbManager::get";
        case PSN_DO_GET:
            return "LdbManager::do_get";
        case PSN_LEVELDB_GET:
            return "leveldb::Get";
        case PSN_MMT_GET:
            return "leveldb::Memtable::Get";
        case PSN_MMT_ITER_SEEK:
            return "leveldb::MMT::Seek";
        case PSN_SST_GET:
            return "leveldb::SST::Get";
        case PSN_CACHE_GET:
            return "cache::raw_get";
        case PSN_CACHE_PUT:
            return "cache::raw_put";
        case PSN_CACHE_DEL:
            return "cache::raw_remove";
        case PSN_DB_MUTEX_LOCK:
            return "LdbManager::mutex";
        case PSN_LEVELDB_MUTEX_LOCK:
            return "leveldb::mutex";
        case PSN_LDB_MANAGER_RANGE:
            return "LdbManager::get_range";
    }
    return "Unknown";
}

void RTProfiler::reset() {
    memset(rtos_, 0, sizeof(rtos_));
}

void RTProfiler::dump() {
    FILE *file = fopen(dump_file_, "w");
    if (file == NULL) {
        file = stderr;
    }
    fprintf(file, ">>>>>>>>>>>>>RTProfiler Dumping>>>>>>>>>>>>>\n");
    for (uint32_t seq = 0; seq < PSN_MAX; ++seq) {
        if (rtos_[seq].opcnt_ != 0) {
            fprintf(file, "===========%s============\n", probe_name(seq));
            rtos_[seq].dump(file);
        }
    }
    fprintf(file, "<<<<<<<<<<<<<RTProfiler Dumping Done<<<<<<<<<<<<<<<\n");
    if (file != stderr) {
        fclose(file);
    }
}

void RTProfiler::do_switch() {
    active_ = !active_;
    log_error("RTProfiler's Status: %s", active_ ? "on" : "off");
}

void RTProfiler::RTObject::dump(FILE *file) {
    char buf[1024];
    size_t size = sizeof(buf);
    size_t len = 0;
    len += snprintf(buf + len, size - len,
                    "n: %lu, acc(us): %lu, avg(us): %lu, max(us): %lu, dist: {\n\t",
                    opcnt_, rt_us_acc_, rt_us_acc_ / opcnt_, rt_us_max_);
    uint32_t i = 0;
    for (; i < (uint32_t) RT_ORDER_MAX; ++i) {
        if (rt_buckets_[i] != 0) {
            len += snprintf(buf + len, size - len,
                            "<%uus: %lu(%.2lf%%),\n\t",
                            1 << i, rt_buckets_[i], (double) rt_buckets_[i] / opcnt_ * 100);
        }
    }
    len += snprintf(buf + len, size - len,
                    ">%uus: %lu(%.2lf%%)\n}\n",
                    1 << (i - 1), rt_buckets_[i], (double) rt_buckets_[i] / opcnt_ * 100);
    fprintf(file, "%s", buf);
}

}
}
