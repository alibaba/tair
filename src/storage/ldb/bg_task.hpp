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

#ifndef TAIR_STORAGE_LDB_BG_TASK_H
#define TAIR_STORAGE_LDB_BG_TASK_H

#include "Timer.h"

namespace leveldb {
class DB;
}

namespace tair {
namespace storage {
namespace ldb {
class LdbInstance;

class LdbCompact;

class LdbCompactTask : public tbutil::TimerTask {
    typedef struct {
        std::string start_key_;
        std::string end_key_;
    } ScanKey;

public:
    LdbCompactTask();

    virtual ~LdbCompactTask();

    virtual void runTimerTask();

    bool init(LdbInstance *db);

    void stop();

    void reset();

    void reset_time_range(const char *time_range);

    void get_files(const std::string &str_levels);

    std::string get_need_repair();

    std::string get_time_range();

    void start_repair_compact(leveldb::ManualCompactionType type, std::string &str_value, bool check);

    void stop_repair_compact(leveldb::ManualCompactionType type);

private:
    bool is_compact_time();

    bool need_compact();

    bool should_compact();

    void do_compact();

    void compact_for_gc();

    void compact_gc(GcType gc_type, bool &all_done);

    void compact_for_expired();

    void compact_for_repair();

    void build_scan_key(GcType type, int32_t key, std::vector<ScanKey> &scan_keys);

private:
    bool stop_;
    LdbInstance *db_;
    int32_t min_time_hour_;
    int32_t max_time_hour_;
    // largest filenumber in this task round
    uint64_t round_largest_filenumber_;
    tbsys::CThreadMutex lock_;
    bool is_compacting_;
    bool need_repair_;
    bool need_repair_check_;
    leveldb::ManualCompactionType repair_type_;
    std::vector<uint64_t> *repair_sst_files_;
};

typedef tbutil::Handle <LdbCompactTask> LdbCompactTaskPtr;

class LdbCleanTask {
public:
    LdbCleanTask();

    ~LdbCleanTask();

    bool start(LdbInstance *db);

    static void *clean_thread(void *argv);

    void do_clean_thread();

    bool need_clean();

    void do_clean();

    void set_max_size(int);

    void set_clean_wait_time(int);

    void stop();

private:
    LdbInstance *db_;
    uint64_t max_size_;
    int clean_wait_time_;
    pthread_t clean_thread_;
    bool stop_;
};

class BgTask {
public:
    BgTask();

    ~BgTask();

    bool start(LdbInstance *db);

    void stop();

    void restart();

    void reset_compact_gc_range(const char *time_range);

    std::string get_need_repair();

    std::string get_time_range();

    void start_repair_compact(leveldb::ManualCompactionType type, std::string &str_value, bool check = true);

    void stop_repair_compact(leveldb::ManualCompactionType type);

    void set_backup_max_size(int size);

    void set_check_clean_interval(int);

private:
    bool init_compact_task(LdbInstance *db);

    void stop_compact_task();

private:
    tbutil::TimerPtr timer_;
    LdbCompactTaskPtr compact_task_;
    LdbCleanTask *clean_task_;
};
}
}
}
#endif
