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

#include "common/define.hpp"
#include "common/log.hpp"
#include "ldb_instance.hpp"
#include "ldb_define.hpp"
#include "bg_task.hpp"
#include "db/db_impl.h"
#include <util/config.h>

namespace tair {
namespace storage {
namespace ldb {
typedef struct file_info_t {
    std::string file_name;
    struct stat file_stat;
} file_info;

bool file_compare(const file_info &file1, const file_info &file2) {
    return file1.file_stat.st_mtime > file2.file_stat.st_mtime;
}

////////// LdbCompactTask
LdbCompactTask::LdbCompactTask()
        : stop_(false), db_(NULL), min_time_hour_(0), max_time_hour_(0),
          round_largest_filenumber_(0), is_compacting_(false), need_repair_(false) {
    repair_sst_files_ = new std::vector<uint64_t>();
}

LdbCompactTask::~LdbCompactTask() {
    stop();

    if (repair_sst_files_ != NULL) {
        delete repair_sst_files_;
    }

}

void LdbCompactTask::runTimerTask() {
    if (should_compact()) {
        do_compact();
        is_compacting_ = false;
    }
}

bool LdbCompactTask::init(LdbInstance *db) {
    bool ret = db != NULL;
    if (ret) {
        db_ = db;

        const char *time_range = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_COMPACT_GC_RANGE, "2-7");
        reset_time_range(time_range);
    } else {
        log_error("leveldb not init");
    }
    return ret;
}

void LdbCompactTask::stop() {
    stop_ = true;
}

void LdbCompactTask::reset() {
    log_warn("reset compact task");
    round_largest_filenumber_ = 0;
}

void LdbCompactTask::get_files(const std::string &str_files) {
    std::vector<char *> files;
    char file_list[str_files.size() + 1];
    strncpy(file_list, str_files.c_str(), str_files.size() + 1);
    tbsys::CStringUtil::split(file_list, ",", files);
    repair_sst_files_->clear();
    for (size_t i = 0; i < files.size(); i++) {
        uint64_t file_number = atol(files.at(i));
        repair_sst_files_->push_back(file_number);
    }
}

void LdbCompactTask::start_repair_compact(leveldb::ManualCompactionType type, std::string &str_value, bool check) {
    repair_type_ = type;
    need_repair_check_ = check;
    if (type == leveldb::KCompactSelfRepairRemoveExtraSSTFile
        || type == leveldb::KCompactSelfRepairRemoveGCSSTFile
        || type == leveldb::KCompactSelfRepairRemoveCorruptionSSTFile) {
        get_files(str_value);
    }
    leveldb::config::kLimitCompactRepair = false;
    need_repair_ = true;
}

void LdbCompactTask::stop_repair_compact(leveldb::ManualCompactionType type) {
    leveldb::config::kLimitCompactRepair = true;
    need_repair_ = false;
    log_warn("stop repair compact type(%d)", type);
}

std::string LdbCompactTask::get_need_repair() {
    char buffer[64];
    snprintf(buffer, 64, "LdbCompactTaskRepair = %s",
             need_repair_ == false ? "false" : "true");
    return std::string(buffer);
}

std::string LdbCompactTask::get_time_range() {
    char buffer[64];
    snprintf(buffer, 64, "LdbCompactTaskGcOrRepairTimeRange = %d-%d",
             min_time_hour_, max_time_hour_);
    return std::string(buffer);
}

void LdbCompactTask::reset_time_range(const char *time_range) {
    if (!util::time_util::get_time_range(time_range, min_time_hour_, max_time_hour_)) {
        log_warn("config compact hour range error: %s, use default 2-7", time_range);
    }
    log_warn("compact hour range: %d - %d", min_time_hour_, max_time_hour_);
}

bool LdbCompactTask::should_compact() {
    tbsys::CThreadGuard guard(&lock_);
    bool ret = !stop_ && !is_compacting_ && is_compact_time() && need_compact();
    ret = ret && (db_->gc_factory()->can_gc() || need_repair_);
    if (ret) {
        is_compacting_ = true;
    }
    return ret;
}

bool LdbCompactTask::need_compact() {
    // // TODO: check load io, etc. .. maybe
    return true;
}

// leveldb compact has its's own mutex
void LdbCompactTask::do_compact() {
    if (need_repair_) {
        compact_for_repair();
        need_repair_ = false;
        return;
    }
    // Compact should consider gc (bucket/area) items and expired item.
    // 1. Gc's priority is highest.
    //    Rules:
    //    1). we compare current leveldb smallest filenumber with
    //        gc's file_number to determine whether this file need compact.
    //        Only files whose file_number <= smallest gc's file_number need compact.
    //    2). After Rule-1, if gc_buckets is not empty, cause buckets number matters to key range,
    //        so first compact files whose range is in gc_buckets' range.
    //    3). If Rule-2 run and gc-buckets is over, gc area. Based on key format, area is matter to range someway,
    //        compact gc-areas.
    // 2. Expired items has nothing to do with range and filenumber, so if we want to compact over all expired
    //    items, we can do nothing except compacting the whole db(that's expensive). Maybe some statics can
    //    join in the strategy.
    compact_for_gc();
    compact_for_expired();
}

void LdbCompactTask::compact_for_gc() {
    if (db_->gc_factory()->can_gc() && !db_->gc_factory()->empty()) {
        bool all_done = false;
        compact_gc(GC_BUCKET, all_done);
        if (all_done) {
            compact_gc(GC_AREA, all_done);
        }
    }
}

void LdbCompactTask::compact_for_repair() {
    log_warn("compact for repair, type(%d) need_check(%d)", repair_type_, need_repair_check_);
    leveldb::Status status;
    std::vector<uint64_t> tmp_files = *repair_sst_files_;
    status = db_->db()->CompactRepairSST(repair_type_, tmp_files, need_repair_check_);
    if (!status.ok()) {
        log_error("compact for repair error: %s, type(%d)", status.ToString().c_str(), repair_type_);
    }

    log_warn("compact for repair over, type(%d)", repair_type_);
}

void LdbCompactTask::compact_for_expired() {
    // TODO
}

void LdbCompactTask::compact_gc(GcType gc_type, bool &all_done) {
    log_debug("compact gc %d", gc_type);
    // db_->gc_factory()->try_evict();
    all_done = db_->gc_factory()->empty(gc_type);

    if (!all_done)          // not evict all
    {
        GcNode gc_node = db_->gc_factory()->pick_gc_node(gc_type);
        DUMP_GCNODE(info, gc_node, "pick gc node, type: %d", gc_type);
        if (gc_node.key_ < 0) {
            all_done = true;
            log_warn("[%d] gc all done, type: %d", db_->index(), gc_type);
        } else {
            uint32_t start_time = time(NULL);
            // get scan key to gc
            std::vector<ScanKey> gc_scan_keys;
            build_scan_key(gc_type, gc_node.key_, gc_scan_keys);

            if (round_largest_filenumber_ <= 0) {
                // new task round, reset largest_filenumber_,
                // 'cause in one new task round, every file whose filenumber is large than round_largest_filenumber
                // is generated after gced(ShouldDrop() filter)
                get_db_stat(db_->db(), round_largest_filenumber_, "largest-filenumber");
                log_warn("[%d] new task round, round_largest_filenumber: %"PRI64_PREFIX"u",
                         db_->index(), round_largest_filenumber_);
            }

            uint64_t limit_filenumber = std::max(round_largest_filenumber_, gc_node.file_number_);

            DUMP_GCNODE(warn, gc_node,
                        "[%d] gc type: %d, count: %zu, start time: %s, limit filenumber: %"PRI64_PREFIX"u",
                        db_->index(), gc_type, gc_scan_keys.size(),
                        tair::util::time_util::time_to_str(start_time).c_str(), limit_filenumber);

            size_t i = 0;
            for (i = 0; !stop_ && db_->gc_factory()->can_gc() && i < gc_scan_keys.size(); ++i) {
                leveldb::Slice comp_start(gc_scan_keys[i].start_key_), comp_end(gc_scan_keys[i].end_key_);
                leveldb::Status status = db_->db()->
                        CompactRangeSelfLevel(limit_filenumber, &comp_start, &comp_end);

                if (!status.ok()) {
                    DUMP_GCNODE(error, gc_node, "[%d] gc fail. type: %d, error: %s",
                                db_->index(), gc_type, status.ToString().c_str());
                    break;
                }
                // just have a rest..
                ::sleep(1);
            }

            if (i >= gc_scan_keys.size()) // all is ok
            {
                DUMP_GCNODE(warn, gc_node, "[%d] gc success. type: %d, cost: %ld",
                            db_->index(), gc_type, (time(NULL) - start_time));
                db_->gc_factory()->remove(gc_node, gc_type);
                // // may can evict some
                // db_->gc_factory()->try_evict();
                all_done = db_->gc_factory()->empty(gc_type);
                if (all_done) {
                    log_warn("[%d] gc all done, type: %d", db_->index(), gc_type);
                }
            }
        }
    }
}

void LdbCompactTask::build_scan_key(GcType type, int32_t key, std::vector<ScanKey> &scan_keys) {
    switch (type) {
        case GC_BUCKET: {
            ScanKey scan_key;
            LdbKey::build_scan_key(key, scan_key.start_key_, scan_key.end_key_);
            scan_keys.push_back(scan_key);
            break;
        }
        case GC_AREA: {
            // build each scan key with one bucket with area
            std::vector<int32_t> buckets;
            db_->get_buckets(buckets);
            for (size_t i = 0; i < buckets.size(); ++i) {
                ScanKey scan_key;
                LdbKey::build_scan_key_with_area(buckets[i], key, scan_key.start_key_, scan_key.end_key_);
                scan_keys.push_back(scan_key);
            }
            break;
        }
        default:
            log_error("[%d] invalid gc type to build scan key, type: %d, key: %d", db_->index(), type, key);
            break;
    }
}

bool LdbCompactTask::is_compact_time() {
    return (min_time_hour_ != max_time_hour_) && util::time_util::is_in_range(min_time_hour_, max_time_hour_);
}

////////// BgTask
BgTask::BgTask() : timer_(0), compact_task_(0), clean_task_(NULL) {

}

BgTask::~BgTask() {
    stop();
    if (clean_task_) {
        delete clean_task_;
        clean_task_ = NULL;
    }
}


bool BgTask::start(LdbInstance *db) {
    bool ret = db != NULL;
    if (ret && timer_ == 0) {
        timer_ = new tbutil::Timer();
        ret = init_compact_task(db);
    }

    clean_task_ = new LdbCleanTask();
    clean_task_->start(db);
    return ret;
}

void BgTask::stop() {
    if (timer_ != 0) {
        stop_compact_task();
        timer_->destroy();
        timer_ = 0;
    }

    if (clean_task_ != NULL) {
        clean_task_->stop();
    }
}

void BgTask::restart() {
    if (compact_task_ != 0) {
        compact_task_->reset();
    }
}

void BgTask::reset_compact_gc_range(const char *time_range) {
    if (compact_task_ != 0) {
        compact_task_->reset_time_range(time_range);
    }
}

bool BgTask::init_compact_task(LdbInstance *db) {
    bool ret = false;
    if (timer_ != 0) {
        compact_task_ = new LdbCompactTask();
        if ((ret = compact_task_->init(db))) {
            ret =
                    timer_->scheduleRepeated(compact_task_, tbutil::Time::seconds(
                            TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_CHECK_COMPACT_INTERVAL, 60))) == 0;
            log_info("schedule compact task %d", ret);
        } else {
            log_error("init compact task fail");
        }
    }
    return ret;
}

void BgTask::stop_compact_task() {
    if (timer_ != 0 && compact_task_ != 0) {
        timer_->cancel(compact_task_);
        compact_task_->stop();
        compact_task_ = 0;
    }
}

void BgTask::start_repair_compact(leveldb::ManualCompactionType type, std::string &str_value, bool check) {
    if (compact_task_ != 0) {
        compact_task_->start_repair_compact(type, str_value, check);
    }
}

void BgTask::stop_repair_compact(leveldb::ManualCompactionType type) {
    if (compact_task_ != 0) {
        compact_task_->stop_repair_compact(type);
    }
}

std::string BgTask::get_need_repair() {
    if (compact_task_ != 0) {
        return compact_task_->get_need_repair();
    }

    return std::string();
}

std::string BgTask::get_time_range() {
    if (compact_task_ != 0) {
        return compact_task_->get_time_range();
    }

    return std::string();
}

void BgTask::set_backup_max_size(int size) {
    if (clean_task_ != NULL) {
        clean_task_->set_max_size(size);
    }
}

void BgTask::set_check_clean_interval(int interval) {
    if (clean_task_ != NULL) {
        clean_task_->set_clean_wait_time(interval);
    }
}

LdbCleanTask::LdbCleanTask() : db_(NULL), stop_(false) {

}

LdbCleanTask::~LdbCleanTask() {
    stop();
    pthread_join(clean_thread_, NULL);
}

void LdbCleanTask::set_max_size(int size) {
    max_size_ = size * 1024 * 1024;
}

void LdbCleanTask::set_clean_wait_time(int sec) {
    clean_wait_time_ = sec;
}

bool LdbCleanTask::start(LdbInstance *db) {
    db_ = db;

    clean_wait_time_ = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_CHECK_CLEAN_INTERVAL, 600);
    max_size_ = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BACKUP_MAX_SIZE, 3) * 1024 * 1024;

    if (pthread_create(&clean_thread_, NULL, clean_thread, this) < 0) {
        log_error("start clean thread failed(%s)", strerror(errno));
        return false;
    }


    return true;
}

void *LdbCleanTask::clean_thread(void *argv) {

    LdbCleanTask *cleanTask = (LdbCleanTask *) argv;
    cleanTask->do_clean_thread();
    return NULL;

}

void LdbCleanTask::do_clean_thread() {

    while (true) {
        if (stop_) {
            break;
        }
        if (need_clean()) {
            do_clean();
        }

        int wait = 0;
        while (wait < clean_wait_time_) {
            if (stop_) {
                break;
            }
            sleep(1);
            wait++;
        }
    }

}

bool LdbCleanTask::need_clean() {
    return true;
}

void LdbCleanTask::stop() {
    stop_ = true;
}

void LdbCleanTask::do_clean() {
    log_warn("do clean, max size(%"PRI64_PREFIX"u)", max_size_);
    leveldb::DBImpl *db = dynamic_cast<leveldb::DBImpl *>(db_->db());
    leveldb::Env *db_env = db->GetEnv();
    std::vector<std::string> filenames;
    const std::string db_backup_dir =
            std::string(db_->db_path()) + std::string("/") + std::string(leveldb::DBImpl::GetBackupDir()) +
            std::string("/");
    leveldb::Status s = db_env->GetChildren(db_backup_dir, &filenames);
    if (!s.ok()) {
        log_error("get filenames failed(%s)", s.ToString().c_str());
        return;
    }

    //sort file by mtime
    std::vector<file_info> files;
    for (size_t indx = 0; indx < filenames.size(); ++indx) {
        file_info file;
        file.file_name = filenames[indx];
        std::string file_path = db_backup_dir + file.file_name;
        struct stat sbuf;
        if (stat(file_path.c_str(), &sbuf) == 0) {
            memcpy(&file.file_stat, &sbuf, sizeof(struct stat));
            files.push_back(file);
        } else {
            log_error("get file stat failed(%s)", strerror(errno));
        }
    }
    std::sort(files.begin(), files.end(), file_compare);

    uint64_t total_size = 0;
    size_t indx = 0;
    for (indx = 0; indx < files.size(); indx++) {
        if (files[indx].file_name == "." || files[indx].file_name == "..") {
            continue;
        }
        if (stop_) {
            break;
        }
        std::string file_path = db_backup_dir + files[indx].file_name;
        if (total_size >= max_size_) {
            s = db_env->DeleteFile(file_path);
            if (s.ok()) {
                log_warn("remove file(%s)", file_path.c_str());
            } else {
                log_error("remove file(%s) failed(%s)", file_path.c_str(), s.ToString().c_str());
            }
            sleep(1);
        } else {
            total_size += (files[indx].file_stat.st_size / 1024);
        }
    }
}

}
}
}
