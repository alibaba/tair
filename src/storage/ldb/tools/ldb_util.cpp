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

#include "leveldb/env.h"
#include "db/version_set.h"

#include "common/util.hpp"

#include "ldb_define.hpp"
#include "ldb_comparator.hpp"
#include "ldb_util.hpp"

namespace tair {
namespace storage {
namespace ldb {

leveldb::Comparator *new_comparator(const char *cmp_desc) {
    leveldb::Comparator *cmp = NULL;
    if (cmp_desc == NULL) {
        cmp = NULL;
    } else if (strcmp(cmp_desc, "bitcmp") == 0) {
        cmp = new BitcmpLdbComparatorImpl(NULL);
    } else if (strncmp(cmp_desc, "numeric", strlen("numeric")) == 0) {
        std::vector<std::string> strs;
        tair::util::string_util::split_str(cmp_desc, ", ", strs);
        if (strs.size() != 3) {
            fprintf(stderr, "numeric description error: %s, format like: numeric,:,2\n", cmp_desc);
            cmp = NULL;
        } else {
            cmp = new NumericalComparatorImpl(NULL, strs[1].c_str()[0], atoi(strs[2].c_str()));
        }
    } else {
        fprintf(stderr, "unkonwn cmp description: %s\n", cmp_desc);
    }
    return cmp;
}

void print_range(const leveldb::VersionSet &versions) {
    char buf[128];
    snprintf(buf, sizeof(buf), "sequence: %lu, filenumber: %lu, lognumber: %lu, filecount: %ld, filesize: %ld\n",
             versions.LastSequence(), versions.NextFileNumber(), versions.LogNumber(),
             versions.NumFiles(), versions.NumBytes());
    std::string result;
    result.append(buf);
    versions.current()->GetAllRange(result, ldb_key_printer);
    fprintf(stderr, "%s\n", result.c_str());
}

leveldb::Status open_db_readonly(const char *db_path, const char *manifest,
                                 const char *cmp_desc, leveldb::Options &options, leveldb::DB *&db) {
    options.comparator = new_comparator(cmp_desc);
    if (options.comparator == NULL) {
        return leveldb::Status::InvalidArgument("invalid comparator description");
    }

    options.kDoSeekCompaction = false;
    options.error_if_exists = false; // exist is ok
    options.create_if_missing = false;
    options.table_cache_size = 20 << 20;
    options.env = leveldb::Env::Instance();

    char buf[32];
    snprintf(buf, sizeof(buf), "./tmp_ldb_log.%d", getpid());
    leveldb::Status s = options.env->NewLogger(buf, &options.info_log);
    if (s.ok()) {
        s = leveldb::DB::Open(options, db_path, manifest, &db);
    }

    if (!s.ok()) {
        log_error("open db with mainfest fail: %s", s.ToString().c_str());
        delete options.comparator;
        delete options.env;
        if (options.info_log != NULL) {
            delete options.info_log;
        }
    }

    return s;
}


DataStat::DataStat() {
}

DataStat::~DataStat() {
    do_destroy(bucket_stats_);
    do_destroy(area_stats_);
}

void DataStat::dump(int32_t bucket, int32_t area) {
    if (bucket >= 0) {
        do_dump(bucket_stats_, bucket, "=== one bucket stats ===");
    }
    if (area >= 0) {
        do_dump(area_stats_, area, "=== one area stats ===");
    }
}

void DataStat::dump_all() {
    do_dump(bucket_stats_, -1, "=== all bucket stats ===");
    do_dump(area_stats_, -1, "=== all area stats ===");
}

void DataStat::update(int32_t bucket, int32_t area, int64_t size, bool skip, bool suc) {
    do_update(bucket_stats_, bucket, size, skip, suc);
    do_update(area_stats_, area, size, skip, suc);
}

void DataStat::do_destroy(std::map<int32_t, Stat *> &stats) {
    for (std::map<int32_t, Stat *>::iterator it = stats.begin(); it != stats.end(); ++it) {
        delete it->second;
    }
    stats.clear();
}

void DataStat::do_dump(std::map<int32_t, Stat *> &stats, int32_t key, const char *desc) {
    Stat *stat = NULL;
    fprintf(stderr, "%s\n", desc);
    fprintf(stderr, "%5s%12s%12s%12s%12s%12s\n", "key", "totalCount", "sucCount", "sucSize", "failCount", "skipCount");
    // dump specified key stat
    if (key >= 0) {
        std::map<int32_t, Stat *>::iterator it = stats.find(key);
        if (it == stats.end()) {
            fprintf(stderr, "NONE STATS\n");
        } else {
            stat = it->second;
            fprintf(stderr, "%5d%12ld%12ld%12ld%12ld%12ld\n", it->first, stat->count_, stat->suc_count_,
                    stat->suc_size_,
                    stat->fail_count_, stat->skip_count_);
        }
    } else                        // dump all stats
    {
        Stat total_stat;
        for (std::map<int32_t, Stat *>::iterator it = stats.begin(); it != stats.end(); ++it) {
            stat = it->second;
            total_stat.add(*stat);
            fprintf(stderr, "%5d%12ld%12ld%12ld%12ld%12ld\n", it->first, stat->count_, stat->suc_count_,
                    stat->suc_size_,
                    stat->fail_count_, stat->skip_count_);
        }
        fprintf(stderr, "%5s%12ld%12ld%12ld%12ld%12ld\n", "total", total_stat.count_, total_stat.suc_count_,
                total_stat.suc_size_,
                total_stat.fail_count_, total_stat.skip_count_);
    }
}

void DataStat::do_update(std::map<int32_t, Stat *> &stats, int key, int64_t size, bool skip, bool suc) {
    if (key >= 0) {
        std::map<int32_t, Stat *>::iterator it = stats.find(key);
        Stat *stat = NULL;
        if (it == stats.end()) {
            stat = new Stat();
            stats[key] = stat;
        } else {
            stat = it->second;
        }
        ++stat->count_;
        if (skip)                 // skip ignore success or failure
        {
            ++stat->skip_count_;
        } else if (suc) {
            ++stat->suc_count_;
            stat->suc_size_ += size;
        } else {
            ++stat->fail_count_;
        }
    }
}


DataFilter::DataFilter(const char *yes_keys, const char *no_keys, KVFilter *filter) {
    init_set(yes_keys, yes_keys_);
    init_set(no_keys, no_keys_);
    filter_ = filter;
}

DataFilter::~DataFilter() {
}

void DataFilter::init_set(const char *keys, std::set<int32_t> &key_set) {
    if (keys != NULL) {
        std::vector<std::string> tmp_keys;
        tair::util::string_util::split_str(keys, ", ", tmp_keys);
        for (size_t i = 0; i < tmp_keys.size(); ++i) {
            key_set.insert(atoi(tmp_keys[i].c_str()));
        }
    }
}

bool DataFilter::ok(int32_t k, LdbKey *key, LdbItem *value) {
    // yes_keys is not empty and match specified key, then no_keys will be ignored
    bool empty = false;
    bool ret = ((empty = yes_keys_.empty()) || yes_keys_.find(k) != yes_keys_.end()) &&
               (!empty || no_keys_.empty() || no_keys_.find(k) == no_keys_.end());
    if (ret && filter_ != NULL && key != NULL && value != NULL) {
        ret = filter_->ok(key, value);
    }
    return ret;
}

}
}
}
