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

#ifndef TAIR_STORAGE_LDB_TOOLS_LDB_UTIL_H
#define TAIR_STORAGE_LDB_TOOLS_LDB_UTIL_H

#include <map>
#include <set>

namespace leveldb {
class Comparator;

class VersionSet;
}

namespace tair {
namespace storage {
namespace ldb {
leveldb::Comparator *new_comparator(const char *cmp_desc);

void print_range(const leveldb::VersionSet &versions);

leveldb::Status open_db_readonly(const char *db_path, const char *manifest, const char *cmp_desc,
                                 leveldb::Options &options, leveldb::DB *&db);

class DataStat {
public:
    DataStat();

    ~DataStat();

    void dump(int32_t bucket, int32_t area);

    void dump_all();

    void update(int32_t bucket, int32_t area, int64_t size, bool skip, bool suc);

private:
    typedef struct Stat {
        Stat() : count_(0), suc_count_(0), suc_size_(0), skip_count_(0), fail_count_(0) {}

        void add(Stat &stat) {
            count_ += stat.count_;
            suc_count_ += stat.suc_count_;
            suc_size_ += stat.suc_size_;
            skip_count_ += stat.skip_count_;
            fail_count_ += stat.fail_count_;
        }

        int64_t count_;
        int64_t suc_count_;
        int64_t suc_size_;
        // skip count indicates count of data that exists but suffers no expected operation.
        int64_t skip_count_;
        int64_t fail_count_;
    } Stat;

    void do_destroy(std::map<int32_t, Stat *> &stats);

    void do_dump(std::map<int32_t, Stat *> &stats, int32_t key, const char *desc);

    void do_update(std::map<int32_t, Stat *> &stats, int key, int64_t size, bool skip, bool suc);

private:
    std::map<int32_t, Stat *> bucket_stats_;
    std::map<int32_t, Stat *> area_stats_;
};

class DataFilter {
public:
    class KVFilter {
    public:
        virtual bool ok(LdbKey *key, LdbItem *value) = 0;

        virtual ~KVFilter() {}
    };

    DataFilter(const char *yes_keys, const char *no_keys, KVFilter *filter = NULL);

    ~DataFilter();

    void init_set(const char *keys, std::set<int32_t> &key_set);

    bool ok(int32_t k, LdbKey *key = NULL, LdbItem *value = NULL);

private:
    std::set<int32_t> yes_keys_;
    std::set<int32_t> no_keys_;
    KVFilter *filter_;
};

}
}
}
#endif
