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

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "db/version_set.h"
#include "db/filename.h"
#include "db/log_writer.h"

#include "ldb_define.hpp"
#include "ldb_comparator.hpp"

#include "ldb_util.hpp"

using namespace tair::storage::ldb;

static bool g_verbose = false;

void print_help(const char *name) {
    fprintf(stderr,
            "%s: pick sstable by limited levels, output filenumber rename file and manifest of picked sstable.\n"
                    "\t-f manifestfile -c comparator -l limit -s start_num\n"
                    "\tcomparator like: bitcmp OR numeric,:,2\n"
                    "\t-v verbose output\n", name);
}

int filter_sst(const leveldb::Options &options, const leveldb::VersionSet &versions,
               int32_t limit, const std::set<int> &buckets, uint64_t start_filenumber) {
    // get buckets
    std::string name_suffix("-000003");

    // open map file
    FILE *num_map_file = fopen((std::string("./filenumber_map") + name_suffix).c_str(), "w");
    if (num_map_file == NULL) {
        fprintf(stderr, "open filenumer map file fail: %s\n", strerror(errno));
        return 1;
    }

    int32_t smallest_bucket = 0, largest_bucket = 0;

    uint64_t filenumber = start_filenumber;
    leveldb::FileMetaData *f = NULL;
    leveldb::VersionEdit edit;
    bool should_add = true;
    int hit_cnt = 0;

    for (int l = 0; l < leveldb::config::kNumLevels && l <= limit; ++l) {
        std::vector<leveldb::FileMetaData *> &metas = versions.current()->FileMetas()[l];

        for (size_t i = 0; i < metas.size(); ++i) {
            should_add = true;
            f = metas[i];
            if (l == limit && !buckets.empty()) {
                should_add = false;
                smallest_bucket = LdbKey::decode_bucket_number(f->smallest.user_key().data() + LDB_EXPIRED_TIME_SIZE);
                largest_bucket = LdbKey::decode_bucket_number(f->largest.user_key().data() + LDB_EXPIRED_TIME_SIZE);
                // [smallest, largest]
                for (int32_t b = smallest_bucket; b <= largest_bucket; ++b) {
                    if (buckets.find(b) != buckets.end()) {
                        std::cout << "#$ target bucket(" << b << ") found in level(" << l << "), file_number is "
                                  << f->number << std::endl;
                        should_add = true;
                        hit_cnt++;
                        break;
                    }
                }
            }
            if (should_add) {
                // add new manifest
                edit.AddFile(l, filenumber, f->file_size, f->smallest, f->largest);
                fprintf(num_map_file, "%lu %lu\n", f->number, filenumber);
                filenumber++;
            }
        }
    }

    std::cout << hit_cnt << " file(s) contains specified bucket" << std::endl;

    // write new manifest(descriptor) file
    std::string filename = std::string("MANIFEST") + name_suffix;
    leveldb::WritableFile *desc_file = NULL;
    leveldb::Status s = options.env->NewWritableFile(filename, &desc_file);
    if (s.ok()) {
        leveldb::log::Writer desc_log(desc_file);

        edit.SetComparatorName(options.comparator->Name());
        edit.SetLogNumber(0);         // ignore lognumber
        edit.SetNextFile(filenumber); // real filenumber
        edit.SetLastSequence(versions.LastSequence());

        std::string record;
        edit.EncodeTo(&record);
        s = desc_log.AddRecord(record);

        if (s.ok()) {
            s = desc_file->Sync();
        }
    }

    if (desc_file != NULL) {
        delete desc_file;
    }

    fclose(num_map_file);

    if (s.ok()) {
        // check
        leveldb::InternalKeyComparator icmp(options.comparator);
        leveldb::VersionSet result_versions("./", &options, NULL, &icmp);
        s = result_versions.Recover(filename.c_str());
        if (!s.ok()) {
            fprintf(stderr, "check manifest fail: %s\n", s.ToString().c_str());
        } else {
            if (g_verbose) {
                fprintf(stderr, "result db range %s:\n", filename.c_str());
                print_range(result_versions);
            }
        }
    }

    if (!s.ok()) {
        fprintf(stderr, "fail with %s\n", s.ToString().c_str());
    }

    return s.ok() ? 0 : 1;
}

int main(int argc, char *argv[]) {
    int i;
    char *manifest = NULL;
    char *cmp_desc = NULL;
    int64_t start_num = 0;
    char *buckets_str = NULL;
    // the meaning of limit:
    // add all files of 0~lmiit-1
    // add specified bucket files of limit
    int32_t limit = 0;

    while ((i = getopt(argc, argv, "f:b:l:s:c:v")) != EOF) {
        switch (i) {
            case 'f':
                manifest = optarg;
                break;
            case 'b':
                buckets_str = optarg;
                break;
            case 'l':
                limit = atoll(optarg);
                break;
            case 's':
                start_num = atoll(optarg);
                break;
            case 'c':
                cmp_desc = optarg;
                break;
            case 'v':
                g_verbose = true;
                break;
            default:
                print_help(argv[0]);
                break;
        }
    }

    if (manifest == NULL || limit <= 0 || start_num <= 0) {
        print_help(argv[0]);
        return 1;
    }

    std::set<int32_t> buckets;
    if (buckets_str != NULL) {
        std::vector<std::string> bucket_strs;
        tair::util::string_util::split_str(buckets_str, ", ", bucket_strs);
        for (size_t i = 0; i < bucket_strs.size(); ++i) {
            buckets.insert(atoi(bucket_strs[i].c_str()));
        }
    }

    int ret = 0;
    leveldb::Options options;
    options.comparator = new_comparator(cmp_desc);
    if (options.comparator == NULL) {
        print_help(argv[0]);
        return 1;
    }
    options.error_if_exists = false; // exist is ok
    options.create_if_missing = false; // create if not exist
    options.env = leveldb::Env::Instance();
    leveldb::InternalKeyComparator icmp(options.comparator);
    leveldb::VersionSet versions("./", &options, NULL, &icmp);
    leveldb::Status s = versions.Recover(manifest);

    if (!s.ok()) {
        fprintf(stderr, "load manifest fail: %s\n", s.ToString().c_str());
        ret = 1;
    } else {
        if (g_verbose) {
            fprintf(stderr, "input db range %s:\n", manifest);
            print_range(versions);
        }

        ret = filter_sst(options, versions, limit, buckets, start_num);
    }

    delete options.comparator;
    delete options.env;

    return ret;
}
