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
    fprintf(stderr, "%s: output bucket distribution by manifestfile.\n"
            "\t-f manifestfile -c comparator\n"
            "\tcomparator like: bitcmp OR numeric,:,2\n"
            "\t-v verbose output\n", name);
}

uint64_t bucket_size[leveldb::config::kNumLevels][MAX_BUCKET_NUMBER] = {{0}};
uint64_t all_bucket_size[MAX_BUCKET_NUMBER] = {0};
std::set<uint64_t> bucket_files[MAX_BUCKET_NUMBER];

int output_bucket_distribution(const leveldb::VersionSet &versions) {
    std::set<int> bucket_distribution[leveldb::config::kNumLevels];

    for (int l = 0; l < leveldb::config::kNumLevels; ++l) {
        std::vector<leveldb::FileMetaData *> &metas = versions.current()->FileMetas()[l];
        for (size_t indx = 0; indx < metas.size(); indx++) {
            leveldb::FileMetaData *file_meta = metas[indx];
            int small_bucket = LdbKey::get_bucket_number(file_meta->smallest.user_key().data());
            int large_bucket = LdbKey::get_bucket_number(file_meta->largest.user_key().data());
            if (small_bucket != large_bucket) {
                printf("file %"PRI64_PREFIX"u contains more than one bucket, small(%d) large(%d)\n", file_meta->number,
                       small_bucket, large_bucket);
                continue;
            }
            int bucket = small_bucket;
            if (bucket_distribution[l].find(bucket) == bucket_distribution[l].end()) {
                bucket_distribution[l].insert(bucket);
            }

            bucket_size[l][bucket] += (file_meta->file_size / 1024);
            bucket_files[bucket].insert(file_meta->number);
            all_bucket_size[bucket] += (file_meta->file_size / 1024);
        }
    }

    for (int l = 0; l < leveldb::config::kNumLevels; ++l) {
        printf("level %d: ", l);
        int count = 0;
        for (std::set<int>::const_iterator iter = bucket_distribution[l].begin();
             iter != bucket_distribution[l].end(); ++iter) {
            printf(" %04d[%08"PRI64_PREFIX"u]", *iter, bucket_size[l][*iter]);
            count++;
            if (count > 5) {
                printf("\nlevel %d: ", l);
                count = 0;
            }
        }
        printf("\n");

    }

    printf("all buckets:\n");
    for (int indx = 0; indx < MAX_BUCKET_NUMBER; ++indx) {
        if (all_bucket_size[indx] > 0) {
            printf("bucket %4d size %08"PRI64_PREFIX"u\n", indx, all_bucket_size[indx]);
        }
    }


    if (g_verbose) {
        for (int indx = 0; indx < MAX_BUCKET_NUMBER; ++indx) {
            std::set<uint64_t> &single_bucket_files = bucket_files[indx];
            if (!single_bucket_files.empty()) {
                printf("bucket[%04d]: ", indx);
                int count = 0;
                for (std::set<uint64_t>::const_iterator iter = single_bucket_files.begin();
                     iter != single_bucket_files.end(); ++iter) {
                    printf(" %08"PRI64_PREFIX"u ", *iter);
                    count++;
                    if (count > 5) {
                        printf("\nbucket[%04d]: ", indx);
                        count = 0;
                    }
                }
                printf("\n");
            }
        }
    }

    return 0;
}

int main(int argc, char *argv[]) {
    int i;
    char *manifest = NULL;
    char *cmp_desc = NULL;

    while ((i = getopt(argc, argv, "f:c:v")) != EOF) {
        switch (i) {
            case 'f':
                manifest = optarg;
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

    if (manifest == NULL || cmp_desc == NULL) {
        print_help(argv[0]);
        return 1;
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
        ret = output_bucket_distribution(versions);
    }

    delete options.comparator;
    delete options.env;

    return ret;
}
