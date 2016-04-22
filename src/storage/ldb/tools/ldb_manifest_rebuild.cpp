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
            "%s: pick sstable by specified buckets, output filenumber rename file and manifest of picked sstable.\n"
                    "\t-f manifestfile -c comparator -b buckets -s start_num -l filenumbers\n"
                    "\tbuckets like: 1,4,5\n"
                    "\tfilenumbers like: 100,400,500\n"
                    "\tcomparator like: bitcmp OR numeric,:,2\n"
                    "\t-v verbose output\n", name);
}

int rebuild_sst(const leveldb::Options &options, const leveldb::VersionSet &versions,
                const char *inputnumbers, const char *now_filenumber) {
    std::vector<std::string> fileno_strs;
    std::set<uint64_t> file_numbers;
    tair::util::string_util::split_str(inputnumbers, ", ", fileno_strs);
    for (size_t i = 0; i < fileno_strs.size(); ++i) {
        fprintf(stderr, "input skip_file_numbers: %llu\n", atoll(fileno_strs[i].c_str()));
        file_numbers.insert(atoll(fileno_strs[i].c_str()));
    }
    if (file_numbers.empty()) {
        fprintf(stderr, "empty file_numbers\n");
        return 1;
    }

    leveldb::FileMetaData *f = NULL;
    leveldb::VersionEdit edit;
    uint64_t max_filenumber = 0;

    for (int l = 0; l < leveldb::config::kNumLevels; ++l) {
        std::vector<leveldb::FileMetaData *> &metas = versions.current()->FileMetas()[l];

        for (size_t i = 0; i < metas.size(); ++i) {
            f = metas[i];
            if (f->number >= max_filenumber) {
                max_filenumber = f->number;
            }

            if (file_numbers.find(f->number) == file_numbers.end()) {
                // add new manifest
                edit.AddFile(l, f->number, f->file_size, f->smallest, f->largest);
            } else {
                printf("skip filenumber: %ld\n", f->number);
            }
        }
    }

    // write new manifest(descriptor) file
    std::string filename = std::string("manifest") + now_filenumber + ".tmp";
    leveldb::WritableFile *desc_file = NULL;
    leveldb::Status s = options.env->NewWritableFile(filename, &desc_file);
    if (s.ok()) {
        leveldb::log::Writer desc_log(desc_file);

        edit.SetComparatorName(options.comparator->Name());
        edit.SetLogNumber(versions.LogNumber());         // ignore lognumber
        fprintf(stderr, "next file: %lu\n", versions.NextFileNumber());
        edit.SetNextFile(versions.NextFileNumber()); // real filenumber
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
    char *filenumbers = NULL;
    char *cmp_desc = NULL;
    char *now_file_num = NULL;

    while ((i = getopt(argc, argv, "f:l:c:n:v")) != EOF) {
        switch (i) {
            case 'f':
                manifest = optarg;
                break;
            case 'l':
                filenumbers = optarg;
                break;
            case 'n':
                now_file_num = optarg;
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

    if (manifest == NULL || now_file_num == NULL || filenumbers == NULL) {
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
        fprintf(stderr, "g_verbose: %d\n", g_verbose);
        if (g_verbose) {
            fprintf(stderr, "input db range %s:\n", manifest);
            print_range(versions);
        }

        ret = rebuild_sst(options, versions, filenumbers, now_file_num);
    }

    delete options.comparator;
    delete options.env;

    return ret;
}
