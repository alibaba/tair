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

typedef struct LevelDBManifest {
    leveldb::Options *options_;
    leveldb::InternalKeyComparator *icmp_;
    leveldb::VersionSet *versions_;

    struct Cmp {
        bool operator()(const LevelDBManifest &l, const LevelDBManifest &r) const {
            // based on files count, decreasing order
            return l.versions_->NumFiles() > r.versions_->NumFiles();
        }
    };
} LevelDBManifest;

typedef std::set<LevelDBManifest, LevelDBManifest::Cmp> MANIFEST_SET;

void print_help(const char *name) {
    fprintf(stderr, "%s: merge specified manifest files.\n"
            "\t-f manifestfiles -c comparator [-v]\n"
            "\tmanifest like: file1,file2\n"
            "\tcomparator like: bitcmp OR numeric,:,2\n"
            "\t-v verbose output\n", name);
}

// not overlap
struct ByRangeKey {
    const leveldb::InternalKeyComparator *icmp_;

    bool operator()(leveldb::FileMetaData *f1, leveldb::FileMetaData *f2) const {
        return icmp_->Compare(f1->largest, f2->smallest) < 0;
    }
};

int merge_manifest(MANIFEST_SET &manifests) {
    uint64_t largest_sequence = 0;
    uint64_t largest_filenumber = 0;
    std::vector<leveldb::FileMetaData *> result_metas[leveldb::config::kNumLevels];

    for (MANIFEST_SET::iterator it = manifests.begin(); it != manifests.end(); ++it) {
        leveldb::VersionSet *versions = it->versions_;
        leveldb::InternalKeyComparator *icmp = it->icmp_;
        ByRangeKey cmp;
        cmp.icmp_ = icmp;

        // check sequence/filenumber
        if (versions->LastSequence() > largest_sequence) {
            largest_sequence = versions->LastSequence();
        }
        if (versions->NextFileNumber() > largest_filenumber) {
            largest_filenumber = versions->NextFileNumber();
        }

        // add filemeta
        for (int l = 0; l < leveldb::config::kNumLevels; ++l) {
            std::vector<leveldb::FileMetaData *> &metas = versions->current()->FileMetas()[l];
            // first one or level-0, just add files
            if (result_metas[l].empty() || l == 0) {
                for (size_t i = 0; i < metas.size(); ++i) {
                    result_metas[l].push_back(metas[i]);
                }
                continue;
            }

            std::vector<leveldb::FileMetaData *> base_metas = result_metas[l];
            result_metas[l].clear();
            std::vector<leveldb::FileMetaData *>::const_iterator base_iter = base_metas.begin();
            std::vector<leveldb::FileMetaData *>::const_iterator base_end = base_metas.end();

            for (size_t i = 0; i < metas.size(); ++i) {
                leveldb::FileMetaData *f = metas[i];
                std::vector<leveldb::FileMetaData *>::const_iterator bpos;
                for (bpos = std::upper_bound(base_iter, base_end, f, cmp);
                     base_iter != bpos;
                     ++base_iter) {
                    result_metas[l].push_back(*base_iter);
                }

                // not overlap
                if (bpos == base_metas.end() || cmp(f, *bpos)) {
                    result_metas[l].push_back(f);
                } else {
                    // oops overlap, just add to level-0.
                    // we may meet one overstaffed level-0..
                    // ..fix me.. try higher level?
                    result_metas[0].push_back(f);
                }
            }

            // remaining
            for (; base_iter != base_end; ++base_iter) {
                result_metas[l].push_back(*base_iter);
            }
        }
    }

    // write new manifest(descriptor) file
    std::string filename = leveldb::DescriptorFileName("./", 1023);
    leveldb::WritableFile *desc_file = NULL;
    leveldb::Status s = manifests.begin()->options_->env->NewWritableFile(filename, &desc_file);
    if (s.ok()) {
        leveldb::log::Writer desc_log(desc_file);

        leveldb::VersionEdit edit;

        edit.SetComparatorName(manifests.begin()->options_->comparator->Name());
        edit.SetLogNumber(0);
        edit.SetNextFile(largest_filenumber); // real filenumber
        edit.SetLastSequence(largest_sequence);

        for (int l = 0; l < leveldb::config::kNumLevels; ++l) {
            std::vector<leveldb::FileMetaData *> &metas = result_metas[l];
            for (size_t i = 0; i < metas.size(); ++i) {
                edit.AddFile(l, metas[i]->number, metas[i]->file_size, metas[i]->smallest, metas[i]->largest);
            }
        }

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
        leveldb::VersionSet versions("./", manifests.begin()->options_, NULL, manifests.begin()->icmp_);
        s = versions.Recover(filename.c_str());
        if (!s.ok()) {
            fprintf(stderr, "check manifest fail: %s\n", s.ToString().c_str());
        } else {
            if (g_verbose) {
                fprintf(stderr, "result db range %s:\n", filename.c_str());
                print_range(versions);
            }
        }
    }

    if (!s.ok()) {
        fprintf(stderr, "fail with %s\n", s.ToString().c_str());
    }

    return s.ok() ? 1 : 0;
}

int main(int argc, char *argv[]) {
    int i;
    char *manifest_files = NULL;
    char *cmp_desc = NULL;

    while ((i = getopt(argc, argv, "f:c:v")) != EOF) {
        switch (i) {
            case 'f':
                manifest_files = optarg;
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

    if (manifest_files == NULL || cmp_desc == NULL) {
        print_help(argv[0]);
        return 1;
    }

    std::vector<std::string> files;
    tair::util::string_util::split_str(manifest_files, ", ", files);
    if (files.empty()) {
        print_help(argv[0]);
        return 1;
    }
    leveldb::Comparator *cmp = new_comparator(cmp_desc);
    if (cmp == NULL) {
        print_help(argv[0]);
        return 1;
    }
    delete cmp;

    // open all manifest
    MANIFEST_SET manifests;
    leveldb::Status s;

    for (size_t i = 0; i < files.size(); ++i) {
        LevelDBManifest manifest;
        manifest.options_ = new leveldb::Options();
        manifest.options_->error_if_exists = false; // exist is ok
        manifest.options_->create_if_missing = true; // create if not exist
        manifest.options_->env = leveldb::Env::Instance();
        manifest.options_->comparator = new_comparator(cmp_desc);
        manifest.icmp_ = new leveldb::InternalKeyComparator(manifest.options_->comparator);
        manifest.versions_ = new leveldb::VersionSet("./", manifest.options_, NULL, manifest.icmp_);

        s = manifest.versions_->Recover(files[i].c_str());
        manifests.insert(manifest);

        if (!s.ok()) {
            fprintf(stderr, "load manifest fail: %s, error: %s\n", files[i].c_str(), s.ToString().c_str());
            break;
        }

        if (g_verbose) {
            fprintf(stderr, "input db range %s:\n", files[i].c_str());
            print_range(*manifest.versions_);
        }
    }

    int ret = s.ok() ? merge_manifest(manifests) : 1;

    for (MANIFEST_SET::iterator it = manifests.begin(); it != manifests.end(); ++it) {
        delete it->versions_;
        delete it->icmp_;
        delete it->options_->comparator;
        delete it->options_->env;
        delete it->options_;
    }

    return ret;
}
