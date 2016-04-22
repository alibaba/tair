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
#include "leveldb/table.h"
#include "db/version_set.h"
#include "db/filename.h"
#include "db/log_writer.h"

#include "ldb_define.hpp"
#include "ldb_comparator.hpp"

#include "ldb_util.hpp"

using namespace tair::storage::ldb;
using namespace leveldb;


char *cmp_desc = NULL;
char *sst_file = NULL;
char *sst_path = NULL;

int wait_time = 1000 * 1000;//1s

void print_help(const char *name) {
    fprintf(stderr, "%s: check sst file by custom compare, default check all sst files.\n"
            "\t-f file_name\n"
            "\t-p path\n"
            "\t-c comparator\n"
            "\t-t wait time(ms), default 1000ms\n"
            "\tcomparator like: bitcmp OR numeric,:,2\n", name);
}


bool check_table(leveldb::Env *env, const std::string &fname, leveldb::Comparator *comparator) {
    uint64_t file_size;
    leveldb::RandomAccessFile *file = NULL;
    leveldb::Table *table = NULL;
    bool valid = true;
    leveldb::Status s = env->GetFileSize(fname, &file_size);
    if (s.ok()) {
        s = env->NewRandomAccessFile(fname, &file);
    }
    printf("check sst file: %s, file_size(%ld)\n", fname.c_str(), file_size);
    leveldb::Options options = Options();
    if (s.ok()) {
        s = leveldb::Table::Open(options, file, file_size, &table);
    }
    if (!s.ok()) {
        fprintf(stdout, "%s\n", s.ToString().c_str());
        valid = true;
        delete table;
        delete file;
        return valid;
    }

    leveldb::ReadOptions ro;
    ro.fill_cache = false;
    bool first = true;
    leveldb::Slice last_key;
    const char *last_type = NULL;
    leveldb::Iterator *iter = table->NewIterator(ro);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        leveldb::ParsedInternalKey key;
        if (!ParseInternalKey(iter->key(), &key)) {
            printf("badkey '%s' => '%s'\n",
                   EscapeString(iter->key()).c_str(),
                   EscapeString(iter->value()).c_str());
        } else {
            char kbuf[20];
            const char *type;
            char *p_key;
            if (key.type == kTypeDeletion) {
                type = "del";
            } else if (key.type == kTypeValue) {
                type = "val";
            } else {
                snprintf(kbuf, sizeof(kbuf), "%d", static_cast<int>(key.type));
                type = kbuf;
            }

            if (first) {
                p_key = (char *) malloc(key.user_key.size());
                memcpy(p_key, key.user_key.data(), key.user_key.size());
                last_key = leveldb::Slice(p_key, key.user_key.size());
                last_type = type;
                first = false;
                continue;
            }

            //printf("process key[%s]\n", EscapeString(key.user_key).c_str());
            if (comparator->Compare(last_key, key.user_key) > 0) {
                printf("invalid key!!!\ncurr,type(%s)[%s]\nlast,type(%s)[%s] \n", type,
                       EscapeString(key.user_key).c_str(), last_type, EscapeString(last_key).c_str());
                valid = false;
                break;
            }
            p_key = (char *) last_key.data();
            free(p_key);
            p_key = NULL;
            p_key = (char *) malloc(key.user_key.size());
            memcpy(p_key, key.user_key.data(), key.user_key.size());
            last_key = leveldb::Slice(p_key, key.user_key.size());
            last_type = type;
        }
    }

    if (!first) {
        free((char *) last_key.data());
    }

    s = iter->status();
    if (!s.ok()) {
        printf("iterator error: %s\n", s.ToString().c_str());
    }

    delete iter;
    delete table;
    delete file;

    return valid;
}

int check_sst_file(const char *file_name, leveldb::Comparator *comparator) {
    leveldb::Env *env_ = leveldb::Env::Instance();
    std::vector<std::string> filenames;
    if (sst_path != NULL) {
        env_->GetChildren(sst_path, &filenames);
    }
    if (sst_file != NULL) {
        if (find(filenames.begin(), filenames.end(), sst_file) == filenames.end()) {
            filenames.push_back(sst_file);
        }
    }
    uint64_t number;
    FileType type;

    std::vector<std::string> wrong_files;
    for (size_t i = 0; i < filenames.size(); i++) {
        char tmp_file_name[256];
        if (sst_path != NULL) {
            sprintf(tmp_file_name, "%s/%s", sst_path, filenames[i].c_str());
        } else {
            sprintf(tmp_file_name, "%s", filenames[i].c_str());
        }
        if (ParseFileName(filenames[i], &number, &type)) {
            switch (type) {
                case kTableFile: {
                    if (!check_table(env_, tmp_file_name, comparator)) {
                        wrong_files.push_back(filenames[i]);
                    }
                    fflush(stdout);
                    usleep(wait_time);
                }
                    break;
                default:
                    break;
            }
        }
    }
    if (wrong_files.empty()) {
        printf("all sst is correct\n");
    } else {
        printf("\nwrong files: ");
        for (int i = 0; i < (int) wrong_files.size(); i++) {
            printf(" %s", wrong_files[i].c_str());
        }
        printf("\n");
    }
    delete env_;
    env_ = NULL;
    return 1;
}


int main(int argc, char *argv[]) {
    int i;
    int ret = 0;

    while ((i = getopt(argc, argv, "f:c:p:t:")) != EOF) {
        switch (i) {
            case 'f':
                sst_file = optarg;
                break;
            case 'c':
                cmp_desc = optarg;
                break;
            case 'p':
                sst_path = optarg;
                break;
            case 't':
                wait_time = atoi(optarg) * 1000;
                break;
            default:
                print_help(argv[0]);
                break;
        }
    }

    if (cmp_desc == NULL || (sst_path == NULL && sst_file == NULL)) {
        print_help(argv[0]);
        return 1;
    }

    leveldb::Comparator *comparator = new_comparator(cmp_desc);
    if (comparator == NULL) {
        print_help(argv[0]);
        return 1;
    }

    ret = check_sst_file(sst_file, comparator);

    delete comparator;
    comparator = NULL;

    return ret;
}
