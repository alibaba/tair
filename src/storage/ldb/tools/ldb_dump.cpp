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

#include <signal.h>

#include "common/util.hpp"

#include "leveldb/env.h"

#include "ldb_define.hpp"
#include "ldb_comparator.hpp"
#include "ldb_util.hpp"

using namespace tair::storage::ldb;

bool g_stop = false;

void sign_handler(int sig) {
    switch (sig) {
        case SIGTERM:
        case SIGINT:
            fprintf(stderr, "catch sig %d\n", sig);
            g_stop = true;
            break;
        default:
            break;
    }
}

void print_help(const char *name) {
    static const char *file_format =
            "\n"
                    "+-------+---+--------+----+---------+-----+-------+------+\n"
                    "|keysize|key|skeysize|skey|valuesize|value|keysize|......|\n"
                    "+-------+---+--------+----+---------+-----+-------+------+\n"
                    "|  int  | ..|  int   |....|   int   | ... |  int  |      |\n"
                    "+-------+---+--------+----+---------+-----+-------+------+\n"
                    "`size's byte order: little endian\n";

    fprintf(stderr, "dump ldb data to file\n"
                    "%s -p db_path -f manifestfile -c comparator_desc -d dump_file [-s one_dump_file_max_size(default 1G)]"
                    " -b buckets [-A no_areas] [-a yes_area]\n"
                    "\tcomparator_desc like: bitcmp OR numeric,:,2\n"
                    "\tbuckets/areas like: 1,2; buckets -1 mean all buckets\n"
                    "\tdump file format: %s\n"
                    "\t-m mean with meta, default yes\n"
                    "\t-o only_key_string, default no\n",
            name, file_format);
}

int32_t encode_ldb_kv(char *buf, LdbKey &key, LdbItem &value, bool with_meta,
                      bool only_key_string) {
    int size = 0;
    int32_t user_key_size = key.key_size() - TAIR_AREA_ENCODE_SIZE;
    int32_t prefix_size = value.prefix_size();
    int32_t pkey_size = (prefix_size == 0) ? user_key_size : prefix_size;
    int32_t skey_size = user_key_size - pkey_size;
    // pkey size
    if (only_key_string == false) {
        tair::util::coding_util::encode_fixed32(buf, pkey_size);
        size += sizeof(int32_t);
    }
    // pkey
    memcpy(buf + size, key.key() + TAIR_AREA_ENCODE_SIZE, pkey_size);
    size += pkey_size;
    if (only_key_string == true) {
        buf[size] = '\n';
        size++;
        return size;
    }

    // skey size(skey_size == 0 means not p-s-key.
    tair::util::coding_util::encode_fixed32(buf + size, skey_size);
    size += sizeof(int32_t);
    if (skey_size > 0) {
        // skey
        memcpy(buf + size, key.key() + TAIR_AREA_ENCODE_SIZE + pkey_size, skey_size);
        size += skey_size;
    }
    // value size
    tair::util::coding_util::encode_fixed32(buf + size, value.value_size());
    size += sizeof(int32_t);
    // value
    memcpy(buf + size, value.value(), value.value_size());
    size += value.value_size();

    if (with_meta == true) {
        // uint8_t  meta_version_;
        // uint8_t  flag_;
        // uint16_t version_;
        // uint32_t cdate_;
        // uint32_t mdate_;
        // uint32_t edate_;
        // meta_version not need
        uint8_t meta_version = 0;
        uint8_t flag = value.flag();
        uint16_t version = value.version();
        uint32_t cdate = value.cdate();
        uint32_t mdate = value.mdate();
        uint32_t edate = value.edate();
        uint16_t area = *(uint16_t *) key.key();
        memcpy(buf + size, &meta_version, sizeof(uint8_t));
        size += sizeof(uint8_t);
        memcpy(buf + size, &flag, sizeof(uint8_t));
        size += sizeof(uint8_t);
        memcpy(buf + size, &version, sizeof(uint16_t));
        size += sizeof(uint16_t);
        memcpy(buf + size, &cdate, sizeof(uint32_t));
        size += sizeof(uint32_t);
        memcpy(buf + size, &mdate, sizeof(uint32_t));
        size += sizeof(uint32_t);
        memcpy(buf + size, &edate, sizeof(uint32_t));
        size += sizeof(uint32_t);
        // uint16_t area;
        memcpy(buf + size, &area, sizeof(uint16_t));
        size += sizeof(uint16_t);
    }

    return size;
}

int do_dump(const char *db_path, const char *manifest, const char *cmp_desc,
            const std::vector<int32_t> &buckets, DataFilter &filter, DataStat &stat,
            const char *dump_file, int64_t dump_file_max_size,
            const bool with_meta, const bool only_key_string) {
    // open db
    leveldb::Options open_options;
    leveldb::DB *db = NULL;
    leveldb::Status s = open_db_readonly(db_path, manifest, cmp_desc, open_options, db);
    if (!s.ok()) {
        fprintf(stderr, "open db fail: %s\n", s.ToString().c_str());
        return 1;
    }

    // get db iterator
    leveldb::ReadOptions scan_options;
    scan_options.verify_checksums = false;
    scan_options.fill_cache = false;
    leveldb::Iterator *db_it = db->NewIterator(scan_options);
    char scan_key[LDB_KEY_META_SIZE];

    int32_t bucket = 0;
    int32_t area = 0;
    LdbKey ldb_key;
    LdbItem ldb_item;
    int32_t size = 0;

    bool skip_in_bucket = false;
    bool skip_in_area = false;

    int dump_fd = -1;
    int32_t dump_file_index = 1;
    int64_t dump_file_size = 0;

    static const int32_t BUF_SIZE = 2 << 20; // 2M
    char *buf = new char[BUF_SIZE];
    int32_t buf_remain = BUF_SIZE;

    int extra = (only_key_string == true ? 1 : 0);

    int ret = 0;

    for (size_t i = 0; !g_stop && i < buckets.size(); ++i) {
        area = -1;
        bucket = buckets[i];
        // seek to bucket
        LdbKey::build_key_meta(scan_key, bucket);

        for (db_it->Seek(leveldb::Slice(scan_key, sizeof(scan_key)));
             !g_stop && db_it->Valid() && ret == 0; db_it->Next()) {
            skip_in_bucket = false;
            skip_in_area = false;

            ldb_key.assign(const_cast<char *>(db_it->key().data()), db_it->key().size());
            ldb_item.assign(const_cast<char *>(db_it->value().data()), db_it->value().size());
            area = LdbKey::decode_area(ldb_key.key());

            // current bucket iterate over
            if (ldb_key.get_bucket_number() != bucket) {
                break;
            }

            // skip this data
            if (!filter.ok(area)) {
                skip_in_bucket = true;
            } else {
                // open new dump file
                if (dump_file_size >= dump_file_max_size || dump_fd < 0) {
                    if (dump_fd > 0) {
                        close(dump_fd);
                    }

                    char name[TAIR_MAX_PATH_LEN];
                    snprintf(name, sizeof(name), "%s.%d", dump_file, dump_file_index);
                    // open dump file
                    dump_fd = open(name, O_RDWR | O_CREAT | O_TRUNC, 0444);
                    if (dump_fd <= 0) {
                        fprintf(stderr, "open dump file fail, file: %s, error: %s\n", name, strerror(errno));
                        ret = 1;
                        break;
                    }
                    dump_file_size = 0;
                    dump_file_index++;
                }

                // appropriate size
                size = ldb_key.key_size() + ldb_item.value_size() + 3 * sizeof(int32_t);
                if (with_meta == true) {
                    // uint8_t  meta_version_;
                    // uint8_t  flag_;
                    // uint16_t version_;
                    // uint32_t cdate_;
                    // uint32_t mdate_;
                    // uint32_t edate_;
                    size += 2 * sizeof(uint8_t) + sizeof(uint16_t) + 3 * sizeof(uint32_t);
                    // uint16_t area;
                    size += sizeof(uint16_t);
                }

                if (size + extra < BUF_SIZE) {
                    if (size + extra > buf_remain) {
                        if (write(dump_fd, buf, BUF_SIZE - buf_remain) != (BUF_SIZE - buf_remain)) {
                            fprintf(stderr, "write file fail: %s\n", strerror(errno));
                            ret = 1;
                        }
                        dump_file_size += (BUF_SIZE - buf_remain);
                        buf_remain = BUF_SIZE;
                    }

                    size = encode_ldb_kv(buf + (BUF_SIZE - buf_remain),
                                         ldb_key, ldb_item, with_meta, only_key_string);
                    buf_remain -= size;
                } else                    // big data
                {
                    char *tmp_buf = new char[size + extra];
                    size = encode_ldb_kv(tmp_buf, ldb_key,
                                         ldb_item, with_meta, only_key_string);
                    if (write(dump_fd, tmp_buf, size) != size) {
                        fprintf(stderr, "write file fail: %s\n", strerror(errno));
                        ret = 1;
                    }
                    delete[] tmp_buf;
                    dump_file_size += size;
                }
            }

            // update stat
            stat.update(bucket, skip_in_bucket ? -1 : area, // skip in bucket, then no area to update
                        ldb_key.key_size() + ldb_item.value_size(), (skip_in_bucket || skip_in_area), ret == 0);
        }

        if (ret != 0) {
            break;
        }

        // only dump bucket stat
        stat.dump(bucket, -1);
    }

    // last data
    if (ret == 0 && buf_remain != BUF_SIZE) {
        if (write(dump_fd, buf, BUF_SIZE - buf_remain) != (BUF_SIZE - buf_remain)) {
            fprintf(stderr, "write file fail: %s\n", strerror(errno));
            ret = 1;
        }
    }
    if (dump_fd > 0) {
        close(dump_fd);
    }

    // cleanup
    delete[] buf;

    if (db_it != NULL) {
        delete db_it;
    }
    if (db != NULL) {
        delete db;
        delete open_options.comparator;
        delete open_options.env;
        delete open_options.info_log;
    }

    stat.dump_all();

    return ret;
}

int main(int argc, char *argv[]) {
    int i;
    char *manifest_file = NULL;
    char *db_path = NULL;
    char *dump_file = NULL;
    char *cmp_desc = NULL;
    char *buckets = NULL;
    char *yes_areas = NULL;
    char *no_areas = NULL;
    const char *with_meta = "yes";
    const char *only_key_string = "no";

    static const int64_t DUMP_FILE_MAX_SIZE = 1 << 30; // 1G
    int64_t dump_file_max_size = DUMP_FILE_MAX_SIZE;

    while ((i = getopt(argc, argv, "p:f:c:d:s:a:A:b:m:o:")) != EOF) {
        switch (i) {
            case 'p':
                db_path = optarg;
                break;
            case 'f':
                manifest_file = optarg;
                break;
            case 'c':
                cmp_desc = optarg;
                break;
            case 'b':
                buckets = optarg;
                break;
            case 'd':
                dump_file = optarg;
                break;
            case 's':
                dump_file_max_size = atoll(optarg);
                break;
            case 'a':
                yes_areas = optarg;
                break;
            case 'A':
                no_areas = optarg;
                break;
            case 'm':
                with_meta = optarg;
                break;
            case 'o':
                only_key_string = optarg;
                break;
            default:
                print_help(argv[0]);
                return 1;
        }
    }

    if (db_path == NULL || manifest_file == NULL || dump_file == NULL || cmp_desc == NULL || buckets == NULL) {
        print_help(argv[0]);
        return 1;
    }

    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);

    // init buckets
    std::vector<int32_t> bucket_container;
    std::vector<std::string> bucket_strs;
    tair::util::string_util::split_str(buckets, ", ", bucket_strs);
    for (size_t i = 0; i < bucket_strs.size(); ++i) {
        int bucket = atoi(bucket_strs[i].c_str());
        if (bucket == -1) {
            for (int j = 0; j <= TAIR_DEFAULT_BUCKET_NUMBER; j++) {
                bucket_container.push_back(j);
            }
            break;
        } else {
            bucket_container.push_back(atoi(bucket_strs[i].c_str()));
        }
    }
    // init data filter
    DataFilter filter(yes_areas, no_areas);
    // init data stat
    DataStat stat;

    return do_dump(db_path, manifest_file, cmp_desc, bucket_container,
                   filter, stat, dump_file, dump_file_max_size,
                   (with_meta[0] == 'y' ? true : false),
                   (only_key_string[0] == 'y' ? true : false));
}
