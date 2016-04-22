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
#include <fstream>
#include <set>

#include "common/util.hpp"
#include "common/data_entry.hpp"

#include <leveldb/env.h>
#include "ldb_define.hpp"
#include "ldb_comparator.hpp"

#include "client/tair_client_api_impl.hpp"
#include "ldb_util.hpp"

using namespace tair::storage::ldb;
using tair::data_entry;
using tair::tair_client_impl;

class CheckStat {
public:
    CheckStat();

    ~CheckStat();

    void dump(int32_t bucket, int32_t area);

    void dump_all();

    void update(int32_t bucket, int32_t area, int64_t size, bool valid, bool suc);

private:
    typedef struct Stat {
        Stat() : count_(0), size_(0), suc_count_(0), invalid_count_(0), del_ok_count_(0), del_fail_count_(0) {}

        void add(Stat &stat) {
            count_ += stat.count_;
            size_ += stat.size_;
            suc_count_ += stat.suc_count_;
            invalid_count_ += stat.invalid_count_;
            del_ok_count_ += stat.del_ok_count_;
            del_fail_count_ += stat.del_fail_count_;
        }

        int64_t count_;
        int64_t size_;
        int64_t suc_count_;
        int64_t invalid_count_;
        int64_t del_ok_count_;
        int64_t del_fail_count_;
    } Stat;

    void do_destroy(std::map<int32_t, Stat *> &stats);

    void do_dump(std::map<int32_t, Stat *> &stats, int32_t key, const char *desc);

    void do_update(std::map<int32_t, Stat *> &stats, int key, int64_t size, bool valid, bool suc);

private:
    std::map<int32_t, Stat *> bucket_stats_;
    std::map<int32_t, Stat *> area_stats_;
};

CheckStat::CheckStat() {}

CheckStat::~CheckStat() {
    do_destroy(bucket_stats_);
    do_destroy(area_stats_);
}

void CheckStat::dump(int32_t bucket, int32_t area) {
    if (bucket >= 0) {
        do_dump(bucket_stats_, bucket, "=== one bucket stats ===");
    }
    if (area >= 0) {
        do_dump(area_stats_, area, "=== one area stats ===");
    }
}

void CheckStat::dump_all() {
    do_dump(bucket_stats_, -1, "=== all bucket stats ===");
    do_dump(area_stats_, -1, "=== all area stats ===");
}

void CheckStat::update(int32_t bucket, int32_t area, int64_t size, bool valid, bool suc) {
    do_update(bucket_stats_, bucket, size, valid, suc);
    do_update(area_stats_, area, size, valid, suc);
}

void CheckStat::do_destroy(std::map<int32_t, Stat *> &stats) {
    for (std::map<int32_t, Stat *>::iterator it = stats.begin(); it != stats.end(); ++it) {
        delete it->second;
    }
    stats.clear();
}

void CheckStat::do_dump(std::map<int32_t, Stat *> &stats, int32_t key, const char *desc) {
    Stat *stat = NULL;
    fprintf(stderr, "%s\n", desc);
    fprintf(stderr, "%5s%12s%12s%12s%20s%15s%20s\n", "key", "totalCount", "totalSize", "sucCount", "invalidCount",
            "delOkCount", "delFailCount");
    // dump specified key stat
    if (key >= 0) {
        std::map<int32_t, Stat *>::iterator it = stats.find(key);
        if (it == stats.end()) {
            fprintf(stderr, "NONE STATS\n");
        } else {
            stat = it->second;
            fprintf(stderr, "%5d%12ld%12ld%12ld%20ld%15ld%20ld\n", it->first, stat->count_, stat->size_,
                    stat->suc_count_, stat->invalid_count_,
                    stat->del_ok_count_, stat->del_fail_count_);
        }
    } else                        // dump all stats
    {
        Stat total_stat;
        for (std::map<int32_t, Stat *>::iterator it = stats.begin(); it != stats.end(); ++it) {
            stat = it->second;
            total_stat.add(*stat);
            fprintf(stderr, "%5d%12ld%12ld%12ld%20ld%15ld%20ld\n", it->first, stat->count_, stat->size_,
                    stat->suc_count_, stat->invalid_count_,
                    stat->del_ok_count_, stat->del_fail_count_);
        }
        fprintf(stderr, "%5s%12ld%12ld%12ld%20ld%15ld%20ld\n", "total", total_stat.count_, total_stat.size_,
                total_stat.suc_count_, total_stat.invalid_count_,
                total_stat.del_ok_count_, total_stat.del_fail_count_);
    }
}

void CheckStat::do_update(std::map<int32_t, Stat *> &stats, int key, int64_t size, bool valid, bool suc) {
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
        stat->size_ += size;
        if (valid) {
            ++stat->suc_count_;
        } else {
            ++stat->invalid_count_;
            if (suc) {
                ++stat->del_ok_count_;
            } else {
                ++stat->del_fail_count_;
            }
        }
    }
}

// global stop sentinel
static bool g_stop = false;
// time to wait after scan or repair 1000 items
static int64_t g_wait_ms = 10;

void sign_handler(int sig) {
    switch (sig) {
        case SIGTERM:
        case SIGINT:
            log_error("catch sig %d", sig);
            g_stop = true;
            break;
        case 51:
            g_wait_ms += 10;
            log_warn("g_wait_ms: %ld", g_wait_ms);
            break;
        case 52:
            g_wait_ms -= 10;
            if (g_wait_ms < 0) {
                g_wait_ms = 0;
            }
            log_warn("g_wait_ms: %ld", g_wait_ms);
            break;
    }
}

int init_cluster_handler(const char *addr, tair_client_impl &handler) {
    int ret = TAIR_RETURN_SUCCESS;
    std::vector<std::string> confs;
    tair::util::string_util::split_str(addr, ", ", confs);

    if (!handler.startup(confs[0].c_str(), confs[1].c_str(), confs[2].c_str())) {
        log_warn("start %s failed", addr);
        ret = TAIR_RETURN_FAILED;
    }

    return ret;
}

// dump err info, type 0 for dump text info, typy 1 for dump binary info
int dump_err_info(int type, std::ofstream &fout, int area, int bucket, const char *key, uint32_t klen,
                  int err_bucket = 0, int prefix_size = 0, int ret = 0) {
    if (type == 0) {
        char *key_str = (char *) malloc(klen * 2 + 1);
        uint32_t i = 0, j = 0;
        for (i = 0; i < klen; i++) {
            sprintf(key_str + j, "%02x", (uint8_t) key[i]);
            j += 2;
        }
        key_str[2 * i] = 0;

        char buf[100 + 2 * i];
        sprintf(buf, "%4s: area(%d), bucket(%d), key(%s), err_bucket(%d), prefix_size(%d)",
                TAIR_RETURN_SUCCESS == ret ? "suc " : "fail", area, bucket, key_str, err_bucket, prefix_size);
        log_debug("%s", buf);
        fout.write(buf, strlen(buf));
        fout.write("\n", 1);

        free(key_str);
        key_str = NULL;
    } else {
        fout.write((char *) &area, sizeof(area));
        fout.write((char *) &bucket, sizeof(bucket));
        fout.write((char *) &klen, sizeof(klen));
        fout.write(key, klen);
    }
    return 0;
}

int32_t get_bucket_num(char *kbuf, uint32_t klen, int32_t prefix_size, int32_t bucket_number) {
    uint32_t hashcode = 0;
    if (prefix_size == 0) {
        hashcode = tair::util::string_util::mur_mur_hash(kbuf, klen);
    } else {
        hashcode = tair::util::string_util::mur_mur_hash(kbuf, prefix_size);
    }
    return hashcode % bucket_number;
}

int do_repair(tair_client_impl *local_handler, const char *filename) {
    ifstream fin(filename, ios::in);
    if (!fin.is_open()) {
        fprintf(stderr, "open file %s failed\n", filename);
        return 1;
    }

    string fout_name(filename);
    fout_name.append(".fail");
    ofstream fout(fout_name.c_str(), std::ofstream::binary);
    if (!fout.is_open()) {
        fprintf(stderr, "open file %s failed\n", fout_name.c_str());
        fin.close();
        return 1;
    }

    int fix_size = 3 * sizeof(uint32_t);
    char buf[fix_size];
    char *kbuf = (char *) malloc(TAIR_MAX_KEY_SIZE);
    uint64_t count = 0, suc_count = 0, not_exist_count = 0, fail_count = 0;

    while (fin.read(buf, fix_size)) {
        count++;
        if (count % 2000 == 0 && g_wait_ms > 0) {
            ::usleep(g_wait_ms * 1000);
        }
        int32_t area = *(int32_t *) buf;
        int32_t bucket = *(int32_t *) (buf + 4);
        int32_t klen = *(int32_t *) (buf + 8);
        if (fin.read(kbuf, klen)) {
            data_entry key(kbuf, klen, true);
            log_debug("area is %d, bucket is %d, klen is %d", area, bucket, klen);
            int ret = local_handler->uniq_remove(area, bucket, key);
            // record the failed key info in text
            if (TAIR_RETURN_SUCCESS != ret && TAIR_RETURN_DATA_NOT_EXIST != ret) {
                fail_count++;
                dump_err_info(1, fout, area, bucket, kbuf, klen);
            } else {
                TAIR_RETURN_SUCCESS == ret ? suc_count++ : not_exist_count++;
            }
        } else {
            fprintf(stderr, "read error while repair\n");
            break;
        }
    }

    free(kbuf);
    kbuf = NULL;

    fin.close();
    fout.close();

    fprintf(stderr, "repair rslt : totalCount is %ld, sucCount is %ld, notExistCount is %ld, failCount is %ld\n",
            count, suc_count, not_exist_count, fail_count);

    return 0;
}

int do_check(std::vector<int32_t> &buckets, int32_t bucket_number,
             const char *db_path, const char *manifest_file, const char *cmp_desc,
             tair_client_impl *local_handler, CheckStat &stat, const char *log_file_name) {
    // open log file : prefix.txt, prefix.bin
    std::string fout_items_name(log_file_name), fout_fails_name(log_file_name);

    fout_items_name.append(".InvalidItems");
    std::ofstream fout_items(fout_items_name.c_str(), std::ios::out);
    if (!fout_items.is_open()) {
        fprintf(stderr, "open file %s failed\n", fout_items_name.c_str());
        return 1;
    }

    fout_fails_name.append(".DelFail");
    std::ofstream fout_fails(fout_fails_name.c_str(), std::ofstream::binary);
    if (!fout_fails.is_open()) {
        fprintf(stderr, "open file %s failed\n", fout_fails_name.c_str());
        fout_items.close();
        return 1;
    }

    // open db with specified manifest(read only)
    leveldb::DB *db = NULL;
    leveldb::Options open_options;
    leveldb::Status s = open_db_readonly(db_path, manifest_file, cmp_desc, open_options, db);
    if (!s.ok()) {
        fprintf(stderr, "open db fail: %s\n", s.ToString().c_str());
        fout_items.close();
        fout_fails.close();
        return 1;
    }

    // get db iterator
    leveldb::ReadOptions scan_options;
    scan_options.verify_checksums = false;
    scan_options.fill_cache = false;
    leveldb::Iterator *db_it = db->NewIterator(scan_options);
    char scan_key[LDB_KEY_META_SIZE];

    int ret = TAIR_RETURN_SUCCESS;

    uint32_t start_time = 0;
    int32_t bucket = -1;
    int32_t area = -1;

    LdbKey ldb_key;
    LdbItem ldb_item;

    if (db_it == NULL) {
        log_error("new db iterator fail.");
        ret = TAIR_RETURN_FAILED;
    } else {
        for (size_t i = 0; !g_stop && i < buckets.size(); ++i) {
            start_time = time(NULL);
            bucket = buckets[i];

            log_warn("begin check bucket %d", bucket);

            // seek to bucket
            LdbKey::build_key_meta(scan_key, bucket);
            int count = 0;
            for (db_it->Seek(leveldb::Slice(scan_key, sizeof(scan_key))); !g_stop && db_it->Valid(); db_it->Next()) {
                count++;
                if (count % 2000 == 0 && g_wait_ms > 0) {
                    ::usleep(g_wait_ms * 1000);
                }
                // just need key here
                ret = TAIR_RETURN_FAILED;

                // invalid key, just skip
                if ((int32_t) db_it->key().size() <= LDB_KEY_CLIENT_USER_KEY_OFFSET) continue;

                ldb_key.assign(const_cast<char *>(db_it->key().data()), db_it->key().size());
                ldb_item.assign(const_cast<char *>(db_it->value().data()), db_it->value().size());

                area = LdbKey::decode_area(ldb_key.key());
                int32_t prefix_size = ldb_item.prefix_size();
                char *kbuf = ldb_key.data() + LDB_KEY_CLIENT_USER_KEY_OFFSET;
                uint32_t klen = ldb_key.size() - LDB_KEY_CLIENT_USER_KEY_OFFSET;

                // current bucket iterate over
                if (ldb_key.get_bucket_number() != bucket) break;

                int32_t err_bucket = get_bucket_num(kbuf, klen, prefix_size, bucket_number);
                if (err_bucket != bucket) {
                    // uniq_remove
                    if (local_handler != NULL) {
                        data_entry key(kbuf, klen, true);
                        ret = local_handler->uniq_remove(area, bucket, key);
                    }

                    dump_err_info(0, fout_items, area, bucket, kbuf, klen, err_bucket, prefix_size, ret);

                    // record the failed key info in text
                    // if (TAIR_RETURN_SUCCESS != ret && TAIR_RETURN_DATA_NOT_EXIST != ret) {
                    if (TAIR_RETURN_SUCCESS != ret) {
                        dump_err_info(1, fout_fails, area, bucket, kbuf, klen);
                    }
                }

                stat.update(bucket, area, ldb_key.key_size() + ldb_item.value_size(), err_bucket == bucket,
                            ret == TAIR_RETURN_SUCCESS);
            }

            log_warn("check bucket %d over, cost: %ld(s), stat:\n",
                     bucket, time(NULL) - start_time);

            // only dump bucket stat
            stat.dump(bucket, -1);
        }
    }

    // close file
    fout_items.close();
    fout_fails.close();

    if (db_it != NULL) {
        delete db_it;
    }

    if (db != NULL) {
        delete db;
        delete open_options.comparator;
        delete open_options.env;
        delete open_options.info_log;
    }

    return ret;
}

void print_help(const char *name) {
    fprintf(stderr,
            "check the bucket of each item is valid or repair .\n"
                    "check bucket usage:\n"
                    "%s -p dbpath -f manifest_file -c cmp_desc -e log_file_name -b buckets [-l local_cluster_addr -n bucket_number]\n"
                    "NOTE:\n"
                    "\tcmp_desc like bitcmp OR numeric,:,2\n"
                    "\tcluster_addr like: xxx.xxx.xxx.xxx:5198,xxx.xxx.xxx.xxx:5198,group_1\n"
                    "\tbuckets like: 1,2,3\n"
                    "\tone of the two setting(local_cluster_addr and bucket_number) should be set\n"
                    "\n"
                    "repair usage:\n"
                    "%s -r -e fail_log -l local_cluster_addr\n", name, name);
}

int main(int argc, char *argv[]) {
    int ret = TAIR_RETURN_SUCCESS;
    char *db_path = NULL;
    char *manifest_file = NULL;
    char *cmp_desc = NULL;
    char *local_cluster_addr = NULL;
    char *log_file_name = NULL;
    char *buckets = NULL;
    char *bucket_num_str = NULL;
    int i = 0;
    bool is_repair = false;

    while ((i = getopt(argc, argv, "p:f:c:l:e:b:n:r")) != EOF) {
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
            case 'l':
                local_cluster_addr = optarg;
                break;
            case 'e':
                log_file_name = optarg;
                break;
            case 'b':
                buckets = optarg;
                break;
            case 'n':
                bucket_num_str = optarg;
                break;
            case 'r':
                is_repair = true;
                log_warn("enter repair mode");
                break;
            default:
                print_help(argv[0]);
                return 1;
        }
    }

    // check parameters
    if ((is_repair && (log_file_name == NULL || local_cluster_addr == NULL)) ||
        (!is_repair && (db_path == NULL || manifest_file == NULL || cmp_desc == NULL ||
                        log_file_name == NULL || buckets == NULL ||
                        (bucket_num_str == NULL && local_cluster_addr == NULL)))) {
        print_help(argv[0]);
        return 1;
    }

    // init signals
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    signal(51, sign_handler);
    signal(52, sign_handler);

    TBSYS_LOGGER.setLogLevel("warn");


    // init local cluster handler(optional)
    tair_client_impl *local_handler = NULL;
    if (local_cluster_addr != NULL) {
        local_handler = new tair_client_impl();
        ret = init_cluster_handler(local_cluster_addr, *local_handler);
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("init local client fail, addr: %s, ret: %d", local_cluster_addr, ret);
            delete local_handler;
            return 1;
        }
    }

    if (is_repair) {
        ret = do_repair(local_handler, log_file_name);
    } else {
        int32_t bucket_num = -1;
        if (bucket_num_str != NULL) {
            bucket_num = atoi(bucket_num_str);
            log_warn("bucket_num set by config : %d", bucket_num);
        }

        if (local_handler != NULL) {
            bucket_num = local_handler->get_bucket_count();
            log_warn("bucket_num set by local cluster: %d", bucket_num);
        }

        // init buckets
        std::vector<int32_t> bucket_container;
        std::vector<std::string> bucket_strs;
        tair::util::string_util::split_str(buckets, ", ", bucket_strs);
        for (size_t i = 0; i < bucket_strs.size(); ++i) {
            bucket_container.push_back(atoi(bucket_strs[i].c_str()));
        }

        std::random_shuffle(bucket_container.begin(), bucket_container.end());

        // init data stat
        CheckStat stat;

        uint32_t start_time = time(NULL);
        log_warn("start check data");

        // do data check
        ret = do_check(bucket_container, bucket_num,
                       db_path, manifest_file, cmp_desc,
                       local_handler, stat, log_file_name);

        log_warn("check data over, stopped: %s, cost: %ld(s), stat:", g_stop ? "yes" : "no", time(NULL) - start_time);
        stat.dump_all();
    }

    // cleanup
    if (local_handler != NULL) {
        delete local_handler;
    }

    return ret == TAIR_RETURN_SUCCESS ? 0 : 1;
}
