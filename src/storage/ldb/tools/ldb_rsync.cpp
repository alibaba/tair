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
#include <set>

#include "common/util.hpp"
#include "common/data_entry.hpp"
#include "common/record_logger_util.hpp"
#include "packets/mupdate_packet.hpp"

// this is an ugly include now, we just need some struct
#include "dataserver/cluster_handler.hpp"
#include "dataserver/remote_sync_manager.hpp"

#include <leveldb/env.h>
#include "ldb_define.hpp"
#include "ldb_comparator.hpp"

#include "ldb_util.hpp"

using namespace tair::storage::ldb;
using tair::data_entry;
using tair::common::RecordLogger;
using tair::common::SequentialFileRecordLogger;
using tair::ClusterHandler;
using tair::FailRecord;
using tair::tair_operc_vector;

// global stop sentinel
static bool g_stop = false;
static int64_t g_wait_ms = 0;

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

int init_cluster_handler(const char *addr, ClusterHandler &handler) {
    int ret = TAIR_RETURN_SUCCESS;
    std::vector<std::string> confs;
    tair::util::string_util::split_str(addr, ", ", confs);
    confs.push_back("2000");      // timeout
    confs.push_back("2000");      // queuelimit
    if ((ret = handler.init_conf(confs)) == TAIR_RETURN_SUCCESS) {
        ret = handler.start();
    }
    return ret;
}

int get_from_local_cluster(ClusterHandler &handler, data_entry &key, data_entry *&value, bool &skip) {
    int ret = handler.client()->get_hidden(key.get_area(), key, value);
    if (ret == TAIR_RETURN_DATA_NOT_EXIST) {
        log_warn("key not exist local");
        skip = true;
        ret = TAIR_RETURN_SUCCESS;
    } else if (ret != TAIR_RETURN_SUCCESS && ret != TAIR_RETURN_HIDDEN) {
        log_error("get local fail, ret: %d", ret);
    } else {
        key.data_meta.cdate = value->data_meta.cdate;
        key.data_meta.edate = value->data_meta.edate;
        key.data_meta.mdate = value->data_meta.mdate;
        key.data_meta.version = value->data_meta.version;
        key.data_meta.keysize = value->data_meta.keysize;
        key.data_meta.valsize = value->data_meta.valsize;
        ret = TAIR_RETURN_SUCCESS;
    }

    return ret;
}

void log_fail(RecordLogger *logger, std::string &info, data_entry *key, int ret) {
    log_error("fail one: %d", ret);
    FailRecord record(key, info, ret);
    data_entry entry;
    FailRecord::record_to_entry(record, entry);
    int tmp_ret = logger->add_record(0, TAIR_REMOTE_SYNC_TYPE_PUT, &entry, NULL);
    if (tmp_ret != TAIR_RETURN_SUCCESS) {
        log_error("add fail record fail, ret: %d", tmp_ret);
    }
}

void log_fail(RecordLogger *logger, std::string &info, tair_operc_vector &kvs, int ret) {
    log_error("fail one %d", ret);
    for (size_t i = 0; i < kvs.size(); ++i) {
        FailRecord record(kvs[i]->key, info, ret);
        data_entry entry;
        FailRecord::record_to_entry(record, entry);
        int tmp_ret = logger->add_record(0, TAIR_REMOTE_SYNC_TYPE_PUT, &entry, NULL);
        if (tmp_ret != TAIR_RETURN_SUCCESS) {
            log_error("add fail record fail, ret: %d", tmp_ret);
        }
    }
}

int do_batch_rsync(ClusterHandler *remote_handler, std::vector<uint64_t> &ds_ids,
                   tair_operc_vector &kvs, RecordLogger *fail_logger) {
    int ret = remote_handler->client()->direct_update(ds_ids, &kvs);
    if (ret != TAIR_RETURN_SUCCESS) {
        // log fail key
        std::string info = remote_handler->get_info();
        log_fail(fail_logger, info, kvs, ret);
    }
    for (size_t i = 0; i < kvs.size(); ++i) {
        delete kvs[i];
    }
    kvs.clear();

    return ret;
}

int do_rsync(std::vector<int32_t> &buckets, DataFilter &filter,
             const char *db_path, const char *manifest_file, const char *cmp_desc,
             ClusterHandler *local_handler, ClusterHandler *remote_handler,
             bool mtime_care, bool batch_mode,
             DataStat &stat, RecordLogger *fail_logger) {
    static const int32_t MAX_BATCH_SIZE = 256 << 10; // 256K
    // open db with specified manifest(read only)
    leveldb::DB *db = NULL;
    leveldb::Options open_options;
    leveldb::Status s = open_db_readonly(db_path, manifest_file, cmp_desc, open_options, db);
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

    bool skip_in_bucket = false;
    bool skip_in_area = false;
    uint32_t start_time = 0;
    int32_t bucket = -1;
    int32_t area = -1;

    LdbKey ldb_key;
    LdbItem ldb_item;
    data_entry *key = NULL;
    data_entry *value = NULL;

    // for batch mode
    std::vector<uint64_t> ds_ids;
    tair_operc_vector kvs;
    int32_t batch_size = 0;

    int32_t mtime_care_flag = mtime_care ? TAIR_CLIENT_DATA_MTIME_CARE : 0;
    int ret = TAIR_RETURN_SUCCESS;

    if (db_it == NULL) {
        log_error("new db iterator fail.");
        ret = TAIR_RETURN_FAILED;
    } else {
        for (size_t i = 0; !g_stop && i < buckets.size(); ++i) {
            start_time = time(NULL);
            area = -1;
            ds_ids.clear();
            batch_size = 0;

            bucket = buckets[i];
            remote_handler->client()->get_server_id(bucket, ds_ids);

            for (size_t i = 0; i < ds_ids.size(); ++i) {
                log_warn("begin rsync bucket %d to %s", bucket, tbsys::CNetUtil::addrToString(ds_ids[i]).c_str());
            }

            // seek to bucket
            LdbKey::build_key_meta(scan_key, bucket);
            for (db_it->Seek(leveldb::Slice(scan_key, sizeof(scan_key))); !g_stop && db_it->Valid(); db_it->Next()) {
                ret = TAIR_RETURN_SUCCESS;
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
                if (!filter.ok(area, &ldb_key, &ldb_item)) {
                    skip_in_bucket = true;
                } else {
                    key = new data_entry(ldb_key.key(), ldb_key.key_size(), true);
                    value = NULL;
                    key->has_merged = true;
                    key->set_prefix_size(ldb_item.prefix_size());

                    // re-get from local
                    if (local_handler != NULL) {
                        ret = get_from_local_cluster(*local_handler, *key, value, skip_in_area);
                    } else {
                        value = new data_entry(ldb_item.value(), ldb_item.value_size(), true);
                        key->data_meta.cdate = value->data_meta.cdate = ldb_item.cdate();
                        key->data_meta.edate = value->data_meta.edate = ldb_item.edate();
                        key->data_meta.mdate = value->data_meta.mdate = ldb_item.mdate();
                        key->data_meta.version = value->data_meta.version = ldb_item.version();
                        key->data_meta.keysize = value->data_meta.keysize = key->get_size();
                        key->data_meta.valsize = value->data_meta.valsize = ldb_item.value_size();
                        value->data_meta.flag = ldb_item.flag();
                    }

                    if (ret == TAIR_RETURN_SUCCESS) {
                        log_debug("@@ k:%d %s %d %d %u %u %u.v:%s %d %d %u %u %u", key->get_area(),
                                  key->get_size() > 6 ? key->get_data() + 6 : "", key->get_size(),
                                  key->get_prefix_size(), key->data_meta.cdate, key->data_meta.mdate,
                                  key->data_meta.edate, value->get_size() > 4 ? value->get_data() + 4 : "",
                                  value->get_size(), value->data_meta.flag, value->data_meta.cdate,
                                  value->data_meta.mdate, value->data_meta.edate);
                        // mtime care / skip cache
                        key->data_meta.flag = mtime_care_flag | TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
                        key->server_flag = TAIR_SERVERFLAG_RSYNC;

                        // batch mode rsync
                        if (batch_mode) {
                            // sync to remote cluster
                            tair::operation_record *oprec = new tair::operation_record();
                            oprec->operation_type = 1;
                            oprec->key = key;
                            oprec->value = value;
                            kvs.push_back(oprec);
                            batch_size += key->get_size() + value->get_size() + sizeof(tair::item_data_info) * 2;

                            if (batch_size > MAX_BATCH_SIZE) {
                                if (g_wait_ms > 0) {
                                    ::usleep(g_wait_ms * 1000);
                                }
                                ret = do_batch_rsync(remote_handler, ds_ids, kvs, fail_logger);
                                batch_size = 0;
                            }
                        } else {
                            // use client put
                            ret = remote_handler->client()->put(key->get_area(), *key, *value, value->data_meta.edate,
                                                                value->data_meta.version, false/* not fill cache */);
                            if (ret == TAIR_RETURN_MTIME_EARLY) {
                                ret = TAIR_RETURN_SUCCESS;
                            }
                            // log failed key
                            if (ret != TAIR_RETURN_SUCCESS) {
                                std::string info = remote_handler->get_info();
                                log_fail(fail_logger, info, key, ret);
                            }

                            // cleanup
                            if (key != NULL) {
                                delete key;
                                key = NULL;
                            }
                            if (value != NULL) {
                                delete value;
                                value = NULL;
                            }
                        }
                    }
                }

                // update stat (actually, stat of batch mode is updated advanced)
                stat.update(bucket, skip_in_bucket ? -1 : area, // skip in bucket, then no area to update
                            ldb_key.key_size() + ldb_item.value_size(), (skip_in_bucket || skip_in_area),
                            ret == TAIR_RETURN_SUCCESS);
            }

            // last part
            if (!kvs.empty()) {
                do_batch_rsync(remote_handler, ds_ids, kvs, fail_logger);
            }

            log_warn("sync bucket %d over, cost: %ld(s), stat:\n",
                     bucket, time(NULL) - start_time);
            // only dump bucket stat
            stat.dump(bucket, -1);
        }
    }

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
            "synchronize one ldb version data of specified buckets to remote cluster.\n"
                    "%s -p dbpath -f manifest_file -c cmp_desc -r remote_cluster_addr -e faillogger_file -b buckets [-a yes_areas] [-A no_areas] [-w wait_ms] [-l local_cluster_addr] [-m] [-n]\n"
                    "NOTE:\n"
                    "\tcmp_desc like bitcmp OR numeric,:,2\n"
                    "\tcluster_addr like: xxx.xxx.xxx.xxx:5198,xxx.xxx.xxx.xxx:5198,group_1\n"
                    "\tbuckets/areas like: 1,2,3\n"
                    "\tconfig local cluster address mean that data WILL BE RE-GOT from local cluster\n"
                    "\twait_ms means wait time after each request\n"
                    "\t-m : batch mode (batch-mode can ONLY be used when data is not updated during rsyncing)\n"
                    "\t-n : NOT mtime_care\n", name);
}

int main(int argc, char *argv[]) {
    int ret = TAIR_RETURN_SUCCESS;
    char *db_path = NULL;
    char *manifest_file = NULL;
    char *cmp_desc = NULL;
    char *local_cluster_addr = NULL;
    char *remote_cluster_addr = NULL;
    char *fail_logger_file = NULL;
    char *buckets = NULL;
    char *yes_areas = NULL;
    char *no_areas = NULL;
    bool batch_mode = false;
    bool mtime_care = true;
    int i = 0;

    while ((i = getopt(argc, argv, "p:f:c:l:r:e:b:a:A:w:mn")) != EOF) {
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
            case 'r':
                remote_cluster_addr = optarg;
                break;
            case 'e':
                fail_logger_file = optarg;
                break;
            case 'b':
                buckets = optarg;
                break;
            case 'a':
                yes_areas = optarg;
                break;
            case 'A':
                no_areas = optarg;
                break;
            case 'w':
                g_wait_ms = atoll(optarg);
                break;
            case 'm':
                batch_mode = true;
                break;
            case 'n':
                mtime_care = false;
                break;
            default:
                print_help(argv[0]);
                return 1;
        }
    }

    if (db_path == NULL || manifest_file == NULL || cmp_desc == NULL ||
        remote_cluster_addr == NULL || fail_logger_file == NULL || buckets == NULL) {
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
    ClusterHandler *local_handler = NULL;
    if (local_cluster_addr != NULL) {
        local_handler = new ClusterHandler();
        ret = init_cluster_handler(local_cluster_addr, *local_handler);
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("init local client fail, addr: %s, ret: %d", local_cluster_addr, ret);
            delete local_handler;
            return 1;
        }
    }

    // init remote cluster handler(must)
    ClusterHandler *remote_handler = new ClusterHandler();
    ret = init_cluster_handler(remote_cluster_addr, *remote_handler);
    if (ret != TAIR_RETURN_SUCCESS) {
        log_error("init remote client fail, addr: %s, ret: %d", remote_cluster_addr, ret);
        delete remote_handler;
        return 1;
    }

    // init buckets
    std::vector<int32_t> bucket_container;
    std::vector<std::string> bucket_strs;
    tair::util::string_util::split_str(buckets, ", ", bucket_strs);
    for (size_t i = 0; i < bucket_strs.size(); ++i) {
        bucket_container.push_back(atoi(bucket_strs[i].c_str()));
    }

    std::random_shuffle(bucket_container.begin(), bucket_container.end());

    // init fail logger
    RecordLogger *fail_logger = new SequentialFileRecordLogger(fail_logger_file, 30 << 20/*30M*/, true/*rotate*/);
    if ((ret = fail_logger->init()) != TAIR_RETURN_SUCCESS) {
        log_error("init fail logger fail, ret: %d", ret);
    } else {
        DataFilter filter(yes_areas, no_areas);
        // init data stat
        DataStat stat;

        log_warn("start rsync data, g_wait_ms: %"PRI64_PREFIX"d, mtime_care: %s", g_wait_ms, mtime_care ? "yes" : "no");
        // do data rsync
        uint32_t start_time = time(NULL);
        ret = do_rsync(bucket_container, filter,
                       db_path, manifest_file, cmp_desc,
                       local_handler, remote_handler,
                       mtime_care, batch_mode,
                       stat, fail_logger);

        log_warn("rsync data over, stopped: %s, cost: %ld(s), stat:", g_stop ? "yes" : "no", time(NULL) - start_time);
        stat.dump_all();
    }

    // cleanup
    delete fail_logger;
    if (local_handler != NULL) {
        delete local_handler;
    }
    if (remote_handler != NULL) {
        delete remote_handler;
    }

    return ret == TAIR_RETURN_SUCCESS ? 0 : 1;
}
