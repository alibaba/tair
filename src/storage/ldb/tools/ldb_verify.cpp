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

#include <fstream>
#include <iterator>
#include <algorithm>
#include <stack>
#include <unistd.h>
#include <cstdlib>
#include <signal.h>
#include <libgen.h>

#include "common/define.hpp"
#include "common/util.hpp"
#include "common/data_entry.hpp"
#include "common/record.hpp"
#include "common/record_logger_util.hpp"
#include "packets/mupdate_packet.hpp"
#include "dataserver/remote_sync_manager.hpp"

#include "leveldb/env.h"

#include "ldb_define.hpp"
#include "ldb_comparator.hpp"
#include "ldb_util.hpp"

using namespace tair::storage::ldb;
using tair::data_entry;
using tair::common::Record;
using tair::common::KVRecord;
using tair::common::RecordLogger;
using tair::common::SequentialFileRecordLogger;
using tair::tair_client_impl;
using tair::FailRecord;
using tair::tair_operc_vector;
using std::string;
using std::vector;
using std::stack;
using std::set;
using tbsys::CConfig;
using tair::util::string_util;

const int MAX_INSTANCES = 64;
const int MAX_WORKERS = 24;
bool g_stop = false;

struct Configure;
struct Statistics;
struct Task;

class DestBucket;

static bool download_local_file_create(Configure *c);

static int data_write_local(Configure *c, const char *keydata, int keysize, const char *valdata, int valsize,
                            bool only_download_key);

static void signal_handler(int sig);

static void help(const char *prog);

static void fill_meta(data_entry *data, LdbKey &key, LdbItem &item);

static bool parse_args(int argc, char **argv, Configure *c);

static bool parse_conf(Configure *c);

static bool parse_area_list(const char *s, Configure *c);

static bool parse_bucket_list(const char *s, Configure *c);

static void
parse_cluster_string(std::vector<std::string> &infos, const char *line, const char *separator, const char *err_msg);

static void parse_rsync_local_cluster(Configure *c, const char *line);

static void parse_rsync_remote_cluster(Configure *c, const char *line);

static bool parse_rsync_info(Configure *c);

static bool fill_bucket_map(Configure *c);

static bool do_backup_db(tair_client_impl *client, uint64_t ds);

static bool do_unload_db(tair_client_impl *client, uint64_t ds);

static bool do_task(Task *t, Configure *c, int target_area);

static void mark_key_interunit_or_innerunit_flag(Configure *c, data_entry *key);

static std::pair<int, DestBucket *>
do_remote_batch_put(Task *task, data_entry *key, data_entry *value, DestBucket *dbt = NULL);

static int block_batch_put_util_success(Task *task, data_entry *key, data_entry *value, DestBucket *dbt = NULL);

static bool load_last_finished_tasks(Configure *c);

static bool is_task_finished_last_time(Configure *c, int instance, int bucket, int area);

static void commit_finish_task(Configure *c, int instance, int bucket, int area);

static bool do_manifest(Configure *c);

static void do_statistics(int area, LdbKey &key, LdbItem &item);

static int get_from_local(tair_client_impl &, data_entry &, data_entry *&, bool &skip);

static int get_from_slave(tair_client_impl &, data_entry &, data_entry *&, bool &skip);

static int get_from_remote(tair_client_impl &, data_entry &, data_entry *&, bool &skip);

static bool
verify_entry(bool remote, int area, const data_entry *, const data_entry *, const data_entry *, bool log_key);

static void summary();

static void *thread(void *args);

static std::string abs_dirname(const std::string &path);

static std::string abs_path(const std::string &path);

static unsigned long strntoul(const char *s, size_t n);

struct Configure {
    bool download;
    bool only_download_key;
    string sync_type;
    bool is_batch_send;
    bool set_rsync_info_by_cmdline;
    bool just_get_key_value_from_local;
    bool is_external;
    bool do_rectify;
    bool only_statistics;
    int localfile;
    int n_instance;
    int n_task;
    int redirect_area;
    time_t range_start;
    time_t range_end;
    bool skip_compare;
    bool skip_slave;
    bool log_key;
    int max_dst_bucket_count;
    int max_area;
    int fix_expire;
    string filter;
    string prefix_key;
    string conf_file;
    string db_base;
    string tair_base;
    string manifest_file[MAX_INSTANCES];
    string cmp_desc;
    uint64_t local_ds;
    string local_master;
    string local_slave;
    string local_group;
    string remote_master;
    string remote_slave;
    string remote_group;
    vector<vector<int> > inst_bucket_map;
    tair_client_impl *local_client;
    tair_client_impl *remote_client;
    CConfig c;
    set<int> areas;
    set<int> buckets;
    pthread_spinlock_t biglock;
    pthread_mutex_t downloadlock;
    set<std::string> last_run_finished_tasks;

    Configure() {
        sync_type = "";
        is_external = false;
        is_batch_send = false;
        do_rectify = false;
        only_statistics = false;
        log_key = false;
        localfile = -1;
        n_instance = 0;
        n_task = 1;
        redirect_area = -1;
        range_start = 0;
        range_end = 0;
        skip_compare = false;
        skip_slave = false;
        local_client = NULL;
        remote_client = NULL;
        fix_expire = 0;
        prefix_key = "";
        max_dst_bucket_count = 0;
        download = false;
        only_download_key = false;
        filter = "";
        max_area = TAIR_MAX_AREA_COUNT;
        set_rsync_info_by_cmdline = false;
        just_get_key_value_from_local = false;
        pthread_spin_init(&biglock, PTHREAD_PROCESS_PRIVATE);
        pthread_mutex_init(&downloadlock, NULL);
    }

    ~Configure() {
        if (local_client != NULL) {
            local_client->close();
            delete local_client;
        }
        if (remote_client != NULL) {
            remote_client->close();
            delete remote_client;
        }
        if (localfile >= 0) {
            close(localfile);
        }
        pthread_spin_destroy(&biglock);
        pthread_mutex_destroy(&downloadlock);
    }

    friend std::ostream &operator<<(std::ostream &os, const Configure &c) {
        os << std::endl;
        os << "download: " << c.download << std::endl;
        os << "only_download_key : " << c.only_download_key << std::endl;
        if (c.buckets.empty()) {
            os << "buckets: all" << std::endl;
        } else {
            os << "buckets: [ " << std::endl;
            std::set<int>::iterator it = c.buckets.begin();
            if (it != c.buckets.end()) {
                os << (*it);
                it++;
                for (; it != c.buckets.end(); it++) {
                    os << ", " << (*it);
                }
            }
            os << "]" << std::endl;
        }
        os << "local file fd: " << c.localfile << std::endl;
        os << "filter: " << c.filter << std::endl;
        os << "max_area: " << c.max_area << std::endl;
        os << "redirect_area: " << c.redirect_area << std::endl;
        os << "sync_type: " << (c.sync_type) << std::endl;
        os << "is_batch_send: " << (c.is_batch_send ? "true" : "false") << std::endl;
        os << "set_rsync_info_by_cmdline: " << (c.set_rsync_info_by_cmdline ? "true" : "false") << std::endl;
        os << "just_get_key_value_from_local: " << (c.just_get_key_value_from_local ? "true" : "false") << std::endl;
        os << "is_external: " << (c.is_external ? "true" : "false") << std::endl;
        os << "do_rectify: " << (c.do_rectify ? "true" : "false") << std::endl;
        os << "only_statistics: " << (c.only_statistics ? "true" : "false") << std::endl;
        os << "log_key: " << (c.log_key ? "true" : "false") << std::endl;
        os << "fix_expire: " << c.fix_expire << std::endl;
        os << "n_instance: " << c.n_instance << std::endl;
        os << "n_task: " << c.n_task << std::endl;
        os << "range_start: " << c.range_start << std::endl;
        os << "range_end: " << c.range_end << std::endl;
        os << "skip_compare: " << (c.skip_compare ? "true" : "false") << std::endl;
        os << "skip_slave: " << (c.skip_slave ? "true" : "false") << std::endl;
        os << "conf_file: " << c.conf_file << std::endl;
        os << "db_base: " << c.db_base << std::endl;
        os << "tair_base: " << c.tair_base << std::endl;
        os << "prefix: " << c.prefix_key << std::endl;
        {
            os << "manifest_file: [";
            for (int i = 0; i < c.n_instance; i++) {
                os << c.manifest_file[i] << " ";
            }
            os << "]" << std::endl;
        }
        os << "cmp_desc: " << c.cmp_desc << std::endl;
        os << "local_ds: " << c.local_ds << std::endl;
        os << "local_master: " << c.local_master << std::endl;
        os << "local_slave: " << c.local_slave << std::endl;
        os << "local_group: " << c.local_group << std::endl;
        os << "remote_master: " << c.remote_master << std::endl;
        os << "remote_slave: " << c.remote_slave << std::endl;
        os << "remote_group: " << c.remote_group << std::endl;
        {
            os << "inst_bucket_map: [" << std::endl;
            for (size_t i = 0; i < c.inst_bucket_map.size(); i++) {
                {
                    os << "\t[";
                    for (size_t j = 0; j < c.inst_bucket_map[i].size(); j++) {
                        os << c.inst_bucket_map[i][j] << " ";
                    }
                    os << "]" << std::endl;
                }
            }
            os << "]" << std::endl;
        }
        {
            os << "areas: [";
            for (std::set<int>::iterator iter = c.areas.begin();
                 iter != c.areas.end(); iter++) {
                os << (*iter) << " ";
            }
            os << "]" << std::endl;
        }

        {
            os << "last finished task: [";
            for (std::set<std::string>::iterator iter = c.last_run_finished_tasks.begin();
                 iter != c.last_run_finished_tasks.end(); iter++) {
                os << (*iter) << " ";
            }
            os << "]" << std::endl;
        }

        return os;
    }
};

class DestBucket {
public:
    DestBucket(int b) {
        records = new std::vector<Record *>();
        bucket = b;
    }

    ~DestBucket() {
        delete records;
    }

    std::vector<Record *> *get_prepared_records() {
        return records;
    }

    void clean_prepared_record() {
        for (size_t i = 0; i < records->size(); i++) {
            delete (*records)[i];
        }
        records->clear();
        records_size = 0;
    }

    int get_bucket() {
        return bucket;
    }

private:
    bool can_sync() {
        if (records->size() == 0) return false;
        return (records_size > 1024 * 1024) || records->size() > 500;
    }

    void prepare_sync(int bucket, data_entry *key, data_entry *value) {
        KVRecord *record = new KVRecord(TAIR_REMOTE_SYNC_TYPE_PUT, bucket, key, value);
        records->push_back(record);
        records_size += key->get_size() + value->get_size();
    }

private:
    int bucket;
    int records_size;
    std::vector<Record *> *records;
private:
    friend struct Task;
};

struct Task {
    Task(int i, int32_t b) : instance(i), bucket(b) {}

    ~Task() {
        std::map<int, DestBucket *>::iterator iter;
        for (iter = destbuckets.begin(); iter != destbuckets.end(); iter++) {
            delete iter->second;
        }
    }

    DestBucket *prepare_sync(data_entry *key, data_entry *value) {
        int destbucket = recalc_dest_bucket(key, c->max_dst_bucket_count);
        std::map<int, DestBucket *>::iterator iter = destbuckets.find(destbucket);
        DestBucket *dbt = NULL;
        if (iter == destbuckets.end()) {
            dbt = new DestBucket(destbucket);
            destbuckets.insert(make_pair<int, DestBucket *>(destbucket, dbt));
        } else {
            dbt = iter->second;
        }
        dbt->prepare_sync(destbucket, key, value);
        if (dbt->can_sync()) return dbt;
        return NULL;
    }

    DestBucket *end() {
        std::map<int, DestBucket *>::iterator iter = destbuckets.begin();
        if (iter == destbuckets.end()) return NULL;
        DestBucket *dbt = iter->second;
        destbuckets.erase(iter);
        return dbt;
    }

    int recalc_dest_bucket(const data_entry *key, uint32_t max_bucket_count) {
        uint32_t hash;
        int prefix_size = key->get_prefix_size();
        int32_t diff_size = key->has_merged ? TAIR_AREA_ENCODE_SIZE : 0;
        if (prefix_size == 0) {
            hash = string_util::mur_mur_hash(key->get_data() + diff_size, key->get_size() - diff_size);
        } else {
            hash = string_util::mur_mur_hash(key->get_data() + diff_size, prefix_size);
        }

        return hash % max_bucket_count;
    }

    int task_id;
    int instance;
    int32_t bucket;
    Configure *c;
    std::map<int, DestBucket *> destbuckets;
};

class TaskList {
public:
    TaskList();

    ~TaskList();

    void push(Task *t);

    Task *pop();

private:
    pthread_spinlock_t lock;
    std::stack<Task *> tasks;
} task_list;

struct Statistics {
    uint64_t size_total;
    uint64_t cold_size_total;
    uint64_t items_total;
    struct {
        uint64_t area_size_total;
        uint64_t area_size_cold;
        uint64_t area_items;
    } metrics[TAIR_MAX_AREA_COUNT];
} statistics;

enum {
    CONFLICT_VALUE,
    CONFLICT_VALUE_SIZE,
    CONFLICT_META_KEY_SIZE,
    CONFLICT_META_VAL_SIZE,
    CONFLICT_META_FLAG,
    CONFLICT_MAX
};

struct conflict_stat_t {
    uint64_t counter[CONFLICT_MAX + 1];
} conflict_stat[2][TAIR_MAX_AREA_COUNT];


int
main(int argc, char **argv) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGALRM, signal_handler);
    signal(SIGPIPE, SIG_IGN);
    alarm(10 * 60);
    TBSYS_LOGGER.setLogLevel("warn");

    Configure c;
    if (!parse_args(argc, argv, &c)) {
        return 1;
    }
    if (!parse_conf(&c)) {
        return 1;
    }

    if (load_last_finished_tasks(&c) == false) {
        return 1;
    }

    if (c.download == true && download_local_file_create(&c) == false) {
        return 1;
    }

    {
        std::ostringstream os;
        os << c;
        log_warn("Configure:\n%s", os.str().c_str());
//    log_warn("You can check Configure for 5 minutes...");
//    sleep(5 * 60);
//    log_warn("Here we go");
    }

    // do nothing always success
    if (!do_backup_db(c.local_client, c.local_ds)) {
        log_error("backup db failed");
        return 1;
    }

    //~ set up tasks, trying to distribute instances among different workers
    bool has_task;
    int task_id = 0;
    do {
        has_task = false;
        for (int i = 0; i < c.n_instance; ++i) {
            if (!c.inst_bucket_map[i].empty()) {
                has_task = true;
                int32_t bucket = c.inst_bucket_map[i].back();
                c.inst_bucket_map[i].pop_back();
                if (c.buckets.empty() == false && c.buckets.find(bucket) == c.buckets.end()) {
                    log_warn("bucket %d is not in my job list", bucket);
                    continue;
                }
                Task *t = new Task(i, bucket);
                t->c = &c;
                t->task_id = task_id;
                task_list.push(t);
                log_warn("task[%d]: instance[%d], bucket[%d]", task_id++, i, bucket);
            }
        }
    } while (has_task);

    pthread_t tids[MAX_WORKERS];

    //~ launch workers
    for (int i = 0; i < c.n_task; ++i) {
        pthread_create(&tids[i], NULL, thread, &c);
    }

    //~ join workers
    for (int i = 0; i < c.n_task; ++i) {
        pthread_join(tids[i], NULL);
    }

    // do nothing always success
    if (!do_unload_db(c.local_client, c.local_ds)) {
        log_error("unload db failed");
    }

    //~ print out summary infos
    summary();

    return 0;
}

void
signal_handler(int sig) {
    log_warn("signal %d caught", sig);
    switch (sig) {
        case SIGTERM:
        case SIGINT:
            g_stop = true;
            break;
        case SIGALRM:
            summary();
            alarm(10 * 60);
            break;
        default:
            break;
    }
}

// ./ldb_verify -f dataserver.conf -l fix.log -t 4 -e -b timerangestart -d timerangeend -k -l -a 2
void
help(const char *prog) {
    fprintf(stderr, "USAGE\n");
    fprintf(stderr,
            "\t%s [-e] [-c] <-f config_file>\n"
                    "-h, --help\n\t"
                    "print this message\n"
                    "-w, --download\n\t"
                    "download data to local like:download.txt.TIMESTAMP, never send to others\n"
                    "-n, --only_download_key\n\t"
                    "only download key to local, used along with -w\n"
                    "-i, --filter\n\t"
                    "prefix match, just support java data\n"
                    "-f, --config-file\n\t"
                    "dataserver.conf\n"
                    "-a, --area-list\n\t"
                    "area list to run verification on\n"
                    "-u, --bucket-list\n\t"
                    "bucket list to run job on\n"
                    "-r, --redirect-area\n\t"
                    "redirect all areas' data to specifies area\n"
                    "-l, --log-file\n\t"
                    "path of log file\n"
                    "-t, --task-count\n\t"
                    "number of tasks doing the work\n"
                    "-e, --external-verify\n\t"
                    "do verification between clusters, meanwhile\n"
                    "-s, --only-statistics\n\t"
                    "only do statistics\n"
                    "-c, --confirm\n\t"
                    "confirm to rectify inconsistent data\n"
                    "-b, --range-start\n\t"
                    "data time range start\n"
                    "-d, --range-end\n\t"
                    "data time range end\n"
                    "-o, --log-key\n\t"
                    "write key which has problem into file\n"
                    "-k, --skip-compare\n\t"
                    "skip compare with key in other node\n"
                    "-R, --max-area\n\t"
                    "max area\n"
                    "-P, --prefix\n\t"
                    "just do work with prefix key\n"
                    /*
                    "-i, --fix-expire\n\t"
                    "fix key's expire time to what you want\n"
                    */
                    "-p, --skip-slave\n\t"
                    "skip do work with slave\n"
                    "-x, --batch-send\n\t"
                    "set ldb verify use batch send\n"
                    "-j, --just-get-key-value-from-local\n\t"
                    "just get key value pair from local machine\n"
                    "-T, --sync-type client|interunit|innerunit\n\t"
                    "set rsync type, default is innerunit\n"
                    "-S, --set-local-cluster master.cs,slave.cs,group\n\t"
                    "set local cluster in cmdline, it will replace which read from dataserver.conf\n"
                    "-D, --set-remote-cluster master.cs,slave.cs,groupname\n\t"
                    "set remote cluster in cmdline, it will replace which read from dataserver.conf\n",
            prog);
    fprintf(stderr,
            "CAVEATS\n"
                    "\t1. backup_db specified dataserver.\n"
                    "\t2. shutdown the compaction acceleration.\n"
                    "\t3. run this tool.\n"
                    "\t4. restart the compaction acceleration if necessary.\n"
                    "\t5. unload_backuped_db the dataserver\n");
}

bool
parse_args(int argc, char **argv, Configure *c) {
    int opt;
    const char *optstr = "xjhcskpeowi:P:f:t:l:a:R:b:d:r:S:D:T:u:n";

    bool arg_src_ok = false;
    bool arg_dst_ok = false;

    struct option long_opts[] = {
            {"help",                          0, NULL, 'h'},
            {"config-file",                   1, NULL, 'f'},
            {"area-list",                     1, NULL, 'a'},
            {"download",                      0, NULL, 'w'},
            {"only_download_key",             0, NULL, 'n'},
            {"filter",                        1, NULL, 'i'},
            {"redirect-area",                 1, NULL, 'r'},
            {"task-count",                    1, NULL, 't'},
            {"external-verify",               0, NULL, 'e'},
            {"only-statistics",               0, NULL, 's'},
            {"confirm",                       0, NULL, 'c'},
            {"log-file",                      1, NULL, 'l'},
            {"range-start",                   1, NULL, 'b'},
            {"range-end",                     1, NULL, 'd'},
            {"skip-compare",                  0, NULL, 'k'},
            {"skip-slave",                    0, NULL, 'p'},
            {"log-key",                       0, NULL, 'o'},
            {"batch-send",                    0, NULL, 'x'},
            {"sync-type",                     1, NULL, 'T'},
            {"set-local-cluster",             1, NULL, 'S'},
            {"set-remote-cluster",            1, NULL, 'D'},
            {"just-get-key-value-from-local", 0, NULL, 'j'},
            {"max-area",                      1, NULL, 'R'},
            {"prefix",                        1, NULL, 'P'},
            {"bucket-list",                   1, NULL, 'u'},
            /* {"fix-expire", 1, NULL, 'i'}, */
            {NULL,                            0, NULL, 0}
    };

    //~ do parsing
    while ((opt = getopt_long(argc, argv, optstr, long_opts, NULL)) != -1) {
        switch (opt) {
            case 'f':
                c->conf_file = optarg;
                break;
            case 'a':
                if (!parse_area_list(optarg, c)) {
                    log_error("illegal area-list: %s", optarg);
                    return false;
                }
                break;
            case 'u':
                if (!parse_bucket_list(optarg, c)) {
                    log_error("illegal bucket-list: %s", optarg);
                    return false;
                }
                break;
            case 'w':
                c->download = true;
                break;
            case 'n':
                c->only_download_key = true;
                break;
            case 'i':
                c->filter = optarg;
                break;
            case 'r':
                c->redirect_area = atoi(optarg);
                break;
            case 'b':
                if (!tair::util::time_util::s2time(optarg, &(c->range_start))) {
                    log_error("time format YYYYMMDDddmmss");
                    return false;
                }
                break;
            case 'd':
                if (!tair::util::time_util::s2time(optarg, &(c->range_end))) {
                    log_error("time format YYYYMMDDddmmss");
                    return false;
                }
                break;
            case 'k':
                c->skip_compare = true;
                break;
            case 'p':
                c->skip_slave = true;
                break;
            case 'R':
                c->max_area = atoi(optarg);
                break;
            case 'o':
                c->log_key = true;
                break;
                /*
              case 'i':
                c->fix_expire = atoi(optarg);
                break;
                */
            case 'x':
                c->is_batch_send = true;
                break;
            case 'j':
                c->just_get_key_value_from_local = true;
                break;
            case 'T':
                c->sync_type = optarg;
                break;
            case 'S':
                parse_rsync_local_cluster(c, optarg);
                arg_src_ok = true;
                break;
            case 'D':
                parse_rsync_remote_cluster(c, optarg);
                arg_dst_ok = true;
                break;
            case 't':
                c->n_task = atoi(optarg);
                break;
            case 'c':
                c->do_rectify = true;
                break;
            case 'e':
                c->is_external = true;
                break;
            case 's':
                c->only_statistics = true;
                break;
            case 'P':
                c->prefix_key = optarg;
                break;
            case 'l':
                TBSYS_LOGGER.setFileName(optarg);
                break;
            case '?':
            case 'h':
            default :
                help(argv[0]);
                exit(1);
                break;
        }
    }

    if (c->areas.empty()) {
        for (int i = 0; i < c->max_area; i++) {
            c->areas.insert(i);
        }
    }

    if (c->is_batch_send == true && c->redirect_area >= 0) {
        log_error("batch mode can't work with redirect area method");
        return false;
    }

    if ((c->is_external == true && arg_src_ok == true && arg_dst_ok == true) ||
        (c->is_external == false && arg_src_ok == true && arg_dst_ok == false)) {
        c->set_rsync_info_by_cmdline = true;
    }

    //~ then checking
    if (c->conf_file.empty()) {
        help(argv[0]);
        return false;
    }
    if (c->n_task > MAX_WORKERS) {
        log_error("too many workers: %d", c->n_task);
        return false;
    }
    if (c->n_task == 0) {
        log_error("illegal arguments for -t/--thread-count");
        return false;
    }
    if (c->is_external) {
        log_error("external");
    }
    //~ all is well
    return true;
}

bool
parse_conf(Configure *c) {
    //~ parse the data server's configure file
    log_warn("loading config file: %s", c->conf_file.c_str());
    if (c->c.load(c->conf_file.c_str()) != 0) {
        log_error("%s: invalid configure file", c->conf_file.c_str());
        return false;
    }

    c->tair_base = abs_dirname(abs_dirname(c->conf_file));
    log_warn("base directory of tair: %s", c->tair_base.c_str());
    if (c->tair_base.empty()) {
        return false;
    }

    c->n_instance = c->c.getInt("ldb", "ldb_db_instance_count");
    c->db_base = abs_path(c->c.getString("ldb", "data_dir", ""));
    log_warn("base directory of ldb: %s", c->db_base.c_str());
    if (c->db_base.empty()) {
        return false;
    }
    c->cmp_desc = c->c.getString("ldb", "ldb_comparator_type", "bitcmp"/*default*/);
    std::string userkey_skip_meta_size = c->c.getString("ldb", "ldb_userkey_skip_meta_size", "0");
    std::string userkey_num_delimiter = c->c.getString("ldb", "ldb_userkey_num_delimiter", " ");

    if (c->cmp_desc == "numeric") {
        std::stringstream os;
        os << c->cmp_desc << ", " << userkey_num_delimiter << ", " << userkey_skip_meta_size;
        c->cmp_desc = os.str();
    }

    if (c->set_rsync_info_by_cmdline == false) {
        //~ retrieve local/remote cluster infos
        if (!parse_rsync_info(c)) {
            log_error("parse rsync info failed");
            return false;
        }
    }

    //~ BTW. establish necessary clients
    c->local_client = new tair_client_impl;
    bool success = c->local_client->startup(c->local_master.c_str(),
                                            c->local_slave.c_str(),
                                            c->local_group.c_str());
    if (c->is_external && !c->only_statistics) {
        c->remote_client = new tair_client_impl;
        success &= c->remote_client->startup(c->remote_master.c_str(),
                                             c->remote_slave.c_str(),
                                             c->remote_group.c_str());
        log_error("connect to remote: %s[%s]",
                  c->remote_master.c_str(), c->remote_group.c_str());
        c->max_dst_bucket_count = c->remote_client->get_max_bucket_count();
        log_error("Remote Cluster Bucket Count %d, Local Cluster Bucket Count %d",
                  c->max_dst_bucket_count, c->local_client->get_max_bucket_count());
    }
    if (!success) {
        return false;
    }

    //~ resolve the local address via device name
    const char *dev_name = c->c.getString(TAIRSERVER_SECTION, TAIR_DEV_NAME, NULL);
    int port = c->c.getInt(TAIRSERVER_SECTION, TAIR_PORT, TAIR_SERVER_DEFAULT_PORT);
    uint32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name);
    if (local_ip == 0U) {
        log_error("cannot resolve local IP[%s]", dev_name);
        return false;
    }

    c->local_ds = tbsys::CNetUtil::ipToAddr(local_ip, port);

    //~ map ldb instances to buckets
    if (!fill_bucket_map(c)) {
        return false;
    }

    //~ retrieve the manifest file of every ldb instance
    if (!do_manifest(c)) {
        return false;
    }

    return true;
}

unsigned long
strntoul(const char *s, size_t n) {
    const char *end = s + n;
    unsigned long result = 0;
    while (s != end) {
        if (*s < '0' || *s > '9') {
            return (unsigned long) -1;
        }
        result *= 10;
        result += *s - '0';
        ++s;
    }
    return result;
}

bool
parse_area_list(const char *s, Configure *c) {
    size_t size = strlen(s);
    const char *e = s + size;
    const char *p;
    do {
        p = strchr(s, ',');
        if (p == NULL) p = e;
        int area = (int) strntoul(s, p - s);
        if (area < 0 || area >= TAIR_MAX_AREA_COUNT) {
            return false;
        }
        c->areas.insert(area);
        s = p + 1;
    } while (p != e && s < e);

    return true;
}

bool
parse_bucket_list(const char *s, Configure *c) {
    size_t size = strlen(s);
    const char *e = s + size;
    const char *p;
    do {
        p = strchr(s, ',');
        if (p == NULL) p = e;
        int bucket = (int) strntoul(s, p - s);
        c->buckets.insert(bucket);
        s = p + 1;
    } while (p != e && s < e);

    return true;
}

void
parse_cluster_string(std::vector<std::string> &infos, const char *line,
                     const char *separator, const char *err_msg) {
    tair::common::string_util::split_str(line, separator, infos);
    if (infos.size() != 3) {
        log_error(err_msg);
        exit(1);
    }
}

void
parse_rsync_local_cluster(Configure *c, const char *line) {
    std::vector<std::string> infos;
    parse_cluster_string(infos, line, ",", "error local cluster info");
    c->local_master = infos[0];
    c->local_slave = infos[1];
    c->local_group = infos[2];
}

void
parse_rsync_remote_cluster(Configure *c, const char *line) {
    std::vector<std::string> infos;
    parse_cluster_string(infos, line, ",", "error remote cluster info");
    c->remote_master = infos[0];
    c->remote_slave = infos[1];
    c->remote_group = infos[2];
}

bool
parse_rsync_info(Configure *c) {
    string rsync_info = c->c.getString("tairserver", "rsync_conf", "");
    if (rsync_info.empty()) {
        log_error("no rsync_conf found in %s", c->conf_file.c_str());
        return false;
    }

    size_t b, e;
    b = rsync_info.find_first_of('[');
    e = rsync_info.find_first_of(']');
    if (b == string::npos || e == string::npos) {
        return false;
    }
    ++b;
    string local_info = rsync_info.substr(b, e - b);

    b = rsync_info.find_last_of('[');
    e = rsync_info.find_last_of(']');
    if (b == string::npos || e == string::npos) {
        return false;
    }
    ++b;
    string remote_info = rsync_info.substr(b, e - b);

    vector<string> infos;
    infos.reserve(4);

    tair::common::string_util::split_str(local_info.c_str(), ", ", infos);
    c->local_master = infos[0];
    c->local_slave = infos[1];
    c->local_group = infos[2];

    infos.clear();
    tair::common::string_util::split_str(remote_info.c_str(), ", ", infos);
    c->remote_master = infos[0];
    c->remote_slave = infos[1];
    c->remote_group = infos[2];

    return true;
}

bool
fill_bucket_map(Configure *c) {
    //~ all buckets this server holds
    std::set<int32_t> buckets;
    c->local_client->get_buckets_by_server(c->local_ds, buckets);
    if (buckets.empty()) return false;
    log_warn("bucket count this server holds: %lu", buckets.size());

    c->inst_bucket_map.resize(c->n_instance);

    string f_bindex = c->tair_base + "/data/bindex/ldb_bucket_index_map";
    log_warn("target bindex file: %s", f_bindex.c_str());
    if (::access(f_bindex.c_str(), F_OK) == 0) {
        ifstream inf(f_bindex.c_str()); //~ C++98 sucks

        istream_iterator <string> end;
        istream_iterator <string> beg(inf);
        ++beg; //~ ignore first line, i.e. instance count

        vector<string> lines;
        lines.reserve(500);

        copy(beg, end, back_inserter(lines));

        vector<string>::iterator itr = lines.begin();
        while (itr != lines.end()) {
            int bucket, instance;
            sscanf(itr->c_str(), "%d:%d", &bucket, &instance);
            fprintf(stderr, "bucket[%d]:instance[%d]\n", bucket, instance);
            if (buckets.find(bucket) != buckets.end()) {
                c->inst_bucket_map[instance].push_back(bucket);
            }
            ++itr;
        }

        inf.close();
    } else {
        for (int i = 0; i < c->n_instance; ++i) {
            copy(buckets.begin(), buckets.end(),
                 back_inserter(c->inst_bucket_map[i]));
        }
    }

    return true;
}


bool
do_backup_db(tair_client_impl *client, uint64_t ds) {
//  std::vector<std::string> cmd_params;
//  std::vector<std::string> infos;
//  int ret = client->op_cmd_to_ds(TAIR_SERVER_CMD_BACKUP_DB, &cmd_params, &infos, NULL);
//  if (ret == TAIR_RETURN_SUCCESS) {
//    return true;
//  }
//  log_error("backup_db fail ret = %d", ret);
    return true;
}

bool
do_unload_db(tair_client_impl *client, uint64_t ds) {
//  std::vector<std::string> cmd_params;
//  std::vector<std::string> infos;
//  int ret = client->op_cmd_to_ds(TAIR_SERVER_CMD_UNLOAD_BACKUPED_DB, &cmd_params, &infos, NULL);
//  if (ret == TAIR_RETURN_SUCCESS) {
//    return true;
//  }
//  log_error("unload_backuped_db fail ret = %d", ret);
    return true;
}

//~ implemented in libmdb.a, anyhow
double get_timespec();

void fill_meta(data_entry *data, LdbKey &key, LdbItem &item) {
    data->data_meta.flag = item.flag();
    data->data_meta.mdate = item.mdate();
    data->data_meta.cdate = item.cdate();
    data->data_meta.edate = item.edate();
    data->data_meta.version = item.version();
    data->data_meta.valsize = item.value_size();
    data->data_meta.keysize = key.key_size();
}

inline bool compare_cstr(const char *s1, size_t len1, const char *s2, size_t len2) {
    if (s2 == NULL || len2 == 0 || len1 != len2) return false;
    return strncmp(s1, s2, len1) == 0;
}

bool
download_local_file_create(Configure *c) {
    // TODO
    string now = tair::util::time_util::time_to_str(time(NULL));
    string name("download.txt.");
    char filename[256] = {0};
    memcpy(filename, name.c_str(), name.length());
    memcpy(filename + name.length(), now.c_str(), now.length());
    c->localfile = open(filename, O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (c->localfile < 0) {
        log_error("can't open %s to download, err: %d, msg: %s", filename, errno, strerror(errno));
        return false;
    }
    return true;
}

int
data_write_local(Configure *c, const char *key, int keysize,
                 const char *val, int valsize, bool only_download_key) {
    int length = keysize + valsize + 2;
    char *b = (char *) malloc(sizeof(char) * (length));
    memcpy(b, key, keysize);
    if (only_download_key) {
        length = keysize + 1;
    } else {
        b[keysize] = ' ';
        memcpy(b + keysize + 1, val, valsize);
    }
    b[length - 1] = '\n';
    for (int i = 0; i < keysize; ++i) {
        if ((unsigned char) b[i] > 127) {
            log_info("exist no ascii character[%u] index: %d, keysize: %d, key_value: %s.",
                     (unsigned char) b[i], i, keysize, b);
            break;
        }
    }

    pthread_mutex_lock(&c->downloadlock);
    int offset = 0;
    while (offset < length) {
        int len = write(c->localfile, b + offset, length - offset);
        if (len < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
        }
        offset += len;
    }
    free(b);
    pthread_mutex_unlock(&c->downloadlock);
    return 0;
}

bool
do_task(Task *t, Configure *c, int target_area) {
    log_warn("start task[%d]: instance[%d], bucket[%d], target_area[%d]",
             t->task_id, t->instance, t->bucket, target_area);
    //~ setup

    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s%d/ldb", c->db_base.c_str(), t->instance + 1);
    leveldb::Options open_opts;
    leveldb::Status status;
    leveldb::DB *db = NULL;
    status = open_db_readonly(db_path,
                              c->manifest_file[t->instance].c_str(),
                              c->cmp_desc.c_str(),
                              open_opts, db);
    if (!status.ok()) {
        log_error("open db[%s] failed: %s", db_path, status.ToString().c_str());
        return 1;
    }

    //~ open an iterator for this task
    leveldb::ReadOptions scan_opts;
    scan_opts.verify_checksums = false;
    scan_opts.fill_cache = false;
    leveldb::Iterator *itr = db->NewIterator(scan_opts);
    char start_key_of_bucket[LDB_KEY_META_SIZE + LDB_KEY_AREA_SIZE] = {0};
    LdbKey ldb_key;
    LdbItem ldb_item;

    int area = -1;
    int bucket = t->bucket;
    tair_client_impl slave_client;

    if (c->skip_slave == false) {
        vector<uint64_t> slaves;
        c->local_client->get_server_id(bucket, slaves, true);
        if (slaves.empty()) {
            log_error("no slaves found for %s", tbsys::CNetUtil::addrToString(c->local_ds).c_str());
            return false;
        }
        string sslave = tbsys::CNetUtil::addrToString(slaves[0]);
        if (!slave_client.directup(sslave.c_str())) {
            log_error("cannot connect to slave ds: %s", sslave.c_str());
            return false;
        }
    }

    //~ scanning
    log_warn("scanning bucket[%d]...", bucket);
    LdbKey::build_key_meta(start_key_of_bucket, bucket);
    memcpy(&start_key_of_bucket[LDB_KEY_META_SIZE], &target_area, LDB_KEY_AREA_SIZE);
    itr->Seek(leveldb::Slice(start_key_of_bucket,
                             target_area == -1 ? LDB_KEY_META_SIZE : sizeof(start_key_of_bucket)));
    int ret = TAIR_RETURN_SUCCESS;
    for (; !g_stop && ret == TAIR_RETURN_SUCCESS && itr->Valid(); itr->Next()) {
        bool skip = false;
        //double ts_beg = get_timespec();
        ldb_key.assign(const_cast<char *>(itr->key().data()), itr->key().size());
        ldb_item.assign(const_cast<char *>(itr->value().data()), itr->value().size());
        area = LdbKey::decode_area(ldb_key.key());
        if (ldb_key.get_bucket_number() != bucket) { //~ shouldn't happen
            break;
        }
        if (target_area != -1 && area != target_area) {
            break;
        }

        if (!(c->range_start == 0 && c->range_end == 0)) {
            uint32_t mdate = ldb_item.mdate();
            if (!(mdate >= c->range_start && mdate <= c->range_end)) {
                continue;
            }
//      std::string sdate = tair::util::time_util::time_to_str(mdate);
//      log_error("%u %s", mdate, sdate.c_str());
        }

        do_statistics(area, ldb_key, ldb_item);
        if (c->download) {
            data_entry *tkey = new data_entry(ldb_key.key(), ldb_key.key_size(), true);
            data_entry *tval = new data_entry(ldb_item.value(), ldb_item.value_size(), true);

            if (tkey->get_size() > 4) {
                const char *keydata = tkey->get_data();
                if (keydata[2] == 0 && keydata[3] == 4) {
                    size_t i = 0;
                    for (i = 0; i < c->filter.length() && i + 4 < (size_t) (tkey->get_size()); i++) {
                        if (keydata[4 + i] != c->filter[i]) break;
                    }
                    if (i == c->filter.length()) {
                        data_write_local(c, tkey->get_data() + 4, tkey->get_size() - 4,
                                         tval->get_data() + 2, tval->get_size() - 2, c->only_download_key);
                    }
                }
            }
            delete tkey;
            delete tval;
        }
        if (c->only_statistics) continue;

        data_entry *key = NULL,
                *local_value = NULL,
                *slave_value = NULL,
                *remote_value = NULL;
        key = new data_entry(ldb_key.key(), ldb_key.key_size(), true);
        fill_meta(key, ldb_key, ldb_item);
        key->area = area;
        key->data_meta.flag = TAIR_CLIENT_DATA_MTIME_CARE | TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
        key->has_merged = true;
        key->set_prefix_size(ldb_item.prefix_size());

        if (c->prefix_key != "") {
            if (compare_cstr(c->prefix_key.c_str(), c->prefix_key.size(),
                             key->get_prefix(), key->get_prefix_size()) == false)
                continue;
        }

        if (c->just_get_key_value_from_local == false) {
            ret = get_from_local(*c->local_client, *key, local_value, skip);
            if (local_value == NULL || skip) {
                delete key;
                delete local_value;
                ret = TAIR_RETURN_SUCCESS;
                continue;
            }
        } else {
            local_value = new data_entry(ldb_item.value(), ldb_item.value_size(), true);
            fill_meta(local_value, ldb_key, ldb_item);
        }

        if (c->fix_expire > 0 && key->data_meta.edate == 0) {
            key->data_meta.edate = key->data_meta.cdate + c->fix_expire;
        }

        //double ts_local = get_timespec() - ts_beg;
        //log_error("local: %lf", ts_local);
        if (c->skip_slave == false) {
            ret = get_from_slave(slave_client, *key, slave_value, skip);
            if (ret != TAIR_RETURN_SUCCESS) {
                delete key;
                delete local_value;
                delete slave_value;
                ret = TAIR_RETURN_SUCCESS;
                continue;
            }
            if (slave_value == NULL || !verify_entry(false, area, key, local_value, slave_value, c->log_key)) {
                if (c->do_rectify) {
                    //~ do rectify
                    key->server_flag = TAIR_SERVERFLAG_DUPLICATE;
                    ret = slave_client.put(key->area, *key, *local_value,
                                           local_value->data_meta.edate,
                                           local_value->data_meta.version, false);
                    if (ret != TAIR_RETURN_SUCCESS) {
                        log_error("rectify on slave failed: %d", ret);
                    } else {
                    }
                    //g_stop = true;
                    //break;
                }
                ret = TAIR_RETURN_SUCCESS;
            }
        }
        //double ts_slave = get_timespec() - ts_beg;
        //log_error("slave: %lf", ts_slave - ts_local);
        bool need_delete_key = true;

        if (c->is_external) {
            if (c->skip_compare == false) {
                ret = get_from_remote(*c->remote_client, *key, remote_value, skip);
                if (ret != TAIR_RETURN_SUCCESS) {
                    delete key;
                    delete slave_value;
                    delete local_value;
                    delete remote_value;
                    ret = TAIR_RETURN_SUCCESS;
                    continue;
                }
            }
            if (remote_value == NULL || c->skip_compare == true ||
                !verify_entry(true, area, key, local_value, remote_value, c->log_key)) {
                if (c->do_rectify) {
                    //~ do rectify
                    mark_key_interunit_or_innerunit_flag(c, key);
                    if (c->is_batch_send) {
                        need_delete_key = false;
                        ret = block_batch_put_util_success(t, key, local_value);
                    } else {
                        int dest_area = key->area;
                        if (c->redirect_area >= 0) { // set new dest area
                            dest_area = c->redirect_area;
                            char *data = key->get_data();
                            data[0] = (dest_area & 0xFF);
                            data[1] = ((dest_area >> 8) & 0xFF);
                        }
                        ret = c->remote_client->put(dest_area, *key, *local_value,
                                                    local_value->data_meta.edate,
                                                    local_value->data_meta.version, false);
                    }
                    if (ret != TAIR_RETURN_SUCCESS) {
                        log_error("rectify on slave failed: %d", ret);
                    }
                }
                ret = TAIR_RETURN_SUCCESS;
            }
        }

        if (key != NULL && need_delete_key) {
            delete key;
            key = NULL;
        }
        if (local_value != NULL && need_delete_key) {
            delete local_value;
            local_value = NULL;
        }
        if (slave_value != NULL) {
            delete slave_value;
            slave_value = NULL;
        }
        if (remote_value != NULL) {
            delete remote_value;
            remote_value = NULL;
        }
    }

    if (c->is_batch_send) {
        DestBucket *dbt = NULL;
        while ((dbt = t->end()) != NULL) {
            ret = block_batch_put_util_success(t, NULL, NULL, dbt);
            if (ret != TAIR_RETURN_SUCCESS) {
                log_error("rectify on slave failed: %d", ret);
            }
            delete dbt;
        }
    }

    if (!g_stop) {
        commit_finish_task(c, t->instance, t->bucket, target_area);
        log_warn("finish task[%d]: instance[%d], bucket[%d], target_area[%d]",
                 t->task_id, t->instance, t->bucket, target_area);
    }

    //~ destroy
    if (itr != NULL) {
        delete itr;
    }
    if (db != NULL) {
        delete db;
        delete open_opts.comparator;
        delete open_opts.env;
        delete open_opts.info_log;
    }
    return (ret == TAIR_RETURN_SUCCESS);
}

void
mark_key_interunit_or_innerunit_flag(Configure *c, data_entry *key) {
    if (strcmp(c->sync_type.c_str(), "client") == 0) {
        key->server_flag = 0;
    } else if (strcmp(c->sync_type.c_str(), "interunit") == 0) {
        key->server_flag = 0;
        set_meta_flag_unit((key->data_meta.flag));
    } else { // innerunit
        key->server_flag = TAIR_SERVERFLAG_RSYNC;
    }
}

std::pair<int, DestBucket *>
do_remote_batch_put(Task *task, data_entry *key, data_entry *value, DestBucket *dbt) {
    // area key value expire version --> record
    if (key != NULL && value != NULL) {
        key->data_meta.cdate = value->data_meta.cdate;
        key->data_meta.edate = value->data_meta.edate;
        key->data_meta.mdate = value->data_meta.mdate;
        key->data_meta.version = value->data_meta.version;
        dbt = task->prepare_sync(key, value);
    }
    if (dbt != NULL) {
        int ret = task->c->remote_client->sync(dbt->get_bucket(), dbt->get_prepared_records(), false, NULL, NULL);
        if (ret == TAIR_RETURN_SUCCESS) {
            dbt->clean_prepared_record();
            return std::pair<int, DestBucket *>(ret, NULL);
        } else {
            log_warn("sync fail: bucket %d errno %d", task->bucket, ret);
        }
        return std::pair<int, DestBucket *>(ret, dbt);
    }
    return std::pair<int, DestBucket *>(TAIR_RETURN_SUCCESS, NULL);
}

int
block_batch_put_util_success(Task *task, data_entry *key, data_entry *value, DestBucket *dbt) {
    int t = 50 * 1000;
    std::pair<int, DestBucket *> p = do_remote_batch_put(task, key, value, dbt);
    int ret = p.first;
    DestBucket *retdbt = p.second;
    if (ret == TAIR_RETURN_SUCCESS) return ret;
    while (!g_stop) {
        log_error("%d. do remote batch put fail: %d", t / 50000, ret);
        if (t >= 1000000) {
            sleep(1);
        } else {
            usleep(t);
            t += 50 * 1000;
        }
        p = do_remote_batch_put(task, NULL, NULL, retdbt);
        ret = p.first;
        if (ret == TAIR_RETURN_SUCCESS) break;
    }
    return TAIR_RETURN_SUCCESS;
}

#define FINISH_TASK_FILE_NAME "tmp_finished_task.txt"

bool load_last_finished_tasks(Configure *c) {
    std::ifstream fin(FINISH_TASK_FILE_NAME);
    if (!fin) return true;

    std::string id;
    while (getline(fin, id)) {
        c->last_run_finished_tasks.insert(id);
    }

    fin.close();

    return true;
}

bool is_task_finished_last_time(Configure *c, int instance, int bucket, int area) {
    std::ostringstream os;
    os << instance << "." << bucket << "." << area;
    std::string s = os.str();
    if (c->last_run_finished_tasks.find(s) != c->last_run_finished_tasks.end()) {
        log_warn("%s has been done at last run ldb_verify time", s.c_str());
        return true;
    }
    return false;
}

void commit_finish_task(Configure *c, int instance, int bucket, int area) {
    pthread_spin_lock(&(c->biglock));

    std::ofstream fout(FINISH_TASK_FILE_NAME, std::fstream::out | std::fstream::app);
    fout << instance << "." << bucket << "." << area << endl;
    fout.close();

    pthread_spin_unlock(&(c->biglock));
}

#undef FINISH_TASK_FILE_NAME

bool
newest_file_finder(const string &s1, const string &s2) {
    struct stat st1;
    struct stat st2;
    stat(s1.c_str(), &st1);
    stat(s2.c_str(), &st2);
    return st1.st_mtime < st2.st_mtime;
}

bool
do_manifest(Configure *c) {
    char mani_path[1024];
    //~ For each instance, we list out all manifest files,
    //~ then choose the newest one.
    for (int i = 0; i < c->n_instance; ++i) {
        snprintf(mani_path, sizeof(mani_path),
                 "%s%d/ldb/backupversions", c->db_base.c_str(), i + 1);
        DIR *dir = NULL;
        struct dirent *dentry = NULL;
        vector<string> files;
        if ((dir = opendir(mani_path)) == NULL) {
            log_error("open %s: %m", mani_path);
            return false;
        }
        while ((dentry = readdir(dir)) != NULL) {
            if (!strcmp(dentry->d_name, ".") || !strcmp(dentry->d_name, "..")) {
                continue;
            }
            files.push_back(string(mani_path) + "/" + dentry->d_name);
        }
        if (files.empty()) {
            log_error("no MANIFEST-XXX found in %s", mani_path);
            return false;
        }
        c->manifest_file[i] = *std::max_element(files.begin(), files.end(), newest_file_finder);
        log_warn("instance: %d, manifest: %s", i, c->manifest_file[i].c_str());
    }
    return true;
}

void
do_statistics(int area, LdbKey &key, LdbItem &item) {
    //static const uint32_t SEC_PER_YEAR = 12 * 30 * 24 * 60 * 60;
    static const uint32_t SEC_PER_5_DAYS = 5 * 24 * 60 * 60;
    uint32_t mtime = item.mdate();
    uint32_t past = time(NULL) - mtime;
    uint64_t size = key.size() + item.size();
    if (past > SEC_PER_5_DAYS) {
        tair::common::atomic_add(&statistics.metrics[area].area_size_cold, size);
        tair::common::atomic_add(&statistics.cold_size_total, size);
    }
    tair::common::atomic_add(&statistics.metrics[area].area_size_total, size);
    tair::common::atomic_inc(&statistics.metrics[area].area_items);
    tair::common::atomic_add(&statistics.size_total, size);
    tair::common::atomic_inc(&statistics.items_total);
}

int
get_from_local(tair_client_impl &client, data_entry &key, data_entry *&value, bool &skip) {
    static uint64_t not_hit = 0;
    int ret = client.get_hidden(key.get_area(), key, value);
    if (ret == TAIR_RETURN_DATA_NOT_EXIST) {
        tair::common::atomic_inc(&not_hit);
        if (not_hit > 5000) {
            log_error("`not_hit` over 5000, reset");
            not_hit = 0;
        }
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
        key.data_meta.flag = value->data_meta.flag;
        ret = TAIR_RETURN_SUCCESS;
    }

    return ret;
}

int
get_from_slave(tair_client_impl &client, data_entry &key, data_entry *&value, bool &skip) {
    skip = false;
    int ret = client.get_hidden(key.get_area(), key, value);
    if (ret != TAIR_RETURN_SUCCESS && ret != TAIR_RETURN_HIDDEN) {
        log_info("get failed, ret: %d", ret); //~ mostly does not exist
    } else {
        ret = TAIR_RETURN_SUCCESS;
    }

    return ret;
}

int
get_from_remote(tair_client_impl &client, data_entry &key, data_entry *&value, bool &skip) {
    return get_from_slave(client, key, value, skip);
}

bool
verify_entry(bool remote, int area, const data_entry *key, const data_entry *local_value,
             const data_entry *remote_value, bool log_key) {
    bool ret = true;
    bool is_remote_newer = remote_value->get_mdate() > local_value->get_mdate();
    if (local_value->get_size() != remote_value->get_size() && !is_remote_newer) {
        log_info("size not matched, l: %d, r: %d",
                 local_value->get_size(), remote_value->get_size());
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_VALUE_SIZE]);
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_MAX]); // total
        ret = false;
    }
    if (memcmp(local_value->get_data(), remote_value->get_data(),
               local_value->get_size()) != 0 && !is_remote_newer) {
        log_info("content not matched");
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_VALUE]);
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_MAX]);
        ret = false;
    }
    if (local_value->data_meta.keysize != remote_value->data_meta.keysize && !is_remote_newer) {
        log_info("meta keysize not matched: l: %hu, r: %hu",
                 local_value->data_meta.keysize, remote_value->data_meta.keysize);
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_META_KEY_SIZE]);
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_MAX]);
        ret = false;
    }
    if (local_value->data_meta.valsize != remote_value->data_meta.valsize && !is_remote_newer) {
        log_info("meta valsize not matched: l: %u, r: %u",
                 local_value->data_meta.valsize, remote_value->data_meta.valsize);
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_META_VAL_SIZE]);
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_MAX]);
        ret = false;
    }
    if (local_value->data_meta.flag != remote_value->data_meta.flag && !is_remote_newer) {
        log_info("meta flag not matched: l: 0x%x, r: 0x%x",
                 local_value->data_meta.flag, remote_value->data_meta.flag);
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_META_FLAG]);
        tair::common::atomic_inc(&conflict_stat[remote][area].counter[CONFLICT_MAX]);
        ret = false;
    }

    if (ret == false && log_key == true) {
        std::string k = key->get_printable_key(true);
        log_error("Unmatched %d %s", area, k.c_str());
    }

    return ret;
}

void
summary() {
    fprintf(stderr,
            "%5s"
                    "%18s"
                    "%18s"
                    "%18s"
                    "%18s"
                    "%18s\n",
            "area", "items", "size_total", "percent", "size_cold", "percent");

    for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
        if (statistics.metrics[i].area_size_total != 0) {
            fprintf(stderr,
                    "%5d\t"
                            "%18lu"
                            "%18lu"
                            "%18lf"
                            "%18lu"
                            "%18lf\n",
                    i,
                    statistics.metrics[i].area_items,
                    statistics.metrics[i].area_size_total,
                    (double) statistics.metrics[i].area_size_total / statistics.size_total,
                    statistics.metrics[i].area_size_cold,
                    (double) statistics.metrics[i].area_size_cold / statistics.metrics[i].area_size_total);
        }
    }

    fprintf(stderr,
            "size_total: %lu, items_total: %lu, cold_size_total: %lu, percent: %lf\n",
            statistics.size_total, statistics.items_total, statistics.cold_size_total,
            (double) statistics.cold_size_total / statistics.size_total);

    fprintf(stderr, "slave:\n");
    fprintf(stderr,
            "%5s"
                    "%18s"
                    "%18s"
                    "%18s"
                    "%18s"
                    "%18s\n",
            "area",
            "value",
            "value_size",
            "meta_key_size",
            "meta_val_size",
            "meta_flag");
    for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
        if (conflict_stat[0][i].counter[CONFLICT_MAX] == 0) {
            continue;
        }
        fprintf(stderr,
                "%5d"
                        "%18lu"
                        "%18lu"
                        "%18lu"
                        "%18lu"
                        "%18lu\n",
                i,
                conflict_stat[0][i].counter[CONFLICT_VALUE],
                conflict_stat[0][i].counter[CONFLICT_VALUE_SIZE],
                conflict_stat[0][i].counter[CONFLICT_META_KEY_SIZE],
                conflict_stat[0][i].counter[CONFLICT_META_VAL_SIZE],
                conflict_stat[0][i].counter[CONFLICT_META_FLAG]);
    }

    fprintf(stderr, "remote:\n");
    fprintf(stderr,
            "%5s"
                    "%18s"
                    "%18s"
                    "%18s"
                    "%18s"
                    "%18s",
            "area",
            "value",
            "value_size",
            "meta_key_size",
            "meta_val_size",
            "meta_flag\n");
    for (int i = 0; i < TAIR_MAX_AREA_COUNT; ++i) {
        if (conflict_stat[1][i].counter[CONFLICT_MAX] == 0) {
            continue;
        }
        fprintf(stderr,
                "%5d"
                        "%18lu"
                        "%18lu"
                        "%18lu"
                        "%18lu"
                        "%18lu\n",
                i,
                conflict_stat[1][i].counter[CONFLICT_VALUE],
                conflict_stat[1][i].counter[CONFLICT_VALUE_SIZE],
                conflict_stat[1][i].counter[CONFLICT_META_KEY_SIZE],
                conflict_stat[1][i].counter[CONFLICT_META_VAL_SIZE],
                conflict_stat[1][i].counter[CONFLICT_META_FLAG]);
    }
}

void *
thread(void *args) {
    Configure *c = reinterpret_cast<Configure *>(args);
    Task *t = NULL;
    bool all_areas = c->areas.empty();
    while ((t = task_list.pop()) != NULL) {
        set<int>::iterator itr = c->areas.begin();
        /* itr would not be evaluated if all_areas is true */
        while (!g_stop) {
            int area = all_areas ? -1 : *itr;
            if (is_task_finished_last_time(c, t->instance, t->bucket, area)) {
                if (all_areas) break;
                if (++itr == c->areas.end()) break;
                continue;
            }
            if (!do_task(t, c, area)) break;
            if (all_areas) break;
            if (!all_areas && ++itr == c->areas.end()) break;
        }
    }

    return NULL;
}

std::string
abs_dirname(const std::string &path) {
    char tmp[1024];
    realpath(path.c_str(), tmp);
    return dirname(tmp);
}

std::string
abs_path(const std::string &path) {
    char tmp[1024];
    realpath(path.c_str(), tmp);
    return tmp;
}

TaskList::TaskList() {
    pthread_spin_init(&lock, PTHREAD_PROCESS_PRIVATE);
}

TaskList::~TaskList() {
    pthread_spin_lock(&lock);
    while (!tasks.empty()) {
        delete tasks.top();
        tasks.pop();
    }
    pthread_spin_unlock(&lock);

    pthread_spin_destroy(&lock);
}

void
TaskList::push(Task *t) {
    pthread_spin_lock(&lock);
    tasks.push(t);
    pthread_spin_unlock(&lock);
}

Task *
TaskList::pop() {
    Task *t = NULL;
    pthread_spin_lock(&lock);
    if (!tasks.empty()) {
        t = tasks.top();
        tasks.pop();
    }
    pthread_spin_unlock(&lock);
    return t;
}

