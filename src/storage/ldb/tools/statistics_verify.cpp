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
#include <string.h>

#include "common/util.hpp"
#include "common/data_entry.hpp"
#include "common/record_logger_util.hpp"
#include "packets/mupdate_packet.hpp"
#include "dataserver/remote_sync_manager.hpp"

#include "leveldb/env.h"

#include "ldb_define.hpp"
#include "ldb_comparator.hpp"
#include "ldb_util.hpp"

using namespace tair::storage::ldb;
using tair::data_entry;
using tair::common::RecordLogger;
using tair::common::SequentialFileRecordLogger;
using tair::common::tair_keyvalue_map;
using tair::tair_client_impl;
using tair::FailRecord;
using tair::tair_operc_vector;
using std::string;
using std::vector;
using std::stack;
using std::set;
using tbsys::CConfig;

const int MAX_INSTANCES = 64;
const int MAX_WORKERS = 24;
bool g_stop = false;

struct Configure;

class Statistics;

class Task;

static void signal_handler(int sig);

static void help(const char *prog);

static bool parse_args(int argc, char **argv, Configure *c);

static bool parse_conf(Configure *c);

static bool fill_bucket_map(Configure *c);

static bool do_task(Task *t, Configure *c);

static bool do_manifest(Configure *c);

static void summary();

static void *thread(void *args);

static std::string abs_dirname(const std::string &path);

static std::string abs_path(const std::string &path);

// verify_type
enum {
    LOCAL_MASTER_TO_LOCAL_SLAVE_BUCKET,
    LOCAL_MASTER_TO_REMOTE_MASTER_BUCKET
};

struct Configure {
    int n_instance;
    int n_task;
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
    vector<set<int> > inst_bucket_map;
    tair_client_impl *local_client;
    tair_client_impl *remote_client;
    CConfig c;
    int32_t count_interval;
    time_t mtime_range_begin;
    time_t mtime_range_end;
    ofstream dismatch_key_record_file;
    int32_t verify_type;
    int32_t dataserver_select_index;
    bool repair;

    Configure() {
        n_instance = 0;
        n_task = 1;
        local_ds = 0;
        local_client = NULL;
        remote_client = NULL;
        count_interval = 1;
        mtime_range_begin = -1;
        mtime_range_end = -1;
        verify_type = LOCAL_MASTER_TO_REMOTE_MASTER_BUCKET;
        dataserver_select_index = 0;
        repair = false;
    }

    ~Configure() {
        if (local_client != NULL) {
            local_client->close();
            delete local_client;
        }
        if (LOCAL_MASTER_TO_REMOTE_MASTER_BUCKET == verify_type)
            if (remote_client != NULL) {
                remote_client->close();
                delete remote_client;
            }
    }

    friend std::ostream &operator<<(std::ostream &os, const Configure &c) {
        os << "verify_type: " << (LOCAL_MASTER_TO_REMOTE_MASTER_BUCKET == c.verify_type
                                  ? "verify_local_master_bucket_to_remote_master_bucket" :
                                  "verify_local_master_bucket_to_local_slave_bucket") << endl;
        os << "dataserver_select_index: " << c.dataserver_select_index << endl;
        os << "local_cluster_instance_total: " << c.n_instance << endl;
        os << "n_task: " << c.n_task << endl;
        os << "conf_file: " << c.conf_file << endl;
        os << "db_base: " << c.db_base << endl;
        os << "tair_base: " << c.tair_base << endl;
        {
            os << "manifest_file:[ ";
            for (int i = 0; i < c.n_instance; ++i)
                os << "instance " << i + 1 << ": " << c.manifest_file[i] << "; ";
            os << "]" << endl;
        }
        os << "cmp_desc: " << c.cmp_desc << endl;
        os << "local_ds: " << c.local_ds << " ip: " << tbsys::CNetUtil::addrToString(c.local_ds) << endl;
        os << "local_master: " << c.local_master << endl;
        os << "local_slave: " << c.local_slave << endl;
        os << "local_group: " << c.local_group << endl;
        os << "remote_master: " << c.remote_master << endl;
        os << "remote_slave: " << c.remote_slave << endl;
        os << "remote_group: " << c.remote_group << endl;
        os << "count_interval: " << c.count_interval << endl;

        os << "mtime_range_begin: " << c.mtime_range_begin << " YYYYMMDDhhmmss: "
           << tair::common::time_util::time_to_str(c.mtime_range_begin) << endl;
        os << "mtime_range_end: " << c.mtime_range_end << " YYYYMMDDhhmmss: "
           << tair::common::time_util::time_to_str(c.mtime_range_end) << endl;
        os << "repair remote dismatch or not found key: " << (c.repair ? "true" : "false") << endl;

        return os;
    }
};


struct StatUnit {
    StatUnit() : itemcount_(0), datasize_(0) {}

    uint64_t itemcount_;
    uint64_t datasize_;
};

class Statistics {
public:
    explicit Statistics(const string &a_print_prefix) : bucket_stat_(NULL) {
        print_prefix_ = a_print_prefix;
    }

    ~Statistics() {
        if (bucket_stat_ != NULL) {
            delete bucket_stat_;
            bucket_stat_ = NULL;
        }
    }

    bool set_bucket_count(int32_t a_bucket_count) {
        if (a_bucket_count <= 0)
            return false;
        bucket_count_ = a_bucket_count;
        bucket_stat_ = new StatUnit[bucket_count_];
        return true;
    }

    void add_stat(int32_t area, int32_t bucket, int32_t itemcount, int32_t datasize) {
        StatUnit &one_area_stat = area_stat_[area];
        StatUnit &one_bucket_stat = bucket_stat_[bucket];

        tair::common::atomic_add(&one_area_stat.itemcount_, itemcount);
        tair::common::atomic_add(&one_area_stat.datasize_, datasize);

        tair::common::atomic_add(&one_bucket_stat.itemcount_, itemcount);
        tair::common::atomic_add(&one_bucket_stat.datasize_, datasize);
    }

    void print() {
        time_t t;
        struct tm tm;
        time(&t);
        localtime_r(&t, &tm);
        printf("[%04d-%02d-%02d %02d:%02d:%02d]\n", tm.tm_year + 1900, tm.tm_mon + 1,
               tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

        // print area stat
        string area_str = print_prefix_ + "area";
        uint64_t total_area_itemcount = 0, total_area_usesize = 0;
        printf("%-40s %15s %15s\n", area_str.c_str(), "itemcount", "datasize");
        for (int32_t i = 0; i < TAIR_MAX_AREA_COUNT; ++i)
            if (area_stat_[i].itemcount_ != 0) {
                printf("%-40d %15lu %15lu\n", i, area_stat_[i].itemcount_, area_stat_[i].datasize_);
                total_area_itemcount += area_stat_[i].itemcount_;
                total_area_usesize += area_stat_[i].datasize_;
            }
        printf("%-40s %15lu %15lu\n", "total_area", total_area_itemcount, total_area_usesize);

        // print bucket stat
        string bucket_str = print_prefix_ + "bucket";
        uint64_t total_bucket_itemcount = 0, total_bucket_usesize = 0;
        printf("%-40s %15s %15s\n", bucket_str.c_str(), "itemcount", "datasize");
        for (int32_t i = 0; i < bucket_count_; ++i)
            if (bucket_stat_[i].itemcount_ != 0) {
                printf("%-40d %15lu %15lu\n", i, bucket_stat_[i].itemcount_, bucket_stat_[i].datasize_);
                total_bucket_itemcount += bucket_stat_[i].itemcount_;
                total_bucket_usesize += bucket_stat_[i].datasize_;
            }
        printf("%-40s %15lu %15lu\n", "total_bucket", total_bucket_itemcount, total_bucket_usesize);

        fflush(stdout);
    }

private:
    StatUnit area_stat_[TAIR_MAX_AREA_COUNT];
    // we don't know how many buckets there are and the maxinum bucket number.
    // so use a map to store bucket stat and a rw-lock to protect the map insert operation.
    StatUnit *bucket_stat_;
    int32_t bucket_count_;

    string print_prefix_;
};

Statistics remote_key_out_mtime_range_statistics("remote_key_out_mtime_range-");
Statistics remote_key_compared_statisics("remote_key_compared-");
Statistics remote_key_equal_statistics("remote_key_equal-");
Statistics remote_key_mdate_great_statistics("remote_key_mdate_great-");
Statistics remote_key_mdate_less_statistics("remote_key_mdate_less-");
Statistics local_key_not_found_statistics("local_key_not_found-");
Statistics remote_key_not_found_statistics("remote_key_not_found-");
Statistics remote_value_dismatch_statistics("remote_value_dismatch-");
Statistics remote_meta_dismatch_statistics("remote_meta_dismatch-");
Statistics remote_key_repair_statistics("remote_key_repair-");
Statistics remote_key_repair_failed_statistics("remote_key_repair_failed-");

class AreaServeridKeys {
    enum cmp_value_result {
        TOTAL_EQUAL,
        VALUE_DISMATCH,
        META_DISMATCH,
        REMOTE_MDATE_GREAT,
        REMOTE_MDATE_LESS,
        REMOTE_KEY_NOT_FOUND,
        LOCAL_KEY_NOT_FOUND,
        LOCAL_EDATE_NEGATIVE
    };

public:
    AreaServeridKeys() : c(NULL), area(-1), approximate_mget_return_packet_size(0) {}

    void verify_and_clear() {
        tair_keyvalue_map result;
        c->remote_client->mget_impl(area, keys, result, c->dataserver_select_index);

        for (size_t i = 0; i < keys.size(); ++i) {
            int32_t local_bucket_number = bucket_numbers[i];
            int32_t datasize = keys[i]->get_size() + ldb_items[i]->value_size();
            if (false == keys[i]->has_merged)
                datasize += 2;

            tair_keyvalue_map::iterator it = result.begin();
            for (; it != result.end(); ++it)
                if (cmp_key(keys[i], it->first) == 0)
                    break;

            remote_key_compared_statisics.add_stat(area, local_bucket_number, 1, datasize);
            cmp_value_result cmp_rc = TOTAL_EQUAL;
            if (it == result.end())
                cmp_rc = REMOTE_KEY_NOT_FOUND;
            else {
                cmp_rc = cmp_value(*ldb_items[i], *it->second);
                data_entry *tmp_key = it->first;
                data_entry *tmp_value = it->second;
                result.erase(it);
                delete tmp_key;
                delete tmp_value;
            }

            data_entry *local_value = NULL;
            data_entry *remote_value = NULL;
            do {
                if (TOTAL_EQUAL == cmp_rc)
                    break;
                do_success_get(c->local_client, area, *keys[i], local_value);
                do_success_get(c->remote_client, area, *keys[i], remote_value, c->dataserver_select_index);
                if (NULL == local_value) {
                    cmp_rc = LOCAL_KEY_NOT_FOUND;
                    break;
                }
                if (NULL == remote_value) {
                    cmp_rc = REMOTE_KEY_NOT_FOUND;
                    break;
                }
                cmp_rc = cmp_value(*local_value, *remote_value);
            } while (false);

            if (c->repair) {
                if (VALUE_DISMATCH == cmp_rc || REMOTE_KEY_NOT_FOUND == cmp_rc) {
                    remote_key_repair_statistics.add_stat(area, local_bucket_number, 1, datasize);
                    int rc = do_success_put(c->remote_client, area, *keys[i], *local_value);
                    if (rc != TAIR_RETURN_SUCCESS)
                        remote_key_repair_failed_statistics.add_stat(area, local_bucket_number, 1, datasize);
                    data_entry *new_remote_value;
                    do_success_get(c->remote_client, area, *keys[i], new_remote_value, c->dataserver_select_index);
                    if (NULL == new_remote_value)
                        cmp_rc = REMOTE_KEY_NOT_FOUND;
                    else
                        cmp_rc = cmp_value(*local_value, *new_remote_value);
                    delete new_remote_value;
                    new_remote_value = NULL;
                }
            }
            delete local_value;
            local_value = NULL;
            delete remote_value;
            remote_value = NULL;

            switch (cmp_rc) {
                case TOTAL_EQUAL:  // total equal
                    remote_key_equal_statistics.add_stat(area, local_bucket_number, 1, datasize);
                    break;
                case VALUE_DISMATCH:  // value dismatch
                    remote_value_dismatch_statistics.add_stat(area, local_bucket_number, 1, datasize);
                    dump_dismatch_key("value_dismatch", keys[i]);
                    break;
                case META_DISMATCH:  // meta dismatch
                    remote_meta_dismatch_statistics.add_stat(area, local_bucket_number, 1, datasize);
                    dump_dismatch_key("meta_dismatch", keys[i]);
                    break;
                case REMOTE_MDATE_GREAT:  // remote mdate great
                    remote_key_mdate_great_statistics.add_stat(area, local_bucket_number, 1, datasize);
                    dump_dismatch_key("remote_mdate_great", keys[i]);
                    break;
                case REMOTE_MDATE_LESS: // remote mdate less
                    remote_key_mdate_less_statistics.add_stat(area, local_bucket_number, 1, datasize);
                    dump_dismatch_key("remote_mdate_less", keys[i]);
                    break;
                case REMOTE_KEY_NOT_FOUND: // remote key not found
                    remote_key_not_found_statistics.add_stat(area, local_bucket_number, 1, datasize);
                    dump_dismatch_key("remote_key_not_found", keys[i]);
                    break;
                case LOCAL_KEY_NOT_FOUND: // local key not found
                    local_key_not_found_statistics.add_stat(area, local_bucket_number, 1, datasize);
                    break;
                default:
                    break;
            }
        }

        // clear
        for (size_t i = 0; i < keys.size(); ++i) {
            delete keys[i];
            delete ldb_items[i];
            delete[] ldb_item_datas[i];
        }
        keys.clear();
        bucket_numbers.clear();
        ldb_items.clear();
        ldb_item_datas.clear();
        approximate_mget_return_packet_size = 30;

        for (tair_keyvalue_map::iterator it = result.begin(); it != result.end();) {
            data_entry *tmp_key = it->first;
            data_entry *tmp_value = it->second;
            ++it;
            delete tmp_key;
            delete tmp_value;
        }
        return;
    }

    int32_t cmp_key(data_entry *left, data_entry *right) {
        char *left_data;
        int32_t left_data_size;
        if (left->has_merged) {
            left_data = left->get_data() + 2;
            left_data_size = left->get_size() - 2;
        } else {
            left_data = left->get_data();
            left_data_size = left->get_size();
        }

        char *right_data;
        int32_t right_data_size;
        if (right->has_merged) {
            right_data = right->get_data() + 2;
            right_data_size = right->get_size() - 2;
        } else {
            right_data = right->get_data();
            right_data_size = right->get_size();
        }

        // area is already same
        if (left_data_size == right_data_size)
            return memcmp(left_data, right_data, left_data_size);
        else
            return left_data_size - right_data_size;
    }

    bool exceed_after_add(data_entry *key, LdbItem *ldb_item) {
        return ((approximate_mget_return_packet_size + key->get_size() + 40 + ldb_item->value_size() + 40) >
                400 * 1024) || (keys.size() + 1) > 200;
    }

    void add_key(data_entry *key, int32_t bucket_number, LdbItem *ldb_item, char *ldb_item_data) {
        keys.push_back(key);
        bucket_numbers.push_back(bucket_number);
        ldb_items.push_back(ldb_item);
        ldb_item_datas.push_back(ldb_item_data);

        approximate_mget_return_packet_size += key->get_size() + 40 + ldb_item->value_size() + 40;
    }

    void set_area(int32_t a_area) {
        area = a_area;
    }

    void set_configure(Configure *a_c) {
        c = a_c;
    }

    bool is_inited() const {
        return (c != NULL) && (area != -1);
    }

    bool empty() const {
        return keys.empty();
    }

private:
    void dump_dismatch_key(const char *message, data_entry *key) {
        if (!c->dismatch_key_record_file.is_open())
            return;
        stringstream ss;
        char *key_data = key->has_merged ? key->get_data() + 2 : key->get_data();
        int32_t key_size = key->has_merged ? key->get_size() - 2 : key->get_size();
        ss << message;

        // print key
        ss << " area: " << area << " key: \"";
        for (int32_t i = 0; i < key_size; ++i)
            ss << key_data[i];
        ss << "\" key_prefix_size: " << key->get_prefix_size() << endl;
        c->dismatch_key_record_file << ss.str();
    }

    /*
     * @return  -- totolly equal
     *          -- value dismatch
     *          -- meta dismatch
     *          -- remote mdate great
     *          -- remote mdate less
     *          -- local edate uint32_t
     */
    cmp_value_result cmp_value(LdbItem &local_value, data_entry &remote_value) {
        // value cmp
        if (local_value.value_size() != remote_value.get_size())
            return VALUE_DISMATCH;
        if (memcmp(local_value.value(), remote_value.get_data(), remote_value.get_size()) != 0)
            return VALUE_DISMATCH;

        // mdate cmp
        if (local_value.mdate() < remote_value.data_meta.mdate)
            return REMOTE_MDATE_GREAT;
        if (local_value.mdate() > remote_value.data_meta.mdate)
            return REMOTE_MDATE_LESS;

        /*
        if (local_value.cdate() > remote_value.data_meta.cdate)
          return META_DISMATCH;
        if (local_value.flag() != remote_value.data_meta.flag)
          return META_DISMATCH;
        if (local_value.version() != remote_value.data_meta.version)
          return META_DISMATCH;
        if ((int32_t)local_value.edate() < 0)
          return LOCAL_EDATE_NEGATIVE;
        if (local_value.edate() != remote_value.data_meta.edate)
          return META_DISMATCH;
        */

        return TOTAL_EQUAL;
    }

    cmp_value_result cmp_value(data_entry &local_value, data_entry &remote_value) {
        // value cmp
        if (local_value.get_size() != remote_value.get_size())
            return VALUE_DISMATCH;
        if (memcmp(local_value.get_data(), remote_value.get_data(), remote_value.get_size()) != 0)
            return VALUE_DISMATCH;

        // mdate cmp
        if (local_value.data_meta.mdate < remote_value.data_meta.mdate)
            return REMOTE_MDATE_GREAT;
        if (local_value.data_meta.mdate > remote_value.data_meta.mdate)
            return REMOTE_MDATE_LESS;
        /*
        if (local_value.cdate() > remote_value.data_meta.cdate)
          return META_DISMATCH;
        if (local_value.flag() != remote_value.data_meta.flag)
          return META_DISMATCH;
        if (local_value.version() != remote_value.data_meta.version)
          return META_DISMATCH;
        if ((int32_t)local_value.edate() < 0)
          return LOCAL_EDATE_NEGATIVE;
        if (local_value.edate() != remote_value.data_meta.edate)
          return META_DISMATCH;
        */

        return TOTAL_EQUAL;
    }

    int32_t do_success_get(tair_client_impl *client, int32_t area, const data_entry &key, data_entry *&local_value,
                           int32_t server_select = 0) {
        int32_t rc = client->get_hidden_impl(area, key, local_value, server_select);
        int i = 0;
        while (TAIR_RETURN_TIMEOUT == rc || TAIR_RETURN_FLOW_CONTROL == rc) {
            ++i;
            if (i % 100 == 0) {
                stringstream ss;
                ss << tbsys::CNetUtil::addrToString(client->get_config_server_list()[0]) << ",";
                ss << client->get_group_name();
                log_error("get from %s, timeout or flow control %d times, last rc %d", ss.str().c_str(), i, rc);
            }
            usleep(10 * 1000);
            rc = client->get_hidden_impl(area, key, local_value, server_select);
        }
        if (TAIR_RETURN_HIDDEN == rc)
            rc = TAIR_RETURN_SUCCESS;
        return rc;
    }

    int32_t do_success_put(tair_client_impl *client, int32_t area, data_entry &key, data_entry &value) {
        key.data_meta.cdate = value.data_meta.cdate;
        key.data_meta.edate = value.data_meta.edate;
        key.data_meta.mdate = value.data_meta.mdate;
        key.data_meta.version = value.data_meta.version;

        key.server_flag = TAIR_SERVERFLAG_RSYNC;
        key.data_meta.flag = TAIR_CLIENT_DATA_MTIME_CARE | TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;

        int rc = client->put(area, key, value, value.data_meta.edate, value.data_meta.version, false);
        int i = 0;
        while (TAIR_RETURN_TIMEOUT == rc || TAIR_RETURN_FLOW_CONTROL == rc) {
            ++i;
            if (i % 100 == 0) {
                stringstream ss;
                ss << tbsys::CNetUtil::addrToString(client->get_config_server_list()[0]) << ",";
                ss << client->get_group_name();
                log_error("get from %s, timeout or flow control %d times, last rc %d", ss.str().c_str(), i, rc);
            }
            usleep(10 * 1000);
            rc = client->put(area, key, value, value.data_meta.edate, value.data_meta.version, false);
        }
        if (rc == TAIR_RETURN_MTIME_EARLY)
            rc = TAIR_RETURN_SUCCESS;
        return rc;
    }

    vector<data_entry *> keys;
    vector<int32_t> bucket_numbers;

    vector<LdbItem *> ldb_items;
    vector<char *> ldb_item_datas;

    Configure *c;
    int32_t area;
    int32_t approximate_mget_return_packet_size;
};

class Task {
public:
    Task(int i) : instance(i), skip_count(0) {}

    int recalc_dest_bucket(const data_entry *key, uint32_t max_bucket_count) {
        uint32_t hash;
        int prefix_size = key->get_prefix_size();
        int32_t diff_size = key->has_merged ? TAIR_AREA_ENCODE_SIZE : 0;
        if (prefix_size == 0) {
            hash = tair::util::string_util::mur_mur_hash(key->get_data() + diff_size, key->get_size() - diff_size);
        } else {
            hash = tair::util::string_util::mur_mur_hash(key->get_data() + diff_size, prefix_size);
        }
        return hash % max_bucket_count;
    }

    void add_key_and_check(leveldb::Iterator *itr) {
        LdbKey ldb_key;
        ldb_key.assign(const_cast<char *>(itr->key().data()), itr->key().size());
        int area = LdbKey::decode_area(ldb_key.key());
        int32_t local_bucket_number = ldb_key.get_bucket_number();
        if (c->inst_bucket_map[instance].find(local_bucket_number) == c->inst_bucket_map[instance].end())
            return;

        ++skip_count;
        if (skip_count < c->count_interval)
            return;
        skip_count = 0;

        LdbItem *ldb_item = new LdbItem();
        // it is free in AreaServeridKeys.verify_and_clear()
        char *ldb_item_data = new char[itr->value().size()];
        memcpy((void *) ldb_item_data, (void *) itr->value().data(), itr->value().size());
        ldb_item->assign(ldb_item_data, itr->value().size());

        time_t mdate = ldb_item->mdate();
        if ((c->mtime_range_begin != -1 && mdate < c->mtime_range_begin)
            || (c->mtime_range_end != -1 && mdate > c->mtime_range_end)) {
            remote_key_out_mtime_range_statistics.add_stat(area, local_bucket_number, 1,
                                                           ldb_key.key_size() + ldb_item->value_size());
            delete ldb_item;
            delete ldb_item_data;
            return;
        }

        data_entry *key = new data_entry(ldb_key.key() + LDB_KEY_AREA_SIZE, ldb_key.key_size() - LDB_KEY_AREA_SIZE,
                                         true);
        key->area = area;
        key->data_meta.flag = TAIR_CLIENT_DATA_MTIME_CARE | TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;
        key->has_merged = false;
        key->set_prefix_size(ldb_item->prefix_size());

        int32_t remote_bucket_number;
        if (LOCAL_MASTER_TO_LOCAL_SLAVE_BUCKET == c->verify_type)
            remote_bucket_number = local_bucket_number;
        else
            remote_bucket_number = recalc_dest_bucket(key, c->remote_client->get_max_bucket_count());

        /*
        vector<uint64_t> remote_server_list;
        if (c->remote_client->get_server_id(remote_bucket_number, remote_server_list) == false)x
        {
          delete ldb_item;
          delete ldb_item_data;
          delete key;
          return;
        }
        */

        AreaServeridKeys &one_area_serverid = area_serverid_keys[key->area][remote_bucket_number];
        if (one_area_serverid.is_inited() == false) {
            one_area_serverid.set_configure(c);
            one_area_serverid.set_area(key->area);
        }

        if (one_area_serverid.exceed_after_add(key, ldb_item))
            one_area_serverid.verify_and_clear();
        one_area_serverid.add_key(key, local_bucket_number, ldb_item, ldb_item_data);
    }

    void finish() {
        map < int32_t, map < int32_t, AreaServeridKeys > > ::iterator
        outer_itr = area_serverid_keys.begin();
        for (; outer_itr != area_serverid_keys.end(); ++outer_itr) {
            map<int32_t, AreaServeridKeys>::iterator inner_itr = outer_itr->second.begin();
            for (; inner_itr != outer_itr->second.end(); ++inner_itr)
                if (!inner_itr->second.empty())
                    inner_itr->second.verify_and_clear();
        }
    }

    int task_id;
    int instance;
    Configure *c;
    int32_t skip_count;
    map <int32_t, map<int32_t, AreaServeridKeys> > area_serverid_keys;
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

int
main(int argc, char **argv) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGALRM, signal_handler);
    signal(SIGPIPE, SIG_IGN);
    alarm(10 * 60);
    TBSYS_LOGGER.setLogLevel("warn");

    Configure c;
    if (!parse_args(argc, argv, &c))
        return 1;

    if (!parse_conf(&c))
        return 1;

    std::ostringstream os;
    os << c;
    log_warn("Configure:\n%s", os.str().c_str());

    remote_key_out_mtime_range_statistics.set_bucket_count(c.local_client->get_max_bucket_count());
    remote_key_compared_statisics.set_bucket_count(c.local_client->get_max_bucket_count());
    remote_key_equal_statistics.set_bucket_count(c.local_client->get_max_bucket_count());
    remote_key_mdate_great_statistics.set_bucket_count(c.local_client->get_max_bucket_count());
    remote_key_mdate_less_statistics.set_bucket_count(c.local_client->get_max_bucket_count());
    local_key_not_found_statistics.set_bucket_count(c.local_client->get_max_bucket_count());
    remote_key_not_found_statistics.set_bucket_count(c.local_client->get_max_bucket_count());
    remote_value_dismatch_statistics.set_bucket_count(c.local_client->get_max_bucket_count());
    remote_meta_dismatch_statistics.set_bucket_count(c.local_client->get_max_bucket_count());
    remote_key_repair_statistics.set_bucket_count(c.local_client->get_max_bucket_count());
    remote_key_repair_failed_statistics.set_bucket_count(c.local_client->get_max_bucket_count());

    //~ set up tasks, trying to distribute instances among different workers
    int task_id = 0;
    for (int i = 0; i < c.n_instance; ++i) {
        Task *t = new Task(i);
        t->c = &c;
        t->task_id = task_id;
        task_list.push(t);
        log_warn("task[%d]: instance[%d]", task_id++, i);
    }

    pthread_t tids[MAX_WORKERS];

    //~ launch workers
    for (int i = 0; i < c.n_task; ++i) {
        pthread_create(&tids[i], NULL, thread, &c);
    }

    //~ join workers
    for (int i = 0; i < c.n_task; ++i) {
        pthread_join(tids[i], NULL);
    }

    //~ print out summary infos  //~ print out summary infos
    printf("end\n");
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

void
help(const char *prog) {
    fprintf(stderr, "USAGE\n");
    fprintf(stderr,
            "\t%s [--option] -S local_master,local_slave,local_group  -D remote_master,remote_slave,remote_group -f dataserver.conf\n"
                    "--type  [local|remote]\n\t"
                    "verify type, local -- verify master and slave bucket of local cluster, need set -S not mustn't set -D\n\t"
                    "             remote -- verify master bucket of local cluster to master bucket of remote cluster, need set -S and -D, default\n"
                    "-f, --config-file dataserver.conf\n\t"
                    "-t, --task-count n\n\t"
                    "number of tasks doing the work\n"
                    "-S, --local-cluster master,slave,group\n\t"
                    "source cluster\n"
                    "-D, --remote-cluster master,slave,group\n\t"
                    "destination cluster\n"
                    "-p, --scan-interval n\n\t"
                    "only compare 1/n keys in master bucket\n"
                    "-r, --repair\n\t"
                    "if specified, repair remote dismatch or not found key. default [No]\n"
                    "-b, --range-begin  YYYYMMDDhhmmss\n\t"
                    "only compare keys whose mdate is after YYYYMMDDhhmmss\n"
                    "-d, --range-end YYYYMMDDhhmmss\n\t"
                    "only compare keys whose mdate is before YYYYMMDDhhmmss\n"
                    "-k, --diff-key-log-file log-file\n\t"
                    "specify the file to record diff key, if not specify, default stderr\n"
                    "-l, --log-file file_path\n\t"
                    "path of log file\n"
                    "-h, --help\n\t"
                    "print this message\n",
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
    const char *optstr = "hf:a:t:l:S:D:p:rb:d:k:";
    struct option long_opts[] = {
            {"help",              no_argument,       NULL, 'h'},
            {"config-file",       required_argument, NULL, 'f'},
            {"task-count",        required_argument, NULL, 't'},
            {"log-file",          required_argument, NULL, 'l'},
            {"local-cluster",     required_argument, NULL, 'S'},
            {"remote-cluster",    required_argument, NULL, 'D'},
            {"scan-interval",     required_argument, NULL, 'p'},
            {"repair",            no_argument,       NULL, 'r'},
            {"range-begin",       required_argument, NULL, 'b'},
            {"range-end",         required_argument, NULL, 'd'},
            {"diff-key-log-file", required_argument, NULL, 'k'},
            {"type",              required_argument, NULL, 1000},
            {NULL,                0,                 NULL, 0}
    };

    string local_cluster_addr;
    string remote_cluster_addr;
    //~ do parsing
    while ((opt = getopt_long(argc, argv, optstr, long_opts, NULL)) != -1) {
        switch (opt) {
            case 'f':
                c->conf_file = optarg;
                break;
            case 't':
                c->n_task = atoi(optarg);
                break;
            case 'l':
                TBSYS_LOGGER.setFileName(optarg);
                break;
            case 'S':
                local_cluster_addr = optarg;
                break;
            case 'D':
                remote_cluster_addr = optarg;
                break;
            case 'p':
                c->count_interval = atoi(optarg);
                break;
            case 'r':
                c->repair = true;
                break;
            case 'b':
                if (tair::util::time_util::s2time(optarg, &(c->mtime_range_begin)) == 0) {
                    log_error("time format YYYYMMDDhhmmss");
                    return false;
                }
                break;
            case 'd':
                if (tair::util::time_util::s2time(optarg, &(c->mtime_range_end)) == 0) {
                    log_error("time format YYYYMMDDhhmmss");
                    return false;
                }
                break;
            case 'k':
                c->dismatch_key_record_file.open(optarg);
                if (!c->dismatch_key_record_file.is_open()) {
                    log_error("open diff key log file %s failed", optarg);
                    return false;
                }
                break;
            case 1000:
                if (strcmp(optarg, "local") == 0)
                    c->verify_type = LOCAL_MASTER_TO_LOCAL_SLAVE_BUCKET;
                else if (strcmp(optarg, "remote") == 0)
                    c->verify_type = LOCAL_MASTER_TO_REMOTE_MASTER_BUCKET;
                else {
                    log_error("--type must be local or remote");
                    return false;
                }
                break;
            case '?':
            case 'h':
            default :
                help(argv[0]);
                exit(1);
                break;
        }
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

    if (LOCAL_MASTER_TO_REMOTE_MASTER_BUCKET == c->verify_type) {
        c->dataserver_select_index = 0;
        if (local_cluster_addr.empty() || remote_cluster_addr.empty()) {
            log_error("verify type is remote, local and remote cluster addr must be set");
            return false;
        }

    } else if (LOCAL_MASTER_TO_LOCAL_SLAVE_BUCKET == c->verify_type) {
        c->dataserver_select_index = 1;
        // local_cluster_addr mustn't be empty and remote_cluster_addr must be empty
        if (local_cluster_addr.empty() || !remote_cluster_addr.empty()) {
            log_error("verify type is local, local cluster addr must be set, remote cluster addr mustn't be set");
            return false;
        }
    }

    if (!local_cluster_addr.empty()) {
        vector<string> confs;
        tair::util::string_util::split_str(local_cluster_addr.c_str(), ", ", confs);
        if (confs.size() == 3) {
            c->local_master = confs[0];
            c->local_slave = confs[1];
            c->local_group = confs[2];
        } else {
            log_error("illegal arguments for -S/--local-cluster");
            return false;
        }
    } else {
        log_error("miss -S/--local-cluster option");
        return false;
    }

    if (!remote_cluster_addr.empty()) {
        vector<string> confs;
        tair::util::string_util::split_str(remote_cluster_addr.c_str(), ", ", confs);
        if (confs.size() == 3) {
            c->remote_master = confs[0];
            c->remote_slave = confs[1];
            c->remote_group = confs[2];
        } else {
            log_error("illegal arguments for -D/--remote-cluster");
            return false;
        }
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
    if ("numeric" == c->cmp_desc) {
        const char *delimiter = c->c.getString("ldb", "ldb_userkey_num_delimiter", ":");
        const char *skip_meta_size = c->c.getString("ldb", "ldb_userkey_skip_meta_size", "2");
        c->cmp_desc.append(",");
        c->cmp_desc.append(delimiter);
        c->cmp_desc.append(",");
        c->cmp_desc.append(skip_meta_size);
    }

    //~ BTW. establish necessary clients
    bool success;
    c->local_client = new tair_client_impl();
    c->local_client->set_timeout(5000);
    success = c->local_client->startup(c->local_master.c_str(), c->local_slave.c_str(), c->local_group.c_str());
    log_warn("connect to local cluster: %s[%s]", c->local_master.c_str(), c->local_group.c_str());

    if (success) {
        if (LOCAL_MASTER_TO_REMOTE_MASTER_BUCKET == c->verify_type) {
            c->remote_client = new tair_client_impl();
            c->remote_client->set_timeout(5000);
            success &= c->remote_client->startup(c->remote_master.c_str(), c->remote_slave.c_str(),
                                                 c->remote_group.c_str());
            log_warn("connect to remote cluster: %s[%s]", c->remote_master.c_str(), c->remote_group.c_str());
        } else {
            c->remote_client = c->local_client;
            if (c->remote_client->get_copy_count() < 2) {
                log_error("verify is local, but local cluster's data copy count %d is not >=2",
                          c->remote_client->get_copy_count());
                return false;
            }
        }
    }

    if (!success) {
        log_error("connect to tair client failed");
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

bool
fill_bucket_map(Configure *c) {
    //~ all buckets this server holds
    std::set<int32_t> buckets;
    c->local_client->get_buckets_by_server(c->local_ds, buckets);
    if (buckets.empty()) return false;
    log_warn("bucket count this server holds: %lu", buckets.size());

    c->inst_bucket_map.resize(c->n_instance);

    string ldb_bucket_index_file_dir = c->c.getString(TAIRLDB_SECTION, LDB_BUCKET_INDEX_FILE_DIR, "");
    ldb_bucket_index_file_dir += "/ldb_bucket_index_map";
    log_warn("target bindex file: %s", ldb_bucket_index_file_dir.c_str());
    if (::access(ldb_bucket_index_file_dir.c_str(), F_OK) == 0) {
        ifstream inf(ldb_bucket_index_file_dir.c_str()); //~ C++98 sucks

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
                c->inst_bucket_map[instance].insert(bucket);
            }
            ++itr;
        }

        inf.close();
    } else {
        for (int i = 0; i < c->n_instance; ++i) {
            c->inst_bucket_map[i].insert(buckets.begin(), buckets.end());
        }
    }

    return true;
}

bool
do_task(Task *t, Configure *c) {
    log_warn("start task[%d]: instance[%d]",
             t->task_id, t->instance);
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

    //~ scanning
    log_warn("scanning instance[%d]...", t->instance + 1);
    int ret = TAIR_RETURN_SUCCESS;

    for (itr->SeekToFirst(); !g_stop && itr->Valid(); itr->Next()) {
        t->add_key_and_check(itr);
    }
    t->finish();

    log_warn("finish task[%d]: instance[%d]", t->task_id, t->instance);

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
        closedir(dir);
    }
    return true;
}

void summary() {
    remote_key_out_mtime_range_statistics.print();
    printf("\n");
    remote_key_compared_statisics.print();
    printf("\n");
    remote_key_equal_statistics.print();
    printf("\n");
    remote_key_mdate_great_statistics.print();
    printf("\n");
    remote_key_mdate_less_statistics.print();
    printf("\n");
    local_key_not_found_statistics.print();
    printf("\n");
    remote_key_not_found_statistics.print();
    printf("\n");
    remote_value_dismatch_statistics.print();
    printf("\n");
    remote_meta_dismatch_statistics.print();
    printf("\n");
    remote_key_repair_statistics.print();
    printf("\n");
    remote_key_repair_failed_statistics.print();
    printf("\n");

}

void *
thread(void *args) {
    Configure *c = reinterpret_cast<Configure *>(args);
    Task *t = NULL;
    while ((t = task_list.pop()) != NULL) {
        if (!g_stop)
            do_task(t, c);
        delete t;
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
