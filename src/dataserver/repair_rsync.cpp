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

#include <tbsys.h>

#include "common/define.hpp"
#include "common/util.hpp"
#include "common/data_entry.hpp"
#include "common/record_logger_util.hpp"

#include "remote_sync_manager.hpp"

using tair::data_entry;
using tair::common::RecordLogger;
using tair::common::SequentialFileRecordLogger;
using tair::ClusterHandler;
using tair::FailRecord;


static bool g_stop = false;
static int64_t g_put_but_local_not_exist_count = 0;
static int64_t g_delete_but_local_exist_count = 0;

void sign_handler(int sig) {
    switch (sig) {
        case SIGTERM:
        case SIGINT:
            log_error("catch sig %d", sig);
            g_stop = true;
            break;
    }
}

int init_cluster_handler(const char *addr, ClusterHandler &handler) {
    int ret = TAIR_RETURN_SUCCESS;
    std::vector<std::string> confs;
    tair::util::string_util::split_str(addr, ", ", confs);
    confs.push_back("2000");      // timeout
    confs.push_back("1000");      // queuelimit
    if ((ret = handler.init_conf(confs)) == TAIR_RETURN_SUCCESS) {
        ret = handler.start();
    }
    return ret;
}

ClusterHandler *get_handler(const std::string &cluster_info, std::map<std::string, tair::ClusterHandler *> &handlers) {
    ClusterHandler *handler = NULL;
    std::map<std::string, tair::ClusterHandler *>::iterator it;

    if (cluster_info.empty()) {
        log_error("empty cluster info");
    } else if ((it = handlers.find(cluster_info)) != handlers.end()) {
        handler = it->second;
    } else {
        handler = new tair::ClusterHandler();
        int ret = handler->init_conf(cluster_info, 2000, 1000);
        if (ret != TAIR_RETURN_SUCCESS || (ret = handler->start()) != TAIR_RETURN_SUCCESS) {
            log_error("init remote cluster fail, ret: %d, info: %s", ret, handler->debug_string().c_str());
            delete handler;
            handler = NULL;
        } else {
            handlers[handler->get_info()] = handler;
        }
    }
    return handler;
}

data_entry *filter_fail_record(data_entry &entry, std::string &info) {
    // entry data is FailRecord actually
    tair::FailRecord record;
    tair::FailRecord::entry_to_record(entry, record);
    info.assign(record.cluster_info_);
    return record.key_;
}

int get_from_local_cluster(ClusterHandler &handler, data_entry &key, data_entry *&value) {
    int ret = handler.client()->get_hidden(key.get_area(), key, value);
    if (ret == TAIR_RETURN_HIDDEN) {
        ret = TAIR_RETURN_SUCCESS;
    }

    return ret;
}

int do_rsync_data(ClusterHandler &local_handler, ClusterHandler &remote_handler, int32_t type, data_entry &key) {
    data_entry *value = NULL;
    // get from local cluster
    int ret = get_from_local_cluster(local_handler, key, value);

    // attach info to key
    if (value != NULL) {
        key.data_meta.cdate = value->data_meta.cdate;
        key.data_meta.edate = value->data_meta.edate;
        key.data_meta.mdate = value->data_meta.mdate;
        key.data_meta.version = value->data_meta.version;
    }

    log_debug("@@ k:%d %s %d %d %u %u %u.v:%s %d %d", key.get_area(), key.get_size() > 6 ? key.get_data() + 6 : "",
              key.get_size(), key.get_prefix_size(), key.data_meta.cdate, key.data_meta.mdate, key.data_meta.edate,
              (value != NULL && value->get_size() > 4) ? value->get_data() + 4 : "",
              value != NULL ? value->get_size() : 0, value != NULL ? value->data_meta.flag : -1);

    key.server_flag = TAIR_SERVERFLAG_RSYNC;
    key.data_meta.flag = TAIR_CLIENT_DATA_MTIME_CARE | TAIR_CLIENT_PUT_SKIP_CACHE_FLAG;

    // do repair
    if (ret == TAIR_RETURN_SUCCESS || ret == TAIR_RETURN_DATA_NOT_EXIST) {
        switch (type) {
            case TAIR_REMOTE_SYNC_TYPE_PUT:
                if (ret == TAIR_RETURN_SUCCESS) {
                    log_debug("@@ edate : %d %d %d", key.data_meta.mdate, key.data_meta.edate, value->data_meta.edate);
                    ret = remote_handler.client()->put(key.get_area(), key, *value, value->data_meta.edate,
                                                       value->data_meta.version, false);
                    if (ret == TAIR_RETURN_MTIME_EARLY) {
                        ret = TAIR_RETURN_SUCCESS;
                    }
                } else {
                    log_info("put but data not exist in local");
                    ++g_put_but_local_not_exist_count;
                    ret = TAIR_RETURN_SUCCESS;
                }
                break;
            case TAIR_REMOTE_SYNC_TYPE_DELETE:
                if (ret == TAIR_RETURN_DATA_NOT_EXIST) {
                    ret = remote_handler.client()->remove(key.get_area(), key);
                    if (ret == TAIR_RETURN_DATA_NOT_EXIST || ret == TAIR_RETURN_MTIME_EARLY) {
                        ret = TAIR_RETURN_SUCCESS;
                    }
                } else {
                    log_info("delete but data exist in local");
                    ++g_delete_but_local_exist_count;
                    ret = TAIR_RETURN_SUCCESS;
                }
                break;
            default:
                log_error("invalid type: %d, ignore", type);
                ret = TAIR_RETURN_SUCCESS;
                break;
        }
    }

    if (value != NULL) {
        delete value;
    }

    return ret;
}

int do_repair_rsync(ClusterHandler &local_handler, RecordLogger *logger, RecordLogger *fail_logger) {
    int32_t index = 0;
    int64_t count = 0;
    int64_t fail_count = 0;
    time_t start_time = time(NULL);

    int32_t type;
    int32_t bucket_num = -1;
    bool force_reget = true;
    data_entry *record_key = NULL;
    data_entry *key = NULL;
    data_entry *value = NULL;

    std::map<std::string, tair::ClusterHandler *> remote_handlers;
    ClusterHandler *remote_handler = NULL;
    std::string cluster_info;

    int ret = TAIR_RETURN_SUCCESS;

    while (!g_stop) {
        // get failed record
        ret = logger->get_record(index, type, bucket_num, record_key, value, force_reget);

        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("get record fail: %d", ret);
            break;
        }
        // just ignore value
        if (value != NULL) {
            delete value;
            value = NULL;
        }
        if (record_key == NULL) {
            log_warn("read record over");
            break;
        }

        ++count;

        // filter real key
        key = filter_fail_record(*record_key, cluster_info);

        remote_handler = get_handler(cluster_info, remote_handlers);

        if (remote_handler != NULL) {
            ret = do_rsync_data(local_handler, *remote_handler, type, *key);
        } else {
            ret = TAIR_RETURN_FAILED;
        }

        // log failed record again
        if (ret != TAIR_RETURN_SUCCESS) {
            if (fail_count == 0) {
                // init fail logger
                int tmp_ret;
                if ((tmp_ret = fail_logger->init()) != TAIR_RETURN_SUCCESS) {
                    log_error("init fail logger fail, ret: %d", tmp_ret);
                    ret = tmp_ret;
                    break;
                }
            }

            ++fail_count;

            log_error("fail one %d", ret);
            ret = fail_logger->add_record(index, type, record_key, NULL);
            if (ret != TAIR_RETURN_SUCCESS) {
                log_error("add to fail logger fail: %d", ret);
                break;
            }
        }

        // cleanup
        if (record_key != NULL) {
            delete record_key;
            record_key = NULL;
        }
        if (key != NULL) {
            delete key;
            key = NULL;
        }
        cluster_info.clear();
        ret = TAIR_RETURN_SUCCESS;
    }

    // cleanup all over
    if (record_key != NULL) {
        delete record_key;
        record_key = NULL;
    }
    if (key != NULL) {
        delete key;
        key = NULL;
    }
    for (std::map<std::string, tair::ClusterHandler *>::iterator it = remote_handlers.begin();
         it != remote_handlers.end(); ++it) {
        delete it->second;
    }

    log_warn(
            "repair over. stopped: %s, total count: %ld, fail count: %ld, put_but_local_not_exist count: %ld, delete_but_local_exist count: %ld, cost: %ld(s)",
            g_stop ? "yes" : "no",
            count, fail_count, g_put_but_local_not_exist_count, g_delete_but_local_exist_count,
            time(NULL) - start_time);

    // consider stop early as fail
    return g_stop ? TAIR_RETURN_FAILED : ret;
}

void print_help(const char *name) {
    fprintf(stderr,
            "repair failed record when doing remote synchronization.\n"
                    "%s -f file -l local_cluster_addr\n"
                    "NOTE:\n"
                    "\tcluster_addr like: xxx.xxx.xxx.xxx:5198,xxx.xxx.xxx.xxx:5198,group_1\n", name);
}

int main(int argc, char *argv[]) {
    char *local_cluster_addr = NULL;
    char *file = NULL;
    int i = 0;

    while ((i = getopt(argc, argv, "f:l:")) != EOF) {
        switch (i) {
            case 'f':
                file = optarg;
                break;
            case 'l':
                local_cluster_addr = optarg;
                break;
            default:
                print_help(argv[0]);
                return 1;
        }
    }

    if (file == NULL || local_cluster_addr == NULL) {
        print_help(argv[0]);
        return 1;
    }

    // init signals
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);

    TBSYS_LOGGER.setLogLevel("warn");

    // just to read
    RecordLogger *logger = new SequentialFileRecordLogger(file, 0, false);
    // log failed record when repairing
    std::string fail_file = std::string(file) + ".fail";
    // lazy init
    RecordLogger *fail_logger = new SequentialFileRecordLogger(fail_file.c_str(), ~((int64_t) 1 << 63), false);

    int ret;
    if ((ret = logger->init()) != TAIR_RETURN_SUCCESS) {
        log_error("init logger fail, file: %s, ret: %d", file, ret);
    } else {
        ClusterHandler local_handler;
        ret = init_cluster_handler(local_cluster_addr, local_handler);
        if (ret != TAIR_RETURN_SUCCESS) {
            log_error("init local cluster fail, ret: %d, addr: %s", ret, local_cluster_addr);
        } else {
            log_warn("start repair rsync faillog: %s", file);
            ret = do_repair_rsync(local_handler, logger, fail_logger);
        }
    }

    delete logger;
    delete fail_logger;

    return ret == TAIR_RETURN_SUCCESS ? 0 : 1;
}
