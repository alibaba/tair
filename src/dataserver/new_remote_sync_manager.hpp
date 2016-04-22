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

#ifndef NEW_REMOTE_SYNC_MANAGER_HPP_
#define NEW_REMOTE_SYNC_MANAGER_HPP_

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)

#include <unordered_map>

#else
#include <tr1/unordered_map>
namespace std {
  using tr1::hash;
  using tr1::unordered_map;
}

#endif

#include "i_remote_sync_manager.hpp"
#include "cluster_handler.hpp"

#include <semaphore.h>

namespace tbsys {
class CThread;

class CDefaultRunnable;
}

namespace tair {
// class test::NewRemoteSyncManagerTest;
class tair_manager;

namespace common {
template<class T>
class Result;

class Record;

class data_entry;
}

/* json is just one way of describing data model, not mean using it for data ETL
 *  {
 *    "source": {
 *      "master": "xxx.xxx.xxx.xxx:5198",
 *      "slave": "xxx.xxx.xxx.xxx:5198",
 *      "group": "group1",
 *      "queuelimit": 1000,
 *      "timeout": 2000,
 *      "type": "self",
 *      "distance": 0,             // at future will useful
 *      "location": "Hangzhou"     // more detail description
 *    },
 *    "distinations": [
 *      {
 *        "master": "xxx.xxx.xxx.xxx:5198",
 *        "slave": "xxx.xxx.xxx.xxx:5198",
 *        "group": "group2",
 *        "queuelimit": 800,
 *        "timeout": 2000,
 *        "type": "innerunit",
 *        "distance": 10,
 *        "location": "Hangzhou"
 *      },
 *      {
 *        "master": "xxx.xxx.xxx.xxx:5199",
 *        "slave": "xxx.xxx.xxx.xxx:5199",
 *        "group": "group3",
 *        "queuelimit": 800,
 *        "timeout": 2000,
 *        "type": "interunit",
 *        "distance": 20,
 *        "location": "Shanghai",
 *        "allows": [0, 1, 2, 4]
 *      },
 *      {
 *        "master": "xxx.xxx.xxx.xxx:5160",
 *        "slave": "xxx.xxx.xxx.xxx:5160",
 *        "group": "group4",
 *        "queuelimit": 800,
 *        "timeout": 2000,
 *        "type": "interunit",
 *        "distance": 40,
 *        "location": "Inner Mongolia",
 *        "forbiddens": [0, 1, 2, 4]
 *      }
 *    ]
 *  }
 * */

class RsyncConfig {
public:
    RsyncConfig();

    ~RsyncConfig();

    std::string to_string() const;

    static RsyncConfig *from_string(std::string &str);

public:
    ClusterHandler::Config *source_; // which cluster this node in
    std::vector<ClusterHandler::Config *> distinations_;
};

class NewRemoteSyncManager : public IRemoteSyncManager, tbsys::CDefaultRunnable {
private:
    typedef std::unordered_map<std::string, RemoteClusterHandler *> CLUSTER_HANDLER_MAP;
public:
    NewRemoteSyncManager(tair_manager *manager);

    ~NewRemoteSyncManager();

    static void callback(int ret, void *arg);

    static void destroy_cluster_map(void *m);

    typedef common::Result<common::Record *> (*FilterRecordFunc)(common::Record *record,
                                                                 RemoteClusterHandler *handler,
                                                                 NewRemoteSyncManager *manager);

    static common::Result<common::Record *> filter(common::Record *record,
                                                   RemoteClusterHandler *handler, NewRemoteSyncManager *manager);

public:
    virtual std::string get_rsync_info();

    virtual int init();

    virtual void pause(bool pause);

    virtual int pause(bool pause, const std::string unit, const std::string group);

    virtual int watch(const std::string unit, const std::string group);

    virtual int statistics(const std::string unit, const std::string group);

    virtual std::string options(const std::string unit, const std::string group);

    virtual int options(const std::string unit, const std::string group, std::map<std::string, int> &m);

    virtual void set_wait_us(int64_t us);

    virtual int64_t get_wait_us();

    virtual std::string get_rsync_config_service();

    virtual void set_rsync_config_service(const std::string &uri);

    virtual int get_rsync_config_update_interval();

    virtual void set_rsync_config_update_interval(int interval);

    virtual std::string get_cluster_info();

    virtual std::string connection_stat();

public:
    void update_thread(int id);

    void work_thread(int id);

    void run(tbsys::CThread *thread, void *args);

private:
    std::vector<std::pair<std::string /* cluster name */, std::string> > get_rsync_info(int instance);

    bool is_master_node(int bucket);

    void do_remote_sync();

    // try clean useless log file
    void try_clean_useless_log_file(int instance);

    int do_remote_for_instance(int instance);

    int do_remote_for_cluster(int instance, RemoteClusterHandler *handler, FilterRecordFunc filter);

    int do_remote_sync_op(int instance, common::Record *record, RemoteClusterHandler *handler);

    common::Result<common::Record *> do_get_from_local_cluster(common::Record *record);

    void attach_info_to_key(common::data_entry *key, common::data_entry *value, int type);

    int init_sync_client();

    int init_sync_conf();

public:
    time_t last_time_;
    bool is_paused_;
    int64_t wait_us_;
    int instance_count_;
    ClusterHandler *local_cluster_handler_;
    // temporary handler
    ClusterHandler *lazy_release_cluster_handler_;
    CLUSTER_HANDLER_MAP *remote_cluster_handlers_;
private:
    int config_version_;
    std::string remote_url_;

    RsyncConfig *get_json_format_config();

private:
    sem_t stop_sem_;
    int rsync_config_update_interval_;
    tair_manager *tair_manager_;
    time_t *last_check_delete_log_file_;
    const static int CHECK_INTERVAL = 60; /* s */
    const static int MAX_WORKER_THREAD_NUM = 1; /* not include update config thread */
    // configuration update related variable
private:
    RsyncConfig *get_rsync_config();

    RsyncConfig *check_rsync_config(RsyncConfig *config);

    // update detail item about cluster config,
    // like: timeout, queuelimit, allowed_buckets, forbidden_buckets
    void update_detail_cluster_config(ClusterHandler *handler, ClusterHandler::Config *config);

    // update self cluster config of rsync
    void update_rsync_self_config(ClusterHandler::Config *&lconfig);

    // judge if need to update remote cluster config of rsync
    bool is_rsync_remote_config_equals(std::vector<ClusterHandler::Config *> &remotes);

    // update remote cluster config of rsync
    void update_rsync_remote_config(std::vector<ClusterHandler::Config *> &remotes);

    // update all rsync cluster config, after call this method, config will be NULL
    void update_rsync_config(RsyncConfig *config);

private:
    typedef void (*garbage_destory_func)(void *);

    // throw garbage into garbage can
    void throw_garbage(void *data, garbage_destory_func func);

    // each update_rsync_config will check and clean garbages
    void clean_garbages(bool force = false);

private:
    typedef struct {
        void *data_;
        garbage_destory_func destroy_;
        time_t dead_time_;
    } Garbage;
    time_t last_garbage_collection_time_;
    std::queue<Garbage *> garbages_;
};
}

#endif
