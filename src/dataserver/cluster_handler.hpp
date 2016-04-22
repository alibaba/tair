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

#ifndef CLUSTER_HANDLER_HPP
#define CLUSTER_HANDLER_HPP

#include <string>
#include <vector>
#include <map>
#include <set>
#include <inttypes.h>

#include "common/blockqueue.hpp"

class ClusterHandlerTest;

typedef struct cJSON cJSON;

namespace tair {
namespace common {
class Record;

class RecordPosition;

template<class T>
class Result;

class RecordLogger;
}

namespace storage {
class storage_manager;
}

class tair_client_impl;

class RemoteClusterHandler;

// client handler
class ClusterHandler {
    // UNINIT --> INITED --> STARTED
public:
    enum {
        UNINIT = 0,
        INITED = 1,
        STARTED = 2
    };
public:
    // cluster conf
    class Config {
    public:
        Config();

        ~Config();

        bool equals(const ClusterHandler::Config *config);

        tair::common::Result<std::set<int> *> cas_forbidden_namespaces(std::set<int> *newnamespaces);

        tair::common::Result<std::set<int> *> cas_allowed_namespaces(std::set<int> *newnamespaces);

    public:
        const static int DEFAULT_TIMEOUT = 2000;
        const static int DEFAULT_QUEUE_LIMIT = 800;
    public:
        bool valid();

        int decode_info();

        void encode_info();

        cJSON *to_json() const;

        static ClusterHandler::Config *from_json(cJSON *json);

    public:
        std::string info_;             // internal
        std::string master_cs_addr_;   // must
        std::string slave_cs_addr_;    // optional, default string("")
        std::string group_name_;       // must
        std::string unit_;             // must
        int queue_limit_;  // optional, default 800
        int timeout_ms_;  // optional, default 2000ms
        int type_;         // optional, default INTERNAL("internal")
        int distance_;     // optional, default 0, use at future
    public:
        /**
         * 1. forbidden_namespaces_ empty, allowed_namespaces_ empty => all namespace are allowed
         * 2. forbidden_namespaces_ not empty, allowed_namespaces_ empty => namespaces in
         *    forbidden_namespaces are not allowed
         * 3. forbidden_namespaces_ not empty, allowed_namespaces_ not empty => some as
         *    number 2, allowed_namespaces_ is invisible
         * 4. forbidden_namespaces_ empty, allowed_namespaces_ not empty => namespaces
         *    in allowed_namespaces area allowed
         */
        std::set<int> *forbidden_namespaces_; // optional, default empty
        std::set<int> *allowed_namespaces_;   // optional, default empty
    public:
        static void destroy_namespaces(void *namespaces);
    };

public:
    ClusterHandler();

    virtual ~ClusterHandler();

    bool conf_inited(); // just be compatible with old rsync, at future will be removed
    inline int get_status() { return status_; }

    void set_queue_limit(int limit);

    void set_time_out(int timeout);

    void set_unit(std::string unit);

    void set_type(int type);

    const std::string connection_stat();

    tair_client_impl *client() const;

    std::string get_info() const;

    int get_queue_limit() const;

    int get_time_out() const;

    std::string id() const;

    std::string sid() const;

    int get_type() const;

    std::string get_unit() const;

    std::string get_group_name() const;

    std::string debug_string() const;

    cJSON *get_cluster_info() const;

    int start();

    int prepare_and_start(ClusterHandler::Config *config);

    bool equals(const ClusterHandler::Config *config);

    tair::common::Result<std::set<int> *> cas_forbidden_namespaces(std::set<int> *newnamespaces);

    tair::common::Result<std::set<int> *> cas_allowed_namespaces(std::set<int> *newnamespaces);

    std::set<int> *get_forbidden_namespaces();

    std::set<int> *get_allowed_namespaces();

public:
    void set_rsync_mode(bool rsync = true);

public: // just for old remote sync manager, at future will make this be private
    int init_conf(std::vector<std::string> &config); // at future will be removed
    int init_conf(const std::string &info, int32_t timeout_ms, int32_t queue_limit); // at future will be removed
    int init_conf(ClusterHandler::Config *config);

private:
    std::string info() const;

    std::string master() const;

    std::string slave() const;

    std::string group_name() const;

    std::string unit() const;

    int timeout() const;

    int queuelimit() const;

    int type() const;

    void destroy_client();

private:
    tair_client_impl *client_;
    ClusterHandler::Config *config_;
    bool rsync_;
    // bool                          inited_;
    int status_;

    friend class RemoteClusterHandler;

public:
    static void destroy(void *handler);
};

class RemoteClusterHandler : public ClusterHandler {
public:
    typedef struct CallbackArg {
        CallbackArg(int instance, int bucket, RemoteClusterHandler *handler) {
            instance_ = instance;
            bucket_ = bucket;
            handler_ = handler;
        }

        int instance_;
        int bucket_;
        RemoteClusterHandler *handler_;
    } CallbackArg;
private:
    class ResponseQueue {
    public:
        virtual ~ResponseQueue();

        virtual common::Result<CallbackArg *> get();

        virtual void put(common::Result<CallbackArg *> result);

        virtual bool empty();

    private:
        CBlockQueue <common::Result<CallbackArg *> > queue_;
    };

public:
    RemoteClusterHandler();

    ~RemoteClusterHandler();

    common::Result<common::Record *> get_record(int instance);

    void update(int instance, int bucket, bool status);

    void delete_log_file(int instance, uint64_t filenumber);

    common::RecordPosition get_min_position(int instance);

    void pause(bool p = true);

    bool paused() const;

    void restart();

    void watch();

    void statistics();

    std::string options();

    void options(const std::map<std::string, int> &m);

    common::Result<CallbackArg *> get_result();

    void put_result(common::Result<CallbackArg *> result);

    bool no_results();

    std::pair<std::string, std::string> get_unit_and_group();

    int start();

    int prepare_and_start(ClusterHandler::Config *config, common::RecordLogger *logger);

public:
    int Ref();

    int UnRef();

    int GetRef();

private:
    void set_logger(common::RecordLogger *logger);

private:
    bool pause_;
    int ref_;
    common::RecordLogger *logger_;
    ResponseQueue queue_;
};

}

#endif
