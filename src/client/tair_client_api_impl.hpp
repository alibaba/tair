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

#ifndef __TAIR_CLIENT_IMPL_H
#define __TAIR_CLIENT_IMPL_H

#include <string>
#include <vector>
#include <map>
#include <ext/hash_map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)

#include <unordered_map>

#else
#include <tr1/unordered_map>
namespace std
{
  using tr1::hash;
  using tr1::unordered_map;
}
#endif

#include <tbsys.h>
#include <easy_io.h>

#include "define.hpp"
#include "databuffer.hpp"
#include "packet_factory.hpp"
#include "tair_client_api.hpp"
#include "util.hpp"
#include "dump_data_info.hpp"
#include "BlockQueueEx.hpp"
#include "flowrate.h"

#include "i_tair_client_impl.hpp"
#include "flow_admin.h"
#include "tair_statistics.h"

namespace tair {
namespace common {
class Record;
namespace timedcollections {
class ITimedCollections;
}
}
}

namespace tair {
using namespace std;
using namespace __gnu_cxx;
using namespace tair::common;
namespace common { struct key_value_pack_t; }
namespace common { class DeStatSchema; }

class operation_record;

const int UPDATE_SERVER_TABLE_INTERVAL = 50;

class request_get;

class request_remove;

class request_mput;

class response_get_group;

class tair_statistics;

typedef map<uint64_t, request_get *> request_get_map;
typedef map<uint64_t, request_remove *> request_remove_map;
typedef map<uint64_t, map<uint32_t, request_mput *> > request_put_map;
typedef struct op_cmd_result {
    op_cmd_result() : retcode(0) {}

    int32_t retcode;
    vector<string> infos;
} op_cmd_result;
// ds -> op_cmd_result
typedef map<uint64_t, op_cmd_result> op_cmd_result_map;

class tair_client_impl : public i_tair_client_impl, public tbsys::Runnable {

public:
    tair_client_impl();

    virtual ~tair_client_impl();

    bool startup(const char *master_addr, const char *slave_addr, const char *group_name);

    bool directup(const char *server_addr);

    bool startup(uint64_t data_server);

    void close();

    bool isinited() { return inited; }

    void set_thread_count(uint32_t thread_count) {
        this->thread_count = thread_count;
    }

    void set_light_mode() {
        light_mode_ = true;
    }

    void set_rsync_mode(bool rsync);

    int get_max_bucket_count() { return bucket_count; }

public:

    int put(int area,
            const data_entry &key,
            const data_entry &data,
            int expire,
            int version,
            bool fill_cache = true,
            TAIRCALLBACKFUNC pfunc = NULL, void *arg = NULL);

    int mput(int area, const tair_client_kv_map &kvs, int &fail_request/*tair_dataentry_vector& fail_keys*/,
             bool compress = true);

    //the caller should release the memory
    int get(int area,
            const data_entry &key,
            data_entry *&data);

    int get(int area,
            const data_entry &key,
            callback_get_pt pf = NULL, void *pargs = NULL);

    int mget(int area,
             const vector<data_entry *> &keys,
             tair_keyvalue_map &data);

    //with callback
    inline int remove(int area, const data_entry &key, TAIRCALLBACKFUNC pfunc = NULL, void *parg = NULL) {
        return do_process_with_key(TAIR_REQ_REMOVE_PACKET, area, key, pfunc, parg);
    }

    inline int removes(int area, const tair_dataentry_set &mkey_set,
                       key_code_map_t *key_code_map,
                       TAIRCALLBACKFUNC_EX pfunc = NULL, void *parg = NULL) {
        return do_process_with_multi_keys(TAIR_REQ_PREFIX_REMOVES_PACKET, area, mkey_set, key_code_map, pfunc, parg);
    }

    inline int hide(int area, const data_entry &key, TAIRCALLBACKFUNC pfunc, void *parg) {
        return do_process_with_key(TAIR_REQ_HIDE_PACKET, area, key, pfunc, parg);
    }

    inline int hides(int area, const tair_dataentry_set &mkey_set,
                     key_code_map_t *key_code_map,
                     TAIRCALLBACKFUNC_EX pfunc = NULL, void *parg = NULL) {
        return do_process_with_multi_keys(TAIR_REQ_PREFIX_HIDES_PACKET, area, mkey_set, key_code_map, pfunc, parg);
    }

    int invalidate(int area, const data_entry &key, const char *groupname, bool is_sync = true);

    int invalidate(int area, const data_entry &key, bool is_sync = true);


    int prefix_invalidate(int area, const data_entry &pkey, const data_entry &skey, const char *groupname,
                          bool is_sync = true);

    int prefix_invalidate(int area, const data_entry &pkey, const data_entry &skey, bool is_sync = true);

    int hide_by_proxy(int area, const data_entry &key, const char *groupname, bool is_sync = true);

    int hide_by_proxy(int area, const data_entry &key, bool is_sync = true);

    int prefix_hide_by_proxy(int area, const data_entry &pkey, const data_entry &skey, const char *groupname,
                             bool is_sync = true);

    int prefix_hide_by_proxy(int area, const data_entry &pkey, const data_entry &skey, bool is_sync = true);

    int retry_all();

    //get inval server's info
    int get_invalidserver_info(const uint64_t &inval_server_id, std::string &buffer);

    int retry_all(uint64_t invalid_server_id);

    int retrieve_invalidserver(vector<uint64_t> &invalid_server_list);

    int query_from_invalidserver(uint64_t invalid_server_id, inval_stat_data_t *&stat);

    int query_from_invalidserver(std::map<uint64_t, inval_stat_data_t *> &stats);

    int get_hidden(int area, const data_entry &key, data_entry *&value);

    int prefix_get(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value);

    int prefix_gets(int area, const data_entry &pkey, const tair_dataentry_set &skey_set,
                    tair_keyvalue_map &result_map, key_code_map_t &failed_map);

    int prefix_gets(int area, const data_entry &pkey, const tair_dataentry_vector &skeys,
                    tair_keyvalue_map &result_map, key_code_map_t &failed_map);

    int prefix_put(int area, const data_entry &pkey, const data_entry &skey,
                   const data_entry &value, int expire, int version);

    int prefix_puts(int area, const data_entry &pkey,
                    const vector<key_value_pack_t *> &skey_value_packs, key_code_map_t &failed_map);

    int prefix_hide(int area, const data_entry &pkey, const data_entry &skey);

    int
    prefix_hides(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map);

    int prefix_get_hidden(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value);

    int prefix_remove(int area, const data_entry &pkey, const data_entry &skey);

    int
    prefix_removes(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map);

    int get_range(int area, const data_entry &pkey, const data_entry &start_key, const data_entry &end_key,
                  int offset, int limit, vector<data_entry *> &values, short type = CMD_RANGE_ALL);

    int hides(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map);

    int removes(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map);

    int mdelete(int area,
                const vector<data_entry *> &keys);

    int hide(int area, const data_entry &key);

    int add_count(int area,
                  const data_entry &key,
                  int count,
                  int *retCount,
                  int init_value = 0,
                  int expire_time = 0);

    int set_count(int area, const data_entry &key, int count,
                  int expire, int version);

    int lock(int area, const data_entry &key, LockType type);

    int expire(int area,
               const data_entry &key,
               int expire);

    int uniq_remove(int area,
                    int bucket,
                    const data_entry &key);

    int sync(int32_t bucket, std::vector<Record *> *records, bool fill_cache = true,
             TAIRCALLBACKFUNC callback = NULL, void *data = NULL);

    int set_flow_limit_bound(uint64_t addr, int &ns,
                             int &lower, int &upper,
                             tair::stat::FlowType &type);

    int set_flow_limit_bound(int ns, int lower, int upper, tair::stat::FlowType type);

    int get_flow(uint64_t addr, int ns,
                 tair::stat::Flowrate &rate);

    void set_timeout(int this_timeout);

    void set_queue_limit(int limit);

    void set_randread(bool rand_flag);

    uint64_t get_queue_limit();

    /* memcached related */
    int mc_ops(int8_t mc_opcode,
               int area,
               const data_entry *key,
               const data_entry *value,
               int expire,
               int version,
               callback_mc_ops_pt callback, void *arg);

    uint32_t get_bucket_count() const { return bucket_count; }

    uint32_t get_copy_count() const { return copy_count; }

    template<typename Type>
    int find_elements_type(Type element);

    void get_server_with_key(const data_entry &key, std::vector<std::string> &servers);

    bool get_group_name_list(uint64_t id1, uint64_t id2, std::vector<std::string> &groupnames);

    int get_servers(std::set<uint64_t> &servers);

    //@override Runnable
    void run(tbsys::CThread *thread, void *arg);

    int direct_update(std::vector<uint64_t> &servers, std::vector<operation_record *> *opercs);

    int clear_area(int area);

    int remove_area(int area, uint64_t server_id);

    // int getStatInfo(int type, int area, vector<ResponseStatPacket *> &list);
    int dump_area(std::set<dump_meta_info> &info);

    // cmd operated directly to configserver
    int op_cmd_to_cs(ServerCmdType cmd, std::vector<std::string> *params, std::vector<std::string> *ret_values);

    int op_cmd_to_ds(ServerCmdType cmd, std::vector<std::string> *params, std::vector<std::string> *infos,
                     const char *dest_server_addr = NULL);

    void op_cmd_to_ds_new(ServerCmdType cmd, const std::vector<std::string> &params,
                          const char *dest_server_addr, op_cmd_result_map &op_cmd_result);

    void set_force_service(bool force) { this->force_service = force; }

    bool get_server_id(int32_t bucket, vector<uint64_t> &server, bool only_slaves = false);

    /*
     * @brief   add or del dataserver in cs
     * @param server_id_list  the server id to operate
     * @param cmd  DATASERVER_CTRL_OP_ADD -- add dataserver to cs
     *             DATASERVER_CTRL_OP_DELETE -- del datserver to cs
     * @return return_code  the result of every server_id, return_code.size() == server_id_list.size() is guaranteed
     *                      -- DATASERVER_CTRL_RETURN_SUCCESS, success
     *                      -- other, failed.
     */
    void manipulate_ds_to_cs(const vector<uint64_t> &server_id_list, DataserverCtrlOpType cmd,
                             vector<DataserverCtrlReturnType> &return_code);

    void get_migrate_status(uint64_t server_id, vector<pair<uint64_t, uint32_t> > &result);

    void query_from_configserver(uint32_t query_type, const string group_name, map<string, string> &,
                                 uint64_t server_id = 0);

    /*
      @params serverId,  specify which server to get, 0 means get all alive server stat.
      @params stat_info, struct which store the stat data.
    */
    int query_from_dataserver(uint64_t serverId, tair_statistics &stat_info);

    uint32_t get_config_version() const;

    int64_t ping(uint64_t server_id);

    int retrieve_server_config(bool update_server_table, tbsys::STR_STR_MAP &config_map, uint32_t &version);

    void get_buckets_by_server(uint64_t server_id, std::set<int32_t> &buckets);

    const char *get_group_name();

    int get_rt(uint64_t sid, uint32_t cmd, int op, std::string *str, bool json = false);

    int get_hidden_impl(int area, const data_entry &key, data_entry *&value, int32_t server_select = 0);

    int mget_impl(int area, const vector<data_entry *> &keys, tair_keyvalue_map &data, int server_select = 0);

#if  0     /* ----- #if 0 : If0Label_1 ----- */
    bool dumpKey(int area, char *file_name, int timeout = 0);

    bool dumpBucket(int db_id, char *path);


    bool ping(int timeout = 0);

    char *getGroupName();

    uint64_t getConfigServerId();

#endif     /* ----- #if 0 : If0Label_1 ----- */
public:
    const std::string stat();

    int push_waitobject(wait_object *obj);

    static int invoke_callback(void *phandler, wait_object *obj);

    uint64_t get_rsync_server(uint64_t data_server);

    flow_admin &get_flow_admin() { return flow_admin_; }

    const vector<uint64_t> &get_config_server_list() { return config_server_list; }

private:
    int packet_handler(easy_request_t *r);

    static int packet_handler_cb(easy_request_t *r) {
        if (r->ms->c == NULL) {
            easy_session_destroy(r->ms);
            return EASY_ERROR;
        }

        tair_client_impl *_this = (tair_client_impl *) r->ms->c->handler->user_data;
        return _this->packet_handler(r);
    }

    easy_io_t eio;
    easy_io_handler_pt handler;
    int32_t thread_count; //~ thread count of easy I/O
private:

    bool startup(uint64_t master_cfgsvr, uint64_t slave_cfgsvr, const char *group_name);

    bool directup(uint64_t data_server);

    bool initialize();

    bool retrieve_server_addr();

    void parse_invalidate_server(response_get_group *rggp);

    void shrink_invalidate_server(uint64_t server_id);

    void start_net();

    void stop_net();

    void wait_net();

    void reset(); //reset enviroment

    bool get_server_id(const data_entry &key, vector<uint64_t> &server);

    int send_request(uint64_t serverId, base_packet *packet, int waitId);

    //int send_request(vector<uint64_t>& server,TAIRPacket *packet,int waitId);

    int get_response(wait_object *cwo, int waitCount, base_packet *&tpacket);

    int get_response(wait_object *cwo, int wait_count, std::vector<base_packet *> &tpacket);

    int get_response_new(wait_object *cwo, int wait_count, vector<base_packet *> &tpacket);

    bool data_entry_check(const data_entry &data);

    bool key_entry_check(const data_entry &data);

    int init_request_map(int area,
                         const vector<data_entry *> &keys,
                         request_get_map &request_gets,
                         int server_select = 0);

    int init_request_map(int area,
                         const vector<data_entry *> &keys,
                         request_remove_map &request_removes);

    int init_put_map(int area,
                     tair_client_kv_map::const_iterator &kv_iter, const tair_client_kv_map::const_iterator &kv_end_iter,
                     request_put_map &request_puts);

    bool get_send_para(const data_entry &key, vector<uint64_t> &server, uint32_t &bucket_number);

    int do_async_timeout(wait_object *);

    //~ do the request operation, delete req if send failed
    template<typename Request, typename Response>
    int do_request(Request *req, Response *&resp, wait_object *cwo, uint64_t server_id) {
        if (req == NULL) {
            log_error("request is null");
            return TAIR_RETURN_SEND_FAILED;
        }
        if (cwo == NULL) {
            log_error("wait object is null");
            return TAIR_RETURN_SEND_FAILED;
        }

        int ret = TAIR_RETURN_SEND_FAILED;
        base_packet *tpacket = NULL;
        do {
            if ((ret = send_request(server_id, req, cwo->get_id())) != TAIR_RETURN_SUCCESS) {
                break;
            }
            if ((ret = get_response(cwo, 1, tpacket)) < 0) {
                break;
            }
            resp = dynamic_cast<Response *>(tpacket);
            if (resp == NULL) {
                log_error("dynamic_cast failed");
                ret = TAIR_RETURN_FAILED;
                break;
            }
            ret = resp->get_code();
        } while (false);
        return ret;
    }

    // send packet to the invalid server
    int send_packet_to_is(uint64_t server_id, base_packet *packet, int id);

    // get packet from the invalid server
    int get_packet_from_is(wait_object *wo, uint64_t server_id, base_packet *&packet);

    // resolve the packet from the invalid server.
    int resolve_packet(base_packet *packet, uint64_t server_id, char *&buffer,
                       unsigned long &buffer_size, int &group_count);

    // resolve the packet from the invalid server.
    int resolve_packet(base_packet *packet, uint64_t server_id);

    // resolve the packet from the invalid server.
    int resolve_packet(base_packet *packet, uint64_t server_id, std::string &info);

    //interact with the invalid server(s)
    //if invalid_server_id == 0, we will select the invalid server randomly from the
    //list of the invalid server, otherwise get the invalid server from the list of
    //the invalid server indicated by `invalid_server_id.
    int do_interaction_with_is(base_packet *packet, uint64_t invalid_server_id = 0);

    //aysnchronous to ineract with ds.
    inline base_packet *init_packet(int pcode, int area, const data_entry &key);

    //aysnchronous to ineract with ds.
    inline base_packet *init_packet(int pcode, int area, const tair_dataentry_set &mkey_set);

    //the implementation of the aysnchronous operation (remove, hide).
    int do_process_with_key(int pcode, int area, const data_entry &key, TAIRCALLBACKFUNC pfunc, void *parg);

    //the implementation of the aysnchronous operation (removes, hides).
    int do_process_with_multi_keys(int pcode, int area,
                                   const tair_dataentry_set &key, key_code_map_t *key_code_map,
                                   TAIRCALLBACKFUNC_EX pfunc, void *parg);

    void check_flow_down(uint64_t serverid, int ns);

private:
    bool inited;
    bool is_stop;
    tbsys::CThread thread;
    tbsys::CThread response_thread; //thread to response packet.

    bool direct;
    uint64_t data_server;

    int timeout; // ms
    uint64_t queue_limit; // connectmanager queue_limit

    vector<uint64_t> my_server_list;
    vector<uint64_t> config_server_list;
    vector<uint64_t> invalid_server_list;
    std::string group_name;
    uint32_t config_version;
    uint32_t new_config_version;
    int send_fail_count;
    wait_object_manager *this_wait_object_manager;
    uint32_t bucket_count;
    uint32_t copy_count;
    // can this client can service forcefully(no server address list eg.)
    bool force_service;
    bool rand_read_flag;
    atomic_t read_seq;
private:
    void do_queue_response();

    int handle_response_obj(wait_object *obj);

    int slow_wait_if_rsync_mode(uint64_t dataserver, int pcode);

    void update_send_count_if_rsync_mode(uint64_t dataserver, int pcode);

private:
    typedef BlockQueueEx<wait_object *> CAsyncCallerQueue;
    CAsyncCallerQueue m_return_object_queue;
private:
    pthread_rwlock_t m_init_mutex;
    //__gun_cxx::hash<template> does not support the type `uint64_t.
    //using __gun_cxx::hash<int>, it also work well.
    hash_map<uint64_t, int, __gnu_cxx::hash<int> > fail_count_map;

    flow_admin flow_admin_;

    bool light_mode_; //~ no user-level background threads
    bool rsync_mode_; // use this mode will control send packet speed
    tair::common::timedcollections::ITimedCollections *timed_collections_;
    time_t log_last_time_;

    //for query_from_dataserver
    tbsys::CThreadMutex deschema_lock_;
    DeStatSchema *deschema_;
};

} /* tair */
#endif //__TAIR_CLIENT_IMPL_H
