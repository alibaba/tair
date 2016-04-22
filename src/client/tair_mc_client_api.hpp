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

#ifndef TAIR_MC_CLIENT_API
#define TAIR_MC_CLIENT_API

#include "diamond_manager.hpp"
#include "cluster_controller.hpp"

namespace tair {
class diamond_manager;

using namespace std;

class tair_mc_client_api : public DIAMOND::SubscriberListener {
public:
    tair_mc_client_api();

    ~tair_mc_client_api();

    /**
     * @brief   Specify which network interface to use to get the diamond dataid context.
     *          If not invoked, it auto try all interface expect the "lo",
     *          and take the one which first match the route rule in dimond dataid-GROUP.
     *
     * @param net_device_name    the network interface, such as "eth0", "bond0".
     */
    void set_net_device_name(const string &net_device_name);

    /*
     * @brief Specify the time interval to check whether the tair info in diamond is changed.
     *        If you don't understand what it means, don't invoke it.
     * @param[in] interval_second  time interval in second
     *                             if <= 0, nothing is changed and the default value will be used.
     */
    void set_tair_info_check_interval(int interval_second);

    /**
     * @brief connect to tair cluster
     *
     * @param config_id      diamond config_id of the tair cluster which you will connect to
     *
     * @return true -- success, false -- fail
     */
    bool startup(const string &config_id);

    /**
     * @brief setup local cache, now, just support cache read.
     *        If you use it, you should call it after startup(), or it will do nothing.
     *        If in diamond server config info, local cache had beed set, it will do nothing too.
     *
     * @param area          namesapce
     * @param capability    cache size, unit number
     * @param expire        entry expire time, unit ms, default 300ms
     */
    void setup_cache(int area, size_t capacity = 30);

    void setup_cache(int area, size_t capacity, uint64_t expire_time);

    /**
     * @brief close tairclient, release resource
     */
    void close();

    const string &get_config_id();

    const string &get_data_group_id();

    /**
     * @brief put data to tair
     *
     * @param area     namespace
     * @param key      key
     * @param data     data
     * @param expire   expire time,realtive time
     * @param version  version,if you don't care the version,set it to 0
     * @param fill_cache whether fill cache when put(only meaningful when server support embedded cache).
     *
     * @return 0 -- success, otherwise fail,you can use get_error_msg(ret) to get more information.
     */
    int put(int area,
            const data_entry &key,
            const data_entry &data,
            int expire,
            int version,
            bool fill_cache = true);

    /**
     * @brief get data from tair cluster
     *
     * @param area    namespace
     * @param key     key
     * @param data    data,a pointer,the caller must release the memory.
     *
     * @return 0 -- success, otherwise fail.
     */
    int get(int area,
            const data_entry &key,
            data_entry *&data);

    /**
     * @brief get multi  data from tair cluster
     *
     * @param area    namespace
     * @param keys    keys list, vector type
     * @param data    data,data hash map,the caller must release the memory.
     *
     * @return 0 -- success, TAIR_RETURN_PARTIAL_SUCCESS -- partly success, otherwise fail.
     */
    int mget(int area,
             const vector<data_entry *> &keys,
             tair_keyvalue_map &data);

    /**
     * @brief  remove data from tair cluster
     *
     * @param area    namespace
     * @param key     key
     *
     * @return  0 -- success, otherwise fail.
     */
    int remove(int area,
               const data_entry &key);

    /**
     * @brief delete data from tair cluster, if there is no invalserver, it acts as remove().
     *
     * @param area    namespace
     * @param key     key
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int invalidate(int area, const data_entry &key, bool is_sync = true);

    /**
     * @brief delete data with prefix key from tair cluster, if there is no invalserver, it acts as prefix_remove().
     *
     * @param area    namespace
     * @param pkey     key
     * @param skey     key
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int prefix_invalidate(int area, const data_entry &key, const data_entry &skey, bool is_sync = true);

    /**
     * @brief hide data
     *
     * @param area    namespace
     * @param key     key
     * @param groupname    group's name.
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int hide_by_proxy(int area, const data_entry &key, const char *groupname, bool is_sync = true);

    /**
     * @brief hide data
     *
     * @param area    namespace
     * @param key     key
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int hide_by_proxy(int area, const data_entry &key, bool is_sync = true);

    /**
     * @brief hide data with prefix key
     *
     * @param area    namespace
     * @param key     key
     * @param groupname    group's name.
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int prefix_hide_by_proxy(int area, const data_entry &pkey, const data_entry &skey, const char *groupname,
                             bool is_sync = true);

    /**
     * @brief hide data with prefix key
     *
     * @param area    namespace
     * @param key     key
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int prefix_hide_by_proxy(int area, const data_entry &pkey, const data_entry &skey, bool is_sync = true);

    /**
     * @brief to hide one item
     * @param area: namespace
     * @param key: key to hide
     */
    int hide(int area, const data_entry &key);

    /**
     * @brief to get one hidden item
     * @param area: namespace
     * @param key: key to get
     * @param value: ptr holding the value returned
     */
    int get_hidden(int area, const data_entry &key, data_entry *&value);

    /**
     * @brief to get one item with prefix key
     * @param area: namespace
     * @param pkey: primary key
     * @param skey: secondary key
     * @param value: ptr holding the value returned
     */
    int prefix_get(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value);

    /**
     * @brief to get multiple items with prefix key
     * @param area: namespace
     * @param pkey: primary key
     * @param skey_set skeys: secondary keys
     *        in set or vector, choose for your concerns
     * @param value: ptr holding the value returned
     * @return code: like normal
     *         failed_map: error code for each failed skey, if any
     */
    int prefix_gets(int area, const data_entry &pkey, const tair_dataentry_set &skey_set,
                    tair_keyvalue_map &result_map, key_code_map_t &failed_map);

    int prefix_gets(int area, const data_entry &pkey, const tair_dataentry_vector &skeys,
                    tair_keyvalue_map &result_map, key_code_map_t &failed_map);

    /**
     * @brief to put one item with prefix key
     * @param area: namespace
     * @param pkey & skey: primary key & secondary key
     * @param value: value to put
     * @param expire&version: expire & version
     */
    int prefix_put(int area, const data_entry &pkey, const data_entry &skey,
                   const data_entry &value, int expire, int version);

    /**
     * @brief to put multiple items with prefix key
     * @param area: namespace
     * @param pkey: primary key
     * @param skey_value_packs: {skey, value, version, expire} packs
     * @param value: ptr holding the value returned
     * @return code: like normal
     *         failed_map: error code for each failed skey, if any
     */
    int prefix_puts(int area, const data_entry &pkey,
                    const vector<key_value_pack_t *> &skey_value_packs, key_code_map_t &failed_map);

    /**
     * @brief to hide one item with prefix key
     * @param area: namespace
     * @param pkey & skey: primary & secondary key
     */
    int prefix_hide(int area, const data_entry &pkey, const data_entry &skey);

    /**
     * @brief like prefix_hide, but with multiple skeys
     * @param pkey: primary key
     * @param skey_set: set of skeys
     * @param key_code_map: return code of each failed key, would be cleared before filled
     * @return success, partial success, or others
     */
    int
    prefix_hides(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map);

    /**
     * @brief to get one hidden item with prefix key
     * @param pkey & skey: primary & secondary key
     * @param value: to receive the result
     */
    int prefix_get_hidden(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value);

    /**
     * @brief to remove one item with prefix key
     * @param area: namespace
     * @param pkey & skey: primary & secondary key
     */
    int prefix_remove(int area, const data_entry &pkey, const data_entry &skey);

    /**
     * @brief like prefix_remove, but with multiple skeys
     * @param pkey: primary key
     * @param skey_set: set of skeys
     * @param key_code_map: return code of each failed skey, would be cleared before filled
     * @return success, partial success, or others
     */
    int
    prefix_removes(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map);

    /**
     * @brief
     * @param pkey: primary key for range query
     * @param start_key: start skey to seek, seek from the first skey if equals ""
     * @param end_key: end skey to seek, seek to the last skey if equals ""
     * @param offset: skip first N skey
     * @param limit: limit response less than N skey
     * @param values: result skey with ascending sortting, free memory by user.
     * @param type: CMD_RANGE_ALL means get key&value response,
     *              CMD_RANGE_VALUE_ONL means get only values,
     *              CMD_RANGE_KEY_ONL means get only keys.
     * @return TAIR_RETURN_SUCCESS -- success, <0 -- fail, or TAIR_HAS_MORE_DATA -- limit by package size(1M default).
     */
    int get_range(int area, const data_entry &pkey, const data_entry &start_key, const data_entry &end_key,
                  int offset, int limit, vector<data_entry *> &values, short type = CMD_RANGE_ALL);

    /**
     * @brief remove multiple items, which were merged with the same prefix key
     * @param mkey_set: set of merged keys
     * @param key_code_map: return code of each failed skey, would be cleared before filled
     * @return success, partial success, or others
     */
    //!NOTE called by invalid server
    int removes(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map);

    /**
     * @brief hide multiple items, which were merged with the same prefix key
     * @param mkey_set: set of merged keys
     * @param key_code_map: return code of each failed skey, would be cleared before filled
     * @return success, partial success, or others
     */
    //!NOTE called by invalid server
    int hides(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map);

    /**
     * @brief remove multi  data from tair cluster
     *
     * @param area    namespace
     * @param keys    key list, vector type
     *
     * @return  0 -- success, TAIR_RETURN_PARTIAL_SUCCESS partly success, otherwise fail.
     */
    int mdelete(int area,
                const vector<data_entry *> &key);

    /**
     * @brief invalid multi  data from tair cluster
     *
     * @param area    namespace
     * @param keys    key list, vector type
     *
     * @return  0 -- success, TAIR_RETURN_PARTIAL_SUCCESS partly success, otherwise fail.
     */
    int minvalid(int area,
                 vector<data_entry *> &key);

    /**
     * @brief incr
     *
     * @param area            namespace
     * @param key             key
     * @param count           add (count) to original value.
     * @param retCount                the latest value
     * @param expire          expire time
     * @param initValue               initialize value
     *
     * @return 0 -- success, otherwise fail.
     */
    int incr(int area,
             const data_entry &key,
             int count,
             int *ret_count,
             int init_value = 0,
             int expire = 0);

    // opposite to incr
    int decr(int area,
             const data_entry &key,
             int count,
             int *ret_count,
             int init_value = 0,
             int expire = 0);

    /**
     *
     * ******this method is deprecated,don't use******
     *
     * @brief   add count
     *
     * @param area            namespace
     * @param key             key
     * @param count           add (count) to original value.
     * @param retCount                the latest value
     * @param initValue               the initialize value
     *
     * @return  0 -- success ,otherwise fail.
     */
    int add_count(int area,
                  const data_entry &key,
                  int count,
                  int *ret_count,
                  int init_value = 0);

    /**
     * @brief set count to key's value, ignore whether this key exists or is not
     *        count type.
     *
     * @param area            namespace
     * @param key             key
     * @param count           set count value
     * @param expire          expire time
     * @param version         version
     *
     * @return 0 -- success, otherwise fail.
     */
    int set_count(int area, const data_entry &key, int count, int expire = 0, int version = 0);

    /**
     * @brief Once a key is locked, it can NOT be updated(put/incr/decr), can only
     *        be get or delete. A locked key can be unlocked(unlock)
     *
     * @param area            namespace
     * @param key             key
     *
     * @return TAIR_RETURN_SUCCESS -- success, TAIR_RETURN_LOCK_EXIST -- fail: already locked
     *         otherwise -- fail
     */
    int lock(int area, const data_entry &key);

    /**
     * @brief opposite to lock
     *
     * @param area            namespace
     * @param key             key
     *
     * @return TAIR_RETURN_SUCCESS -- success,
     *         TAIR_RETURN_LOCK_NOT_EXIST -- fail: key is not locked
     *         otherwise -- fail
     */
    int unlock(int area, const data_entry &key);

    /**
     * @brief set the expire time
     *
     * @param area     namespace
     * @param key      key
     * @param expire   expire time,realtive time
     *
     * @return 0 -- success, otherwise fail.
     */
    int expire(int area,
               const data_entry &key,
               int expire);

    /**
     * @brief set log level
     *
     * @param level    "DEBUG"|"WARN"|"INFO"|"ERROR"
     */
    void set_log_level(const char *level);

    /**
     * @brief set log file
     *
     * @param log_file  the filename of log file
     */
    void set_log_file(const char *log_file);

    /**
     * @brief set timout of each method, ms
     *
     * @param timeout
     */
    void set_timeout(int timeout);

    /**
     * @brief set read from random server
     *
     * @param rand_flag: true means read from all replicas.
     */
    void set_randread(bool rand_flag);

#ifdef WITH_COMPRESS

    /**
     * @brief set the compress algorithm type
     *
     * @param type: the type of compress algorithm, 0 means zlib
     */
    void set_compress_type(TAIR_COMPRESS_TYPE type);

    /**
     * @brief set the compress threshold
     *
     * @param threshold: the threshold of compress, if the size of data is lower than
     *                   this value, skip the compress method. the unit is byte.
     */
    void set_compress_threshold(int threshold);
#endif

    const char *get_error_msg(int ret);

    /*** Following interface is specially useful for multi-cluster (FastDump, etc.). ***/
    /**
     ** @brief batch put data to tair
     ** @ NOTE: This interface is specially for dump data to server
     **         in some specific condition(FastDump etc.).
     **         Do NOT use it only when you konw what you're doing.
     **
     ** @param area     namespace
     ** @param kvs      key && value
     ** @param fail_request fail_keys
     ** @param compress whether compress data
     **
     ** @return 0 -- success, otherwise fail,you can use get_error_msg(ret) to get more information.
     **/

    int mput(int area,
             const tair_client_kv_map &kvs,
             int &fail_request,
             bool compress = true);

private:
    //@override
    void configOnChanged(const char *dataId, const char *groupId, const char *newContent);

private:
    diamond_manager *diamond_client;
    cluster_controller controller;
    int tair_info_check_interval_second;
};
}

#endif
