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

#include "cluster_controller.hpp"
#include <cJSON.h>

#include <stdlib.h>
#include <time.h>

namespace tair {
cluster_controller::cluster_controller() : timeout_ms(TAIR_DEFAULT_TIMEOUT),
                                           client_vector_rwlock(tbsys::WRITE_PRIORITY) {
    srand((unsigned) time(NULL));
    // at the beginning, put a uninitialized tair_client_api to all_cluster,
    // in case of that process core if tair::tair_mc_client_api::startup() return false and put()、get() is invoked.
    all_cluster.reset(new client_vector());
    read_cluster.reset(new client_vector());
    write_cluster.reset(new client_vector());
    tair_client_wrapper_sptr tair_client_non_init(new tair_client_wrapper());
    tair_client_non_init->read_weight = 1;
    tair_client_non_init->write_weight = 1;
    all_cluster->push_back(tair_client_non_init);
    read_cluster->push_back(tair_client_non_init);
    write_cluster->push_back(tair_client_non_init);
}

cluster_controller::~cluster_controller() {
}

bool cluster_controller::extract_cluster_config(cJSON *root, cluster_info_vector &cluster_config) {
    // clusters
    cJSON *clusters = cJSON_GetObjectItem(root, "clusters");
    if (NULL == clusters || clusters->type != cJSON_Array) {
        log_error("diamond config info format is invalid, json node \"clusters\" type should be arrayValue");
        return false;
    }
    int32_t clusters_num = (int32_t) cJSON_GetArraySize(clusters);
    for (int32_t i = 0; i < clusters_num; ++i) {
        cluster_info_sptr one_cluster_config(new tair_cluster_info);
        // one cluster info
        cJSON *one_cluster = cJSON_GetArrayItem(clusters, i);
        if (one_cluster->type != cJSON_Object) {
            log_error("diamond config info format is invalid, "
                    "the array element of \"clusters\" type should be objectValue");
            return false;
        }

        // address
        cJSON *address = cJSON_GetObjectItem(one_cluster, "address");
        if (NULL == address || address->type != cJSON_Object) {
            log_error("diamond information miss tair configserver address");
            return false;
        }

        // master
        cJSON *master = cJSON_GetObjectItem(address, "master");
        if (NULL == master || NULL == master->valuestring) {
            log_error("diamond information miss tair configserver master address");
            return false;
        }
        one_cluster_config->master = master->valuestring;

        // slave, it is optional
        cJSON *slave = cJSON_GetObjectItem(address, "slave");
        if (NULL != slave && NULL != slave->valuestring)
            one_cluster_config->slave = slave->valuestring;
        else
            log_warn("diamond information has no tair configserver slave address");

        // group
        cJSON *group = cJSON_GetObjectItem(address, "group");
        if (NULL == group || NULL == group->valuestring) {
            log_error("diamond information miss tair configserver group address");
            return false;
        }
        one_cluster_config->group = group->valuestring;

        // namespace_offset
        cJSON *namespace_offset_json = cJSON_GetObjectItem(address, "namespaceOffset");
        if (NULL != namespace_offset_json && NULL != namespace_offset_json->valuestring)
            one_cluster_config->namespace_offset = atoi(namespace_offset_json->valuestring);
        else
            log_warn("diamond information has no namespace_offset");
        if ((one_cluster_config->namespace_offset < 0) ||
            (one_cluster_config->namespace_offset >= TAIR_MAX_AREA_COUNT)) {
            log_error("diamond config info namespaceOffset %d out of range",
                      one_cluster_config->namespace_offset);
            return false;
        }

        // readWeight infomation
        cJSON *readWeight = cJSON_GetObjectItem(one_cluster, "readWeight"); //readWeight
        if (NULL == readWeight || NULL == readWeight->valuestring) {
            log_error("diamond config info miss readWeight");
            return false;
        }
        one_cluster_config->read_weight = atoi(readWeight->valuestring);
        if ((one_cluster_config->read_weight < 0) || (one_cluster_config->read_weight > MAX_WEIGHT)) {
            log_error("diamond config info readWeight invalid value, < 0 or > %d", MAX_WEIGHT);
            return false;
        }

        //writeWeight information
        cJSON *writeWeight = cJSON_GetObjectItem(one_cluster, "writeWeight");
        if (NULL == writeWeight || NULL == writeWeight->valuestring) {
            log_error("diamond config info miss writeWeight");
            return false;
        }
        one_cluster_config->write_weight = atoi(writeWeight->valuestring);
        if ((one_cluster_config->write_weight < 0) || (one_cluster_config->write_weight > MAX_WEIGHT)) {
            log_error("diamond config info writeWeight invalid value, < 0 or > %d", MAX_WEIGHT);
            return false;
        }

        cluster_config.push_back(one_cluster_config);
    }
    return true;
}

bool cluster_controller::extract_localcache_config(cJSON *root, localcache_info_vector &localcache_config) {
    cJSON *localcache = cJSON_GetObjectItem(root, "localcache");
    // localcache is optional, no is ok
    if (localcache != NULL) {
        if (localcache->type != cJSON_Array) {
            log_error("diamond config info format is invalid, json node \"localcache\" type should be arrayValue");
            return false;
        }
        int32_t localcache_num = cJSON_GetArraySize(localcache);
        for (int32_t i = 0; i < localcache_num; ++i) {
            cJSON *one_localcache = cJSON_GetArrayItem(localcache, i);
            if (one_localcache->type != cJSON_Object) {
                log_error("diamond config info format is invalid, "
                        "the array element of \"localcache\" type should be objectValue");
                return false;
            }
            localcache_info_sptr one_localcache_config(new localcache_info);
            cJSON *ns = cJSON_GetObjectItem(one_localcache, "ns");
            if (NULL == ns || NULL == ns->valuestring) {
                log_error("diamond config info format is invalid, localcache miss \"ns\" info");
                return false;
            }
            one_localcache_config->ns = atoi(ns->valuestring);
            if (one_localcache_config->ns < 0 || one_localcache_config->ns > TAIR_MAX_AREA_COUNT) {
                log_error("diamond config info localcache \"ns\" value is invalid, < 0 or > %d",
                          TAIR_MAX_AREA_COUNT);
                return false;
            }

            cJSON *cap = cJSON_GetObjectItem(one_localcache, "cap");
            if (NULL == cap || NULL == cap->valuestring) {
                log_error("diamond config info format is invalid, localcache miss \"cap\" info");
                return false;
            }
            one_localcache_config->cap = atoi(cap->valuestring);
            if (one_localcache_config->cap <= 0) {
                log_error("diamond config info localcache \"cap\" value is invalid, <= 0");
                return false;
            }

            cJSON *ttl = cJSON_GetObjectItem(one_localcache, "ttl");
            if (NULL == ttl || NULL == ttl->valuestring) {
                log_error("diamond config info format is invalid, localcache miss \"ttl\" info");
                return false;
            }
            one_localcache_config->ttl = atoi(ttl->valuestring);
            if (one_localcache_config->ttl <= 0) {
                log_error("diamond config info localcache \"ttl\" value is invalid, <= 0");
                return false;
            }

            localcache_config.push_back(one_localcache_config);
        }
    }
    return true;
}

bool cluster_controller::update_config(const string &json_str) {
    cJSON *root = NULL;
    root = cJSON_Parse(json_str.c_str());
    if (NULL == root) {
        log_error("parse json info failed, at:\n %s\n", cJSON_GetErrorPtr());
        return false;
    }
    cluster_info_vector cluster_config;
    localcache_info_vector localcache_config;
    bool parse_ok = true;
    //parse cluster config
    if (root->type != cJSON_Object) {
        log_error("diamond config info format is invalid, json node \"root\" type should be objectValue");
        parse_ok = false;
    }
    if (parse_ok)
        parse_ok = extract_cluster_config(root, cluster_config);
    if (parse_ok)
        parse_ok = extract_localcache_config(root, localcache_config);
    // free the memory
    cJSON_Delete(root);

    // update controller's tair configserver
    if (parse_ok)
        parse_ok = update_config_cluster(cluster_config);
    //update controller's client localcache config
    if (parse_ok)
        update_config_localcache(localcache_config);
    //parse localecache config
    return parse_ok;
}

void cluster_controller::close() {
    client_vector_sptr temp_all_cluster;
    get_all_cluster_vector(temp_all_cluster);

    for (client_vector::iterator it = temp_all_cluster->begin(); it != temp_all_cluster->end(); ++it)
        (*it)->delegate.close();
}

#ifdef WITH_COMPRESS
void cluster_controller::set_compress_type(TAIR_COMPRESS_TYPE type)
{
  tair_client_api::set_compress_type(type);
}

void cluster_controller::set_compress_threshold(int threshold)
{
  tair_client_api::set_compress_threshold(threshold);
}
#endif

void cluster_controller::setup_cache(int area, size_t capacity) {
    client_vector_sptr temp_all_cluster;
    get_all_cluster_vector(temp_all_cluster);

    for (client_vector::iterator it = temp_all_cluster->begin(); it != temp_all_cluster->end(); ++it) {
        int32_t new_area = area + (*it)->namespace_offset;
        (*it)->delegate.setup_cache(new_area, capacity);
    }
}

void cluster_controller::setup_cache(int area, size_t capacity, uint64_t expire_time) {
    client_vector_sptr temp_all_cluster;
    get_all_cluster_vector(temp_all_cluster);

    for (client_vector::iterator it = temp_all_cluster->begin(); it != temp_all_cluster->end(); ++it) {
        int32_t new_area = area + (*it)->namespace_offset;
        (*it)->delegate.setup_cache(new_area, capacity, expire_time);
    }
}

void cluster_controller::set_timeout_ms(int a_timeout_ms) {
    timeout_ms = a_timeout_ms;
    client_vector_sptr temp_all_cluster;
    get_all_cluster_vector(temp_all_cluster);

    for (client_vector::iterator it = temp_all_cluster->begin(); it != temp_all_cluster->end(); ++it)
        (*it)->delegate.set_timeout(timeout_ms);
}

void cluster_controller::set_randread(bool rand_flag) {
    client_vector_sptr temp_read_cluster;
    get_read_cluster_vector(temp_read_cluster);

    if (!temp_read_cluster.get())
        return;
    for (client_vector::iterator it = temp_read_cluster->begin(); it != temp_read_cluster->end(); ++it)
        (*it)->delegate.set_randread(rand_flag);
}

// before invoked, it can be sured that read_cluster != NULL, and read_cluster is not empty.
tair_client_wrapper_sptr cluster_controller::choose_client_wrapper_for_read() {
    client_vector_sptr temp_read_cluster;
    get_read_cluster_vector(temp_read_cluster);

    // most is single cluster
    if (LIKELY(temp_read_cluster->size() == 1))
        return (*temp_read_cluster)[0];
    int sum_read_weight = (temp_read_cluster->back())->read_weight_interval;
    int random = rand() % sum_read_weight;
    client_vector::const_iterator it;
    for (it = temp_read_cluster->begin(); it != temp_read_cluster->end(); ++it)
        if (random < (*it)->read_weight_interval)
            return *it;

    /**flow must not come here**/
    log_error("There is fatal error, there is no tair cluster to read key/value");
    return (*temp_read_cluster)[0];
}

struct great_compare_read {
    bool operator()(const tair_client_wrapper_sptr &a, const tair_client_wrapper_sptr &b) {
        return a->read_weight > b->read_weight;
    }
} sort_read;

struct great_compare_write {
    bool operator()(const tair_client_wrapper_sptr &a, const tair_client_wrapper_sptr &b) {
        return a->write_weight > b->write_weight;
    }
} sort_write;

bool cluster_controller::update_config_cluster(const cluster_info_vector &cluster_config) {
    client_vector_sptr temp_all_cluster(new client_vector());
    client_vector_sptr temp_read_cluster(new client_vector());
    client_vector_sptr temp_write_cluster(new client_vector());
    for (cluster_info_vector::const_iterator it = cluster_config.begin(); it != cluster_config.end(); ++it) {
        tair_client_wrapper_sptr tair_client(new tair_client_wrapper());
        tair_client->delegate.set_timeout(timeout_ms);
        if (!tair_client->delegate.startup((*it)->master.c_str(), (*it)->slave.c_str(), (*it)->group.c_str())) {
            log_error("connect to config server failed, master=%s, slave=%s, group=%s",
                      (*it)->master.c_str(), (*it)->slave.c_str(), (*it)->group.c_str());
            return false;
            break;
        }

        tair_client->master = (*it)->master;
        tair_client->slave = (*it)->slave;
        tair_client->group = (*it)->group;
        tair_client->read_weight_interval = (*it)->read_weight;
        tair_client->read_weight = (*it)->read_weight;
        tair_client->write_weight = (*it)->write_weight;
        tair_client->namespace_offset = (*it)->namespace_offset;
        if (tair_client->read_weight > 0)
            temp_read_cluster->push_back(tair_client);
        if (tair_client->write_weight > 0)
            temp_write_cluster->push_back(tair_client);
        temp_all_cluster->push_back(tair_client);
    }// end of for

    if (temp_read_cluster->empty()) {
        log_error("There is no tair cluster to read");
        return false;
    }
    if (temp_write_cluster->empty()) {
        log_error("There is no tair cluster to write");
        return false;
    }

    sort(temp_read_cluster->begin(), temp_read_cluster->end(), sort_read);
    for (unsigned i = 1; i < temp_read_cluster->size(); ++i)
        (*temp_read_cluster)[i]->read_weight_interval += (*temp_read_cluster)[i - 1]->read_weight_interval;
    sort(temp_write_cluster->begin(), temp_write_cluster->end(), sort_write);

    client_vector_rwlock.wrlock();
    // use swap to avoid delete between lock
    read_cluster.swap(temp_read_cluster);
    write_cluster.swap(temp_write_cluster);
    all_cluster.swap(temp_all_cluster);
    client_vector_rwlock.unlock();

    client_vector_sptr temp_all_cluster_for_print;
    get_all_cluster_vector(temp_all_cluster_for_print);
    stringstream log_info;
    log_info << "the tair clusters is loaded successfully, include:" << endl;
    for (client_vector::const_iterator it = temp_all_cluster_for_print->begin();
         it != temp_all_cluster_for_print->end(); ++it) {
        log_info << "master: " << (*it)->master << ", slave: " << (*it)->slave << ", group: " << (*it)->group
                 << ", readWeight: " << (*it)->read_weight << ", writeWeight: " << (*it)->write_weight
                 << ", namespaceOffset: " << (*it)->namespace_offset
                 << endl;
    }
    log_warn("%s", log_info.str().c_str());
    return true;
}

void cluster_controller::update_config_localcache(const localcache_info_vector &localcache_config) {
    if (localcache_config.empty())
        return;

    client_vector_sptr temp_all_cluster;
    get_all_cluster_vector(temp_all_cluster);

    for (client_vector::const_iterator it = temp_all_cluster->begin(); it != temp_all_cluster->end(); ++it) {
        for (localcache_info_vector::const_iterator config = localcache_config.begin();
             config != localcache_config.end(); ++config) {
            (*it)->delegate.setup_cache((*config)->ns, (*config)->cap, (*config)->ttl);
        }
    }

    stringstream log_info;
    log_info << "tair local cache is set: " << endl;
    for (localcache_info_vector::const_iterator config = localcache_config.begin();
         config != localcache_config.end(); ++config)
        log_info << "ns: " << (*config)->ns << ", cap: " << (*config)->cap << ", ttl: " << (*config)->ttl << endl;
    log_warn("%s", log_info.str().c_str());
}

}
