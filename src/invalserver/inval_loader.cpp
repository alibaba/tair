
#include "inval_loader.hpp"

namespace tair {
    InvalLoader::InvalLoader() {
        loading = true;
    }

    InvalLoader::~InvalLoader() {
        for (size_t i = 0; i < client_list.size(); ++i) {
            if (client_list[i] != NULL) {
                delete client_list[i];
                client_list[i] = NULL;
            }
        }
    }

    std::vector<tair_client_api*>* InvalLoader::get_client_list(const char *groupname) {
        if (groupname == NULL) {
            log_error("groupname is NULL!");
            return NULL;
        }
        if (loading) {
            return NULL;
        }
        CLIENT_HELPER_MAP::iterator it = client_helper_map.find(groupname);
        if ( it == client_helper_map.end()) {
            return NULL;
        }

        return &(it->second);
    }

    void InvalLoader::run(tbsys::CThread *thread, void *arg) {
        load_group_name();
        log_info("load groupname complete.");

        const char *pkey = "invalid_server_counter";
        data_entry key(pkey, strlen(pkey), false);
        int value;

        while (!_stop) {
            for (size_t i = 0; i < client_list.size(); ++i) {
                client_list[i]->add_count(0, key, 1, &value);
                log_debug("client %d, 'invalid_server_counter': %d.", i, value);
                if (_stop) {
                    break;
                }
            }
            log_debug("KeepAlive: client count: %d", client_list.size());
            TAIR_SLEEP(_stop, 5);
        }
    }

    void InvalLoader::stop() {
        _stop = true;
        for (size_t i = 0; i < client_list.size(); ++i) {
            client_list[i]->close();
        }
    }

    void InvalLoader::load_group_name() {
        log_info("start loading groupnames.");
        int defaultPort = TBSYS_CONFIG.getInt(CONFSERVER_SECTION,
                TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
        std::vector<std::string> keylist;
        TBSYS_CONFIG.getSectionKey(INVALSERVER_SECTION, keylist);
        SERVER_PAIRID_MAP spmap;
        SERVER_PAIRID_MAP::iterator it;
        for (size_t i = 0; i < keylist.size(); ++i) {
            const char *key = keylist[i].c_str();
            if (key == NULL || strncmp(key, "config_server", 13) != 0) {
                continue;
            }
            std::vector<const char*> strList = TBSYS_CONFIG.getStringList(INVALSERVER_SECTION, key);
            for (size_t j = 0; j < strList.size(); ++j) {
                uint64_t id = tbsys::CNetUtil::strToAddr(strList[j], defaultPort);
                if (id == 0) {
                    continue;
                }
                it = spmap.find(key);
                if (it == spmap.end()) {
                    ServerPairId *spid = new ServerPairId();
                    spid->addServerId(id);
                    spmap.insert(SERVER_PAIRID_MAP::value_type(key, spid));
                } else {
                    it->second->addServerId(id);
                }
                log_info("add configserver %s:%s.", key, strList[j]);
            }
        }

        int done = 0;
        while (done < 5 && !_stop) {
            int ret = EXIT_SUCCESS;
            for (it = spmap.begin(); it != spmap.end(); ++it) {
                ServerPairId *spid = it->second;
                if (spid->loaded == false) {
                    log_info("load group %s %s %s",
                            it->first.c_str(),
                            tbsys::CNetUtil::addrToString(spid->id1).c_str(),
                            tbsys::CNetUtil::addrToString(spid->id2).c_str());
                    tair_client_api client;
                    if (client.get_group_name_list(spid->id1, spid->id2, spid->group_name_list) == false) {
                        log_error("error when fetch group name from %s.",
                                tbsys::CNetUtil::addrToString(spid->id1).c_str());
                        ret = EXIT_FAILURE;
                        continue;
                    }
                    spid->loaded = true;
                }

                for (size_t i = 0; i < spid->group_name_list.size(); ++i) {
                    if (spid->group_name_list[i].size() == 0)
                        continue;
                    tair_client_api * client = new tair_client_api();
                    if (client->startup(tbsys::CNetUtil::addrToString(spid->id1).c_str(),
                                tbsys::CNetUtil::addrToString(spid->id2).c_str(),
                                (spid->group_name_list[i]).c_str()) == false) {
                        log_error("cannot connect to configserver %s.",
                                tbsys::CNetUtil::addrToString(spid->id1).c_str());
                        ret = EXIT_FAILURE;
                        delete client;
                        continue;
                    }
                    log_info("connected: %s => %s.",
                            tbsys::CNetUtil::addrToString(spid->id1).c_str(),
                            spid->group_name_list[i].c_str());
                    int timeout = TBSYS_CONFIG.getInt("invalserver", "client_timeout", 1000);
                    log_info("timeout of clients set to %dms.", timeout);
                    client->set_timeout(timeout);
                    client_list.push_back(client);
                    client_helper_map[spid->group_name_list[i].c_str()].push_back(client);
                    spid->group_name_list[i] = "";
                }
            }
            if (ret == EXIT_SUCCESS) {
                break;
            } else if (_stop == false) {
                TAIR_SLEEP(_stop, 1);
            }
            ++done;
        }
        for (it = spmap.begin(); it != spmap.end(); ++it) {
            delete it->second;
        }
        loading = false;
        log_info("load complete, group count: %d, client count: %d.",
                client_helper_map.size(), client_list.size());
    }
}
