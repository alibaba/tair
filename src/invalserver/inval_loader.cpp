#include "tair_client_api_impl.hpp"
#include "inval_loader.hpp"
#include <algorithm>
#include <functional>
#include <cstdlib>

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
  void InvalLoader::retrieve_group_names()
  {
    //re-connect group
    uint64_t master = 0;
    uint64_t slave = 0;
    log_debug("do_check_client, cluster_without_groupnames size: %d", cluster_without_groupnames.size());
    vector<std::string> group_names;
    for (cluster_info_map::iterator it = cluster_without_groupnames.begin();
        it != cluster_without_groupnames.end();) {
      master = it->first;
      slave = it->second;
      if (master != 0) {
        group_names.clear();
        tair_client_impl client;
        if (client.get_group_name_list(master, slave, group_names) == true) {
          for (int i = 0; i < group_names.size(); i++) {
            if (group_names[i].size() > 0) {
              disconnected_client_map_insert(master, slave, group_names[i]);
              log_debug("got the group names, master: %s, slave: %s, group name: %s",
                  tbsys::CNetUtil::addrToString(master).c_str(),
                  tbsys::CNetUtil::addrToString(slave).c_str(),
                  group_names[i].c_str());
            }
          }
          // remove the cluster info from the cluster_without_groupnames.
          cluster_without_groupnames.erase(it++);
        }
        else {
          it++;
        }
      }
      else {
        // remove master == 0
        cluster_without_groupnames.erase(it++);
      }
    }
  }
  void InvalLoader::build_connections()
  {
    //re startup client
    uint64_t master = 0;
    uint64_t slave = 0;
    int timeout = TBSYS_CONFIG.getInt("invalserver", "client_timeout", 1000);
    log_debug("disconnected_client_map size: %d", disconnected_client_map.size());
    std::string group_name;
    for (group_client_map::iterator it = disconnected_client_map.begin();
        it != disconnected_client_map.end(); it++) {
      group_info_map *gi = it->second;
      if (gi != NULL) {
        for (group_info_map::iterator i = gi->begin(); i != gi->end(); i++) {
          group_client_info *gc = i->second;
          if (gc != NULL) {
            master = gc->master;
            slave = gc->slave;
            group_name = gc->group_name;
            //restartup
            tair_client_api * client = new tair_client_api();
            if (client->startup(tbsys::CNetUtil::addrToString(master).c_str(),
                  tbsys::CNetUtil::addrToString(slave).c_str(),
                  group_name.c_str()) == true) {
              //remove from the disconnected_client_map
              log_debug("connected successfully to the group, master: %s, slave: %s, group name: %s",
                  tbsys::CNetUtil::addrToString(master).c_str(),
                  tbsys::CNetUtil::addrToString(slave).c_str(),
                  group_name.c_str());
              disconnected_client_map_markup(master, group_name);
              //add instance tair_client_api to list.
              log_info("timeout of clients set to %dms.", timeout);
              client->set_timeout(timeout);
              client_list.push_back(client);
              client_helper_map[group_name.c_str()].push_back(client);
            }
            else {
              log_error("failed to connect the group, master: %s, slave: %s, group name: %s",
                  tbsys::CNetUtil::addrToString(master).c_str(),
                  tbsys::CNetUtil::addrToString(slave).c_str(),
                  group_name.c_str());
              delete client;
              client = NULL;
            }
          }
          else {
            log_error("[FATAL ERROR], group_client_info is null");
          }
        }
      }
      else { //gi == NULL, group_info_map is null
        log_error("[FATAL ERROR], group_info_map is null, master: %s",
            tbsys::CNetUtil::addrToString(master).c_str());
      }
    }
    disconnected_client_map_remove();
  }
  void InvalLoader::run(tbsys::CThread *thread, void *arg) {
    load_group_name();
    log_info("load groupname complete.");

    const char *pkey = "invalid_server_counter";
    data_entry key(pkey, strlen(pkey), false);
    int value;

    while (!_stop) {
      if (cluster_without_groupnames.size() > 0) {
        retrieve_group_names();
      }
      if (disconnected_client_map.size() > 0) {
        build_connections();
      }
      for (int i = 0; i < client_list.size(); i++) {
        client_list[i]->add_count(0, key, 1, &value);
        log_debug("client: %d, 'invalid_server_counter': %d", i, value);
        if (_stop) {
          break;
        }
      }
      log_info("KeepAlive client count: %d", client_list.size());
      TAIR_SLEEP(_stop, 10);
    }
  }

  void InvalLoader::stop() {
    _stop = true;
    for (size_t i = 0; i < client_list.size(); ++i) {
      client_list[i]->close();
    }
  }
  void InvalLoader::disconnected_client_map_insert(const uint64_t &master,
      const uint64_t &slave, const std::string &group_name) {
    group_client_map::iterator it = disconnected_client_map.find(master);
    if (it != disconnected_client_map.end()) { // exist
      group_info_map *gi = it->second;
      if (gi != NULL) {
        group_info_map::iterator i = gi->find(group_name);
        if (i != gi->end()) { // exist the group name, only update
          (*(disconnected_client_map[master]))[group_name]->master = master;
          (*(disconnected_client_map[master]))[group_name]->slave = slave;
          (*(disconnected_client_map[master]))[group_name]->group_name = group_name;
          (*(disconnected_client_map[master]))[group_name]->removed = false;
        }
        else {
          group_client_info * gc = new group_client_info();
          gc->master = master;
          gc->slave = slave;
          gc->group_name = group_name;
          gc->removed = false;
          gi->insert(group_info_map::value_type(group_name, gc));
        }
      }
    }
    else {
      group_info_map *gi = new group_info_map();
      group_client_info * gc = new group_client_info();
      gc->master = master;
      gc->slave = slave;
      gc->group_name = group_name;
      gc->removed = false;
      gi->insert(group_info_map::value_type(group_name, gc));
      disconnected_client_map[master] = gi;
    }
    log_debug("insert disconnected client maps, master: %s, slave: %s, group name: %s",
        tbsys::CNetUtil::addrToString(master).c_str(),
        tbsys::CNetUtil::addrToString(slave).c_str(),
        group_name.c_str());
  }
  void InvalLoader::disconnected_client_map_markup(const uint64_t &master, const std::string &group_name) {
    if (disconnected_client_map.find(master) != disconnected_client_map.end()) {
      group_info_map *gi = disconnected_client_map[master];
      if (gi != NULL) {
        group_info_map::iterator it = gi->find(group_name);
        if (it != gi->end()) {
          it->second->removed = true;
          log_debug("markup disconnected client maps, master: %s, group name: %s",
              tbsys::CNetUtil::addrToString(master).c_str(),
              group_name.c_str());

        }
      }
    }
  }

  void InvalLoader::disconnected_client_map_remove()
  {
    for (group_client_map::iterator it = disconnected_client_map.begin();
        it != disconnected_client_map.end();) {
      group_info_map *gi = it->second;
      if (gi != NULL) {
        for (group_info_map::iterator i = gi->begin(); i != gi->end();) {
          group_client_info *gc = i->second;
          if (gc != NULL && gc ->removed ) {
            gi->erase(i++);
            uint64_t master = gc->master;
            uint64_t slave = gc->slave;
            std::string group_name = gc->group_name;
            log_debug("remove disconnected client maps, master: %s, slave: %s, group name: %s",
                tbsys::CNetUtil::addrToString(master).c_str(),
                tbsys::CNetUtil::addrToString(slave).c_str(),
                group_name.c_str());
            delete gc;
            gc = NULL;
          }
          else {
            i++;
          }
        }
        // removed
        if (gi ->size() == 0) {
          disconnected_client_map.erase(it++);
          delete gi;
          gi = NULL;
        }
        else {
          ++it;
        }
      }
      else {
        disconnected_client_map.erase(it++);
      }
    }
  }

  void InvalLoader::load_group_name() {
    log_info("start loading groupnames.");
    int defaultPort = TBSYS_CONFIG.getInt(CONFSERVER_SECTION,
        TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
    //collect all group names
    std::vector<std::string> group_names;
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
      }
    }
    int done = 0;
    while (done < 1 && !_stop) {
      int ret = EXIT_SUCCESS;
      for (it = spmap.begin(); it != spmap.end(); ++it) {
        ServerPairId *spid = it->second;
        if (spid->loaded == false) {
          log_debug("load group %s %s %s",
              it->first.c_str(),
              tbsys::CNetUtil::addrToString(spid->id1).c_str(),
              tbsys::CNetUtil::addrToString(spid->id2).c_str());
          tair_client_impl client;
          if (client.get_group_name_list(spid->id1, spid->id2, spid->group_name_list) == false) {
            log_error("exception, when fetch group name from %s.",
                tbsys::CNetUtil::addrToString(spid->id1).c_str());
            //save <master, slave>
            cluster_without_groupnames[spid->id1] = spid->id2;
            ret = EXIT_FAILURE;
            continue;
          }
          spid->loaded = true;
        }
        for (size_t i = 0; i < spid->group_name_list.size(); ++i) {
          if (spid->group_name_list[i].size() == 0)
            continue;
          //collect all group names for INVAL_STAT_HELPER
          std::vector<std::string>::iterator it = std::find(group_names.begin(),
              group_names.end(),spid->group_name_list[i]);
          if (it == group_names.end()) {
            group_names.push_back(spid->group_name_list[i]);
          }
          tair_client_api * client = new tair_client_api();
          if (client->startup(tbsys::CNetUtil::addrToString(spid->id1).c_str(),
                tbsys::CNetUtil::addrToString(spid->id2).c_str(),
                (spid->group_name_list[i]).c_str()) == false) {
            log_error("cannot connect to configserver %s.",
                tbsys::CNetUtil::addrToString(spid->id1).c_str());
            disconnected_client_map_insert(spid->id1, spid->id2, spid->group_name_list[i]);
            ret = EXIT_FAILURE;
            delete client;
            continue;
          }
          log_debug("connected: %s => %s.",
              tbsys::CNetUtil::addrToString(spid->id1).c_str(),
              spid->group_name_list[i].c_str());
          int timeout = TBSYS_CONFIG.getInt("invalserver", "client_timeout", 1000);
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
    //set the parameters for INVAL_STAT_HELPER
    if (TAIR_INVAL_STAT.setThreadParameter(group_names) == false) {
      log_error("[FATAL ERROR] Can't set the group's name for instance of TAIR_INVAL_STAT.");
    }

    loading = false;
    log_debug("load complete, group count: %d, client count: %d.",
        client_helper_map.size(), client_list.size());
    //show  debug information
    for (cluster_info_map::iterator it = cluster_without_groupnames.begin();
        it != cluster_without_groupnames.end(); it++) {
      log_debug("failed to get group names from cluster, master: %s, slave: %s ",
          tbsys::CNetUtil::addrToString(it->first).c_str(),
          tbsys::CNetUtil::addrToString(it->second).c_str());
    }
    for (group_client_map::iterator it = disconnected_client_map.begin();
        it != disconnected_client_map.end(); it++)
    {
      if (it->second != NULL) {
        group_info_map *gi = it->second;
        for (group_info_map::iterator i = gi->begin(); i != gi->end(); i++) {
          group_client_info *gc = i->second;
          log_debug("failed to connected to the cluster, master: %s, slave: %s, group name: %s",
              tbsys::CNetUtil::addrToString(gc->master).c_str(),
              tbsys::CNetUtil::addrToString(gc->slave).c_str(),
              gc->group_name.c_str());
        }
      }
    }
  }
}
