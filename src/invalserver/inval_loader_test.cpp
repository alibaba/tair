#include "tair_client_api_impl.hpp"
#include "inval_loader_test.hpp"
#include <algorithm>
#include <functional>
#include <cstdlib>
#include <iostream>
#include <string>
using namespace std;

namespace tair {
  inval_loader_test::inval_loader_test(bool this_failed_abtain_group_names, bool this_failed_startup)
    : failed_abtain_group_names(this_failed_abtain_group_names),
    failed_startup(this_failed_startup),tair::InvalLoader()
  {
  }
  inval_loader_test::~inval_loader_test()
  {
  }

  void inval_loader_test::retrieve_group_names()
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
              printf("got the group names, master: %s, slave: %s, group name: %s\n",
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
        it++;
      }
    }
  }
  void inval_loader_test::build_connections()
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
              printf("connected successfully to the group, master: %s, slave: %s, group name: %s\n",
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
  void inval_loader_test::run(tbsys::CThread *thread, void *arg) {
    load_group_name();
    cout<<"load groupname complete.";

    const char *pkey = "invalid_server_counter";
    data_entry key(pkey, strlen(pkey), false);
    int value;

    while (!_stop) {
      if (cluster_without_groupnames.size() > 0) {
        cout<<"retrieve_group_names"<<endl;
        retrieve_group_names();
      }
      if (disconnected_client_map.size() > 0) {
        cout<<"build_connections"<<endl;
        build_connections();
      }
      cout<<"while"<<endl;
      for (int i = 0; i < client_list.size(); i++) {
        client_list[i]->add_count(0, key, 1, &value);
        printf("client: %d, 'invalid_server_counter': %d\n", i, value);
        if (_stop) {
          break;
        }
      }
      cout<<"KeepAlive client count: "<< client_list.size();
      TAIR_SLEEP(_stop, 10);
    }
  }
  void inval_loader_test::load_group_name() {
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
          if (client.get_group_name_list(spid->id1, spid->id2, spid->group_name_list) == false
              || failed_abtain_group_names == true) {
            log_error("exception, when fetch group name from %s.",
                tbsys::CNetUtil::addrToString(spid->id1).c_str());
            //save <master, slave>
            cluster_without_groupnames[spid->id1] =  spid->id2;
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
                (spid->group_name_list[i]).c_str()) == false
              || failed_startup == true) {
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
      printf("failed to get group names from cluster, master: %s, slave: %s\n",
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
          printf("failed to connected to the cluster, master: %s, slave: %s, group name: %s\n",
              tbsys::CNetUtil::addrToString(gc->master).c_str(),
              tbsys::CNetUtil::addrToString(gc->slave).c_str(),
              gc->group_name.c_str());
        }
      }
    }
  }
}
