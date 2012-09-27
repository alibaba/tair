
#ifndef INVAL_LOADER_H
#define INVAL_LOADER_H

#include <unistd.h>
#include <vector>
#include <string>

#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "server_pair_id.hpp"
#include "tair_client_api.hpp"
#include "data_entry.hpp"
#include "log.hpp"
#include "inval_stat_helper.hpp"
namespace tair {
    class ServerPairId;
    typedef __gnu_cxx::hash_map<std::string, ServerPairId*, tbsys::str_hash> SERVER_PAIRID_MAP;
    typedef __gnu_cxx::hash_map<std::string, std::vector<tair_client_api*>, tbsys::str_hash > CLIENT_HELPER_MAP;

    class InvalLoader: public tbsys::CDefaultRunnable {
    public:
        InvalLoader();
        virtual ~InvalLoader();

        std::vector<tair_client_api*>* get_client_list(const char *groupname);
        void run(tbsys::CThread *thread, void *arg);
        bool is_loading() const {
          return loading;
        }
        void stop();

    //private:
    protected:
        void load_group_name();
        void do_check_client();
    protected:
        bool loading;
        CLIENT_HELPER_MAP client_helper_map;
        std::vector<tair_client_api*> client_list;
        std::vector<tair_client_api*> disconnected_client_list;
        //The instance of `tair_client_api will be created for every group in cluster,
        //whose master configserver ID is `master, and slave configserver ID is `slave.
        //The object of `group_client_info contains such information as: the cluster's
        //master configserver, slave configserver, group's name, and status.
        struct group_client_info {
          uint64_t master;
          uint64_t slave;
          std::string group_name;
          bool removed;
        };
        //group's name --> instance of `group_client_info.
        typedef __gnu_cxx::hash_map<std::string, group_client_info*, tbsys::str_hash > group_info_map;
        //configserver ID --> group_info_map*
        typedef __gnu_cxx::hash_map<uint64_t, group_info_map*, __gnu_cxx::hash<int> > group_client_map;
        //collect the information of ervery cluster managed by invalid server.
        typedef __gnu_cxx::hash_map<uint64_t, uint64_t, __gnu_cxx::hash<int> > cluster_info_map;
        //The interaction, that builds connection with every group in the cluster, with the cluster will
        //be parsed.
        //In the first parse, will abtain all group's name for every cluster, according to the master
        //and slave. If failed to abtain the group's name because of a network unreachable, the parameter
        //such as master, slave should be collected, and saved in `cluster_without_group_name.
        cluster_info_map cluster_without_groupnames;
        //As noted above, in the 2nd parse, will create the instance of `tair_client_api for every group
        //in the cluster, using the parameters `master, `slave, and group's name. Supposing fail to connect
        //with the cluster for the same reason, a network unreachable, the information such as master, slave
        //and group's name should be saved in the `disconnected_client_map.
        group_client_map disconnected_client_map;
        inline void disconnected_client_map_insert(const uint64_t &master, const uint64_t &slave,
            const std::string &group_name);
        //markup the item should be removed.
        inline void disconnected_client_map_markup(const uint64_t &master, const std::string &group_name);
        inline void disconnected_client_map_remove();

        //If all 2 parses described above are failed, will retry the action that retrieving the group's names
        //for every cluster, and building the connections with the cluster's group.
        void retrieve_group_names();
        void build_connections();
    };
}

#endif
