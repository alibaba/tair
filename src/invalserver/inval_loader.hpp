
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

namespace tair {
    class ServerPairId;
    typedef __gnu_cxx::hash_map<std::string, ServerPairId*, tbsys::str_hash> SERVER_PAIRID_MAP;
    typedef __gnu_cxx::hash_map<std::string, std::vector<tair_client_api*>, tbsys::str_hash > CLIENT_HELPER_MAP;

    class InvalLoader: public tbsys::CDefaultRunnable {
    public:
        InvalLoader();
        ~InvalLoader();

        std::vector<tair_client_api*>* get_client_list(const char *groupname);
        void run(tbsys::CThread *thread, void *arg);
        bool is_loading() const {
          return loading;
        }
        void stop();

    private:
        void load_group_name();
    private:
        bool loading;
        CLIENT_HELPER_MAP client_helper_map;
        std::vector<tair_client_api*> client_list;
    };
}

#endif
