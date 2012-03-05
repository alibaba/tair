
#ifndef SERVER_PAIR_ID_H
#define SERVER_PAIR_ID_H

#include <unistd.h>
#include <signal.h>
#include <string>
#include <vector>

#include <tbsys.h>
#include <tbnet.h>


namespace tair {
    class ServerPairId {
    public:
        ServerPairId() {
            id1 = id2 = 0;
            loaded = false;
        }

        void addServerId(uint64_t id) {
            if (id1 == 0) {
                id1 = id;
            } else if (id2 == 0) {
                id2 = id;
            }
        }

    public:
        uint64_t id1;
        uint64_t id2;
        std::vector<std::string> group_name_list;
        bool loaded;
    };
}
#endif
