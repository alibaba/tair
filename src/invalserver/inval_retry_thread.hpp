#ifndef INVAL_RETRY_THREAD_HPP
#define INVAL_RETRY_THREAD_HPP

#include <tbsys.h>
#include <tbnet.h>

#include "inval_loader.hpp"
#include "log.hpp"
#include "base_packet.hpp"
#include "invalid_packet.hpp"
#include "hide_by_proxy_packet.hpp"

#ifndef PACKET_GROUP_NAME
#define PACKET_GROUP_NAME(ipacket, hpacket) \
  ((ipacket) ? (ipacket)->group_name : (hpacket)->group_name)
#endif

namespace tair {
    class InvalRetryThread: public tbsys::CDefaultRunnable {
    public:
        InvalRetryThread();
        ~InvalRetryThread();

        void setThreadParameter(InvalLoader *loader);
        void stop();
        void run(tbsys::CThread *thread, void *arg);
        void add_packet(base_packet *packet, int index);
    private:
        static const int RETRY_COUNT = 3;
        static const int MAX_QUEUE_SIZE = 10000;
        InvalLoader *invalid_loader;
        tbsys::CThreadCond queue_cond[RETRY_COUNT];
        tbnet::PacketQueue retry_queue[RETRY_COUNT];
    };
}
#endif
