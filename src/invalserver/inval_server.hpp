
#ifndef SERVER_PAIR_ID_H
#define SERVER_PAIR_ID_H

#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <string>
#include <ext/hash_map>

#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "base_packet.hpp"
#include "packet_factory.hpp"
#include "invalid_packet.hpp"
#include "ping_packet.hpp"
#include "util.hpp"
#include "inval_loader.hpp"
#include "inval_retry_thread.hpp"
#include "inval_async_task_thread.hpp"


namespace tair {
    class InvalServer: public tbnet::IServerAdapter, public tbnet::IPacketQueueHandler {
    public:
        InvalServer();
        ~InvalServer();

        void start();
        void stop();

        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
        bool handlePacketQueue(tbnet::Packet *packet, void *args);
    private:
        int do_invalid(request_invalid *req);
        int do_hide(request_hide_by_proxy *req);
        bool init();
        bool destroy();
    private:
        bool _stop;
        bool ignore_zero_area;

        tair_packet_factory packet_factory;
        tbnet::DefaultPacketStreamer streamer;
        tbnet::Transport transport;

        tbnet::PacketQueueThread task_queue_thread;
        InvalLoader invalid_loader;
        InvalRetryThread retry_thread;
        AsyncTaskThread async_thread;
    };
}
#endif
