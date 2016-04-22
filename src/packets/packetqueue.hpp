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

#ifndef TAIR_PACKET_QUEUE_HPP
#define TAIR_PACKET_QUEUE_HPP

#include "packet.hpp"

namespace tair {
class PacketQueue {
public:
    PacketQueue();

    ~PacketQueue();

    Packet *pop();

    void clear();

    void push(Packet *packet);

    int size();

    bool empty();

    void move_to(PacketQueue *dest_queue);

    Packet *get_packet_list();

    Packet *head() {
        return _head;
    }

    Packet *tail() {
        return _tail;
    }

protected:
    Packet *_head;
    Packet *_tail;
    int _size;
};
}
#endif

