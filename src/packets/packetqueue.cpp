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

#include "packetqueue.hpp"

namespace tair {
PacketQueue::PacketQueue() {
    _head = NULL;
    _tail = NULL;
    _size = 0;
}

PacketQueue::~PacketQueue() {
    clear();
}

Packet *PacketQueue::pop() {
    if (_head == NULL) {
        return NULL;
    }
    Packet *packet = _head;
    _head = _head->_next;
    if (_head == NULL) {
        _tail = NULL;
    }
    _size--;
    return packet;
}

void PacketQueue::clear() {
    if (_head == NULL) {
        return;
    }
    while (_head != NULL) {
        Packet *packet = _head;
        _head = packet->_next;
        packet->free();
    }
    _head = _tail = NULL;
    _size = 0;
}

void PacketQueue::push(Packet *packet) {
    if (packet == NULL) {
        log_info("packet is null.");
        return;
    }
    packet->_next = NULL;
    if (_tail == NULL) {
        _head = packet;
    } else {
        _tail->_next = packet;
    }
    _tail = packet;
    _size++;
}

int PacketQueue::size() {
    return _size;
}

bool PacketQueue::empty() {
    return (_size == 0);
}

void PacketQueue::move_to(PacketQueue *dest_queue) {
    if (_head == NULL) {
        return;
    }
    if (dest_queue->_tail == NULL) {
        dest_queue->_head = _head;
    } else {
        dest_queue->_tail->_next = _head;
    }
    dest_queue->_tail = _tail;
    dest_queue->_size += _size;
    _head = _tail = NULL;
    _size = 0;
}

Packet *PacketQueue::get_packet_list() {
    return _head;
}
}

