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

#ifndef TAIR_PACKET_FACTORY_H
#define TAIR_PACKET_FACTORY_H

#include "base_packet.hpp"
#include <easy_io.h>
#include <easy_io_struct.h>
#include <easy_string.h>
#include <databuffer.hpp>

namespace tair {
class packet_factory {
public:
    packet_factory() {}

    ~packet_factory() {}

public:
    static void *decode_cb(easy_message_t *m);

    static int encode_cb(easy_request_t *r, void *packet);

    static uint64_t get_packet_id_cb(easy_connection_t *c, void *packet);

    static base_packet *create_packet(int pcode);

    static base_packet *create_dup_packet(base_packet *ipacket);

    static base_packet *create_response(const base_packet *p);

    static easy_atomic32_t chid;
};
}
#endif
