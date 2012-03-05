/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * invalid packet
 *
 * Version: $Id: get_hidden_packet.hpp 28 2011-09-05 05:18:09Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_FORCE_GET_PACKET_HPP
#define TAIR_PACKET_FORCE_GET_PACKET_HPP
#include "get_packet.hpp"
namespace tair {
    class request_get_hidden: public request_get {
    public:
        request_get_hidden(): request_get() {
            setPCode(TAIR_REQ_GET_HIDDEN_PACKET);
        }

        request_get_hidden(request_get_hidden &packet): request_get(packet) {
            setPCode(TAIR_REQ_GET_HIDDEN_PACKET);
        }

        bool encode(tbnet::DataBuffer *output) {
            if (request_get::encode(output) == false) {
                return false;
            }
            return true;
        }

        bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
            if (request_get::decode(input, header) == false) {
                return false;
            }
            return true;
        }
    };
}
#endif
