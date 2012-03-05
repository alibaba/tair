/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * invalid packet
 *
 * Version: $Id: hide_by_proxy_packet.hpp 28 2011-09-05 05:18:09Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_INVAL_HIDE_PACKET_HPP
#define TAIR_PACKET_INVAL_HIDE_PACKET_HPP
#include "get_packet.hpp"
namespace tair {
    class request_hide_by_proxy: public request_invalid {
    public:
        request_hide_by_proxy(): request_invalid() {
            setPCode(TAIR_REQ_HIDE_BY_PROXY_PACKET);
        }
        request_hide_by_proxy(request_hide_by_proxy &packet): request_invalid(packet) {
            setPCode(TAIR_REQ_HIDE_BY_PROXY_PACKET);
        }

        bool encode(tbnet::DataBuffer *output) {
            if (request_invalid::encode(output) == false) {
                return false;
            }
            return true;
        }

        bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
            if (request_invalid::decode(input, header) == false) {
                return false;
            }
            return true;
        }
    };
}
#endif
