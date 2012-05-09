/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * invalid packet
 *
 * Version: $Id: hide_packet.hpp 28 2011-09-05 05:18:09Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_HIDE_PACKET_HPP
#define TAIR_PACKET_HIDE_PACKET_HPP
#include "get_packet.hpp"
namespace tair {
    class request_hide: public request_get {
    public:
        request_hide(): request_get() {
            setPCode(TAIR_REQ_HIDE_PACKET);
        }

        request_hide(request_hide &packet): request_get(packet) {
            setPCode(TAIR_REQ_HIDE_PACKET);
        }
    };
}
#endif
