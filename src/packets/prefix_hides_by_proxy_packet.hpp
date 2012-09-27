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
#ifndef PREFIX_HIDES_BY_PROXY_PACKET_HPP
#define PREFIX_HIDES_BY_PROXY_PACKET_HPP
#include "hide_by_proxy_packet.hpp"
namespace tair {
  class request_prefix_hides_by_proxy : public request_hide_by_proxy {
  public:
    request_prefix_hides_by_proxy() {
      setPCode(TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET);
    }

    request_prefix_hides_by_proxy(request_prefix_hides_by_proxy &packet) : request_hide_by_proxy(packet) {
      setPCode(TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET);
    }
  };
}

#endif
