/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * get packet
 *
 * Version: $Id: get_packet.hpp 392 2012-4-025 02:02:41Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu.hfl <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */

#ifndef PREFIX_GET_HIDDES_PACKET_HPP
#define PREFIX_GET_HIDDES_PACKET_HPP
#include "get_packet.hpp"
namespace tair {
  class request_prefix_get_hiddens : public request_get {
  public:
    request_prefix_get_hiddens() {
      setPCode(TAIR_REQ_PREFIX_GET_HIDDENS_PACKET);
    }
    request_prefix_get_hiddens(request_prefix_get_hiddens &rhs) : request_get(rhs) {
      setPCode(TAIR_REQ_PREFIX_GET_HIDDENS_PACKET);
    }
  };
}

#endif
