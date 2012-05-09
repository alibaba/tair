/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * invalid packet
 *
 * Version: $Id: prefix_hides_packet.hpp 28 2011-09-05 05:18:09Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */

#ifndef PREFIX_HIDES_PACKET_HPP
#define PREFIX_HIDES_PACKET_HPP

namespace tair {
  class request_prefix_hides : public request_hide {
  public:
    request_prefix_hides() {
        setPCode(TAIR_REQ_PREFIX_HIDES_PACKET);
    }

    request_prefix_hides(request_prefix_hides &packet) : request_hide(packet) {
      setPCode(TAIR_REQ_PREFIX_HIDES_PACKET);
    }
  };
}

#endif
