/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * invalid packet
 *
 * Version: $Id: invalid_packet.hpp 28 2011-09-05 05:18:09Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef PREFIX_INVALIDS_PACKET_HPP
#define PREFIX_INVALIDS_PACKET_HPP

#include "invalid_packet.hpp"
namespace tair {
  class request_prefix_invalids : public request_invalid {
  public:
    request_prefix_invalids() {
      setPCode(TAIR_REQ_PREFIX_INVALIDS_PACKET);
    }

    request_prefix_invalids(request_prefix_invalids &packet) : request_invalid(packet) {
      setPCode(TAIR_REQ_PREFIX_INVALIDS_PACKET);
    }
  };
}

#endif
