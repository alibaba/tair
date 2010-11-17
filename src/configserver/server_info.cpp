/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include <inttypes.h>
#include "server_info.hpp"
#include "group_info.hpp"

namespace tair {
  namespace config_server {
    server_info::server_info()
    {
      last_time = 0;
      status = 0;
    }
    server_info::~server_info()
    {
    }
    void server_info::print()
    {
      printf("_serverId:%s\n",
             tbsys::CNetUtil::addrToString(server_id).c_str());
      printf("_lastTime:%u ", last_time);
      printf("_status:%d ", status);
      printf("_groupInfo:%p", group_info_data);
      printf("_nodeInfo:%p\n", node_info_data);
      printf("-------------------------\n");
    }
    node_info::node_info(server_info * m) {
      assert(m != NULL);
      server = m;
    }
    node_info::~node_info() {
    }
    bool node_info::operator<(const node_info & node) const
    {
      return server->server_id < node.server->server_id;
    }

  }
}
