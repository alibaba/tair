/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * request dispatcher header
 *
 * Authors:
 *   YeXiang <yexiang.ych@taobao.com>
 *     - initial release
 *
 */
#include "tair_manager.hpp"
#include "stat_manager.h"
#include "stat_cmd_packet.hpp"
#include "flow_control_packet.hpp"
#include "flow_view.hpp"
#include "flow_controller.h"


namespace tair 
{

class stat_processor
{
 public:
  stat_processor(tair_manager *tair_mgr, 
      tair::stat::StatManager  *stat_mgr,
      tair::stat::FlowController *flow_ctrl);

  virtual ~stat_processor() {}
  // view stat
  uint32_t process(stat_cmd_view_packet *request);
  // view flow stat
  uint32_t process(flow_view_request *request);
  // set flow limit
  uint32_t process(flow_control_set *request);
  uint32_t process(flow_check *request);

 private:
  uint32_t send_response(base_packet *request, base_packet *response, bool control_packet = false);

 private:
  tair_manager                *tair_mgr;
  tair::stat::StatManager     *stat_mgr;
  tair::stat::FlowController  *flow_ctrl;
};

}

