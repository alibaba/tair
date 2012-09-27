/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_client_proxy.hpp 1050 2012-09-26 08:40:35Z mobing $
 *
 * Authors:
 *   MaoQi <fengmao.pj@taobao.com>
 *
 */
#ifndef TAIR_CLIENT_PROXY_API
#define TAIR_CLIENT_PROXY_API
#include"tair_client_api.hpp"
class tair_client_impl;
using namespace std;
using namespace tair::common;
class tair_client_proxy : public tair::tair_client_api
{
public:
  /**
   *       * debug support, query information of runtime.
   */
  int debug_support(uint64_t invalid_server, std::vector<std::string> &infos);
};
#endif
