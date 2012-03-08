/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Authors:
 *   xinshu<xinshu.wzx@taobao.com>
 *
 */
#ifndef TAIR_PACKET_REQUEST_SYNC_H
#define TAIR_PACKET_REQUEST_SYNC_H


#include <tbsys.h>
#include <tbnet.h>

#include "data_entry.hpp"


namespace tair {

  class request_sync
  {
    public:
      ~request_sync()
      {
      }
      request_sync()
      {
        failed=0;
      }
	  /**
      request_sync(int _area,const tair::common::data_entry& _key)
      {
        area=_area;
        key.clone(_key);
        failed=0;
      }
	  **/
      request_sync(int _action,int _area, const tair::common::data_entry * _pkey)
      {
        action=_action;
        area=_area;
        key.clone(*_pkey);
        failed=0;
      }
      void encode(tbnet::DataBuffer* output) 
      {
        key.merge_area(area);
        key.encode(output);
      }
      bool decode(tbnet::DataBuffer* input) 
      {
        key.decode(input);
        area=key.area;
        return area>=0 && area<1024;
      }
      //bool operator ==(const request_sync& right) const
    public:
      int action;
      int area;
      tair::common::data_entry key;
      tair::common::data_entry value;
      int expired;
      int version;
      int failed;
  };
}
#endif
