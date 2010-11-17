/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * structure holds informations needed by dump thread
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_DUMP_DATA_INFO
#define TAIR_DUMP_DATA_INFO
namespace tair {
   typedef struct _dump_meta_info {
      _dump_meta_info() : start_time(0), end_time(0), area(0) {}
      bool operator < (const _dump_meta_info& _info) const
      {
         if (start_time < _info.start_time) return true;
         if (start_time > _info.start_time) return false;
         if (end_time < _info.end_time) return true;
         if (end_time > _info.end_time) return false;
         if (area < _info.area) return true;
         return false;
      }
      uint32_t start_time;
      uint32_t end_time;
      int32_t area;
   } dump_meta_info;
}
#endif
